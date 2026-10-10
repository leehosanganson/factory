package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
)

func TestRESTServerJobListingProcessE2E(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process semantics")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	config, apiKey := runtimeFixture(t)
	privateState := filepath.Join(t.TempDir(), "private-state")
	if err := os.Mkdir(privateState, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(privateState, "jobs.db")}
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 3
	config.Limits.MaxRecords = 8
	config.Limits.TaskBytes = 128
	harness := filepath.Join(t.TempDir(), "harness.sh")
	script := "#!/bin/sh\nif [ \"$1\" = hold-listing ]; then trap 'exit 0' TERM; while :; do sleep 1; done; fi\nexit 1\n"
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configFile := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configFile, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "factory")
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build server binary: %v: %s", err, output)
	}
	readyFile := filepath.Join(t.TempDir(), "ready-url")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+testProcessStateHome(t), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_NO_PROVIDER=1", "FACTORY_E2E_REPORT_RUNTIME_ERRORS=1", "FACTORY_E2E_CONFIG="+configFile, "FACTORY_E2E_READY="+readyFile)
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait(); _ = logFile.Close() }()
	defer func() {
		if child != nil && child.Process != nil {
			_ = child.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = child.Process.Kill()
				<-done
			}
		}
	}()
	baseURL := waitForProcessURL(t, done, readyFile, logPath)
	client := &http.Client{Timeout: 3 * time.Second}
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	first, err := submitProcessJob(client, baseURL, apiKey, "trusted", "first list task private", "listing-first")
	if err != nil || first.ID == "" {
		t.Fatalf("first synthetic admission=%+v err=%v", first, err)
	}
	waitForProcessFailure(t, client, baseURL, first.ID, apiKey)
	second, err := submitProcessJob(client, baseURL, apiKey, "trusted", "hold-listing", "listing-second")
	if err != nil || second.ID == "" {
		t.Fatalf("second synthetic admission=%+v err=%v", second, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && getProcessJob(t, client, baseURL, second.ID, apiKey).Status != restjobs.StatusRunning {
		time.Sleep(10 * time.Millisecond)
	}
	queued, err := submitProcessJob(client, baseURL, apiKey, "trusted", "queued list task private", "listing-third")
	if err != nil || queued.ID == "" {
		t.Fatalf("third synthetic admission=%+v err=%v", queued, err)
	}
	unauthorized, err := client.Get(baseURL + "/v1/jobs?limit=1")
	if err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated process list status=%d", unauthorized.StatusCode)
	}
	list := func(path string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return response, data
	}
	response, body := list("/v1/jobs?limit=1")
	var page restjobs.JobPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != first.ID || page.Jobs[0].Status != restjobs.StatusFailed || !page.HasMore {
		t.Fatalf("first process page status=%d body=%s", response.StatusCode, body)
	}
	if strings.Contains(string(body), "first list task private") || strings.Contains(string(body), "private") || strings.Contains(string(body), "provider") {
		t.Fatalf("process page leaked private fields: %s", body)
	}
	third, err := submitProcessJob(client, baseURL, apiKey, "trusted", "admitted between pages private", "listing-fourth")
	if err != nil || third.ID == "" {
		t.Fatalf("concurrent process admission=%+v err=%v", third, err)
	}
	path := fmt.Sprintf("/v1/jobs?limit=1&after=%d&snapshot=%d", page.NextSequence, page.SnapshotSequence)
	response, body = list(path)
	var secondPage restjobs.JobPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, &secondPage) != nil || len(secondPage.Jobs) != 1 || secondPage.Jobs[0].ID != second.ID || secondPage.Jobs[0].Status != restjobs.StatusRunning || !secondPage.HasMore || strings.Contains(string(body), third.ID) {
		t.Fatalf("continuation process page status=%d body=%s", response.StatusCode, body)
	}
	thirdPath := fmt.Sprintf("/v1/jobs?limit=1&after=%d&snapshot=%d", secondPage.NextSequence, secondPage.SnapshotSequence)
	response, body = list(thirdPath)
	var thirdPage restjobs.JobPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, &thirdPage) != nil || len(thirdPage.Jobs) != 1 || thirdPage.Jobs[0].ID != queued.ID || thirdPage.HasMore || strings.Contains(string(body), third.ID) {
		t.Fatalf("third process page status=%d body=%s", response.StatusCode, body)
	}
	if status, _ := getProcessJobResponse(t, client, baseURL, second.ID, apiKey); status.Status != restjobs.StatusRunning {
		t.Fatalf("running job changed after listing: %+v", status)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("listing process server shutdown: %v", err)
	}
	child = nil
}

func TestRESTServerBackupRestoreProcessE2E(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	temp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(temp, 0o700); err != nil {
		t.Fatal(err)
	}
	privateDir := func(name string) string {
		t.Helper()
		path := filepath.Join(temp, name)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	liveDir, backupDir, restoreDir := privateDir("live"), privateDir("backup"), privateDir("restore")
	config, apiKey := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(liveDir, "jobs.db")}
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 4
	harness := filepath.Join(temp, "harness.sh")
	harnessRuns := filepath.Join(temp, "harness-runs")
	harnessPID := filepath.Join(temp, "harness-pid")
	script := fmt.Sprintf("#!/bin/sh\ntask=\nfor arg in \"$@\"; do case \"$arg\" in backup-terminal|hold-backup-job|backup-queued) task=$arg ;; esac; done\nprintf '%%s\\n' \"$task\" >> %q\nif [ \"$task\" = hold-backup-job ]; then printf '%%s\\n' \"$$\" > %q; exec sleep 300; fi\nprintf 'verified\\n' > result.txt\n", harnessRuns, harnessPID)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	check := filepath.Join(temp, "verify.sh")
	if err := os.WriteFile(check, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config.VerificationChecks = [][]string{{check}}
	configPath := filepath.Join(liveDir, "server.json")
	writeConfig := func(path string, cfg restserver.Config) {
		t.Helper()
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig(configPath, config)

	factoryBinary := filepath.Join(temp, "factory")
	build := exec.Command("go", "build", "-o", factoryBinary, "./cmd/factory")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory CLI: %v: %s", err, output)
	}
	readyDir := privateDir("ready")
	startServer := func(name, configFile string) (*exec.Cmd, <-chan error, string, string) {
		t.Helper()
		xdgStateHome := privateDir("xdg-state-" + name)
		readyPath := filepath.Join(readyDir, name)
		logPath := filepath.Join(temp, name+".log")
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
		child.Env = append(os.Environ(), "XDG_STATE_HOME="+xdgStateHome, "FACTORY_E2E_HELPER=1", "FACTORY_E2E_NO_PROVIDER=1", "FACTORY_E2E_REPORT_RUNTIME_ERRORS=1", "FACTORY_E2E_CONFIG="+configFile, "FACTORY_E2E_READY="+readyPath)
		child.Stdout, child.Stderr = logFile, logFile
		if err := child.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- child.Wait()
			_ = logFile.Close()
		}()
		return child, done, waitForBackupServerURL(t, child, done, readyPath, logPath), logPath
	}
	client := &http.Client{Timeout: 5 * time.Second}
	var liveChild, restoredChild *exec.Cmd
	var liveDone, restoredDone <-chan error
	stopChild := func(child *exec.Cmd, done <-chan error) {
		t.Helper()
		if child == nil || child.ProcessState != nil {
			return
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
	}
	defer func() {
		stopChild(restoredChild, restoredDone)
		stopChild(liveChild, liveDone)
		if data, err := os.ReadFile(harnessPID); err == nil {
			var pid int
			if _, err := fmt.Sscanf(string(data), "%d", &pid); err == nil && pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}()
	liveChild, liveDone, liveURL, liveLog := startServer("live", configPath)
	for _, endpoint := range []string{"/healthz", "/readyz"} {
		waitForStatus(t, liveURL+endpoint, http.StatusOK)
	}

	terminal, err := submitProcessJob(client, liveURL, apiKey, "trusted", "backup-terminal", "backup-terminal-key")
	if err != nil || terminal.StatusCode != http.StatusAccepted || terminal.ID == "" {
		t.Fatalf("terminal synthetic job admission=%+v err=%v", terminal, err)
	}
	waitForBackupJob(t, client, liveURL, terminal.ID, apiKey, restjobs.StatusSucceeded)
	terminalBefore := getProcessJob(t, client, liveURL, terminal.ID, apiKey)
	terminalHistory := getProcessHistory(t, client, liveURL, terminal.ID, apiKey)
	if terminalBefore.Verification == nil || len(terminalBefore.Verification.Checks) != 1 || terminalBefore.Verification.Checks[0] != (restjobs.VerificationCheck{Name: "check-01", Outcome: restjobs.VerificationPassed}) || len(terminalHistory.Events) < 3 {
		t.Fatalf("terminal fixture lacks verification/history: job=%+v history=%+v", terminalBefore, terminalHistory)
	}

	running, err := submitProcessJob(client, liveURL, apiKey, "trusted", "hold-backup-job", "backup-running-key")
	if err != nil || running.StatusCode != http.StatusAccepted || running.ID == "" {
		t.Fatalf("running synthetic job admission=%+v err=%v", running, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(harnessPID); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(harnessPID); err != nil {
		t.Fatal("synthetic running job did not reach harness barrier")
	}
	if got := getProcessJob(t, client, liveURL, running.ID, apiKey); got.Status != restjobs.StatusRunning {
		t.Fatalf("barrier job state=%s, want running", got.Status)
	}
	queued, err := submitProcessJob(client, liveURL, apiKey, "trusted", "backup-queued", "backup-queued-key")
	if err != nil || queued.StatusCode != http.StatusAccepted || queued.ID == "" {
		t.Fatalf("queued synthetic job admission=%+v err=%v", queued, err)
	}
	if got := getProcessJob(t, client, liveURL, queued.ID, apiKey); got.Status != restjobs.StatusQueued {
		t.Fatalf("queued fixture state=%s, want queued", got.Status)
	}

	backupPath := filepath.Join(backupDir, "jobs.db")
	backup := exec.Command(factoryBinary, "server", "backup", "--config", configPath, "--destination", backupPath)
	output, err := backup.CombinedOutput()
	if err != nil || len(output) != 0 {
		t.Fatalf("documented live backup CLI failed (output length %d, error %v)", len(output), err)
	}
	backupInfo, err := os.Stat(backupPath)
	if err != nil || backupInfo.Mode().Perm() != 0o600 {
		t.Fatalf("backup mode=%v err=%v, want 0600", backupInfo, err)
	}
	if filepath.Clean(backupPath) == filepath.Clean(config.Persistence.Path) {
		t.Fatal("backup destination aliases live source")
	}
	for _, tc := range []struct {
		name, destination string
	}{
		{name: "source database", destination: config.Persistence.Path},
		{name: "group-readable destination directory", destination: filepath.Join(liveDir, "must-not-exist.db")},
	} {
		if tc.name == "group-readable destination directory" {
			if err := os.Chmod(liveDir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		command := exec.Command(factoryBinary, "server", "backup", "--config", configPath, "--destination", tc.destination)
		output, err := command.CombinedOutput()
		if err == nil || strings.Contains(string(output), apiKey) || strings.Contains(string(output), filepath.Dir(config.APIKeyFile)) {
			t.Fatalf("backup accepted %s or exposed sensitive output (output length %d, error %v)", tc.name, len(output), err)
		}
		if tc.name == "group-readable destination directory" {
			if err := os.Chmod(liveDir, 0o700); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := liveChild.Process.Kill(); err != nil {
		t.Fatalf("interrupt live server after backup: %v", err)
	}
	if err := <-liveDone; err == nil {
		t.Fatal("live server did not report forced interruption")
	}
	liveChild = nil
	if data, err := os.ReadFile(harnessPID); err == nil {
		var pid int
		if _, err := fmt.Sscanf(string(data), "%d", &pid); err == nil && pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	restoredDB := filepath.Join(restoreDir, "restored.db")
	source, err := os.Open(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := os.OpenFile(restoredDB, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	closeDestinationErr := destination.Close()
	closeSourceErr := source.Close()
	if copyErr != nil || closeDestinationErr != nil || closeSourceErr != nil {
		t.Fatalf("copy backup to disposable restore location: copy=%v destination=%v source=%v", copyErr, closeDestinationErr, closeSourceErr)
	}
	restoredConfig := config
	restoredConfig.Persistence.Path = restoredDB
	restoredConfigPath := filepath.Join(restoreDir, "server.json")
	writeConfig(restoredConfigPath, restoredConfig)

	restoredChild, restoredDone, restoredURL, restoredLog := startServer("restored", restoredConfigPath)
	for _, endpoint := range []string{"/healthz", "/readyz"} {
		waitForStatus(t, restoredURL+endpoint, http.StatusOK)
	}
	restoredTerminal := getProcessJob(t, client, restoredURL, terminal.ID, apiKey)
	restoredHistory := getProcessHistory(t, client, restoredURL, terminal.ID, apiKey)
	if restoredTerminal.Status != terminalBefore.Status || restoredTerminal.Request != terminalBefore.Request || restoredTerminal.Verification == nil || !reflect.DeepEqual(restoredTerminal.Verification, terminalBefore.Verification) || !reflect.DeepEqual(restoredHistory, terminalHistory) {
		t.Fatalf("restored terminal snapshot/evidence/history differs: before=%+v after=%+v beforeHistory=%+v afterHistory=%+v", terminalBefore, restoredTerminal, terminalHistory, restoredHistory)
	}
	replay, err := submitProcessJob(client, restoredURL, apiKey, "trusted", "backup-terminal", "backup-terminal-key")
	if err != nil || replay.StatusCode != http.StatusAccepted || !replay.Replayed || replay.ID != terminal.ID {
		t.Fatalf("restored idempotency replay=%+v err=%v, want original %q", replay, err, terminal.ID)
	}
	waitForBackupJob(t, client, restoredURL, queued.ID, apiKey, restjobs.StatusSucceeded)
	restoredQueuedHistory := getProcessHistory(t, client, restoredURL, queued.ID, apiKey)
	if len(restoredQueuedHistory.Events) < 3 || restoredQueuedHistory.Events[0].Type != "queued" || restoredQueuedHistory.Events[1].Type != "running" || restoredQueuedHistory.Events[len(restoredQueuedHistory.Events)-1].Type != string(restjobs.StatusSucceeded) {
		t.Fatalf("restored queued job history=%+v", restoredQueuedHistory)
	}
	restoredRunning := getProcessJob(t, client, restoredURL, running.ID, apiKey)
	if restoredRunning.Status != restjobs.StatusRunning {
		t.Fatalf("interrupted running snapshot changed/replayed after restore: %+v", restoredRunning)
	}
	runs, err := os.ReadFile(harnessRuns)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(runs), "\nhold-backup-job\n") != 1 {
		t.Fatalf("restored server replayed interrupted running work: harness task counts=%q", runs)
	}
	for _, path := range []string{restoredDB, backupPath} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("private database %q mode=%v err=%v", filepath.Base(path), info, err)
		}
	}
	corruptDB := filepath.Join(restoreDir, "corrupt.db")
	if err := os.WriteFile(corruptDB, []byte("synthetic corrupt SQLite content"), 0o600); err != nil {
		t.Fatal(err)
	}
	corruptConfig := restoredConfig
	corruptConfig.Persistence.Path = corruptDB
	corruptConfigPath := filepath.Join(restoreDir, "corrupt-server.json")
	writeConfig(corruptConfigPath, corruptConfig)
	corruptServer := exec.Command(factoryBinary, "server", "--config", corruptConfigPath)
	corruptOutput, corruptErr := corruptServer.CombinedOutput()
	if corruptErr == nil || len(corruptOutput) == 0 || strings.Contains(string(corruptOutput), apiKey) || strings.Contains(string(corruptOutput), config.APIKeyFile) || strings.Contains(string(corruptOutput), corruptDB) {
		t.Fatalf("corrupt backup startup did not fail safely (output length %d, error %v)", len(corruptOutput), corruptErr)
	}
	for _, logPath := range []string{liveLog, restoredLog} {
		logData, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(logData), apiKey) || strings.Contains(string(logData), "backup-terminal-key") {
			t.Fatalf("server log %q exposed synthetic auth/idempotency data", filepath.Base(logPath))
		}
	}
}

func waitForBackupServerURL(t *testing.T, child *exec.Cmd, childDone <-chan error, readyPath, logPath string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(readyPath); err == nil {
			return "http://" + strings.TrimSpace(string(data))
		}
		select {
		case err := <-childDone:
			logData, _ := os.ReadFile(logPath)
			t.Fatalf("REST server helper exited before readiness: %v; log=%s", err, logData)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = child.Process.Kill()
	<-childDone
	logData, _ := os.ReadFile(logPath)
	t.Fatalf("REST server process did not become ready; log=%s", logData)
	return ""
}

func waitForBackupJob(t *testing.T, client *http.Client, baseURL, id, apiKey string, want restjobs.Status) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		job := getProcessJob(t, client, baseURL, id, apiKey)
		if job.Status.Terminal() || job.Status == want {
			if job.Status != want {
				history := getProcessHistory(t, client, baseURL, id, apiKey)
				t.Fatalf("job %s status=%s, want %s; history=%+v", id, job.Status, want, history)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, want)
}

func TestCanonicalTestStateHomeResolvesSymlinks(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(t.TempDir(), "state-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	got := canonicalTestStateHome(t, filepath.Join(alias, "state"))
	want, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	want = filepath.Join(want, "state")
	if got != want {
		t.Fatalf("canonical test state home = %q, want %q", got, want)
	}
}

func canonicalTestStateHome(t *testing.T, home string) string {
	t.Helper()
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(home))
	if err != nil {
		t.Fatalf("resolve test state home parent: %v", err)
	}
	return filepath.Join(resolvedParent, filepath.Base(home))
}

func testProcessStateHome(t *testing.T) string {
	t.Helper()
	return canonicalTestStateHome(t, filepath.Join(t.TempDir(), "state"))
}

func TestRESTServerPerJobCancellationProcessE2E(t *testing.T) {
	if testing.Short() || (runtime.GOOS != "linux" && runtime.GOOS != "darwin") {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 4
	config.Limits.JobTimeout = "30s"
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("synthetic-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	providerWrites := filepath.Join(t.TempDir(), "provider-writes.jsonl")
	barrier := filepath.Join(t.TempDir(), "running-barrier")
	harnessRuns := filepath.Join(t.TempDir(), "harness-runs")
	harness := filepath.Join(t.TempDir(), "harness.sh")
	script := fmt.Sprintf("#!/bin/sh\ntask=\nfor arg in \"$@\"; do case \"$arg\" in cancel-running|cancel-queued|unrelated-job) task=$arg ;; esac; done\nprintf '%%s\\n' \"$task\" >> %q\nif [ \"$task\" = cancel-running ]; then printf 'ready\\n' > %q; sleep 300; fi\nprintf 'verified\\n' > result.txt\n", harnessRuns, barrier)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	factoryBinary := filepath.Join(t.TempDir(), "factory")
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", factoryBinary, "./cmd/factory")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory CLI: %v: %s", err, output)
	}
	readyPath := filepath.Join(t.TempDir(), "ready")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+providerWrites, "FACTORY_E2E_PROVIDER_MODE=cancel-e2e")
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait(); _ = logFile.Close() }()
	stopped := false
	stop := func() {
		if stopped || child.ProcessState != nil {
			return
		}
		stopped = true
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
	}
	defer stop()
	baseURL := waitForProcessURL(t, done, readyPath, logPath)
	client := &http.Client{Timeout: 5 * time.Second}
	postCancel := func(id string, auth bool) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/"+id+"/cancel", nil)
		if err != nil {
			t.Fatal(err)
		}
		if auth {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, string(body)
	}
	running, err := submitProcessJob(client, baseURL, apiKey, "trusted", "cancel-running", "cancel-running-key")
	if err != nil || running.StatusCode != http.StatusAccepted || running.ID == "" {
		t.Fatalf("running job admission=%+v err=%v", running, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(barrier); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(barrier); err != nil {
		t.Fatalf("running harness did not reach barrier: %v", err)
	}
	queued, err := submitProcessJob(client, baseURL, apiKey, "trusted", "cancel-queued", "cancel-queued-key")
	if err != nil || queued.StatusCode != http.StatusAccepted || queued.ID == "" {
		t.Fatalf("queued job admission=%+v err=%v", queued, err)
	}
	if status, _ := postCancel(queued.ID, false); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cancel status=%d, want 401", status)
	}
	if status, body := postCancel(queued.ID, true); status != http.StatusOK || !strings.Contains(body, `"status":"canceled"`) {
		t.Fatalf("queued cancel status=%d body=%s, want 200 canceled", status, body)
	}
	if status, body := postCancel(running.ID, true); status != http.StatusAccepted || !strings.Contains(body, `"cancellation_requested":true`) {
		t.Fatalf("running cancel status=%d body=%s, want accepted cancellation request", status, body)
	}
	if status, body := postCancel(running.ID, true); status != http.StatusAccepted || !strings.Contains(body, `"cancellation_requested":true`) {
		t.Fatalf("repeated running cancel status=%d body=%s, want idempotent pending request", status, body)
	}
	waitForProcessStatus(t, client, baseURL, running.ID, apiKey, restjobs.StatusCanceled)
	canceledRunning := getProcessJob(t, client, baseURL, running.ID, apiKey)
	if canceledRunning.Verification == nil || canceledRunning.Provider != nil {
		t.Fatalf("canceled running outcome lost evidence or fabricated provider result: %+v", canceledRunning)
	}
	if status, body := postCancel(running.ID, true); status != http.StatusConflict || !strings.Contains(body, `"code":"job_not_cancelable"`) {
		t.Fatalf("terminal cancel status=%d body=%s, want 409 conflict", status, body)
	}
	uncertain, err := submitProcessJob(client, baseURL, apiKey, "trusted", "provider-uncertain", "provider-uncertain-key")
	if err != nil || uncertain.StatusCode != http.StatusAccepted || uncertain.ID == "" {
		t.Fatalf("uncertain-provider job admission=%+v err=%v", uncertain, err)
	}
	uncertainAttempt := providerWrites + ".uncertain-attempt"
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(uncertainAttempt); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(uncertainAttempt); err != nil {
		t.Fatalf("fake provider did not reach uncertain-write barrier: %v", err)
	}
	if status, body := postCancel(uncertain.ID, true); status != http.StatusAccepted || !strings.Contains(body, `"cancellation_requested":true`) {
		t.Fatalf("uncertain provider cancel status=%d body=%s", status, body)
	}
	waitForProcessStatus(t, client, baseURL, uncertain.ID, apiKey, restjobs.StatusFailed)
	uncertainJob := getProcessJob(t, client, baseURL, uncertain.ID, apiKey)
	if uncertainJob.CancellationRequested || uncertainJob.Provider != nil || uncertainJob.Verification == nil {
		t.Fatalf("uncertain provider side effect was mislabeled or evidence lost: %+v", uncertainJob)
	}
	uncertainHistory := getProcessHistory(t, client, baseURL, uncertain.ID, apiKey)
	if len(uncertainHistory.Events) < 4 || uncertainHistory.Events[len(uncertainHistory.Events)-2].Type != "cancel_requested" || uncertainHistory.Events[len(uncertainHistory.Events)-1].Type != string(restjobs.StatusFailed) {
		t.Fatalf("uncertain provider cancellation history=%+v", uncertainHistory)
	}
	queuedHistory := getProcessHistory(t, client, baseURL, queued.ID, apiKey)
	runningHistory := getProcessHistory(t, client, baseURL, running.ID, apiKey)
	if len(queuedHistory.Events) != 2 || queuedHistory.Events[1].Type != string(restjobs.StatusCanceled) || len(runningHistory.Events) != 4 || runningHistory.Events[2].Type != "cancel_requested" || runningHistory.Events[3].Type != string(restjobs.StatusCanceled) {
		t.Fatalf("cancellation histories queued=%+v running=%+v", queuedHistory, runningHistory)
	}
	resultsPath := filepath.Join(stateDir, aliasDirectory("trusted"), "results")
	if _, err := os.Stat(filepath.Join(resultsPath, running.ID, "worktree")); err != nil {
		t.Fatalf("canceled running workspace not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(resultsPath, running.ID, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("canceled running job has completion marker: %v", err)
	}
	if _, err := os.Stat(filepath.Join(resultsPath, uncertain.ID, "worktree")); err != nil {
		t.Fatalf("uncertain provider workspace not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(resultsPath, uncertain.ID, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("uncertain provider job has completion marker: %v", err)
	}
	if runs, err := os.ReadFile(harnessRuns); err != nil || strings.Count(string(runs), "cancel-running") != 1 || strings.Contains(string(runs), "cancel-queued") {
		t.Fatalf("harness executions=%q err=%v; queued job ran or running count differs", runs, err)
	}
	if writes, err := os.ReadFile(providerWrites); (err != nil && !os.IsNotExist(err)) || len(writes) != 0 {
		t.Fatalf("canceled jobs caused confirmed provider writes: data=%q err=%v", writes, err)
	}
	attempts, err := os.ReadFile(uncertainAttempt)
	if err != nil || strings.TrimSpace(string(attempts)) != uncertain.ID {
		t.Fatalf("uncertain provider attempts=%q err=%v, want one attempt for selected job", attempts, err)
	}
	stop()
	stopped = true
	persisted, err := restworker.NewLocalJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer persisted.Close()
	for _, id := range []string{queued.ID, running.ID} {
		job, err := persisted.Get(id)
		if err != nil || job.Status != restjobs.StatusCanceled {
			t.Fatalf("persisted canceled job=%+v err=%v", job, err)
		}
	}
	persistedUncertain, err := persisted.Get(uncertain.ID)
	if err != nil || persistedUncertain.Status != restjobs.StatusFailed || persistedUncertain.Provider != nil || persistedUncertain.Verification == nil {
		t.Fatalf("persisted uncertain provider cancellation=%+v err=%v", persistedUncertain, err)
	}
}

func TestRESTServerProcessHelper(t *testing.T) {
	if os.Getenv("FACTORY_E2E_HELPER") != "1" {
		return
	}
	config, err := restserver.LoadConfig(os.Getenv("FACTORY_E2E_CONFIG"))
	if err != nil {
		os.Exit(11)
	}
	var publisher restprovider.Publisher
	if os.Getenv("FACTORY_E2E_NO_PROVIDER") != "1" {
		publisher = &e2ePublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
		switch os.Getenv("FACTORY_E2E_PROVIDER_MODE") {
		case "uncertain":
			publisher = &uncertainE2EPublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
		case "confirmed-wait":
			publisher = &confirmedWaitingE2EPublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
		case "cancel-e2e":
			publisher = &cancelE2EPublisher{path: os.Getenv("FACTORY_E2E_OUTCOMES")}
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	listen := func(string, string) (net.Listener, error) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(os.Getenv("FACTORY_E2E_READY"), []byte(listener.Addr().String()), 0o600); err != nil {
			_ = listener.Close()
			return nil, err
		}
		return listener, nil
	}
	if err := run(ctx, config, runtimeOptions{Publisher: publisher, Listen: listen}); err != nil {
		if os.Getenv("FACTORY_E2E_REPORT_RUNTIME_ERRORS") == "1" {
			t.Fatalf("REST server helper runtime failed: %v", err)
		}
		os.Exit(12)
	}
	os.Exit(0)
}

type e2ePublisher struct{ path string }
type cancelE2EPublisher struct{ path string }

type uncertainE2EPublisher struct{ path string }

type confirmedWaitingE2EPublisher struct{ path string }

type uncertainProviderRecord struct {
	JobID      string               `json:"job_id"`
	Repository string               `json:"repository"`
	Branch     string               `json:"branch"`
	Commit     string               `json:"commit"`
	Outcome    restprovider.Outcome `json:"outcome"`
	Creates    int                  `json:"creates"`
	Attempts   int                  `json:"attempts"`
}

func (p *confirmedWaitingE2EPublisher) Ping(context.Context) error { return nil }

func (p *confirmedWaitingE2EPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	branch, branchErr := exec.CommandContext(ctx, "git", "-C", request.Worktree, "branch", "--show-current").Output()
	commit, commitErr := exec.CommandContext(ctx, "git", "-C", request.Worktree, "rev-parse", "HEAD").Output()
	if branchErr != nil || strings.TrimSpace(string(branch)) != request.Branch || commitErr != nil || strings.TrimSpace(string(commit)) != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("confirmed provider request identity mismatch")
	}
	record := uncertainProviderRecord{
		JobID: request.JobID, Repository: request.Repository, Branch: request.Branch, Commit: request.Commit,
		Outcome: restprovider.Outcome{Provider: "github", Repository: request.Repository, Number: 72, URL: fmt.Sprintf("https://github.com/%s/pull/72", request.Repository), Branch: request.Branch, Commit: request.Commit, State: "open"},
		Creates: 1, Attempts: 1,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	if err := os.WriteFile(p.path+".confirmed.json", data, 0o600); err != nil {
		return restprovider.Outcome{}, err
	}
	if err := os.WriteFile(p.path+".publish-waiting", []byte("ready"), 0o600); err != nil {
		return restprovider.Outcome{}, err
	}
	for {
		if _, err := os.Stat(p.path + ".release-publish"); err == nil {
			return record.Outcome, nil
		} else if !os.IsNotExist(err) {
			return restprovider.Outcome{}, err
		}
		select {
		case <-ctx.Done():
			return restprovider.Outcome{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (p *confirmedWaitingE2EPublisher) Reconcile(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	data, err := os.ReadFile(p.path + ".confirmed.json")
	if err != nil {
		return restprovider.Outcome{}, fmt.Errorf("confirmed PR unavailable")
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(data, &record); err != nil || record.JobID != request.JobID || record.Repository != request.Repository || record.Branch != request.Branch || record.Commit != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("confirmed PR identity mismatch")
	}
	if err := os.WriteFile(p.path+".read-only-lookup", []byte("confirmed"), 0o600); err != nil {
		return restprovider.Outcome{}, err
	}
	return record.Outcome, nil
}

func (p *uncertainE2EPublisher) Ping(context.Context) error { return nil }

func (p *uncertainE2EPublisher) Reconcile(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	lookup, err := os.OpenFile(p.path+".reconcile-lookups", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	if _, err := lookup.WriteString(request.JobID + "\n"); err != nil {
		_ = lookup.Close()
		return restprovider.Outcome{}, err
	}
	if err := lookup.Close(); err != nil {
		return restprovider.Outcome{}, err
	}
	data, err := os.ReadFile(p.path + ".uncertain.json")
	if err != nil {
		return restprovider.Outcome{}, fmt.Errorf("fake provider unavailable")
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(data, &record); err != nil || record.JobID != request.JobID || record.Repository != request.Repository || record.Branch != request.Branch || record.Commit != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("fake provider identity mismatch")
	}
	mode, _ := os.ReadFile(p.path + ".reconcile-mode")
	switch strings.TrimSpace(string(mode)) {
	case "outage", "missing", "multiple", "malformed":
		return restprovider.Outcome{}, fmt.Errorf("fake provider could not confirm PR")
	case "mismatch":
		record.Outcome.Commit = "different-commit"
	case "repository":
		record.Outcome.Repository = "other/repository"
	case "branch":
		record.Outcome.Branch = "other-branch"
	}
	return record.Outcome, nil
}

func (p *uncertainE2EPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	path := p.path + ".uncertain.json"
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		branch, branchErr := exec.CommandContext(ctx, "git", "-C", request.Worktree, "branch", "--show-current").Output()
		commit, commitErr := exec.CommandContext(ctx, "git", "-C", request.Worktree, "rev-parse", "HEAD").Output()
		if branchErr != nil || strings.TrimSpace(string(branch)) != request.Branch || commitErr != nil || strings.TrimSpace(string(commit)) != request.Commit {
			return restprovider.Outcome{}, fmt.Errorf("uncertain provider request identity mismatch")
		}
		record := uncertainProviderRecord{
			JobID: request.JobID, Repository: request.Repository, Branch: request.Branch, Commit: request.Commit,
			Outcome: restprovider.Outcome{Provider: "github", Repository: request.Repository, Number: 71, URL: fmt.Sprintf("https://github.com/%s/pull/71", request.Repository), Branch: request.Branch, Commit: request.Commit, State: "open"},
			Creates: 1, Attempts: 1,
		}
		data, err = json.Marshal(record)
		if err != nil {
			return restprovider.Outcome{}, err
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return restprovider.Outcome{}, err
		}
		<-ctx.Done()
		return restprovider.Outcome{}, fmt.Errorf("%w: fake response was lost after create", restprovider.ErrUncertain)
	}
	if err != nil {
		return restprovider.Outcome{}, err
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return restprovider.Outcome{}, err
	}
	if record.JobID != request.JobID || record.Repository != request.Repository || record.Branch != request.Branch || record.Commit != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("uncertain provider identity mismatch")
	}
	record.Attempts++
	data, err = json.Marshal(record)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return restprovider.Outcome{}, err
	}
	return record.Outcome, nil
}

func (p *cancelE2EPublisher) Ping(context.Context) error { return nil }

func (p *cancelE2EPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	if err := os.WriteFile(p.path+".uncertain-attempt", []byte(request.JobID), 0o600); err != nil {
		return restprovider.Outcome{}, err
	}
	<-ctx.Done()
	return restprovider.Outcome{}, fmt.Errorf("%w: fake accepted provider write response lost", restprovider.ErrUncertain)
}

func (p *cancelE2EPublisher) Reconcile(context.Context, restprovider.PublishRequest) (restprovider.Outcome, error) {
	return restprovider.Outcome{}, fmt.Errorf("unexpected reconciliation in cancellation E2E")
}

func (p *e2ePublisher) Ping(context.Context) error { return nil }

func (p *e2ePublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	if marker := os.Getenv("FACTORY_E2E_PROVIDER_REQUIRES_MARKER"); marker != "" {
		if _, err := os.Stat(marker); err != nil {
			return restprovider.Outcome{}, fmt.Errorf("required pre-publication integration marker is missing")
		}
	}
	if request.JobID == "" {
		return restprovider.Outcome{}, fmt.Errorf("missing job identity")
	}
	started, err := os.OpenFile(p.path+".started", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	if _, err := started.WriteString(request.JobID + "\n"); err != nil {
		_ = started.Close()
		return restprovider.Outcome{}, err
	}
	if err := started.Close(); err != nil {
		return restprovider.Outcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	branch, err := exec.CommandContext(ctx, "git", "-C", request.Worktree, "branch", "--show-current").Output()
	if err != nil || strings.TrimSpace(string(branch)) != request.Branch {
		return restprovider.Outcome{}, fmt.Errorf("published branch mismatch")
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", request.Worktree, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(commit)) != request.Commit {
		return restprovider.Outcome{}, fmt.Errorf("published commit mismatch")
	}
	number := len(request.JobID)
	outcome := restprovider.Outcome{Provider: "github", Repository: request.Repository, Number: number, URL: fmt.Sprintf("https://github.com/%s/pull/%d", request.Repository, number), Branch: request.Branch, Commit: request.Commit, State: "open"}
	file, err := os.OpenFile(p.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return restprovider.Outcome{}, err
	}
	encodeErr := json.NewEncoder(file).Encode(struct {
		JobID      string `json:"job_id"`
		Repository string `json:"repository"`
		Worktree   string `json:"worktree"`
		Branch     string `json:"branch"`
		Commit     string `json:"commit"`
	}{request.JobID, request.Repository, request.Worktree, request.Branch, request.Commit})
	closeErr := file.Close()
	if encodeErr != nil {
		return restprovider.Outcome{}, encodeErr
	}
	if closeErr != nil {
		return restprovider.Outcome{}, closeErr
	}
	return outcome, nil
}

func TestRESTServerProcessRecoversQueuedAndRetainsInterruptedRunningSQLiteJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 2
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	harnessRuns := filepath.Join(t.TempDir(), "harness-runs")
	harnessPID := filepath.Join(t.TempDir(), "harness-pid")
	harness := filepath.Join(t.TempDir(), "restart-harness.sh")
	script := fmt.Sprintf("#!/bin/sh\ntask=\nfor arg in \"$@\"; do case \"$arg\" in hold-running|queued-after-restart) task=$arg ;; esac; done\ncase \"$task\" in hold-running) printf 'hold-running\\n' >> %q; printf '%%s\\n' \"$$\" > %q; exec sleep 300 ;; queued-after-restart) printf 'queued-after-restart\\n' >> %q ;; esac\nprintf 'verified\\n' >> result.txt\n", harnessRuns, harnessPID, harnessRuns)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomesPath := filepath.Join(t.TempDir(), "provider-outcomes.jsonl")
	readyDir := t.TempDir()
	startServer := func(readyName string) (*exec.Cmd, <-chan error, string) {
		t.Helper()
		readyPath := filepath.Join(readyDir, readyName)
		logPath := filepath.Join(t.TempDir(), readyName+".log")
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
		child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath)
		child.Stdout, child.Stderr = logFile, logFile
		if err := child.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- child.Wait()
			_ = logFile.Close()
		}()
		return child, done, waitForProcessURL(t, done, readyPath, logPath)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	var child *exec.Cmd
	var childDone <-chan error
	var harnessPIDValue int
	defer func() {
		if child != nil && child.ProcessState == nil {
			_ = child.Process.Kill()
			<-childDone
		}
		if harnessPIDValue > 0 {
			_ = syscall.Kill(harnessPIDValue, syscall.SIGKILL)
		}
	}()
	child, childDone, baseURL := startServer("listener-first")
	running, err := submitProcessJob(client, baseURL, apiKey, "trusted", "hold-running", "restart-running")
	if err != nil || running.StatusCode != http.StatusAccepted || running.ID == "" {
		t.Fatalf("running job admission=%+v err=%v", running, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(harnessPID); err == nil {
			if _, err := fmt.Sscanf(string(data), "%d", &harnessPIDValue); err != nil || harnessPIDValue <= 0 {
				t.Fatalf("blocked harness PID=%q err=%v", data, err)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if harnessPIDValue == 0 {
		t.Fatal("running job harness did not reach its interruption barrier")
	}
	queued, err := submitProcessJob(client, baseURL, apiKey, "trusted", "queued-after-restart", "restart-queued")
	if err != nil || queued.StatusCode != http.StatusAccepted || queued.ID == "" {
		t.Fatalf("queued job admission=%+v err=%v", queued, err)
	}
	beforeRunning := getProcessJob(t, client, baseURL, running.ID, apiKey)
	beforeQueued := getProcessJob(t, client, baseURL, queued.ID, apiKey)
	if beforeRunning.Status != restjobs.StatusRunning || beforeQueued.Status != restjobs.StatusQueued {
		t.Fatalf("pre-interruption states running=%+v queued=%+v", beforeRunning, beforeQueued)
	}
	beforeRunningHistory := getProcessHistory(t, client, baseURL, running.ID, apiKey)
	if len(beforeRunningHistory.Events) != 2 || beforeRunningHistory.Events[0].Type != "queued" || beforeRunningHistory.Events[1].Type != "running" {
		t.Fatalf("running job history before interruption=%+v", beforeRunningHistory)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err == nil {
		t.Fatal("server process exited successfully after forced interruption; expected SIGKILL")
	}
	child = nil
	_ = syscall.Kill(harnessPIDValue, syscall.SIGKILL)

	child, childDone, restartedURL := startServer("listener-restarted")
	restartedRunning := getProcessJob(t, client, restartedURL, running.ID, apiKey)
	if restartedRunning.Status != restjobs.StatusRunning {
		t.Fatalf("interrupted running job was replayed or changed on restart: %+v", restartedRunning)
	}
	restartedRunningHistory := getProcessHistory(t, client, restartedURL, running.ID, apiKey)
	if len(restartedRunningHistory.Events) != len(beforeRunningHistory.Events) || restartedRunningHistory.Events[len(restartedRunningHistory.Events)-1].Type != "running" {
		t.Fatalf("interrupted running history changed on restart: before=%+v after=%+v", beforeRunningHistory, restartedRunningHistory)
	}
	waitForProcessJob(t, client, restartedURL, queued.ID, apiKey)
	restartedQueued := getProcessJob(t, client, restartedURL, queued.ID, apiKey)
	queuedHistory := getProcessHistory(t, client, restartedURL, queued.ID, apiKey)
	if restartedQueued.Status != restjobs.StatusSucceeded || len(queuedHistory.Events) < 3 || queuedHistory.Events[0].Type != "queued" || queuedHistory.Events[1].Type != "running" || queuedHistory.Events[len(queuedHistory.Events)-1].Type != string(restjobs.StatusSucceeded) {
		t.Fatalf("queued job did not resume to success with lifecycle history: job=%+v history=%+v", restartedQueued, queuedHistory)
	}
	unauthenticated, err := client.Get(restartedURL + "/v1/operations")
	if err != nil {
		t.Fatal(err)
	}
	_ = unauthenticated.Body.Close()
	if unauthenticated.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated operations status=%d, want %d", unauthenticated.StatusCode, http.StatusUnauthorized)
	}
	operationsRequest, err := http.NewRequest(http.MethodGet, restartedURL+"/v1/operations", nil)
	if err != nil {
		t.Fatal(err)
	}
	operationsRequest.Header.Set("Authorization", "Bearer "+apiKey)
	operationsResponse, err := client.Do(operationsRequest)
	if err != nil {
		t.Fatal(err)
	}
	var summary restjobs.OperationalSummary
	decodeErr := json.NewDecoder(operationsResponse.Body).Decode(&summary)
	_ = operationsResponse.Body.Close()
	if decodeErr != nil || operationsResponse.StatusCode != http.StatusOK || summary.Running != 1 || summary.Queued != 0 || summary.Succeeded != 1 || summary.RecoveryNeeded != 1 {
		t.Fatalf("authenticated restart operations status=%d summary=%+v decodeErr=%v", operationsResponse.StatusCode, summary, decodeErr)
	}
	harnessRunData, err := os.ReadFile(harnessRuns)
	if err != nil {
		t.Fatal(err)
	}
	runs := strings.Fields(string(harnessRunData))
	runningRuns, queuedRuns := 0, 0
	for _, task := range runs {
		switch task {
		case "hold-running":
			runningRuns++
		case "queued-after-restart":
			queuedRuns++
		default:
			t.Fatalf("unexpected harness task execution %q in %q", task, harnessRunData)
		}
	}
	if runningRuns != 1 || queuedRuns != 3 {
		t.Fatalf("harness executions running=%d queued=%d task-count=%q, want interrupted job once and resumed job three times", runningRuns, queuedRuns, harnessRunData)
	}
	providerAttempts, err := os.ReadFile(outcomesPath + ".started")
	if err != nil || string(providerAttempts) != queued.ID+"\n" {
		t.Fatalf("provider publish attempts=%q err=%v, want only resumed queued job %q", providerAttempts, err, queued.ID)
	}
	providerOutcomes, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.Fields(string(providerOutcomes))) == 0 || strings.Count(strings.TrimSpace(string(providerOutcomes)), "\n") != 0 {
		t.Fatalf("provider outcomes=%q, want exactly one successful publication", providerOutcomes)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("restarted server graceful shutdown: %v", err)
	}
	child = nil
}

func TestRESTServerProcessSQLiteWriteLockFailsReadinessAndAdmission(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	harness := filepath.Join(t.TempDir(), "readiness-harness.sh")
	if err := os.WriteFile(harness, []byte("#!/bin/sh\nprintf verified > result.txt\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "listener")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_NO_PROVIDER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath)
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() {
		childDone <- child.Wait()
		_ = logFile.Close()
	}()
	client := &http.Client{Timeout: 8 * time.Second}
	defer func() {
		if child.ProcessState == nil {
			_ = child.Process.Signal(syscall.SIGTERM)
			select {
			case <-childDone:
			case <-time.After(5 * time.Second):
				_ = child.Process.Kill()
				<-childDone
			}
		}
	}()
	baseURL := waitForProcessURL(t, childDone, readyPath, logPath)
	locked := lockSQLiteWrites(t, config.Persistence.Path)
	lockHeld := true
	defer func() {
		if lockHeld {
			locked()
		}
	}()
	request := func(method, path, body string) (int, string, time.Duration, error) {
		t.Helper()
		started := time.Now()
		req, err := http.NewRequest(method, baseURL+path, strings.NewReader(body))
		if err != nil {
			return 0, "", time.Since(started), err
		}
		if path == "/v1/jobs" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "sqlite-lock-readiness")
		}
		resp, err := client.Do(req)
		elapsed := time.Since(started)
		if err != nil {
			return 0, "", elapsed, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(data), elapsed, err
	}
	if code, body, elapsed, err := request(http.MethodGet, "/healthz", ""); err != nil || code != http.StatusOK {
		t.Fatalf("health under SQLite write lock status=%d body=%s elapsed=%s err=%v", code, body, elapsed, err)
	}
	if code, body, elapsed, err := request(http.MethodGet, "/readyz", ""); err != nil || code != http.StatusServiceUnavailable || !strings.Contains(body, `"status":"not_ready"`) || elapsed > 7*time.Second {
		t.Fatalf("readiness under SQLite write lock status=%d body=%s elapsed=%s err=%v", code, body, elapsed, err)
	}

	type readinessResult struct {
		status  int
		body    string
		elapsed time.Duration
		err     error
	}
	const concurrentProbes = 12
	startProbes := make(chan struct{})
	probeResults := make(chan readinessResult, concurrentProbes)
	for range concurrentProbes {
		go func() {
			<-startProbes
			status, body, elapsed, err := request(http.MethodGet, "/readyz", "")
			probeResults <- readinessResult{status: status, body: body, elapsed: elapsed, err: err}
		}()
	}
	close(startProbes)
	if code, body, elapsed, err := request(http.MethodGet, "/healthz", ""); err != nil || code != http.StatusOK || elapsed > time.Second {
		t.Fatalf("health alongside concurrent SQLite probes status=%d body=%s elapsed=%s err=%v", code, body, elapsed, err)
	}
	var slow, prompt int
	for range concurrentProbes {
		result := <-probeResults
		if result.err != nil || result.status != http.StatusServiceUnavailable || !strings.Contains(result.body, `"status":"not_ready"`) {
			t.Fatalf("concurrent readiness under SQLite lock status=%d body=%s elapsed=%s err=%v", result.status, result.body, result.elapsed, result.err)
		}
		if result.elapsed > 6*time.Second {
			t.Fatalf("admitted readiness check exceeded bounded response time: %s", result.elapsed)
		}
		if result.elapsed > time.Second {
			slow++
		} else {
			prompt++
		}
	}
	if slow == 0 || slow > 4 || prompt == 0 {
		t.Fatalf("concurrent readiness checks: admitted slow=%d (limit 4), promptly rejected=%d; want both", slow, prompt)
	}

	const submission = `{"repository":"trusted","task":"run after SQLite recovery"}`
	if code, body, elapsed, err := request(http.MethodPost, "/v1/jobs", submission); err != nil || code != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"not_ready"`) || elapsed > 7*time.Second {
		t.Fatalf("admission under SQLite write lock status=%d body=%s elapsed=%s err=%v", code, body, elapsed, err)
	}
	locked()
	lockHeld = false
	if code, body, elapsed, err := request(http.MethodGet, "/readyz", ""); err != nil || code != http.StatusOK || !strings.Contains(body, `"status":"ready"`) {
		t.Fatalf("readiness after SQLite lock release status=%d body=%s elapsed=%s err=%v", code, body, elapsed, err)
	}
	code, body, elapsed, err := request(http.MethodPost, "/v1/jobs", submission)
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("admission after SQLite lock release status=%d body=%s elapsed=%s err=%v", code, body, elapsed, err)
	}
	var admitted struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	if err := json.Unmarshal([]byte(body), &admitted); err != nil || admitted.Job.ID == "" || admitted.Replayed {
		t.Fatalf("same-key retry after lock release was not a fresh admission: response=%s err=%v", body, err)
	}
	code, body, elapsed, err = request(http.MethodPost, "/v1/jobs", submission)
	var replayed struct {
		Job      restjobs.Snapshot `json:"job"`
		Replayed bool              `json:"replayed"`
	}
	decodeErr := json.Unmarshal([]byte(body), &replayed)
	if err != nil || decodeErr != nil || code != http.StatusAccepted || !replayed.Replayed || replayed.Job.ID != admitted.Job.ID {
		t.Fatalf("admission idempotency after recovery status=%d body=%s elapsed=%s err=%v decodeErr=%v", code, body, elapsed, err, decodeErr)
	}
}

func TestRESTServerProcessRejectsUnavailableSQLiteBeforeListening(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, _ := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{
		Backend: restserver.PersistenceBackendSQLite,
		Path:    filepath.Join(t.TempDir(), "missing-parent", "jobs.db"),
	}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "listener")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+filepath.Join(t.TempDir(), "outcomes.jsonl"))
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err := <-done:
		if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 12 {
			t.Fatalf("server process exit=%v, want configured SQLite startup failure", err)
		}
	case <-time.After(10 * time.Second):
		_ = child.Process.Kill()
		<-done
		t.Fatal("server process did not fail promptly with unavailable configured SQLite")
	}
	if _, err := os.Stat(readyPath); !os.IsNotExist(err) {
		t.Fatalf("server published a listener despite unavailable configured SQLite: stat err=%v", err)
	}
}

func TestRESTServerProcessRejectsAndRetriesAtQueueCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	config.Limits.Workers = 1
	config.Limits.QueueCapacity = 1
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	barrier := filepath.Join(t.TempDir(), "worker-started")
	harness := filepath.Join(t.TempDir(), "capacity-harness.sh")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = hold-worker ] && [ ! -e '%s' ]; then touch '%s'; sleep 2; fi\nprintf 'verified\\n' >> result.txt\n", barrier, barrier)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "listener")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+filepath.Join(t.TempDir(), "outcomes.jsonl"))
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	defer func() {
		if child.Process == nil {
			return
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
	}()
	baseURL := waitForProcessURL(t, done, readyPath, logPath)
	client := &http.Client{Timeout: 3 * time.Second}
	first, err := submitProcessJob(client, baseURL, apiKey, "trusted", "hold-worker", "capacity-first")
	if err != nil || first.StatusCode != http.StatusAccepted || first.ID == "" {
		t.Fatalf("worker job admission=%+v err=%v", first, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(barrier); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(barrier); err != nil {
		t.Fatalf("worker did not start before filling queue: %v", err)
	}
	second, err := submitProcessJob(client, baseURL, apiKey, "trusted", "queued-job", "capacity-second")
	if err != nil || second.StatusCode != http.StatusAccepted || second.ID == "" {
		t.Fatalf("queued job admission=%+v err=%v", second, err)
	}
	full, err := submitProcessJob(client, baseURL, apiKey, "trusted", "retry-after-capacity", "capacity-retry")
	if err != nil || full.StatusCode != http.StatusServiceUnavailable || full.ErrorCode != "queue_full" || full.ID != "" {
		t.Fatalf("full-queue admission=%+v err=%v, want an unrecorded queue_full rejection", full, err)
	}
	request, err := http.NewRequest(http.MethodGet, baseURL+"/v1/operations", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var summary restjobs.OperationalSummary
	decodeErr := json.NewDecoder(response.Body).Decode(&summary)
	_ = response.Body.Close()
	if decodeErr != nil || response.StatusCode != http.StatusOK || summary.Running != 1 || summary.Queued != 1 || summary.QueueCapacity != 1 || !summary.QueueSaturated {
		t.Fatalf("operations while full: status=%d summary=%+v decodeErr=%v", response.StatusCode, summary, decodeErr)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := getProcessJob(t, client, baseURL, first.ID, apiKey)
		if job.Status == restjobs.StatusSucceeded {
			break
		}
		if job.Status.Terminal() {
			t.Fatalf("running job ended unexpectedly: %+v", job)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job := getProcessJob(t, client, baseURL, first.ID, apiKey); job.Status != restjobs.StatusSucceeded {
		t.Fatalf("worker did not complete and release capacity: %+v", job)
	}
	waitForProcessJob(t, client, baseURL, second.ID, apiKey)
	retried, err := submitProcessJob(client, baseURL, apiKey, "trusted", "retry-after-capacity", "capacity-retry")
	if err != nil || retried.StatusCode != http.StatusAccepted || retried.ID == "" || retried.Replayed {
		t.Fatalf("same-key retry after capacity freed=%+v err=%v, want new admission", retried, err)
	}
}

func TestRESTServerProcessRunsParallelSubtasksBeforeSingleProviderPublication(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	config.Limits.Workers = 1
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	workRoot := filepath.Dir(config.Repositories["trusted"])
	parallelBarrier := filepath.Join(t.TempDir(), "parallel-barrier")
	if err := os.Mkdir(parallelBarrier, 0o700); err != nil {
		t.Fatal(err)
	}
	plannerPlan := `{"subtasks":[{"id":"first","task":"write first","files":["first.txt"],"depends_on":[]},{"id":"second","task":"write second","files":["second.txt"],"depends_on":[]},{"id":"dependent","task":"write dependent","files":["dependent.txt"],"depends_on":["first","second"]}]}`
	planPath := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(planPath, []byte(plannerPlan), 0o600); err != nil {
		t.Fatal(err)
	}
	harness := filepath.Join(t.TempDir(), "pi-compatible-harness.sh")
	barrier := parallelBarrier
	script := fmt.Sprintf(`#!/bin/sh
task=
for arg in "$@"; do task=$arg; done
case "$task" in
  *"Create an implementation plan"*) cat %q; exit 0 ;;
  *"write first"*) touch %q/first-started; while [ ! -e %q/second-started ]; do sleep 0.02; done; printf first > first.txt ;;
  *"write second"*) touch %q/second-started; while [ ! -e %q/first-started ]; do sleep 0.02; done; printf second > second.txt ;;
  *"write dependent"*) test -f first.txt || exit 41; test -f second.txt || exit 42; printf dependent > dependent.txt ;;
esac
case "$1" in *"Review the requested work"*) test -f first.txt && test -f second.txt && test -f dependent.txt || exit 44; touch %q/integrated;; esac
`, planPath, barrier, barrier, barrier, barrier, barrier)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{system_prompt}", "{task}"}}
	maxConcurrency := 2
	config.ParallelSubtasks = &restserver.ParallelSubtasksConfig{Enabled: true, MaxConcurrency: &maxConcurrency}
	providerPath := filepath.Join(t.TempDir(), "provider-records")
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyPath, logPath := filepath.Join(t.TempDir(), "listener"), filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+testProcessStateHome(t), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_REPORT_RUNTIME_ERRORS=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+providerPath, "FACTORY_E2E_PROVIDER_REQUIRES_MARKER="+filepath.Join(barrier, "integrated"))
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	defer func() {
		if child.Process != nil && child.ProcessState == nil {
			_ = child.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = child.Process.Kill()
				<-done
			}
		}
	}()
	baseURL := waitForProcessURL(t, done, readyPath, logPath)
	client := &http.Client{Timeout: 5 * time.Second}
	admitted, err := submitProcessJob(client, baseURL, apiKey, "trusted", "implement three related files", "parallel-subtask-e2e")
	if err != nil || admitted.StatusCode != http.StatusAccepted || admitted.ID == "" {
		t.Fatalf("parallel job admission=%+v err=%v", admitted, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var completed restjobs.Snapshot
	for time.Now().Before(deadline) {
		completed = getProcessJob(t, client, baseURL, admitted.ID, apiKey)
		if completed.Status.Terminal() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if completed.Status != restjobs.StatusSucceeded || completed.Provider == nil {
		t.Fatalf("parallel job outcome=%+v history=%+v", completed, getProcessHistory(t, client, baseURL, admitted.ID, apiKey))
	}
	for _, marker := range []string{"first-started", "second-started", "integrated"} {
		if _, err := os.Stat(filepath.Join(barrier, marker)); err != nil {
			t.Fatalf("parallel behavior marker %q missing: %v", marker, err)
		}
	}
	providerRecord, err := os.ReadFile(providerPath)
	if err != nil || strings.Count(string(providerRecord), `"job_id"`) != 1 {
		t.Fatalf("provider publication records=%q err=%v, want exactly one publication after integration", providerRecord, err)
	}
	if _, err := os.Stat(filepath.Join(workRoot, "first.txt")); !os.IsNotExist(err) {
		t.Fatalf("parallel changes escaped isolated job workspace: %v", err)
	}
}

func TestRESTServerProcessKeepsParallelSubtasksSerialByDefaultAndWhenDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	for _, tc := range []struct {
		name        string
		parallel    *restserver.ParallelSubtasksConfig
		plannerFail bool
	}{
		{name: "omitted"},
		{name: "disabled", parallel: &restserver.ParallelSubtasksConfig{Enabled: false}},
		{name: "enabled planner failure", parallel: &restserver.ParallelSubtasksConfig{Enabled: true}, plannerFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, apiKey := runtimeFixture(t)
			config.Limits.Workers = 1
			config.ParallelSubtasks = tc.parallel
			providerToken := filepath.Join(t.TempDir(), "provider-token")
			if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
				t.Fatal(err)
			}
			config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
			plannerCalled := filepath.Join(t.TempDir(), "planner-called")
			harness := filepath.Join(t.TempDir(), "serial-harness.sh")
			script := fmt.Sprintf("#!/bin/sh\ntask=\nfor arg in \"$@\"; do task=$arg; done\ncase \"$task\" in *\"Create an implementation plan\"*) touch %q; exit 42;; *serial-only*) printf serial > result.txt;; esac\n", plannerCalled)
			if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{system_prompt}", "{task}"}}
			providerPath := filepath.Join(t.TempDir(), "provider-records")
			configPath := filepath.Join(t.TempDir(), "server.json")
			configData, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(configPath, configData, 0o600); err != nil {
				t.Fatal(err)
			}
			readyPath, logPath := filepath.Join(t.TempDir(), "listener"), filepath.Join(t.TempDir(), "server.log")
			logFile, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			defer logFile.Close()
			child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
			child.Env = append(os.Environ(), "XDG_STATE_HOME="+testProcessStateHome(t), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_REPORT_RUNTIME_ERRORS=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+providerPath)
			child.Stdout, child.Stderr = logFile, logFile
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- child.Wait() }()
			defer func() {
				if child.Process != nil && child.ProcessState == nil {
					_ = child.Process.Signal(syscall.SIGTERM)
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						_ = child.Process.Kill()
						<-done
					}
				}
			}()
			baseURL := waitForProcessURL(t, done, readyPath, logPath)
			client := &http.Client{Timeout: 5 * time.Second}
			admitted, err := submitProcessJob(client, baseURL, apiKey, "trusted", "serial-only implementation", "serial-"+tc.name)
			if err != nil || admitted.StatusCode != http.StatusAccepted || admitted.ID == "" {
				t.Fatalf("serial job admission=%+v err=%v", admitted, err)
			}
			wantStatus := restjobs.StatusSucceeded
			if tc.plannerFail {
				wantStatus = restjobs.StatusFailed
			}
			waitForBackupJob(t, client, baseURL, admitted.ID, apiKey, wantStatus)
			_, plannerErr := os.Stat(plannerCalled)
			if tc.plannerFail && plannerErr != nil {
				t.Fatalf("planner was not called in enabled mode: %v", plannerErr)
			}
			if !tc.plannerFail && !os.IsNotExist(plannerErr) {
				t.Fatalf("planner ran in %s mode: %v", tc.name, plannerErr)
			}
			providerRecord, err := os.ReadFile(providerPath)
			if tc.plannerFail {
				if !os.IsNotExist(err) {
					t.Fatalf("failed planner published provider record=%q err=%v", providerRecord, err)
				}
			} else if err != nil || strings.Count(string(providerRecord), `"job_id"`) != 1 {
				t.Fatalf("provider publication records=%q err=%v, want exactly one serial publication", providerRecord, err)
			}
		})
	}
}

func TestRESTServerProcessRunsConcurrentSQLiteJobsThroughProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	config, apiKey := runtimeFixture(t)
	root := filepath.Dir(config.Repositories["trusted"])
	secondRepo := filepath.Join(root, "second-repository")
	if err := os.Mkdir(secondRepo, 0o700); err != nil {
		t.Fatal(err)
	}
	initE2ERepository(t, secondRepo)
	config.Repositories = map[string]string{"first": config.Repositories["trusted"], "second": secondRepo}
	config.Limits.Workers = 2
	config.Limits.QueueCapacity = 4
	config.Limits.MaxRecords = 8
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"first": "acme/first", "second": "acme/second"}}
	barrier := filepath.Join(t.TempDir(), "barrier")
	if err := os.Mkdir(barrier, 0o700); err != nil {
		t.Fatal(err)
	}
	harness := filepath.Join(t.TempDir(), "concurrent-harness.sh")
	script := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in fail-*) exit 42;; cancel-*) touch '%s/cancel-started'; sleep 30; exit 0;; esac\nmkdir '%s/'\"$1\"\nsleep 0.2\nprintf 'implemented\\n' >> \"$PWD/result.txt\"\n", barrier, barrier)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	checkPath := filepath.Join(t.TempDir(), "verify-check.sh")
	if err := os.WriteFile(checkPath, []byte("#!/bin/sh\nprintf 'private verification transcript marker\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config.VerificationChecks = [][]string{{checkPath}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	readyDir, outcomeDir := t.TempDir(), t.TempDir()
	readyPath := filepath.Join(readyDir, "listener")
	outcomesPath := filepath.Join(outcomeDir, "outcomes.jsonl")
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath)
	childLogPath := filepath.Join(t.TempDir(), "server.log")
	childLog, err := os.Create(childLogPath)
	if err != nil {
		t.Fatal(err)
	}
	defer childLog.Close()
	child.Stdout, child.Stderr = childLog, childLog
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()
	defer func() {
		if child.Process != nil {
			_ = child.Process.Signal(syscall.SIGTERM)
			select {
			case <-childDone:
			case <-time.After(2 * time.Second):
				_ = child.Process.Kill()
				<-childDone
			}
		}
	}()
	baseURL := waitForProcessURL(t, childDone, readyPath, childLogPath)
	client := &http.Client{Timeout: 3 * time.Second}
	type response struct {
		status int
		body   struct {
			Job struct {
				ID string `json:"id"`
			} `json:"job"`
			Replayed bool `json:"replayed"`
		}
		err error
	}
	results := make(chan response, 2)
	for _, item := range []struct{ alias, task, key string }{{"first", "task-first", "key-first"}, {"second", "task-second", "key-second"}} {
		item := item
		go func() {
			body := strings.NewReader(fmt.Sprintf(`{"repository":%q,"task":%q}`, item.alias, item.task))
			req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", body)
			if err != nil {
				results <- response{err: err}
				return
			}
			req.Header.Set("Authorization", "Bearer "+apiKey)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", item.key)
			resp, err := client.Do(req)
			if err != nil {
				results <- response{err: err}
				return
			}
			defer resp.Body.Close()
			var result response
			result.status = resp.StatusCode
			result.err = json.NewDecoder(resp.Body).Decode(&result.body)
			results <- result
		}()
	}
	ids := make([]string, 0, 2)
	for range 2 {
		result := <-results
		if result.err != nil || result.status != http.StatusAccepted || result.body.Replayed {
			t.Fatalf("admission status=%d replayed=%v err=%v", result.status, result.body.Replayed, result.err)
		}
		ids = append(ids, result.body.Job.ID)
	}
	if ids[0] == ids[1] {
		t.Fatalf("independent submissions shared job id %q", ids[0])
	}
	jobIDs := make(map[string]string, len(ids))
	requestIDs := make(map[string]string, len(ids))
	for _, id := range ids {
		request := getProcessJob(t, client, baseURL, id, apiKey)
		if request.Status != restjobs.StatusQueued && request.Status != restjobs.StatusRunning {
			t.Fatalf("job became terminal before polling: %+v", request)
		}
		expected := map[string]string{"task-first": "first", "task-second": "second"}[request.Request.Task]
		if expected == "" || request.Request.Repository != expected {
			t.Fatalf("accepted job request is not isolated: %+v", request.Request)
		}
		if request.Request.Task != "task-first" && request.Request.Task != "task-second" {
			t.Fatalf("unexpected task in inspectable result: %q", request.Request.Task)
		}
		jobIDs[request.Request.Task] = id
		requestIDs[request.Request.Task] = request.Request.Repository
	}
	if len(jobIDs) != 2 {
		t.Fatalf("request results did not preserve separate jobs: %v", jobIDs)
	}
	for _, id := range ids {
		waitForProcessJob(t, client, baseURL, id, apiKey)
		job, responseBody := getProcessJobResponse(t, client, baseURL, id, apiKey)
		if job.Verification == nil || len(job.Verification.Checks) != 1 || job.Verification.Checks[0] != (restjobs.VerificationCheck{Name: "check-01", Outcome: restjobs.VerificationPassed}) {
			t.Fatalf("inspectable verification evidence = %+v, want passed check-01", job.Verification)
		}
		if job.Verification == nil || len(job.Verification.Limitations) != 1 || job.Verification.Limitations[0] != restjobs.LimitationAgentNotVerdict {
			t.Fatalf("verification limitations = %+v, want agent-verdict limitation", job.Verification)
		}
		for _, forbidden := range []string{"private verification transcript marker", checkPath, "workflow.log", "pipeline-check-01.log", root} {
			if strings.Contains(responseBody, forbidden) {
				t.Fatalf("job inspection exposed private verification data %q: %s", forbidden, responseBody)
			}
		}
		history := getProcessHistory(t, client, baseURL, id, apiKey)
		if len(history.Events) < 3 || history.Events[0].Type != "queued" || history.Events[1].Type != "running" {
			t.Fatalf("job history is not inspectable: %+v", history)
		}
	}
	expectedAfterRestart := getProcessJob(t, client, baseURL, jobIDs["task-first"], apiKey)
	startedData, err := os.ReadFile(outcomesPath + ".started")
	if err != nil {
		t.Fatal(err)
	}
	if attempts := strings.Fields(string(startedData)); len(attempts) != 2 {
		t.Fatalf("provider attempts=%d, want one per successful job", len(attempts))
	}
	failed, err := submitProcessJob(client, baseURL, apiKey, "first", "fail-workflow", "key-failure")
	if err != nil || failed.StatusCode != http.StatusAccepted || failed.ID == "" {
		t.Fatalf("failure admission: job=%+v err=%v", failed, err)
	}
	waitForProcessFailure(t, client, baseURL, failed.ID, apiKey)
	duplicate, err := submitProcessJob(client, baseURL, apiKey, "first", "task-first", "key-first")
	if err != nil || duplicate.StatusCode != http.StatusAccepted || !duplicate.Replayed || duplicate.ID != jobIDs["task-first"] {
		t.Fatalf("same-key retry did not return original job: %+v err=%v", duplicate, err)
	}
	entries, err := os.ReadDir(barrier)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("independent workflows did not overlap: %v", entries)
	}
	canceled, err := submitProcessJob(client, baseURL, apiKey, "first", "cancel-shutdown", "key-canceled")
	if err != nil || canceled.StatusCode != http.StatusAccepted || canceled.ID == "" {
		t.Fatalf("cancellation admission: job=%+v err=%v", canceled, err)
	}
	cancelStarted := filepath.Join(barrier, "cancel-started")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(cancelStarted); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(cancelStarted); err != nil {
		t.Fatalf("cancellation workflow did not start: %v", err)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("server process exit: %v", err)
	}
	child.Process = nil

	createsBeforeRestart, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	startedBeforeRestart, err := os.ReadFile(outcomesPath + ".started")
	if err != nil {
		t.Fatal(err)
	}
	restartedReady := filepath.Join(readyDir, "listener-restarted")
	restartedLogPath := filepath.Join(t.TempDir(), "server-restarted.log")
	restartedLog, err := os.Create(restartedLogPath)
	if err != nil {
		t.Fatal(err)
	}
	restartedChild := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	restartedChild.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+restartedReady, "FACTORY_E2E_OUTCOMES="+outcomesPath)
	restartedChild.Stdout, restartedChild.Stderr = restartedLog, restartedLog
	if err := restartedChild.Start(); err != nil {
		_ = restartedLog.Close()
		t.Fatal(err)
	}
	restartedDone := make(chan error, 1)
	restartedExited := make(chan struct{})
	go func() {
		defer close(restartedExited)
		restartedDone <- restartedChild.Wait()
		_ = restartedLog.Close()
	}()
	restartedStopped := false
	stopRestarted := func() {
		if restartedStopped {
			return
		}
		restartedStopped = true
		_ = restartedChild.Process.Signal(syscall.SIGTERM)
		select {
		case <-restartedExited:
		case <-time.After(2 * time.Second):
			_ = restartedChild.Process.Kill()
			<-restartedExited
		}
	}
	defer stopRestarted()
	restartedURL := waitForProcessURL(t, restartedDone, restartedReady, restartedLogPath)
	replayed, err := submitProcessJob(client, restartedURL, apiKey, "first", "task-first", "key-first")
	if err != nil || replayed.StatusCode != http.StatusAccepted || !replayed.Replayed || replayed.ID != expectedAfterRestart.ID {
		stopRestarted()
		t.Fatalf("HTTP idempotency retry after process restart=%+v err=%v, want original job %q", replayed, err, expectedAfterRestart.ID)
	}
	restartedJob := getProcessJob(t, client, restartedURL, replayed.ID, apiKey)
	if restartedJob.ID != expectedAfterRestart.ID || restartedJob.Status != expectedAfterRestart.Status || restartedJob.Status != restjobs.StatusSucceeded || restartedJob.Provider == nil || expectedAfterRestart.Provider == nil || *restartedJob.Provider != *expectedAfterRestart.Provider || restartedJob.Verification == nil || expectedAfterRestart.Verification == nil || len(restartedJob.Verification.Checks) != len(expectedAfterRestart.Verification.Checks) || len(restartedJob.Verification.Checks) != 1 || restartedJob.Verification.Checks[0] != expectedAfterRestart.Verification.Checks[0] || len(restartedJob.Verification.Limitations) != len(expectedAfterRestart.Verification.Limitations) || restartedJob.Verification.Limitations[0] != expectedAfterRestart.Verification.Limitations[0] {
		stopRestarted()
		t.Fatalf("HTTP replay changed successful provider/verification outcome: before=%+v after=%+v", expectedAfterRestart, restartedJob)
	}
	createsAfterReplay, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	startedAfterReplay, err := os.ReadFile(outcomesPath + ".started")
	if err != nil {
		t.Fatal(err)
	}
	if string(createsAfterReplay) != string(createsBeforeRestart) || string(startedAfterReplay) != string(startedBeforeRestart) {
		t.Fatalf("HTTP replay caused fake-provider create: before=%q after=%q", createsBeforeRestart, createsAfterReplay)
	}
	if err := restartedChild.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-restartedDone; err != nil {
		restartedStopped = true
		t.Fatalf("restarted server process exit: %v", err)
	}
	restartedStopped = true
	persisted, err := restworker.NewLocalJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer persisted.Close()
	for _, id := range ids {
		job, err := persisted.Get(id)
		if err != nil || job.Status != restjobs.StatusSucceeded || job.Provider == nil {
			t.Fatalf("durable outcome job=%+v err=%v", job, err)
		}
		history, err := persisted.History(id)
		if err != nil || len(history.Events) < 3 || history.Events[len(history.Events)-1].Type != string(restjobs.StatusSucceeded) {
			t.Fatalf("terminal provider outcome history=%+v err=%v", history, err)
		}
		key := map[string]string{jobIDs["task-first"]: "key-first", jobIDs["task-second"]: "key-second"}[id]
		replayed, duplicate, err := persisted.Admit(key, job.Request)
		if err != nil || !duplicate || replayed.ID != id {
			t.Fatalf("restart idempotency replay=(%+v,%v,%v)", replayed, duplicate, err)
		}
	}
	canceledJob, err := persisted.Get(canceled.ID)
	if err != nil || canceledJob.Status != restjobs.StatusCanceled || canceledJob.Provider != nil {
		t.Fatalf("shutdown-canceled durable job=%+v err=%v", canceledJob, err)
	}
	canceledHistory, err := persisted.History(canceled.ID)
	if err != nil || len(canceledHistory.Events) < 3 || canceledHistory.Events[len(canceledHistory.Events)-1].Type != string(restjobs.StatusCanceled) {
		t.Fatalf("shutdown-canceled history=%+v err=%v", canceledHistory, err)
	}
	canceledResult := filepath.Join(stateDir, aliasDirectory("first"), "results", canceled.ID)
	if _, err := os.Stat(filepath.Join(canceledResult, "worktree")); err != nil {
		t.Fatalf("shutdown-canceled workspace was not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(canceledResult, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("shutdown-canceled workspace has completion marker: %v", err)
	}
	type providerRecord struct {
		JobID      string `json:"job_id"`
		Repository string `json:"repository"`
		Worktree   string `json:"worktree"`
		Branch     string `json:"branch"`
		Commit     string `json:"commit"`
	}
	var records []providerRecord
	outcomeData, err := os.ReadFile(outcomesPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(outcomeData)), "\n") {
		var record providerRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 2 || records[0].Worktree == records[1].Worktree || records[0].Branch == records[1].Branch {
		t.Fatalf("provider calls do not reflect isolated jobs: %+v", records)
	}
	for _, record := range records {
		if record.Commit == "" || !strings.HasPrefix(record.Branch, "factory/job/") {
			t.Fatalf("incomplete provider request: %+v", record)
		}
		expectedTask := map[string]string{jobIDs["task-first"]: "task-first", jobIDs["task-second"]: "task-second"}[record.JobID]
		wantRepository := map[string]string{"task-first": "acme/first", "task-second": "acme/second"}[expectedTask]
		if expectedTask == "" || record.Repository != wantRepository || requestIDs[expectedTask] == "" {
			t.Fatalf("provider repository does not match request alias: %+v", record)
		}
	}
}

func TestRESTServerProcessReconcilesConfirmedProviderWriteAfterOutcomePersistenceFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	config.Limits.JobTimeout = "30s"
	harness := filepath.Join(t.TempDir(), "harness.sh")
	harnessCount := filepath.Join(t.TempDir(), "harness-count")
	if err := os.WriteFile(harness, []byte(fmt.Sprintf("#!/bin/sh\nprintf 'run\\n' >> %q\nprintf 'verified\\n' >> result.txt\n", harnessCount)), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomesPath := filepath.Join(t.TempDir(), "provider-outcomes")
	readyPath := filepath.Join(t.TempDir(), "listener")
	logPath := filepath.Join(t.TempDir(), "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
	child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath, "FACTORY_E2E_PROVIDER_MODE=confirmed-wait")
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	childDone := make(chan error, 1)
	go func() {
		childDone <- child.Wait()
		_ = logFile.Close()
	}()
	baseURL := waitForProcessURL(t, childDone, readyPath, logPath)
	client := &http.Client{Timeout: 15 * time.Second}
	defer func() {
		if child.Process == nil {
			return
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-childDone:
		case <-time.After(3 * time.Second):
			_ = child.Process.Kill()
			<-childDone
		}
	}()
	admitted, err := submitProcessJob(client, baseURL, apiKey, "trusted", "publish with confirmed response", "confirmed-outcome-lock-key")
	if err != nil || admitted.StatusCode != http.StatusAccepted || admitted.ID == "" {
		t.Fatalf("confirmed-outcome admission=%+v err=%v", admitted, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(outcomesPath + ".publish-waiting"); err == nil {
			break
		}
		select {
		case err := <-childDone:
			t.Fatalf("server exited before confirmed provider publish: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(outcomesPath + ".publish-waiting"); err != nil {
		t.Fatalf("provider did not reach confirmed publish: %v", err)
	}
	unlockSQLite := lockSQLiteWrites(t, config.Persistence.Path)
	locked := true
	defer func() {
		if locked {
			unlockSQLite()
		}
	}()
	if err := os.WriteFile(outcomesPath+".release-publish", []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5500 * time.Millisecond)
	unlockSQLite()
	locked = false
	waitForProcessFailure(t, client, baseURL, admitted.ID, apiKey)
	failed := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
	if failed.Provider != nil {
		t.Fatalf("outcome persistence failure recorded provider result: %+v", failed)
	}
	failedHistory := getProcessHistory(t, client, baseURL, admitted.ID, apiKey)
	for _, event := range failedHistory.Events {
		if event.Type == "provider_reconciled" || event.Type == string(restjobs.StatusSucceeded) {
			t.Fatalf("outcome persistence failure recorded completion: %+v", failedHistory)
		}
	}
	workspace := filepath.Join(stateDir, aliasDirectory("trusted"), "results", admitted.ID)
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("failed provider outcome workspace was not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("failed provider outcome workspace has completion marker: %v", err)
	}
	persisted, err := restworker.NewLocalJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer persisted.Close()
	providerAttemptStore, ok := persisted.(*restjobs.SQLiteStore)
	if !ok {
		t.Fatalf("persisted store type=%T, want SQLiteStore", persisted)
	}
	attempt, err := providerAttemptStore.ProviderAttempt(admitted.ID)
	if err != nil || attempt.Uncertain {
		t.Fatalf("confirmed provider attempt=%+v err=%v, want recorded non-uncertain attempt", attempt, err)
	}
	if err := os.Remove(outcomesPath + ".release-publish"); err != nil {
		t.Fatal(err)
	}
	failed = getProcessJob(t, client, baseURL, admitted.ID, apiKey)
	if failed.Status != restjobs.StatusFailed || failed.Provider != nil {
		t.Fatalf("post-lock job state=%+v, want failed without outcome", failed)
	}
	harnessRunsBeforeReconcile, err := os.ReadFile(harnessCount)
	if err != nil {
		t.Fatal(err)
	}
	reconcile := func() (int, string) {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/"+admitted.ID+"/reconcile", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	if code, body := reconcile(); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("confirmed-attempt reconcile status=%d body=%s", code, body)
	}
	if code, body := reconcile(); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("repeated confirmed-attempt reconcile status=%d body=%s", code, body)
	}
	reconciled := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
	if reconciled.Status != restjobs.StatusSucceeded || reconciled.Provider == nil || reconciled.Provider.Number != 72 {
		t.Fatalf("reconciled confirmed provider result=%+v", reconciled)
	}
	if _, err := os.Stat(outcomesPath + ".read-only-lookup"); err != nil {
		t.Fatalf("provider was not confirmed by read-only reconciliation: %v", err)
	}
	providerRecord, err := os.ReadFile(outcomesPath + ".confirmed.json")
	if err != nil {
		t.Fatal(err)
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(providerRecord, &record); err != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("provider create record=%+v err=%v", record, err)
	}
	harnessRuns, err := os.ReadFile(harnessCount)
	if err != nil || string(harnessRuns) != string(harnessRunsBeforeReconcile) {
		t.Fatalf("reconciliation reran harness: before=%q after=%q err=%v", harnessRunsBeforeReconcile, harnessRuns, err)
	}
}

func TestRESTServerProcessRecoversUncertainProviderCreateWithoutDuplicate(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	config.Limits.JobTimeout = "2s"
	harness := filepath.Join(t.TempDir(), "harness.sh")
	harnessCount := filepath.Join(t.TempDir(), "harness-count")
	harnessScript := fmt.Sprintf("#!/bin/sh\nprintf 'run\\n' >> %q\nprintf 'verified\\n' >> result.txt\n", harnessCount)
	if err := os.WriteFile(harness, []byte(harnessScript), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomesPath := filepath.Join(t.TempDir(), "provider-outcomes.jsonl")
	startServer := func() (*exec.Cmd, <-chan error, string) {
		t.Helper()
		readyPath := filepath.Join(t.TempDir(), "listener")
		logPath := filepath.Join(t.TempDir(), "server.log")
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
		child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath, "FACTORY_E2E_PROVIDER_MODE=uncertain")
		child.Stdout, child.Stderr = logFile, logFile
		if err := child.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- child.Wait()
			_ = logFile.Close()
		}()
		return child, done, waitForProcessURL(t, done, readyPath, logPath)
	}

	child, childDone, baseURL := startServer()
	defer func() {
		if child.Process == nil {
			return
		}
		_ = child.Process.Signal(syscall.SIGTERM)
		select {
		case <-childDone:
		case <-time.After(2 * time.Second):
			_ = child.Process.Kill()
			<-childDone
		}
	}()
	client := &http.Client{Timeout: 10 * time.Second}
	admitted, err := submitProcessJob(client, baseURL, apiKey, "trusted", "publish with uncertain response", "uncertain-create-key")
	if err != nil || admitted.StatusCode != http.StatusAccepted || admitted.ID == "" {
		t.Fatalf("uncertain-create admission=%+v err=%v", admitted, err)
	}
	recordPath := outcomesPath + ".uncertain.json"
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(recordPath); err == nil {
			break
		}
		job := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
		if job.Status.Terminal() {
			t.Fatalf("job became terminal before fake provider create; job=%+v history=%+v", job, getProcessHistory(t, client, baseURL, admitted.ID, apiKey))
		}
		select {
		case err := <-childDone:
			t.Fatalf("server exited before provider create became uncertain: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(recordPath); err != nil {
		job := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
		t.Fatalf("fake provider did not record accepted PR create: %v; job=%+v history=%+v", err, job, getProcessHistory(t, client, baseURL, admitted.ID, apiKey))
	}
	waitForProcessFailure(t, client, baseURL, admitted.ID, apiKey)
	recordData, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var record uncertainProviderRecord
	if err := json.Unmarshal(recordData, &record); err != nil {
		t.Fatal(err)
	}
	if record.JobID != admitted.ID || record.Creates != 1 || record.Attempts != 1 || record.Outcome.Number != 71 {
		t.Fatalf("fake provider create record=%+v", record)
	}
	harnessRuns, err := os.ReadFile(harnessCount)
	if err != nil || len(harnessRuns) == 0 {
		t.Fatalf("harness run count unavailable: %q %v", harnessRuns, err)
	}
	initialHarnessRuns := string(harnessRuns)
	failed := getProcessJob(t, client, baseURL, admitted.ID, apiKey)
	if failed.Status != restjobs.StatusFailed || failed.Provider != nil {
		t.Fatalf("job reported success despite ambiguous provider timeout: %+v", failed)
	}
	failedHistory := getProcessHistory(t, client, baseURL, admitted.ID, apiKey)
	if len(failedHistory.Events) < 3 || failedHistory.Events[len(failedHistory.Events)-1].Type != string(restjobs.StatusFailed) {
		t.Fatalf("ambiguous provider timeout evidence was not retained: %+v", failedHistory)
	}
	workspace := filepath.Join(stateDir, aliasDirectory("trusted"), "results", admitted.ID)
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("ambiguous provider workspace was not retained before restart: %v", err)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("server shutdown after provider timeout: %v", err)
	}
	child.Process = nil

	persisted, err := restworker.NewLocalJobManager(config)
	if err != nil {
		t.Fatal(err)
	}
	defer persisted.Close()
	persistedJob, err := persisted.Get(admitted.ID)
	if err != nil || persistedJob.Status != restjobs.StatusFailed || persistedJob.Provider != nil {
		t.Fatalf("timed-out job was reported successful or lost: %+v err=%v", persistedJob, err)
	}
	persistedHistory, err := persisted.History(admitted.ID)
	if err != nil || len(persistedHistory.Events) < 3 || persistedHistory.Events[len(persistedHistory.Events)-1].Type != string(restjobs.StatusFailed) {
		t.Fatalf("timed-out provider evidence was not durable: history=%+v err=%v", persistedHistory, err)
	}
	recovery, err := persisted.Recover(context.Background())
	if err != nil || len(recovery.Terminal) != 1 || recovery.Terminal[0].ID != admitted.ID {
		t.Fatalf("timed-out job recovery classification=%+v err=%v", recovery, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("uncertain provider workspace was not retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".completion.json")); !os.IsNotExist(err) {
		t.Fatalf("uncertain provider workspace has completion marker: %v", err)
	}

	// Starting a fresh server must keep the failed job inspectable and must not
	// replay a provider write without explicit reconciliation.
	restarted, restartedDone, restartedURL := startServer()
	defer func() {
		if restarted.Process == nil {
			return
		}
		_ = restarted.Process.Signal(syscall.SIGTERM)
		select {
		case <-restartedDone:
		case <-time.After(2 * time.Second):
			_ = restarted.Process.Kill()
			<-restartedDone
		}
	}()
	restartedJob := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	if restartedJob.Status != restjobs.StatusFailed || restartedJob.Provider != nil {
		t.Fatalf("restart changed unresolved provider job: %+v", restartedJob)
	}
	time.Sleep(100 * time.Millisecond)
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("restart blindly retried unresolved provider write: record=%+v err=%v", record, err)
	}
	if err := restarted.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-restartedDone; err != nil {
		t.Fatalf("restarted server shutdown: %v", err)
	}
	restarted.Process = nil

	// Explicit operator action reconciles by the persisted identity through the
	// restarted process, records a single audit event, and never calls Publish.
	restarted, restartedDone, restartedURL = startServer()
	operatorReconcile := func(auth string) (int, string) {
		req, requestErr := http.NewRequest(http.MethodPost, restartedURL+"/v1/jobs/"+admitted.ID+"/reconcile", nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		resp, requestErr := client.Do(req)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	if code, _ := operatorReconcile(""); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated reconcile status=%d", code)
	}
	for _, mode := range []string{"mismatch", "repository", "branch", "missing", "multiple", "outage", "malformed"} {
		if err := os.WriteFile(outcomesPath+".reconcile-mode", []byte(mode), 0o600); err != nil {
			t.Fatal(err)
		}
		if code, body := operatorReconcile(apiKey); code != http.StatusConflict || strings.Contains(body, "fake provider") || strings.Contains(body, "provider-test-token") {
			t.Fatalf("%s reconciliation status=%d body=%s", mode, code, body)
		}
		unchanged := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
		if unchanged.Status != restjobs.StatusFailed || unchanged.Provider != nil {
			t.Fatalf("%s reconciliation changed job: %+v", mode, unchanged)
		}
		if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
			t.Fatalf("%s reconciliation removed evidence: %v", mode, err)
		}
	}
	if err := os.Remove(outcomesPath + ".reconcile-mode"); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	lockedJobBefore := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	lockedHistoryBefore := getProcessHistory(t, client, restartedURL, admitted.ID, apiKey)
	providerAttemptStore, ok := persisted.(*restjobs.SQLiteStore)
	if !ok {
		t.Fatalf("persisted store type=%T, want SQLiteStore", persisted)
	}
	attemptBeforeLock, err := providerAttemptStore.ProviderAttempt(admitted.ID)
	if err != nil || !attemptBeforeLock.Uncertain {
		t.Fatalf("pre-lock uncertain provider attempt=%+v err=%v", attemptBeforeLock, err)
	}
	unlockSQLite := lockSQLiteWrites(t, config.Persistence.Path)
	locked := true
	defer func() {
		if locked {
			unlockSQLite()
		}
	}()
	if code, body := operatorReconcile(apiKey); code != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"not_ready"`) || strings.Contains(body, "SQLite") || strings.Contains(body, "provider-test-token") {
		t.Fatalf("reconciliation during SQLite write outage status=%d body=%s", code, body)
	}
	lockedJob, err := providerAttemptStore.Get(admitted.ID)
	if err != nil {
		t.Fatalf("inspect SQLite job while write lock held: %v", err)
	}
	lockedHistory, err := providerAttemptStore.History(admitted.ID)
	if err != nil {
		t.Fatalf("inspect SQLite history while write lock held: %v", err)
	}
	attemptDuringLock, err := providerAttemptStore.ProviderAttempt(admitted.ID)
	if err != nil || !reflect.DeepEqual(lockedJob, lockedJobBefore) || !samePublicJobHistory(lockedHistory, lockedHistoryBefore) || attemptDuringLock != attemptBeforeLock {
		t.Fatalf("SQLite lock changed persisted reconciliation state: before=(%+v,%+v,%+v) after=(%+v,%+v,%+v) err=%v", lockedJobBefore, lockedHistoryBefore, attemptBeforeLock, lockedJob, lockedHistory, attemptDuringLock, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("SQLite write lock removed retained workspace: %v", err)
	}
	if currentRuns, err := os.ReadFile(harnessCount); err != nil || string(currentRuns) != initialHarnessRuns {
		t.Fatalf("SQLite write lock reran harness: before=%q after=%q err=%v", initialHarnessRuns, currentRuns, err)
	}
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("SQLite write lock reran provider write: record=%+v err=%v", record, err)
	}
	unlockSQLite()
	locked = false
	outageJob := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	if outageJob.Status != restjobs.StatusFailed || outageJob.Provider != nil {
		t.Fatalf("SQLite write outage changed job: %+v", outageJob)
	}
	outageHistory := getProcessHistory(t, client, restartedURL, admitted.ID, apiKey)
	for _, event := range outageHistory.Events {
		if event.Type == "provider_reconciled" {
			t.Fatalf("SQLite write outage recorded successful reconciliation: %+v", outageHistory)
		}
	}
	attemptDuringOutage, err := providerAttemptStore.ProviderAttempt(admitted.ID)
	if err != nil || !attemptDuringOutage.Uncertain {
		t.Fatalf("SQLite write outage lost uncertain provider attempt: %+v err=%v", attemptDuringOutage, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("SQLite write outage removed retained workspace: %v", err)
	}
	if currentRuns, err := os.ReadFile(harnessCount); err != nil || string(currentRuns) != initialHarnessRuns {
		t.Fatalf("SQLite write outage reran harness: before=%q after=%q err=%v", initialHarnessRuns, currentRuns, err)
	}
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("SQLite write outage reran provider write: record=%+v err=%v", record, err)
	}
	if err := os.Remove(outcomesPath + ".reconcile-mode"); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if code, body := operatorReconcile(apiKey); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("authenticated reconcile status=%d body=%s", code, body)
	}
	if code, body := operatorReconcile(apiKey); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("repeated reconcile status=%d body=%s", code, body)
	}
	reconciled := getProcessJob(t, client, restartedURL, admitted.ID, apiKey)
	if reconciled.Status != restjobs.StatusSucceeded || reconciled.Provider == nil || reconciled.Provider.Number != record.Outcome.Number {
		t.Fatalf("reconciled job=%+v", reconciled)
	}
	reconciledHistory := getProcessHistory(t, client, restartedURL, admitted.ID, apiKey)
	reconciledEvents := 0
	for _, event := range reconciledHistory.Events {
		if event.Type == "provider_reconciled" {
			reconciledEvents++
		}
	}
	if reconciledEvents != 1 {
		t.Fatalf("reconciliation audit count=%d history=%+v", reconciledEvents, reconciledHistory)
	}
	recordData, err = os.ReadFile(recordPath)
	if err != nil || json.Unmarshal(recordData, &record) != nil || record.Creates != 1 || record.Attempts != 1 {
		t.Fatalf("reconciliation reran publish or duplicated create: record=%+v err=%v", record, err)
	}
	harnessRuns, err = os.ReadFile(harnessCount)
	if err != nil || string(harnessRuns) != initialHarnessRuns {
		t.Fatalf("reconciliation reran harness: before=%q after=%q err=%v", initialHarnessRuns, harnessRuns, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "worktree")); err != nil {
		t.Fatalf("reconciliation removed retained workspace: %v", err)
	}
	if err := restarted.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-restartedDone; err != nil {
		t.Fatalf("reconciled server shutdown: %v", err)
	}
	restarted.Process = nil
}

func TestRESTServerProcessReconcilesInterruptedJobsWithoutReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level REST E2E")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process-level REST E2E requires supported local process signal semantics")
	}
	config, apiKey := runtimeFixture(t)
	config.Limits.Workers = 2
	config.Limits.QueueCapacity = 2
	stateDir := testResultsBase(t)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendSQLite, Path: filepath.Join(stateDir, "jobs.db")}
	providerToken := filepath.Join(t.TempDir(), "provider-token")
	if err := os.WriteFile(providerToken, []byte("test-provider-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Provider = restserver.ProviderConfig{Backend: "github", TokenFile: providerToken, BaseBranch: "main", Repositories: map[string]string{"trusted": "acme/widget"}}
	config.Limits.JobTimeout = "30s"
	harnessCount := filepath.Join(t.TempDir(), "harness-count")
	harnessPID := filepath.Join(t.TempDir(), "harness-pid")
	harness := filepath.Join(t.TempDir(), "interrupted-harness.sh")
	script := fmt.Sprintf("#!/bin/sh\nprintf 'run\\n' >> %q\ncase \"$1\" in hold-no-provider) printf '%%s\\n' \"$$\" > %q; exec sleep 300 ;; esac\nprintf 'verified\\n' >> result.txt\n", harnessCount, harnessPID)
	if err := os.WriteFile(harness, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{task}", "{system_prompt}"}}
	configPath := filepath.Join(t.TempDir(), "server.json")
	configData, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	outcomesPath := filepath.Join(t.TempDir(), "provider-record")
	readyDir := t.TempDir()
	startServer := func(name string) (*exec.Cmd, <-chan error, string) {
		t.Helper()
		readyPath := filepath.Join(readyDir, name)
		logPath := filepath.Join(t.TempDir(), name+".log")
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestRESTServerProcessHelper$")
		child.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Dir(filepath.Dir(stateDir)), "FACTORY_E2E_HELPER=1", "FACTORY_E2E_CONFIG="+configPath, "FACTORY_E2E_READY="+readyPath, "FACTORY_E2E_OUTCOMES="+outcomesPath, "FACTORY_E2E_PROVIDER_MODE=uncertain")
		child.Stdout, child.Stderr = logFile, logFile
		if err := child.Start(); err != nil {
			_ = logFile.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- child.Wait(); _ = logFile.Close() }()
		return child, done, waitForProcessURL(t, done, readyPath, logPath)
	}
	client := &http.Client{Timeout: 12 * time.Second}
	child, childDone, baseURL := startServer("initial-listener")
	noAttemptPID := 0
	defer func() {
		if child != nil && child.ProcessState == nil {
			_ = child.Process.Kill()
			<-childDone
		}
		if noAttemptPID > 0 {
			_ = syscall.Kill(noAttemptPID, syscall.SIGKILL)
		}
	}()
	providerJob, err := submitProcessJob(client, baseURL, apiKey, "trusted", "provider-interrupted", "recovery-provider")
	if err != nil || providerJob.StatusCode != http.StatusAccepted {
		t.Fatalf("provider job admission=%+v err=%v", providerJob, err)
	}
	noAttemptJob, err := submitProcessJob(client, baseURL, apiKey, "trusted", "hold-no-provider", "recovery-no-provider")
	if err != nil || noAttemptJob.StatusCode != http.StatusAccepted {
		t.Fatalf("no-attempt job admission=%+v err=%v", noAttemptJob, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(outcomesPath + ".uncertain.json"); err == nil {
			if data, readErr := os.ReadFile(harnessPID); readErr == nil {
				_, _ = fmt.Sscanf(string(data), "%d", &noAttemptPID)
				if noAttemptPID > 0 {
					break
				}
			}
		}
		select {
		case err := <-childDone:
			t.Fatalf("server exited before recovery barriers: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if noAttemptPID <= 0 {
		t.Fatal("jobs did not reach provider and harness interruption barriers")
	}
	if getProcessJob(t, client, baseURL, providerJob.ID, apiKey).Status != restjobs.StatusRunning || getProcessJob(t, client, baseURL, noAttemptJob.ID, apiKey).Status != restjobs.StatusRunning {
		t.Fatal("both jobs must be running before abrupt service termination")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err == nil {
		t.Fatal("server was not abruptly interrupted")
	}
	child = nil
	_ = syscall.Kill(noAttemptPID, syscall.SIGKILL)

	child, childDone, baseURL = startServer("recovery-listener")
	action := func(id, path, authorization, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/"+id+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if authorization != "" {
			req.Header.Set("Authorization", "Bearer "+authorization)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(data)
	}
	if code, _ := action(providerJob.ID, "/reconcile", "wrong-key", ""); code != http.StatusUnauthorized {
		t.Fatalf("wrong-auth reconcile status=%d", code)
	}
	if code, _ := action(providerJob.ID, "/reconcile", apiKey, `{"number":900}`); code != http.StatusBadRequest {
		t.Fatalf("body-bearing reconcile status=%d", code)
	}
	for _, mode := range []string{"mismatch", "outage"} {
		if err := os.WriteFile(outcomesPath+".reconcile-mode", []byte(mode), 0o600); err != nil {
			t.Fatal(err)
		}
		if code, body := action(providerJob.ID, "/reconcile", apiKey, ""); code != http.StatusConflict || strings.Contains(body, "test-provider-token") {
			t.Fatalf("%s interrupted reconcile status=%d body=%s", mode, code, body)
		}
		unchanged := getProcessJob(t, client, baseURL, providerJob.ID, apiKey)
		if unchanged.Status != restjobs.StatusRunning || unchanged.Provider != nil || unchanged.Verification == nil {
			t.Fatalf("%s reconciliation changed interrupted job or lost evidence: %+v", mode, unchanged)
		}
		if _, err := os.Stat(filepath.Join(stateDir, aliasDirectory("trusted"), "results", providerJob.ID, "worktree")); err != nil {
			t.Fatalf("%s reconciliation removed retained workspace: %v", mode, err)
		}
	}
	if code, _ := action(noAttemptJob.ID, "/reconcile", apiKey, ""); code != http.StatusConflict {
		t.Fatalf("provider reconciliation without persisted attempt status=%d", code)
	}
	if err := os.Remove(outcomesPath + ".reconcile-mode"); err != nil {
		t.Fatal(err)
	}
	if code, _ := action(providerJob.ID, "/disposition/failed", apiKey, ""); code != http.StatusConflict {
		t.Fatalf("provider-attempt job accepted status-only disposition status=%d", code)
	}
	lockWrites := lockSQLiteWrites(t, config.Persistence.Path)
	if code, body := action(providerJob.ID, "/reconcile", apiKey, ""); code != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"not_ready"`) {
		lockWrites()
		t.Fatalf("SQLite-lock reconcile status=%d body=%s", code, body)
	}
	lockWrites()
	if job := getProcessJob(t, client, baseURL, providerJob.ID, apiKey); job.Status != restjobs.StatusRunning || job.Provider != nil || job.Verification == nil {
		t.Fatalf("SQLite lock changed provider job or lost evidence: %+v", job)
	}
	if _, err := os.Stat(filepath.Join(stateDir, aliasDirectory("trusted"), "results", providerJob.ID, "worktree")); err != nil {
		t.Fatalf("SQLite lock removed retained workspace: %v", err)
	}
	if code, body := action(providerJob.ID, "/reconcile", apiKey, ""); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("interrupted provider reconcile status=%d body=%s", code, body)
	}
	if code, body := action(providerJob.ID, "/reconcile", apiKey, ""); code != http.StatusOK || !strings.Contains(body, `"status":"succeeded"`) {
		t.Fatalf("repeated interrupted provider reconcile status=%d body=%s", code, body)
	}
	if code, _ := action(noAttemptJob.ID, "/disposition/failed", "wrong-key", ""); code != http.StatusUnauthorized {
		t.Fatalf("wrong-auth disposition status=%d", code)
	}
	if code, _ := action(noAttemptJob.ID, "/disposition/succeeded", apiKey, ""); code != http.StatusBadRequest {
		t.Fatalf("success disposition status=%d", code)
	}
	if code, _ := action(noAttemptJob.ID, "/disposition/failed", apiKey, `{"evidence":"caller supplied"}`); code != http.StatusBadRequest {
		t.Fatalf("body-bearing no-attempt disposition status=%d", code)
	}
	if code, body := action(noAttemptJob.ID, "/disposition/failed", apiKey, ""); code != http.StatusOK || !strings.Contains(body, `"status":"failed"`) {
		t.Fatalf("no-attempt disposition status=%d body=%s", code, body)
	}
	if _, err := os.Stat(filepath.Join(stateDir, aliasDirectory("trusted"), "results", noAttemptJob.ID, "worktree")); err != nil {
		t.Fatalf("failed disposition removed retained workspace: %v", err)
	}
	providerHistory := getProcessHistory(t, client, baseURL, providerJob.ID, apiKey)
	if len(providerHistory.Events) != 4 || providerHistory.Events[2].Type != "succeeded" || providerHistory.Events[3].Type != "provider_reconciled" || providerHistory.Events[3].Message != "Operator confirmed provider outcome" {
		t.Fatalf("provider recovery history=%+v", providerHistory)
	}
	noAttemptHistory := getProcessHistory(t, client, baseURL, noAttemptJob.ID, apiKey)
	foundDisposition := false
	for _, event := range noAttemptHistory.Events {
		if event.Type == "operator_disposition" && event.Message == "failed" {
			foundDisposition = true
		}
	}
	if !foundDisposition {
		t.Fatalf("no-attempt disposition audit missing: %+v", noAttemptHistory)
	}
	providerRecord, err := os.ReadFile(outcomesPath + ".uncertain.json")
	if err != nil {
		t.Fatal(err)
	}
	lookupCalls, err := os.ReadFile(outcomesPath + ".reconcile-lookups")
	if err != nil || string(lookupCalls) != providerJob.ID+"\n"+providerJob.ID+"\n"+providerJob.ID+"\n" {
		t.Fatalf("read-only provider lookup calls=%q err=%v", lookupCalls, err)
	}
	var provider uncertainProviderRecord
	if json.Unmarshal(providerRecord, &provider) != nil || provider.Creates != 1 || provider.Attempts != 1 {
		t.Fatalf("provider write replayed: %+v", provider)
	}
	harnessBefore, err := os.ReadFile(harnessCount)
	if err != nil || strings.TrimSpace(string(harnessBefore)) == "" {
		t.Fatalf("harness run evidence unavailable before final restart: %q err=%v", harnessBefore, err)
	}
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-childDone; err != nil {
		t.Fatalf("recovery server shutdown: %v", err)
	}
	child = nil
	child, childDone, baseURL = startServer("final-listener")
	finalProvider := getProcessJob(t, client, baseURL, providerJob.ID, apiKey)
	finalNoAttempt := getProcessJob(t, client, baseURL, noAttemptJob.ID, apiKey)
	if finalProvider.Status != restjobs.StatusSucceeded || finalProvider.Provider == nil || finalNoAttempt.Status != restjobs.StatusFailed {
		t.Fatalf("recovery not durable across restart: provider=%+v noAttempt=%+v", finalProvider, finalNoAttempt)
	}
	if got := getProcessHistory(t, client, baseURL, providerJob.ID, apiKey); !reflect.DeepEqual(got, providerHistory) {
		t.Fatalf("provider audit changed after restart: before=%+v after=%+v", providerHistory, got)
	}
	if got := getProcessHistory(t, client, baseURL, noAttemptJob.ID, apiKey); !reflect.DeepEqual(got, noAttemptHistory) {
		t.Fatalf("disposition audit changed after restart: before=%+v after=%+v", noAttemptHistory, got)
	}
	harnessAfter, err := os.ReadFile(harnessCount)
	if err != nil || string(harnessAfter) != string(harnessBefore) {
		t.Fatalf("restart/recovery replayed harness: before=%q after=%q err=%v", harnessBefore, harnessAfter, err)
	}
	providerAfter, err := os.ReadFile(outcomesPath + ".uncertain.json")
	if err != nil || string(providerAfter) != string(providerRecord) {
		t.Fatalf("restart/recovery repeated provider write: before=%q after=%q err=%v", providerRecord, providerAfter, err)
	}
	lookupAfter, err := os.ReadFile(outcomesPath + ".reconcile-lookups")
	if err != nil || string(lookupAfter) != string(lookupCalls) {
		t.Fatalf("restart repeated provider lookup: before=%q after=%q err=%v", lookupCalls, lookupAfter, err)
	}
}

func lockSQLiteWrites(t *testing.T, path string) func() {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+url.PathEscape(filepath.ToSlash(path))+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open SQLite lock connection: %v", err)
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatalf("get SQLite lock connection: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("acquire SQLite immediate write lock: %v", err)
	}
	return func() {
		t.Helper()
		if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
			t.Errorf("release SQLite immediate write lock: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close SQLite lock connection: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close SQLite lock database: %v", err)
		}
	}
}

func initE2ERepository(t *testing.T, root string) {
	t.Helper()
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.name", "E2E"}, {"config", "user.email", "e2e@localhost"}} {
		if output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"add", "README.md"}, {"commit", "-q", "-m", "baseline"}} {
		if output, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
}

func waitForProcessURL(t *testing.T, childDone <-chan error, readyPath, logPath string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(readyPath); err == nil && strings.TrimSpace(string(data)) != "" {
			return "http://" + strings.TrimSpace(string(data))
		}
		select {
		case err := <-childDone:
			logData, _ := os.ReadFile(logPath)
			t.Fatalf("REST server helper exited early: %v; log=%s", err, logData)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	logData, _ := os.ReadFile(logPath)
	t.Fatalf("REST server process did not become ready; childState=%v log=%s", childDone, logData)
	return ""
}

type submittedProcessJob struct {
	ID         string `json:"id"`
	Replayed   bool   `json:"replayed"`
	ErrorCode  string
	StatusCode int
}

func submitProcessJob(client *http.Client, baseURL, apiKey, alias, task, key string) (submittedProcessJob, error) {
	body := strings.NewReader(fmt.Sprintf(`{"repository":%q,"task":%q}`, alias, task))
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", body)
	if err != nil {
		return submittedProcessJob{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := client.Do(req)
	if err != nil {
		return submittedProcessJob{}, err
	}
	defer resp.Body.Close()
	var result struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
		Replayed bool `json:"replayed"`
		Error    struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return submittedProcessJob{}, err
	}
	return submittedProcessJob{ID: result.Job.ID, Replayed: result.Replayed, ErrorCode: result.Error.Code, StatusCode: resp.StatusCode}, nil
}

func waitForProcessFailure(t *testing.T, client *http.Client, baseURL, id, apiKey string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := getProcessJob(t, client, baseURL, id, apiKey)
		if job.Status.Terminal() {
			if job.Status != restjobs.StatusFailed || job.Provider != nil {
				t.Fatalf("failed workflow result=%+v", job)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not fail before timeout", id)
}

func samePublicJobHistory(left, right restjobs.History) bool {
	if left.JobID != right.JobID || left.Truncated != right.Truncated || len(left.Events) != len(right.Events) {
		return false
	}
	for index := range left.Events {
		if left.Events[index].At != right.Events[index].At || left.Events[index].Type != right.Events[index].Type || left.Events[index].Message != right.Events[index].Message {
			return false
		}
	}
	return true
}

func getProcessHistory(t *testing.T, client *http.Client, baseURL, id, apiKey string) restjobs.History {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id+"/history", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("job history status=%d", resp.StatusCode)
	}
	var history restjobs.History
	if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
		t.Fatal(err)
	}
	return history
}

func getProcessJob(t *testing.T, client *http.Client, baseURL, id, apiKey string) restjobs.Snapshot {
	t.Helper()
	job, _ := getProcessJobResponse(t, client, baseURL, id, apiKey)
	return job
}

func getProcessJobResponse(t *testing.T, client *http.Client, baseURL, id, apiKey string) (restjobs.Snapshot, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("job inspect status=%d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var job restjobs.Snapshot
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job, string(body)
}

func waitForProcessStatus(t *testing.T, client *http.Client, baseURL, id, apiKey string, want restjobs.Status) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := getProcessJob(t, client, baseURL, id, apiKey)
		if job.Status.Terminal() {
			if job.Status != want {
				t.Fatalf("job %s terminal status=%s, want %s", id, job.Status, want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach terminal state %s", id, want)
}

func waitForProcessJob(t *testing.T, client *http.Client, baseURL, id, apiKey string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		var job restjobs.Snapshot
		decodeErr := json.NewDecoder(resp.Body).Decode(&job)
		_ = resp.Body.Close()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if job.Status.Terminal() {
			if job.Status != restjobs.StatusSucceeded || job.Provider == nil || job.Provider.URL == "" {
				t.Fatalf("job %s ended without durable provider outcome: %+v", id, job)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish before timeout", id)
}
