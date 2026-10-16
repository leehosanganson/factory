package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/leehosanganson/factory/internal/restjobs"
	"github.com/leehosanganson/factory/internal/restprovider"
	"github.com/leehosanganson/factory/internal/restserver"
	"github.com/leehosanganson/factory/internal/restworker"
)

func TestRunConfiguredPostgresStoreIntegration(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 runtime integration tests")
	}
	parsedURL, err := url.Parse(dsn)
	if err != nil || (parsedURL.Scheme != "postgres" && parsedURL.Scheme != "postgresql") {
		t.Fatal("runtime PostgreSQL integration test requires a PostgreSQL URL DSN")
	}
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test PostgreSQL URL")
	}
	admin := stdlib.OpenDB(*adminConfig)
	defer admin.Close()
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil {
		t.Fatal("PostgreSQL test service is unavailable")
	}
	if version < 180000 || version >= 190000 {
		t.Fatalf("PostgreSQL server_version_num=%d, integration coverage requires PostgreSQL 18", version)
	}

	schema := fmt.Sprintf("factory_runtime_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal("create PostgreSQL runtime test schema")
	}
	defer func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop PostgreSQL runtime test schema: %v", err)
		}
	}()
	query := parsedURL.Query()
	query.Set("search_path", schema)
	parsedURL.RawQuery = query.Encode()
	scopedDSN := parsedURL.String()

	controlPlaneConfig, _ := runtimeFixture(t)
	workersDisabled := false
	controlPlaneConfig.WorkersEnabled = &workersDisabled
	controlPlaneConfig.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendPostgres, DSN: scopedDSN}
	var openedStore restjobs.Store
	listenErr := run(context.Background(), controlPlaneConfig, runtimeOptions{
		OpenStore: func(config restserver.Config) (restjobs.Store, error) {
			store, err := restworker.NewLocalServerJobManager(config)
			openedStore = store
			return store, err
		},
		Listen: func(string, string) (net.Listener, error) {
			return nil, fmt.Errorf("bind private listener detail")
		},
	})
	if listenErr == nil || listenErr.Error() != "listen for REST server" {
		t.Fatalf("workers-disabled PostgreSQL listener error=%v, want sanitized listen error", listenErr)
	}
	postgresStore, ok := openedStore.(interface{ Ping(context.Context) error })
	if !ok {
		t.Fatal("runtime did not open a PostgreSQL store")
	}
	if err := postgresStore.Ping(context.Background()); err == nil {
		t.Fatal("PostgreSQL store remained open after listener failure")
	}

	seedConfig := restjobs.Config{
		QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 10, MaxEventsPerJob: 20, MaxTaskBytes: 4096,
	}
	t.Run("expired attempt with simulated provider delay stays fenced until injected read-only reconciliation result", func(t *testing.T) {
		openStore := func() (*restjobs.PostgresStore, error) {
			dbConfig, err := pgx.ParseConfig(scopedDSN)
			if err != nil {
				return nil, err
			}
			return restjobs.NewPostgresStore(stdlib.OpenDB(*dbConfig), seedConfig)
		}
		workerStore, err := openStore()
		if err != nil {
			t.Fatal("open provider worker store")
		}
		defer workerStore.CloseStore()
		job, _, err := workerStore.Admit("in-flight-provider-write", restjobs.Request{Repository: "trusted", Task: "hold provider write across lease expiry"})
		if err != nil {
			t.Fatal("admit in-flight provider job")
		}
		claim, err := workerStore.ClaimNextAttempt()
		if err != nil || claim.Job.ID != job.ID || claim.Attempt.Generation != 1 {
			t.Fatalf("attempt-aware claim=(%+v,%v), want generation 1 for %q", claim, err, job.ID)
		}
		providerAttempt := restjobs.ProviderAttempt{
			Provider: "github", Repository: "acme/widget", Branch: "factory/job/" + job.ID, Commit: "abcdef0",
		}
		if err := workerStore.RecordProviderAttemptForAttempt(claim.Attempt, providerAttempt); err != nil {
			t.Fatal("persist provider identity before simulated provider request")
		}
		if err := workerStore.MarkProviderAttemptUncertainForAttempt(claim.Attempt); err != nil {
			t.Fatal("mark provider request uncertain before simulated side effect")
		}
		providerAttempt.Uncertain = true

		outcome := restjobs.ProviderOutcome{
			Provider: "github", Repository: providerAttempt.Repository, Number: 42,
			URL: "https://github.com/acme/widget/pull/42", Branch: providerAttempt.Branch, Commit: providerAttempt.Commit, State: "open",
		}
		providerStarted := make(chan struct{})
		releaseProvider := make(chan struct{})
		providerDone := make(chan struct{})
		providerState := make(chan restjobs.ProviderOutcome, 1)
		released := false
		defer func() {
			if !released {
				close(releaseProvider)
				<-providerDone
			}
		}()
		go func() {
			close(providerStarted)
			<-releaseProvider
			// Model a remote provider committing the write after its response
			// was delayed. This is a local fake side effect, not a network call.
			providerState <- outcome
			close(providerDone)
		}()
		lookupProvider := func() (restjobs.ProviderOutcome, bool) {
			select {
			case <-providerDone:
				return <-providerState, true
			default:
				return restjobs.ProviderOutcome{}, false
			}
		}
		<-providerStarted

		if _, err := admin.Exec(`UPDATE `+schema+`.factory_rest_worker_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, job.ID); err != nil {
			t.Fatal("expire provider worker lease")
		}
		recoveryStore, err := openStore()
		if err != nil {
			t.Fatal("reopen store after provider lease expiry")
		}
		defer recoveryStore.CloseStore()
		if !recoveryStore.RecoveryNeeded(job.ID) {
			t.Fatal("expired in-flight job was not classified recovery-needed")
		}

		// This injected lookup result models an early no-match; it is not a live
		// provider call. A negative result must not trigger replay.
		if _, found := lookupProvider(); found {
			t.Fatal("injected provider lookup unexpectedly matched before the simulated delay ended")
		}
		beforeRelease, err := recoveryStore.Get(job.ID)
		if err != nil || beforeRelease.Status != restjobs.StatusRunning || beforeRelease.Provider != nil {
			t.Fatalf("no-match lookup changed recovered job=(%+v,%v), want running without outcome", beforeRelease, err)
		}
		persistedAttempt, err := recoveryStore.ProviderAttempt(job.ID)
		if err != nil || persistedAttempt != providerAttempt {
			t.Fatalf("persisted provider attempt=(%+v,%v), want identity retained before side effect", persistedAttempt, err)
		}
		if !persistedAttempt.Uncertain {
			t.Fatal("provider attempt was not durably marked uncertain before lease expiry")
		}
		if _, err := recoveryStore.ClaimNextAttempt(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
			t.Fatalf("claim after no-match lookup=%v, want no queued replay", err)
		}
		var generation int64
		if err := admin.QueryRow(`SELECT generation FROM `+schema+`.factory_rest_worker_ownership WHERE job_id=$1`, job.ID).Scan(&generation); err != nil || generation != claim.Attempt.Generation {
			t.Fatalf("generation after blocked write lookup=(%d,%v), want unchanged generation %d", generation, err, claim.Attempt.Generation)
		}

		close(releaseProvider)
		released = true
		<-providerDone
		confirmed, found := lookupProvider()
		if !found || confirmed != outcome {
			t.Fatalf("injected provider lookup after simulated delay=(%+v,%t), want matching test outcome", confirmed, found)
		}
		for name, mutate := range map[string]func() error{
			"heartbeat":         func() error { return workerStore.HeartbeatAttempt(claim.Attempt) },
			"provider outcome":  func() error { return workerStore.RecordProviderOutcomeForAttempt(claim.Attempt, outcome) },
			"successful finish": func() error { return workerStore.FinishAttempt(claim.Attempt, restjobs.StatusSucceeded) },
			"mark uncertain":    func() error { return workerStore.MarkProviderAttemptUncertainForAttempt(claim.Attempt) },
		} {
			if err := mutate(); !errors.Is(err, restjobs.ErrStaleAttempt) {
				t.Errorf("late old-attempt %s mutation=%v, want ErrStaleAttempt", name, err)
			}
		}
		stillRunning, err := recoveryStore.Get(job.ID)
		if err != nil || stillRunning.Status != restjobs.StatusRunning || stillRunning.Provider != nil {
			t.Fatalf("old attempt terminalized or recorded an outcome=(%+v,%v), want running without outcome", stillRunning, err)
		}
		if _, err := recoveryStore.ClaimNextAttempt(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
			t.Fatalf("claim after delayed provider completion=%v, want no automatic replay", err)
		}
		if err := admin.QueryRow(`SELECT generation FROM `+schema+`.factory_rest_worker_ownership WHERE job_id=$1`, job.ID).Scan(&generation); err != nil || generation != claim.Attempt.Generation {
			t.Fatalf("generation after delayed provider completion=(%d,%v), want unchanged generation %d", generation, err, claim.Attempt.Generation)
		}
		if err := recoveryStore.ReconcileProviderOutcome(job.ID, confirmed); err != nil {
			t.Fatalf("reconcile injected matching outcome after simulated delay: %v", err)
		}
		if err := recoveryStore.ReconcileProviderOutcome(job.ID, confirmed); err != nil {
			t.Fatalf("repeat matching durable reconciliation: %v", err)
		}
		reconciled, err := recoveryStore.Get(job.ID)
		if err != nil || reconciled.Status != restjobs.StatusSucceeded || reconciled.Provider == nil || *reconciled.Provider != outcome {
			t.Fatalf("reconciled job=(%+v,%v), want matching succeeded outcome", reconciled, err)
		}
		history, err := recoveryStore.History(job.ID)
		if err != nil {
			t.Fatal("read reconciled history")
		}
		reconciliations := 0
		for _, event := range history.Events {
			if event.Type == "provider_reconciled" {
				reconciliations++
			}
		}
		if reconciliations != 1 {
			t.Fatalf("provider reconciliation audit events=%d, want exactly one", reconciliations)
		}
	})
	t.Run("runtime FactoryExecutor fences a delayed fake provider write across lease expiry", func(t *testing.T) {
		config, key := runtimeFixture(t)
		config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendPostgres, DSN: scopedDSN}
		config.Limits.Workers = 1
		tokenPath := filepath.Join(t.TempDir(), "provider-token")
		if err := os.WriteFile(tokenPath, []byte("fake-provider-token"), 0o600); err != nil {
			t.Fatal(err)
		}
		config.Provider = restserver.ProviderConfig{
			Backend: "github", TokenFile: tokenPath, BaseBranch: "main",
			Repositories: map[string]string{"trusted": "acme/widget"},
		}
		harness := filepath.Join(t.TempDir(), "harness.sh")
		harnessScript := "#!/bin/sh\nprintf 'verified change\\n' > delayed-provider-result.txt\nprintf 'PASS\\n'\n"
		if err := os.WriteFile(harness, []byte(harnessScript), 0o700); err != nil {
			t.Fatal(err)
		}
		piBin := filepath.Join(t.TempDir(), "bin")
		if err := os.Mkdir(piBin, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(piBin, "pi"), []byte("#!/bin/sh\nprintf 'PASS\\n'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", piBin+string(os.PathListSeparator)+os.Getenv("PATH"))
		config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{system_prompt}", "{task}"}}
		resultsBase := testResultsBase(t)
		t.Setenv("XDG_STATE_HOME", filepath.Join(filepath.Dir(filepath.Dir(resultsBase)), "cli-state"))

		publisher := newLeaseExpiryPublisher()
		listenerReady := make(chan net.Listener, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- run(ctx, config, runtimeOptions{
				Publisher: publisher, ResultsBase: resultsBase,
				Listen: func(network, _ string) (net.Listener, error) {
					listener, err := net.Listen(network, "127.0.0.1:0")
					if err == nil {
						listenerReady <- listener
					}
					return listener, err
				},
			})
		}()
		var listener net.Listener
		select {
		case listener = <-listenerReady:
		case err := <-done:
			cancel()
			t.Fatalf("PostgreSQL FactoryExecutor runtime did not become ready: %v", err)
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatal("PostgreSQL FactoryExecutor runtime did not open listener")
		}
		defer func() {
			publisher.releaseWrite()
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("PostgreSQL FactoryExecutor runtime shutdown: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("PostgreSQL FactoryExecutor runtime did not shut down")
			}
		}()
		baseURL := "http://" + listener.Addr().String()
		waitForStatus(t, baseURL+"/readyz", http.StatusOK)
		request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(`{"repository":"trusted","task":"publish through a delayed fake adapter"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "postgres-runtime-delayed-publisher")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal("submit delayed-provider PostgreSQL job")
		}
		var admission struct {
			Job restjobs.Snapshot `json:"job"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&admission)
		response.Body.Close()
		if response.StatusCode != http.StatusAccepted || decodeErr != nil || admission.Job.ID == "" {
			t.Fatalf("delayed-provider admission status=%d decode=%v job=%+v", response.StatusCode, decodeErr, admission.Job)
		}
		var publishRequest restprovider.PublishRequest
		select {
		case publishRequest = <-publisher.started:
		case <-time.After(20 * time.Second):
			snapshot := getRuntimeIntegrationJob(t, baseURL, key, admission.Job.ID)
			t.Fatalf("FactoryExecutor did not reach fake provider Publish; status=%s progress=%+v", snapshot.Status, snapshot.Progress)
		}
		if publishRequest.JobID != admission.Job.ID || publishRequest.Repository != "acme/widget" || publishRequest.Branch != restprovider.JobBranch(admission.Job.ID) {
			t.Fatalf("fake provider received unexpected publication identity: %+v", publishRequest)
		}

		if _, err := admin.Exec(`UPDATE `+schema+`.factory_rest_worker_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, admission.Job.ID); err != nil {
			t.Fatal("expire runtime provider worker lease")
		}
		select {
		case <-publisher.canceled:
		case <-time.After(12 * time.Second):
			t.Fatal("expired PostgreSQL lease heartbeat did not cancel executor context")
		}
		if got := getRuntimeIntegrationJob(t, baseURL, key, admission.Job.ID); got.Status != restjobs.StatusRunning {
			t.Fatalf("job after heartbeat loss=%s, want recovery-needed running", got.Status)
		}

		// The fake adapter's first reconciliation is read-only and sees no write
		// while Publish remains blocked; this is not a live-provider guarantee.
		if status := reconcileRuntimeIntegrationJob(t, baseURL, key, admission.Job.ID); status != http.StatusConflict {
			t.Fatalf("pre-write fake reconciliation status=%d, want conflict for no match", status)
		}
		if publisher.publishes.Load() != 1 || publisher.writes.Load() != 0 || publisher.reconciliations.Load() != 1 {
			t.Fatalf("before delayed write: Publish=%d writes=%d Reconcile=%d, want 1/0/1", publisher.publishes.Load(), publisher.writes.Load(), publisher.reconciliations.Load())
		}

		jobStoreConfig := restjobs.Config{
			QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
			MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
			MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
		}
		dbConfig, err := pgx.ParseConfig(scopedDSN)
		if err != nil {
			t.Fatal("parse scoped DSN for lease recovery assertions")
		}
		recoveryStore, err := restjobs.NewPostgresStore(stdlib.OpenDB(*dbConfig), jobStoreConfig)
		if err != nil {
			t.Fatal("open recovery assertion store")
		}
		defer recoveryStore.CloseStore()
		if !recoveryStore.RecoveryNeeded(admission.Job.ID) {
			t.Fatal("expired runtime attempt was not classified recovery-needed")
		}
		if _, err := recoveryStore.ClaimNextAttempt(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
			t.Fatalf("claim while fake Publish blocked=%v, want no queued replay", err)
		}
		attempt, err := recoveryStore.ProviderAttempt(admission.Job.ID)
		if err != nil || attempt.Repository != publishRequest.Repository || attempt.Branch != publishRequest.Branch || attempt.Commit != publishRequest.Commit {
			t.Fatalf("persisted provider attempt=(%+v,%v), want original publication identity", attempt, err)
		}
		var outcomeCount int
		if err := admin.QueryRow(`SELECT COUNT(*) FROM `+schema+`.factory_job_provider_outcomes WHERE job_id=$1`, admission.Job.ID).Scan(&outcomeCount); err != nil || outcomeCount != 0 {
			t.Fatalf("provider outcome before delayed write count=%d err=%v, want none", outcomeCount, err)
		}

		publisher.releaseWrite()
		select {
		case <-publisher.returned:
		case <-time.After(10 * time.Second):
			t.Fatal("fake delayed provider write did not return ErrUncertain")
		}
		if publisher.publishes.Load() != 1 || publisher.writes.Load() != 1 {
			t.Fatalf("after delayed write: Publish=%d writes=%d, want exactly one each", publisher.publishes.Load(), publisher.writes.Load())
		}
		// Let the old coordinator call FinishAttempt after the executor returns.
		time.Sleep(150 * time.Millisecond)
		stillRunning, err := recoveryStore.Get(admission.Job.ID)
		if err != nil || stillRunning.Status != restjobs.StatusRunning || stillRunning.Provider != nil || !recoveryStore.RecoveryNeeded(admission.Job.ID) {
			t.Fatalf("old attempt changed recovered job=(%+v,%v), want recovery-needed running without outcome", stillRunning, err)
		}
		if _, err := recoveryStore.ClaimNextAttempt(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
			t.Fatalf("claim after delayed write=%v, want no replay", err)
		}
		if status := reconcileRuntimeIntegrationJob(t, baseURL, key, admission.Job.ID); status != http.StatusOK {
			t.Fatalf("post-write read-only reconciliation status=%d, want success", status)
		}
		reconciled := getRuntimeIntegrationJob(t, baseURL, key, admission.Job.ID)
		if reconciled.Status != restjobs.StatusSucceeded || reconciled.Provider == nil || reconciled.Provider.Repository != "acme/widget" || reconciled.Provider.Branch != publishRequest.Branch || reconciled.Provider.Commit != publishRequest.Commit {
			t.Fatalf("reconciled runtime job=%+v, want confirmed provider success", reconciled)
		}
		if publisher.publishes.Load() != 1 || publisher.writes.Load() != 1 || publisher.reconciliations.Load() != 2 {
			t.Fatalf("final fake provider calls: Publish=%d writes=%d Reconcile=%d, want 1/1/2", publisher.publishes.Load(), publisher.writes.Load(), publisher.reconciliations.Load())
		}
		if err := admin.QueryRow(`SELECT COUNT(*) FROM `+schema+`.factory_job_provider_outcomes WHERE job_id=$1`, admission.Job.ID).Scan(&outcomeCount); err != nil || outcomeCount != 1 {
			t.Fatalf("provider outcome after reconciliation count=%d err=%v, want one durable outcome", outcomeCount, err)
		}
	})
	seedDBConfig, err := pgx.ParseConfig(scopedDSN)
	if err != nil {
		t.Fatal("parse scoped test DSN")
	}
	seedDB := stdlib.OpenDB(*seedDBConfig)
	seedStore, err := restjobs.NewPostgresStore(seedDB, seedConfig)
	if err != nil {
		t.Fatal("initialize PostgreSQL recovery fixture")
	}
	interrupted, _, err := seedStore.Admit("runtime-interrupted", restjobs.Request{Repository: "trusted", Task: "operator disposition test"})
	if err != nil {
		t.Fatal("admit PostgreSQL recovery fixture")
	}
	if _, err := seedStore.ClaimNextAttempt(); err != nil {
		t.Fatal("claim PostgreSQL recovery fixture")
	}
	if err := seedStore.CloseStore(); err != nil {
		t.Fatal("close PostgreSQL recovery fixture")
	}
	if _, err := admin.Exec(`UPDATE `+schema+`.factory_rest_worker_ownership SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, interrupted.ID); err != nil {
		t.Fatal("expire PostgreSQL recovery fixture lease")
	}

	config, key := runtimeFixture(t)
	config.Persistence = restserver.PersistenceConfig{Backend: restserver.PersistenceBackendPostgres, DSN: scopedDSN}
	resultsBase := testResultsBase(t)
	listenerReady := make(chan net.Listener, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config, runtimeOptions{
			ResultsBase: resultsBase,
			Executor:    runtimeExecutor(func(context.Context, restjobs.Snapshot) error { return nil }),
			Listen: func(network, _ string) (net.Listener, error) {
				listener, err := net.Listen(network, "127.0.0.1:0")
				if err == nil {
					listenerReady <- listener
				}
				return listener, err
			},
		})
	}()
	var listener net.Listener
	select {
	case listener = <-listenerReady:
	case err := <-done:
		cancel()
		t.Fatalf("PostgreSQL runtime did not become ready: %v", err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("PostgreSQL runtime did not open listener")
	}
	baseURL := "http://" + listener.Addr().String()
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs", strings.NewReader(`{"repository":"trusted","task":"persist through PostgreSQL runtime"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "postgres-runtime-integration")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("submit PostgreSQL-backed job")
	}
	var admission struct {
		Job restjobs.Snapshot `json:"job"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&admission)
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || decodeErr != nil || admission.Job.ID == "" {
		t.Fatalf("PostgreSQL-backed admission status=%d decode=%v", response.StatusCode, decodeErr)
	}
	jobURL := baseURL + "/v1/jobs/" + admission.Job.ID
	deadline := time.Now().Add(5 * time.Second)
	succeeded := false
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, jobURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			var snapshot restjobs.Snapshot
			decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
			response.Body.Close()
			if decodeErr == nil && snapshot.Status == restjobs.StatusSucceeded {
				succeeded = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !succeeded {
		t.Fatal("PostgreSQL-backed job did not complete successfully")
	}
	recoveryRequest, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/"+interrupted.ID+"/disposition/canceled", nil)
	if err != nil {
		t.Fatal(err)
	}
	recoveryRequest.Header.Set("Authorization", "Bearer "+key)
	recoveryResponse, err := http.DefaultClient.Do(recoveryRequest)
	if err != nil {
		t.Fatal("resolve PostgreSQL interrupted job")
	}
	var recovered restjobs.Snapshot
	recoveryDecodeErr := json.NewDecoder(recoveryResponse.Body).Decode(&recovered)
	recoveryResponse.Body.Close()
	if recoveryResponse.StatusCode != http.StatusOK || recoveryDecodeErr != nil || recovered.Status != restjobs.StatusCanceled {
		t.Fatalf("PostgreSQL recovery response status=%d snapshot=%+v decode=%v", recoveryResponse.StatusCode, recovered, recoveryDecodeErr)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PostgreSQL runtime shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PostgreSQL runtime did not close its store on shutdown")
	}

	sharedConfig := restjobs.Config{
		QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
		MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
		MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
	}
	firstInstance, err := restjobs.OpenPostgresServerStore(scopedDSN, sharedConfig)
	if err != nil {
		t.Fatal("open first shared PostgreSQL server store")
	}
	queued, _, err := firstInstance.Admit("shared-instance-shutdown", restjobs.Request{Repository: "trusted", Task: "remain queued for a peer"})
	if err != nil {
		t.Fatal("admit peer-shared queued job")
	}
	if err := firstInstance.CloseStore(); err != nil {
		t.Fatal("close first shared PostgreSQL server store")
	}
	secondInstance, err := restjobs.OpenPostgresServerStore(scopedDSN, sharedConfig)
	if err != nil {
		t.Fatal("open second shared PostgreSQL server store")
	}
	queuedSnapshot, err := secondInstance.Get(queued.ID)
	if err != nil || queuedSnapshot.Status != restjobs.StatusQueued {
		t.Fatalf("peer-shared job after server close=(%+v,%v), want queued", queuedSnapshot, err)
	}
	claim, err := secondInstance.ClaimNextAttempt()
	if err != nil || claim.Job.ID != queued.ID {
		t.Fatalf("peer claim=(%+v,%v), want queued job %q", claim, err, queued.ID)
	}
	if err := secondInstance.CloseStore(); err != nil {
		t.Fatal("close second shared PostgreSQL server store")
	}

	scopedConfig, err := pgx.ParseConfig(scopedDSN)
	if err != nil {
		t.Fatal("parse scoped test DSN")
	}
	db := stdlib.OpenDB(*scopedConfig)
	defer db.Close()
	store, err := restjobs.NewPostgresStore(db, restjobs.Config{
		QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers,
		MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob,
		MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes,
	})
	if err != nil {
		t.Fatal("reopen PostgreSQL store after runtime shutdown")
	}
	defer store.CloseStore()
	persisted, err := store.Get(admission.Job.ID)
	if err != nil || persisted.Status != restjobs.StatusSucceeded {
		t.Fatalf("persisted runtime job=(%+v,%v), want successful durable record", persisted, err)
	}
}

type leaseExpiryPublisher struct {
	started         chan restprovider.PublishRequest
	canceled        chan struct{}
	returned        chan struct{}
	release         chan struct{}
	releaseOnce     sync.Once
	publishes       atomic.Int32
	writes          atomic.Int32
	reconciliations atomic.Int32
	mu              sync.Mutex
	outcome         restprovider.Outcome
	written         bool
}

func newLeaseExpiryPublisher() *leaseExpiryPublisher {
	return &leaseExpiryPublisher{
		started: make(chan restprovider.PublishRequest, 1), canceled: make(chan struct{}),
		returned: make(chan struct{}), release: make(chan struct{}),
	}
}

func (p *leaseExpiryPublisher) Ping(context.Context) error { return nil }

// Publish models an adapter that has accepted a remote write but whose response
// is delayed. It deliberately ignores ctx after observing cancellation; this
// fake is for exercising Factory's fencing and is not a live-provider guarantee.
func (p *leaseExpiryPublisher) Publish(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	p.publishes.Add(1)
	p.started <- request
	<-ctx.Done()
	close(p.canceled)
	<-p.release
	outcome := restprovider.Outcome{
		Provider: "github", Repository: request.Repository, Number: 73,
		URL: "https://github.com/acme/widget/pull/73", Branch: request.Branch, Commit: request.Commit, State: "open",
	}
	p.mu.Lock()
	p.outcome = outcome
	p.written = true
	p.mu.Unlock()
	p.writes.Add(1)
	close(p.returned)
	return restprovider.Outcome{}, fmt.Errorf("%w: fake accepted write response was delayed", restprovider.ErrUncertain)
}

// Reconcile is strictly read-only: it inspects the fake's in-memory delayed
// write result and never calls Publish or creates another provider-side effect.
func (p *leaseExpiryPublisher) Reconcile(ctx context.Context, request restprovider.PublishRequest) (restprovider.Outcome, error) {
	p.reconciliations.Add(1)
	if err := ctx.Err(); err != nil {
		return restprovider.Outcome{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.written || p.outcome.Repository != request.Repository || p.outcome.Branch != request.Branch || p.outcome.Commit != request.Commit {
		return restprovider.Outcome{}, errors.New("fake provider has no matching outcome")
	}
	return p.outcome, nil
}

func (p *leaseExpiryPublisher) releaseWrite() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func getRuntimeIntegrationJob(t *testing.T, baseURL, key, id string) restjobs.Snapshot {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, baseURL+"/v1/jobs/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("read PostgreSQL runtime job")
	}
	defer response.Body.Close()
	var snapshot restjobs.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); response.StatusCode != http.StatusOK || err != nil {
		t.Fatalf("read PostgreSQL runtime job status=%d decode=%v", response.StatusCode, err)
	}
	return snapshot
}

func reconcileRuntimeIntegrationJob(t *testing.T, baseURL, key, id string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/"+id+"/reconcile", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal("reconcile PostgreSQL runtime job")
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestPostgresControlPlaneAndSeparateWorkerProcess(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("PostgreSQL test DSN is required")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL 18 process integration")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	adminConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*adminConfig)
	defer admin.Close()
	var version int
	if err := admin.QueryRow(`SHOW server_version_num`).Scan(&version); err != nil || version < 180000 || version >= 190000 {
		t.Fatalf("requires PostgreSQL 18: version=%d err=%v", version, err)
	}
	schema := fmt.Sprintf("factory_worker_process_%d", time.Now().UnixNano())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	q := parsed.Query()
	q.Set("search_path", schema)
	parsed.RawQuery = q.Encode()
	scopedDSN := parsed.String()
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.email", "factory@test.invalid"}, {"config", "user.name", "Factory"}, {"-c", "core.hooksPath=/dev/null", "add", "-A"}, {"-c", "user.name=Factory", "-c", "user.email=factory@test.invalid", "commit", "--allow-empty", "-qm", "init"}} {
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git setup: %v %s", err, out)
		}
	}
	harness := filepath.Join(root, "harness.sh")
	if err := os.WriteFile(harness, []byte("#!/bin/sh\nprintf 'safe result\\n' > result.txt\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := restserver.DefaultConfig()
	config.Repositories = map[string]string{"trusted": repo}
	config.Harness = restserver.HarnessConfig{Executable: harness, Args: []string{"{system_prompt}", "{task}"}}
	config.APIKeyFile = filepath.Join(root, "api-key")
	if err := os.WriteFile(config.APIKeyFile, []byte("worker-process-api-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	config.Principals = []restserver.PrincipalConfig{{ID: "operator", Role: "operator", TokenFile: filepath.Join(root, "operator-token.json"), Enabled: true}}
	digest := sha256.Sum256([]byte("operator-token"))
	verifier, _ := json.Marshal(map[string]any{"version": 1, "principals": []any{map[string]string{"id": "operator", "token_sha256": fmt.Sprintf("%x", digest)}}})
	if err := os.WriteFile(config.Principals[0].TokenFile, verifier, 0o600); err != nil {
		t.Fatal(err)
	}
	config.Persistence = restserver.PersistenceConfig{Backend: "postgres", DSN: scopedDSN}
	disabled := false
	config.WorkersEnabled = &disabled
	config.WorkerID = "control-plane"
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config.ListenAddress = probe.Addr().String()
	_ = probe.Close()
	serverPath := filepath.Join(root, "server.json")
	workerPath := filepath.Join(root, "worker.json")
	serverBytes, _ := json.Marshal(config)
	if err := os.WriteFile(serverPath, serverBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	workerConfig := config
	workerConfig.APIKeyFile = filepath.Join(root, "must-not-load-api-key")
	workerConfig.Principals[0].TokenFile = filepath.Join(root, "must-not-load-principal-verifier")
	workerConfig.WorkerID = "process-worker"
	workerBytes, _ := json.Marshal(workerConfig)
	if err := os.WriteFile(workerPath, workerBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "factory")
	build := exec.Command("go", "build", "-o", binary, "./cmd/factory")
	build.Dir = filepath.Join("..", "..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build factory: %v: %s", err, output)
	}
	serverLog, err := os.Create(filepath.Join(root, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	server := exec.Command(binary, "server", "--config", serverPath)
	server.Stdout = serverLog
	server.Stderr = serverLog
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if server.Process != nil {
			_ = server.Process.Signal(os.Interrupt)
			_ = server.Wait()
		}
		_ = serverLog.Close()
	}()
	baseURL := "http://" + config.ListenAddress
	client := &http.Client{Timeout: 3 * time.Second}
	waitForStatus(t, baseURL+"/readyz", http.StatusOK)
	admitted, err := submitProcessJob(client, baseURL, "worker-process-api-key", "trusted", "run bounded safe workflow", "separate-worker-process")
	if err != nil {
		t.Fatal(err)
	}
	worker := exec.Command(binary, "worker", "--config", workerPath)
	worker.Env = append(os.Environ(), "XDG_STATE_HOME="+filepath.Join(root, "state"))
	workerLog, err := os.Create(filepath.Join(root, "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	worker.Stdout = workerLog
	worker.Stderr = workerLog
	if err := worker.Start(); err != nil {
		_ = workerLog.Close()
		t.Fatal(err)
	}
	workerStopped := false
	defer func() {
		if !workerStopped && worker.Process != nil {
			_ = worker.Process.Kill()
			_ = worker.Wait()
		}
		_ = workerLog.Close()
	}()
	jobURL := baseURL + "/v1/jobs/" + admitted.ID
	deadline := time.Now().Add(20 * time.Second)
	var completed restjobs.Snapshot
	for time.Now().Before(deadline) {
		completed = getProcessJob(t, client, baseURL, admitted.ID, "worker-process-api-key")
		if completed.Status.Terminal() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if completed.Status != restjobs.StatusSucceeded || completed.Worker == nil || completed.Worker.WorkerID != "process-worker" || completed.Progress == nil || completed.Progress.Stage != "document" || completed.Progress.State != "completed" {
		t.Fatalf("API did not expose successful worker attribution/progress: %+v", completed)
	}
	request, err := http.NewRequest(http.MethodGet, jobURL+"/history", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer worker-process-api-key")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var history restjobs.History
	decodeErr := json.NewDecoder(response.Body).Decode(&history)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || decodeErr != nil || history.Worker == nil || history.Progress == nil {
		t.Fatalf("API history summary status=%d history=%+v err=%v", response.StatusCode, history, decodeErr)
	}
	store, err := restjobs.OpenPostgresStore(scopedDSN, restjobs.Config{QueueCapacity: config.Limits.QueueCapacity, MaxConcurrentJobs: config.Limits.Workers, MaxRecords: config.Limits.MaxRecords, MaxEventsPerJob: config.Limits.MaxEventsPerJob, MaxTaskBytes: config.Limits.TaskBytes, RegistryBytes: config.Limits.RegistryBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer store.CloseStore()
	stored, err := store.Get(admitted.ID)
	if err != nil || stored.Status != restjobs.StatusSucceeded || stored.Worker == nil || stored.Progress == nil {
		t.Fatalf("shared store state=%+v err=%v", stored, err)
	}
	_ = worker.Process.Signal(os.Interrupt)
	if err := worker.Wait(); err != nil {
		t.Fatalf("worker shutdown: %v", err)
	}
	workerStopped = true
}
