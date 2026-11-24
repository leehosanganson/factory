package restapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func newPostgresRemoteWorkerHTTPFixture(t *testing.T) (*restjobs.PostgresStore, *Handler) {
	t.Helper()
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL HTTP Worker integration tests")
	}

	admin, err := openAPIPostgresDB(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	var schemaBytes [12]byte
	if _, err := rand.Read(schemaBytes[:]); err != nil {
		t.Fatal(err)
	}
	schema := "factory_api_test_" + hex.EncodeToString(schemaBytes[:])
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	store, err := restjobs.NewPostgresStore(db, restjobs.Config{
		QueueCapacity: 4, MaxConcurrentJobs: 2, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: 128,
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.CloseStore() })

	principalHandler := newPrincipalTestHandler(t, store,
		restserver.PrincipalConfig{ID: "alice", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "bob", Role: "submitter", Enabled: true},
		restserver.PrincipalConfig{ID: "admin", Role: "operator", Enabled: true},
	)
	handler, err := New(store, principalHandler.key, Config{
		MaxRequestBodyBytes: 2048,
		MaxTaskBytes:        128,
		RepositoryAliases:   map[string]struct{}{"widget": {}},
		Ready:               func() bool { return true },
		ReadyError:          func(context.Context) error { return nil },
		RemoteWorkerAPI:     &RemoteWorkerAPIConfig{Secret: []byte(remoteTestSecret), Store: store},
	})
	if err != nil {
		t.Fatal(err)
	}

	return store, handler
}

func TestPostgresRemoteWorkerHTTPFinishingFinalThreadJobNotifiesOwnerOnce(t *testing.T) {
	store, handler := newPostgresRemoteWorkerHTTPFixture(t)

	thread, _, err := store.AdmitThreadAs("remote-http-thread-notification", "alice", restjobs.Request{Repository: "widget", Task: "private first task"})
	if err != nil {
		t.Fatal(err)
	}
	_, finalJob, _, err := store.AdmitThreadJobAs(thread.Thread.ID, "remote-http-thread-notification-final", "alice", restjobs.Request{Repository: "widget", Task: "private final task"})
	if err != nil {
		t.Fatal(err)
	}

	firstClaim := claimPostgresHTTPWorkerJob(t, handler, "claim-first")
	if firstClaim.Job.ID != thread.Jobs[0].ID {
		t.Fatalf("first HTTP claim returned Job %s, want %s", firstClaim.Job.ID, thread.Jobs[0].ID)
	}
	firstFinish := finishPostgresHTTPWorkerJob(t, handler, firstClaim)
	if firstFinish.Code != http.StatusOK {
		t.Fatalf("finish first Thread Job status=%d body=%s", firstFinish.Code, firstFinish.Body.String())
	}
	assertPostgresHTTPInbox(t, handler, "alice", 0)

	finalClaim := claimPostgresHTTPWorkerJob(t, handler, "claim-final")
	if finalClaim.Job.ID != finalJob.ID {
		t.Fatalf("final HTTP claim returned Job %s, want %s", finalClaim.Job.ID, finalJob.ID)
	}
	finalFinishBody := postgresHTTPFinishBody(finalClaim)
	finishResponse := remoteRequest(handler, "/v1/worker/attempts/"+finalClaim.Job.ID+"/finish", finalFinishBody, remoteTestSecret)
	if finishResponse.Code != http.StatusOK {
		t.Fatalf("finish final Thread Job status=%d body=%s", finishResponse.Code, finishResponse.Body.String())
	}
	retry := remoteRequest(handler, "/v1/worker/attempts/"+finalClaim.Job.ID+"/finish", finalFinishBody, remoteTestSecret)
	if retry.Code != http.StatusOK {
		t.Fatalf("exact final result retry status=%d body=%s", retry.Code, retry.Body.String())
	}

	ownerInbox := assertPostgresHTTPInbox(t, handler, "alice", 1)
	if !strings.Contains(ownerInbox.Body.String(), thread.Thread.ID) || strings.Contains(ownerInbox.Body.String(), "private first task") || strings.Contains(ownerInbox.Body.String(), "private final task") {
		t.Fatalf("owner notification response=%s, want safe notification for Thread %s", ownerInbox.Body.String(), thread.Thread.ID)
	}
	threadResponse := principalRequest(handler, http.MethodGet, "/v1/threads/"+thread.Thread.ID, "", namedPrincipalToken("alice"))
	var finished restjobs.ThreadSnapshot
	if threadResponse.Code != http.StatusOK || json.Unmarshal(threadResponse.Body.Bytes(), &finished) != nil || finished.Thread.Status != restjobs.ThreadStatusDone {
		t.Fatalf("owner Thread response status=%d body=%s, want Done", threadResponse.Code, threadResponse.Body.String())
	}
	for _, principal := range []string{"bob", "admin"} {
		assertPostgresHTTPInbox(t, handler, principal, 0)
	}

	create := httptest.NewRequest(http.MethodPost, "/v1/threads", strings.NewReader(`{"repository":"widget","task":"board task"}`))
	create.Header.Set("Content-Type", "application/json")
	create.Header.Set("Idempotency-Key", "board-thread")
	created := httptest.NewRecorder()
	handler.ServeAsDashboardSubmitter(created, create, restserver.Principal{ID: "alice", Role: "submitter"})
	var board dashboardThreadCreateResponse
	if created.Code != http.StatusAccepted || json.Unmarshal(created.Body.Bytes(), &board) != nil || board.Job.ID == "" {
		t.Fatalf("board Thread create: %d %s", created.Code, created.Body.String())
	}
	_, followUp, _, err := store.AdmitThreadJobAs(board.Thread.ID, "board-follow-up", "alice", restjobs.Request{Repository: "widget", Task: "board follow-up"})
	if err != nil {
		t.Fatal(err)
	}
	legacy, _, err := store.AdmitThreadAs("legacy-after-board", "alice", restjobs.Request{Repository: "widget", Task: "legacy Thread job"})
	if err != nil {
		t.Fatal(err)
	}
	claimBoard := func(id, capability string) *httptest.ResponseRecorder {
		t.Helper()
		body := fmt.Sprintf(`{"claim_id":%q,"worker_id":"http-worker","boot_id":"http-boot","repositories":["widget"]%s}`, id, capability)
		return remoteRequest(handler, "/v1/worker/claims", body, remoteTestSecret)
	}
	old := claimBoard("old", "")
	var oldClaim postgresHTTPWorkerClaim
	if old.Code != http.StatusOK || json.Unmarshal(old.Body.Bytes(), &oldClaim) != nil || oldClaim.Job.ID != legacy.Jobs[0].ID || strings.Contains(old.Body.String(), `"thread_`) {
		t.Fatalf("legacy HTTP claim: %d %s", old.Code, old.Body.String())
	}
	if response := finishPostgresHTTPWorkerJob(t, handler, oldClaim); response.Code != http.StatusOK {
		t.Fatalf("legacy finish: %d %s", response.Code, response.Body.String())
	}
	if claim, err := store.ClaimNextAttempt(); !errors.Is(err, restjobs.ErrNoQueuedJobs) {
		t.Fatalf("direct claim picked %q, err=%v; want no Jobs", claim.Job.ID, err)
	}
	const pi = `,"thread_sessions":true`
	checkBoardClaim := func(id, jobID string, position int) postgresHTTPWorkerClaim {
		t.Helper()
		response := claimBoard(id, pi)
		var claim postgresHTTPWorkerClaim
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &claim) != nil || claim.Job.ID != jobID || claim.Job.ThreadID != board.Thread.ID || claim.Job.ThreadPosition != position {
			t.Fatalf("Pi claim: %d %s, want Job %s at position %d", response.Code, response.Body.String(), jobID, position)
		}
		return claim
	}
	first := checkBoardClaim("pi-first", board.Job.ID, 1)
	if retry := checkBoardClaim("pi-first", board.Job.ID, 1); retry != first {
		t.Fatal("Pi claim retry changed the attempt")
	}
	if retry := claimBoard("pi-first", `,"thread_sessions":false`); retry.Code != http.StatusConflict {
		t.Fatalf("claim capability change: %d %s", retry.Code, retry.Body.String())
	}
	if next := claimBoard("pi-next", pi); next.Code != http.StatusNoContent {
		t.Fatalf("claim before predecessor finished: %d %s", next.Code, next.Body.String())
	}
	if response := finishPostgresHTTPWorkerJob(t, handler, first); response.Code != http.StatusOK {
		t.Fatalf("board finish: %d %s", response.Code, response.Body.String())
	}
	checkBoardClaim("pi-next", followUp.ID, 2)

}

func TestPostgresRemoteWorkerHTTPClaimCancelOrdering(t *testing.T) {
	for _, claimFirst := range []bool{false, true} {
		name := "cancel-before-claim"
		if claimFirst {
			name = "claim-before-cancel"
		}
		t.Run(name, func(t *testing.T) {
			_, handler := newPostgresRemoteWorkerHTTPFixture(t)
			created := principalRequest(handler, http.MethodPost, "/v1/jobs", `{"repository":"widget","task":"claim and cancel"}`, namedPrincipalToken("alice"))
			var admission struct{ Job restjobs.Snapshot }
			if created.Code != http.StatusAccepted || json.Unmarshal(created.Body.Bytes(), &admission) != nil || admission.Job.ID == "" || admission.Job.Status != restjobs.StatusQueued {
				t.Fatalf("admit Job: %d %s, want queued Job", created.Code, created.Body.String())
			}
			jobPath := "/v1/jobs/" + admission.Job.ID
			assertJob := func(operation string, response *httptest.ResponseRecorder, code int, status restjobs.Status, pending bool) restjobs.Snapshot {
				t.Helper()
				var job restjobs.Snapshot
				if response.Code != code || json.Unmarshal(response.Body.Bytes(), &job) != nil || job.ID != admission.Job.ID || job.Status != status || job.CancellationRequested != pending {
					t.Fatalf("%s: %d %s, want %d with Job %s %s (cancellation requested: %t)", operation, response.Code, response.Body.String(), code, admission.Job.ID, status, pending)
				}
				return job
			}
			cancel := func() *httptest.ResponseRecorder {
				return principalRequest(handler, http.MethodPost, jobPath+"/cancel", "", namedPrincipalToken("admin"))
			}
			readJob := func() *httptest.ResponseRecorder {
				return principalRequest(handler, http.MethodGet, jobPath, "", namedPrincipalToken("alice"))
			}
			assertNoAssignment := func(claimID string) {
				t.Helper()
				body := fmt.Sprintf(`{"claim_id":%q,"worker_id":"other-worker","boot_id":"other-boot","repositories":["widget"]}`, claimID)
				response := remoteRequest(handler, "/v1/worker/claims", body, remoteTestSecret)
				if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
					t.Fatalf("claim %s: %d %s, want 204 with no assignment", claimID, response.Code, response.Body.String())
				}
			}

			// Each handler call completes before the next starts; this does not exercise lock contention.
			if !claimFirst {
				assertJob("cancel queued Job", cancel(), http.StatusOK, restjobs.StatusCanceled, false)
				assertNoAssignment("after-queued-cancel")
				job := assertJob("read canceled Job", readJob(), http.StatusOK, restjobs.StatusCanceled, false)
				if job.Worker != nil {
					t.Fatalf("Job canceled before claim has Worker %+v, want no Worker", job.Worker)
				}
				return
			}

			claim := claimPostgresHTTPWorkerJob(t, handler, "before-cancel")
			if claim.Job.ID != admission.Job.ID {
				t.Fatalf("claim returned Job %s, want %s", claim.Job.ID, admission.Job.ID)
			}
			assertJob("cancel claimed Job", cancel(), http.StatusAccepted, restjobs.StatusRunning, true)
			// The fixture allows two concurrent Jobs, so an otherwise empty queue has a spare slot.
			assertNoAssignment("while-cancellation-pending")
			job := assertJob("read Job with pending cancellation", readJob(), http.StatusOK, restjobs.StatusRunning, true)
			if job.Worker == nil || job.Worker.WorkerID != claim.Attempt.WorkerID || job.Worker.BootID != claim.Attempt.BootID {
				t.Fatalf("pending cancellation changed Worker: %+v, want %s/%s", job.Worker, claim.Attempt.WorkerID, claim.Attempt.BootID)
			}

			attemptPath := "/v1/worker/attempts/" + claim.Job.ID
			attempt := fmt.Sprintf(`"generation":%d,"owner_token":%q,"worker_id":%q,"boot_id":%q`,
				claim.Attempt.Generation, claim.Attempt.OwnerToken, claim.Attempt.WorkerID, claim.Attempt.BootID)
			heartbeat := remoteRequest(handler, attemptPath+"/heartbeat", "{"+attempt+"}", remoteTestSecret)
			var reply remoteHeartbeatResponse
			if heartbeat.Code != http.StatusOK || json.Unmarshal(heartbeat.Body.Bytes(), &reply) != nil || !reply.CancellationRequested {
				t.Fatalf("heartbeat: %d %s, want worker cancellation signal", heartbeat.Code, heartbeat.Body.String())
			}
			finish := remoteRequest(handler, attemptPath+"/finish",
				fmt.Sprintf(`{%s,"status":"canceled","evidence":{"checks":[],"limitations":[%q]}}`, attempt, restjobs.LimitationAgentNotVerdict), remoteTestSecret)
			if finish.Code != http.StatusOK {
				t.Fatalf("finish canceled Job: %d %s", finish.Code, finish.Body.String())
			}
			assertNoAssignment("after-worker-finish")
			assertJob("read Job after worker finish", readJob(), http.StatusOK, restjobs.StatusCanceled, false)
		})
	}
}

type postgresHTTPWorkerClaim struct {
	Job struct {
		ID             string `json:"id"`
		ThreadID       string `json:"thread_id"`
		ThreadPosition int    `json:"thread_position"`
	} `json:"job"`
	Attempt struct {
		Generation int64  `json:"generation"`
		OwnerToken string `json:"owner_token"`
		WorkerID   string `json:"worker_id"`
		BootID     string `json:"boot_id"`
	} `json:"attempt"`
}

func openAPIPostgresDB(dsn string) (*sql.DB, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*config), nil
}

func claimPostgresHTTPWorkerJob(t *testing.T, handler http.Handler, claimID string) postgresHTTPWorkerClaim {
	t.Helper()
	body := fmt.Sprintf(`{"claim_id":%q,"worker_id":"http-worker","boot_id":"http-boot","repositories":["widget"]}`, claimID)
	response := remoteRequest(handler, "/v1/worker/claims", body, remoteTestSecret)
	var claim postgresHTTPWorkerClaim
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &claim) != nil || claim.Job.ID == "" || claim.Attempt.Generation < 1 || claim.Attempt.OwnerToken == "" {
		t.Fatalf("HTTP claim status=%d body=%s", response.Code, response.Body.String())
	}
	return claim
}

func postgresHTTPFinishBody(claim postgresHTTPWorkerClaim) string {
	return fmt.Sprintf(`{"generation":%d,"owner_token":%q,"worker_id":%q,"boot_id":%q,"status":"succeeded","evidence":{"checks":[],"limitations":[%q]}}`,
		claim.Attempt.Generation, claim.Attempt.OwnerToken, claim.Attempt.WorkerID, claim.Attempt.BootID, restjobs.LimitationAgentNotVerdict)
}

func finishPostgresHTTPWorkerJob(t *testing.T, handler http.Handler, claim postgresHTTPWorkerClaim) *httptest.ResponseRecorder {
	t.Helper()
	return remoteRequest(handler, "/v1/worker/attempts/"+claim.Job.ID+"/finish", postgresHTTPFinishBody(claim), remoteTestSecret)
}

func assertPostgresHTTPInbox(t *testing.T, handler http.Handler, principal string, want int) *httptest.ResponseRecorder {
	t.Helper()
	response := principalRequest(handler, http.MethodGet, "/v1/notifications", "", namedPrincipalToken(principal))
	var page restjobs.NotificationPage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Notifications) != want {
		t.Fatalf("%s HTTP inbox status=%d body=%s, want %d notifications", principal, response.Code, response.Body.String(), want)
	}
	if want == 1 && page.Notifications[0].Status != restjobs.ThreadStatusDone {
		t.Fatalf("%s notification status=%s, want done", principal, page.Notifications[0].Status)
	}
	return response
}
