package restapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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

func TestPostgresRemoteWorkerHTTPFinishingFinalThreadJobNotifiesOwnerOnce(t *testing.T) {
	dsn := os.Getenv("FACTORY_TEST_POSTGRES_URL")
	if dsn == "" {
		if os.Getenv("FACTORY_REQUIRE_POSTGRES_TEST") == "1" {
			t.Fatal("FACTORY_REQUIRE_POSTGRES_TEST=1 but FACTORY_TEST_POSTGRES_URL is unset")
		}
		t.Skip("set FACTORY_TEST_POSTGRES_URL to run PostgreSQL HTTP Worker Thread notification integration test")
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
		QueueCapacity: 4, MaxConcurrentJobs: 1, MaxRecords: 16, MaxEventsPerJob: 8, MaxTaskBytes: 128,
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
}

type postgresHTTPWorkerClaim struct {
	Job struct {
		ID string `json:"id"`
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
