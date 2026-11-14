package restapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	restjobs "github.com/leehosanganson/factory/internal/jobs"
	restserver "github.com/leehosanganson/factory/internal/server"
)

func TestWorkerSessionListIsOperatorOnlyBoundedAndRedacted(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "worker-sessions.db"), managerConfig(8, 16, 8))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	active, _, err := store.RegisterWorkerSession(context.Background(), "api-worker-active", "worker-supplied-boot")
	if err != nil {
		t.Fatal(err)
	}
	retired, _, err := store.RegisterWorkerSession(context.Background(), "api-worker-retired", "retired-boot-label")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetireWorkerSession(context.Background(), retired.PrincipalID, retired.BootID, retired.ID); err != nil {
		t.Fatal(err)
	}
	handler := newPrincipalTestHandler(t, store,
		restserver.PrincipalConfig{ID: "worker-operator", Role: "operator", Enabled: true},
		restserver.PrincipalConfig{ID: "worker-submitter", Role: "submitter", Enabled: true},
	)

	response := principalRequest(handler, http.MethodGet, "/v1/worker-sessions?limit=1", "", namedPrincipalToken("worker-operator"))
	if response.Code != http.StatusOK {
		t.Fatalf("operator list status=%d body=%s", response.Code, response.Body.String())
	}
	var page struct {
		Sessions      []restjobs.WorkerSessionSummary `json:"sessions"`
		HasMore       bool                            `json:"has_more"`
		NextCreatedAt *time.Time                      `json:"next_created_at"`
		NextID        string                          `json:"next_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 1 || !page.HasMore || page.NextID == "" || page.NextCreatedAt == nil || page.Sessions[0].Status != "retired" || page.Sessions[0].RetiredAt == nil {
		t.Fatalf("bounded operator page=%+v, want newest retired row and continuation", page)
	}
	if !strings.Contains(response.Body.String(), `"principal_id":"api-worker-retired"`) {
		t.Fatalf("operator projection omitted configured principal ID: %s", response.Body.String())
	}
	for _, forbidden := range []string{"boot_id", "worker-supplied-boot", "retired-boot-label", "capability", "capability_hash", "capability_expires_at", active.Capability, retired.Capability} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("worker session API exposed forbidden field %q: %s", forbidden, response.Body.String())
		}
	}
	cursor := "before_created_at=" + url.QueryEscape(page.NextCreatedAt.Format(time.RFC3339Nano)) + "&before_id=" + url.QueryEscape(page.NextID)
	continued := principalRequest(handler, http.MethodGet, "/v1/worker-sessions?limit=1&"+cursor, "", namedPrincipalToken("worker-operator"))
	if continued.Code != http.StatusOK || !strings.Contains(continued.Body.String(), `"status":"active"`) || !strings.Contains(continued.Body.String(), `"retired_at":null`) || !strings.Contains(continued.Body.String(), `"heartbeat_at":null`) {
		t.Fatalf("operator continuation status=%d body=%s, want active row and explicit nullable timestamps", continued.Code, continued.Body.String())
	}

	unauthenticated := request(handler, http.MethodGet, "/v1/worker-sessions", "", false)
	assertError(t, unauthenticated, http.StatusUnauthorized, "unauthenticated")
	submitter := principalRequest(handler, http.MethodGet, "/v1/worker-sessions", "", namedPrincipalToken("worker-submitter"))
	assertError(t, submitter, http.StatusNotFound, "not_found")
	handler.config.DashboardOperatorID = "worker-operator"
	dashboardResponse := httptest.NewRecorder()
	handler.ServeAsDashboardOperator(dashboardResponse, httptest.NewRequest(http.MethodGet, "/v1/worker-sessions", nil))
	if dashboardResponse.Code != http.StatusNotFound {
		t.Fatalf("dashboard exposed worker sessions: status=%d body=%s", dashboardResponse.Code, dashboardResponse.Body.String())
	}
}

func TestWorkerSessionListRejectsBadQueriesAndMethod(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	manager, err := restjobs.OpenSQLiteStore(filepath.Join(dir, "worker-query.db"), managerConfig(4, 8, 8))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	handler := newPrincipalTestHandler(t, manager, restserver.PrincipalConfig{ID: "worker-query-operator", Role: "operator", Enabled: true})
	for _, query := range []string{
		"limit=0", "limit=101", "limit=1&limit=2", "unexpected=1", "before_created_at=2026-01-01T00%3A00%3A00Z&before_id=worker&before_id=worker", "before_created_at=not-a-cursor&before_id=worker", "before_id=worker", "before_created_at=2026-01-01T00%3A00%3A00Z&before_id=bad/id", "limit=1%26limit=2", "limit=%zz",
	} {
		t.Run(query, func(t *testing.T) {
			response := principalRequest(handler, http.MethodGet, "/v1/worker-sessions?"+query, "", namedPrincipalToken("worker-query-operator"))
			assertError(t, response, http.StatusBadRequest, "invalid_request")
		})
	}
	method := principalRequest(handler, http.MethodPost, "/v1/worker-sessions", "", namedPrincipalToken("worker-query-operator"))
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("POST status=%d Allow=%q, want 405 GET", method.Code, method.Header().Get("Allow"))
	}
}
