package restapi

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func isOpenAPI3Version(version string) bool {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, digit := range part {
			if digit < '0' || digit > '9' {
				return false
			}
		}
	}
	major, err := strconv.Atoi(parts[0])
	return err == nil && major == 3
}

func TestOpenAPIDocumentationNavigationAndVersionPolicy(t *testing.T) {
	link := "[OpenAPI specification](../openapi/rest-api-v1.json)"
	policy := "Compatible API changes update the spec and info.version; incompatible API changes publish a new versioned document."
	documents := []string{
		filepath.Join("..", "..", "docs", "features", "rest-server.md"),
		filepath.Join("..", "..", "docs", "features", "rest-server-operations.md"),
	}

	for _, path := range documents {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read feature documentation %s: %v", path, err)
			}
			content := string(data)
			if !strings.Contains(content, link) {
				t.Errorf("feature documentation %s must include %s", path, link)
			}
			if !strings.Contains(content, policy) {
				t.Errorf("feature documentation %s must state API version policy: %s", path, policy)
			}
		})
	}
}

func TestIssueProvenanceOpenAPIValidationParity(t *testing.T) {
	specPath := filepath.Join("..", "..", "docs", "openapi", "rest-api-v1.json")
	data, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read OpenAPI spec %s: %v", specPath, err)
	}
	var document struct {
		Components struct {
			Schemas map[string]map[string]json.RawMessage `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode OpenAPI spec: %v", err)
	}
	provenance := document.Components.Schemas["IssueProvenance"]
	if provenance == nil {
		t.Fatal("OpenAPI spec is missing IssueProvenance schema")
	}
	var properties map[string]map[string]json.RawMessage
	if err := json.Unmarshal(provenance["properties"], &properties); err != nil {
		t.Fatalf("decode IssueProvenance properties: %v", err)
	}

	t.Run("title UTF-8 byte bound", func(t *testing.T) {
		title := properties["title"]
		var maxLength, maxBytes int
		var description string
		if err := json.Unmarshal(title["maxLength"], &maxLength); err != nil || maxLength != 4096 {
			t.Fatalf("title maxLength = %d, %v; want 4096 characters", maxLength, err)
		}
		if err := json.Unmarshal(title["x-maxBytes"], &maxBytes); err != nil || maxBytes != 4096 {
			t.Fatalf("title x-maxBytes = %d, %v; want 4096 bytes", maxBytes, err)
		}
		if err := json.Unmarshal(title["description"], &description); err != nil || !strings.Contains(description, "UTF-8 byte length") || !strings.Contains(description, "maxLength counts characters") {
			t.Fatalf("title description %q must distinguish UTF-8 byte length from character-count maxLength", description)
		}

		handler := newTestHandler(t, newTestManager(t, managerConfig(4, 4, 8)), nil)
		handler.config.MaxRequestBodyBytes = 8192
		requestBody := func(value string) string {
			body, err := json.Marshal(map[string]any{
				"repository": "widget",
				"task":       "bounded title test",
				"issue":      42,
				"issue_provenance": map[string]any{
					"tracker": "github", "source_repository": "acme/widget", "issue_number": 42,
					"version": strings.Repeat("a", 64), "url": "https://github.com/acme/widget/issues/42", "title": value,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			return string(body)
		}
		if response := postJob(handler, requestBody(strings.Repeat("é", 2048)), "title-at-byte-limit"); response.Code != 202 {
			t.Fatalf("2048 two-byte characters (4096 UTF-8 bytes) status=%d, body=%s; want accepted", response.Code, response.Body.String())
		}
		assertError(t, postJob(handler, requestBody(strings.Repeat("é", 2049)), "title-over-byte-limit"), 400, "invalid_request")
	})

	t.Run("source repository excludes dot components", func(t *testing.T) {
		var pattern string
		if err := json.Unmarshal(properties["source_repository"]["pattern"], &pattern); err != nil {
			t.Fatalf("decode source_repository pattern: %v", err)
		}
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("compile source_repository pattern: %v", err)
		}
		for _, repository := range []string{"acme/widget", "owner/-repo", "owner/.hidden", "owner/..."} {
			if !compiled.MatchString(repository) {
				t.Errorf("source_repository pattern rejects valid server repository %q", repository)
			}
		}
		for _, repository := range []string{"owner/.", "owner/..", "owner/../repo", "owner/repo/extra"} {
			if compiled.MatchString(repository) {
				t.Errorf("source_repository pattern accepts invalid server repository %q", repository)
			}
		}
	})
}

func TestOpenAPIRouteInventory(t *testing.T) {
	specPath := filepath.Join("..", "..", "docs", "openapi", "rest-api-v1.json")
	data, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read OpenAPI spec %s: %v", specPath, err)
	}

	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("decode OpenAPI spec %s: invalid JSON: %v", specPath, err)
	}
	if document == nil {
		t.Fatalf("decode OpenAPI spec %s: root must be a JSON object", specPath)
	}

	var openapiVersion string
	if err := json.Unmarshal(document["openapi"], &openapiVersion); err != nil || !isOpenAPI3Version(openapiVersion) {
		t.Fatalf("OpenAPI spec %s: openapi must be a version in the 3.x.y series, got %q", specPath, openapiVersion)
	}

	var info map[string]json.RawMessage
	if err := json.Unmarshal(document["info"], &info); err != nil || info == nil {
		t.Fatalf("OpenAPI spec %s: info must be an object", specPath)
	}
	for _, field := range []string{"title", "version"} {
		var value string
		if err := json.Unmarshal(info[field], &value); err != nil || strings.TrimSpace(value) == "" {
			t.Fatalf("OpenAPI spec %s: info.%s must be a non-empty string", specPath, field)
		}
	}

	var paths map[string]json.RawMessage
	if err := json.Unmarshal(document["paths"], &paths); err != nil || paths == nil {
		t.Fatalf("OpenAPI spec %s: paths must be an object", specPath)
	}

	want := map[string]string{
		"GET /healthz":                            "healthCheck",
		"GET /readyz":                             "readinessCheck",
		"GET /v1/jobs":                            "listJobs",
		"POST /v1/jobs":                           "createJob",
		"GET /v1/jobs/{id}":                       "getJob",
		"GET /v1/jobs/{id}/history":               "getJobHistory",
		"GET /v1/jobs/{id}/events":                "streamJobEvents",
		"POST /v1/jobs/{id}/cancel":               "cancelJob",
		"POST /v1/jobs/{id}/reconcile":            "reconcileJob",
		"POST /v1/jobs/{id}/disposition/failed":   "dispositionJobFailed",
		"POST /v1/jobs/{id}/disposition/canceled": "dispositionJobCanceled",
		"GET /v1/operations":                      "getOperations",
	}
	methods := map[string]struct{}{
		"get": {}, "put": {}, "post": {}, "delete": {}, "options": {},
		"head": {}, "patch": {}, "trace": {},
	}
	got := make(map[string]string)
	for path, rawPathItem := range paths {
		var pathItem map[string]json.RawMessage
		if err := json.Unmarshal(rawPathItem, &pathItem); err != nil || pathItem == nil {
			t.Fatalf("OpenAPI spec %s: path %q must be an object", specPath, path)
		}
		for method, rawOperation := range pathItem {
			if _, isMethod := methods[method]; !isMethod {
				continue
			}
			var operation map[string]json.RawMessage
			if err := json.Unmarshal(rawOperation, &operation); err != nil || operation == nil {
				t.Fatalf("OpenAPI spec %s: %s %s operation must be an object", specPath, strings.ToUpper(method), path)
			}
			var operationID string
			if err := json.Unmarshal(operation["operationId"], &operationID); err != nil || strings.TrimSpace(operationID) == "" {
				t.Fatalf("OpenAPI spec %s: %s %s operation must have a non-empty operationId", specPath, strings.ToUpper(method), path)
			}
			var responses map[string]json.RawMessage
			if err := json.Unmarshal(operation["responses"], &responses); err != nil || len(responses) == 0 {
				t.Errorf("OpenAPI operation %s %s must have a non-empty responses object", strings.ToUpper(method), path)
			} else if _, ok := responses["405"]; !ok {
				t.Errorf("OpenAPI operation %s %s must document a 405 response", strings.ToUpper(method), path)
			}
			key := strings.ToUpper(method) + " " + path
			got[key] = operationID
		}
	}

	var missing, extra []string
	for route, operationID := range want {
		gotID, ok := got[route]
		if !ok {
			missing = append(missing, route)
		} else if gotID != operationID {
			t.Errorf("OpenAPI operation %s has operationId %q, want %q", route, gotID, operationID)
		}
	}
	for route := range got {
		if _, ok := want[route]; !ok {
			extra = append(extra, route)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Errorf("OpenAPI route/method inventory mismatch: missing [%s]; extra [%s]", strings.Join(missing, ", "), strings.Join(extra, ", "))
	}

	seenIDs := make(map[string]string, len(got))
	for route, operationID := range got {
		if prior, exists := seenIDs[operationID]; exists {
			t.Errorf("OpenAPI operationId %q is used by both %s and %s", operationID, prior, route)
		} else {
			seenIDs[operationID] = route
		}
	}
}
