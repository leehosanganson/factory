package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunProbeReportsOnlyReadyResponses(t *testing.T) {
	for _, test := range []struct {
		name    string
		code    int
		body    string
		wantErr bool
	}{
		{name: "ready", code: http.StatusOK, body: `{"status":"ready"}`},
		{name: "not ready", code: http.StatusServiceUnavailable, body: `{"status":"not_ready"}`, wantErr: true},
		{name: "wrong body", code: http.StatusOK, body: `{"status":"ok"}`, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/readyz" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(test.code)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			var output strings.Builder
			err := runProbe([]string{"--url", server.URL + "/readyz"}, &output)
			if (err != nil) != test.wantErr {
				t.Fatalf("runProbe() error = %v", err)
			}
			if test.wantErr && output.Len() != 0 {
				t.Fatalf("unexpected output %q", output.String())
			}
			if !test.wantErr && output.String() != "ready\n" {
				t.Fatalf("output = %q", output.String())
			}
		})
	}
}

func TestRunProbeDoesNotFollowRedirects(t *testing.T) {
	var destinationRequests int
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		destinationRequests++
		_, _ = io.WriteString(w, `{"status":"ready"}`)
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL+"/readyz", http.StatusFound)
	}))
	defer redirect.Close()

	if err := runProbe([]string{"--url", redirect.URL + "/readyz"}, io.Discard); err == nil {
		t.Fatal("runProbe() followed a redirect")
	}
	if destinationRequests != 0 {
		t.Fatalf("redirect destination received %d requests", destinationRequests)
	}
}

func TestRunProbeTimesOutAfterTwoSeconds(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-time.After(3 * time.Second)
		_, _ = io.WriteString(w, `{"status":"ready"}`)
	}))
	defer server.Close()

	begin := time.Now()
	err := runProbe([]string{"--url", server.URL + "/readyz"}, io.Discard)
	elapsed := time.Since(begin)
	<-started
	if err == nil {
		t.Fatal("runProbe() succeeded despite a stalled response")
	}
	if elapsed > 2900*time.Millisecond {
		t.Fatalf("runProbe() exceeded its 2s timeout: %v", elapsed)
	}
}

func TestRunProbeRejectsResponseLargerThanLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"status":"ready"}`+strings.Repeat(" ", readinessProbeBodyLimit))
	}))
	defer server.Close()
	var output strings.Builder
	if err := runProbe([]string{"--url", server.URL + "/readyz"}, &output); err == nil {
		t.Fatal("runProbe() accepted a response larger than its body limit")
	}
	if output.Len() != 0 {
		t.Fatalf("unexpected output %q", output.String())
	}
}

func TestRunProbeRejectsInvalidEndpointsWithoutRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("invalid endpoint reached server")
	}))
	defer server.Close()
	for _, endpoint := range []string{
		server.URL + "/healthz",
		server.URL + "/readyz?token=secret",
		"http://user:password@localhost/readyz",
		"file:///readyz",
	} {
		if err := runProbe([]string{"--url", endpoint}, io.Discard); err == nil {
			t.Errorf("runProbe(%q) succeeded", endpoint)
		}
	}
}
