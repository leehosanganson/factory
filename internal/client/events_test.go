package restclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFollowEventsCarriesCursorAndRecognizesExpiredCursor(t *testing.T) {
	var gotCursor string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCursor = r.Header.Get("Last-Event-ID")
		if gotCursor == "7" {
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprint(w, `{"code":"cursor_expired"}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "id: 8\nevent: running\ndata: {\"type\":\"running\",\"at\":\"2026-01-01T00:00:00Z\"}\n\n")
	}))
	defer server.Close()
	client := New(Config{BaseURL: server.URL, Token: "secret"})
	var events []Event
	cursor, expired, err := client.FollowEvents(context.Background(), "job-1", 6, func(event Event) { events = append(events, event) })
	if err != nil || expired || cursor != 8 || len(events) != 1 || events[0].Type != "running" {
		t.Fatalf("follow = cursor %d expired %v events %+v err %v", cursor, expired, events, err)
	}
	_, expired, err = client.FollowEvents(context.Background(), "job-1", 7, nil)
	if err == nil || !expired || gotCursor != "7" {
		t.Fatalf("expired follow = expired %v cursor %q err %v", expired, gotCursor, err)
	}
}
