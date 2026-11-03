package issue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestIssueSnapshotJSONCompatibility(t *testing.T) {
	snapshot := IssueSnapshot{
		Repository: "acme/widget",
		Number:     7,
		Title:      "Issue",
		Body:       "Details",
		State:      "open",
		URL:        "https://github.com/acme/widget/issues/7",
		UpdatedAt:  time.Date(2025, 1, 2, 3, 4, 5, 123000000, time.UTC),
		Version:    "version",
	}
	got, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"repository":"acme/widget","number":7,"title":"Issue","body":"Details","state":"open","url":"https://github.com/acme/widget/issues/7","updated_at":"2025-01-02T03:04:05.123Z","version":"version"}`
	if string(got) != want {
		t.Fatalf("IssueSnapshot JSON = %s, want %s", got, want)
	}
}

func TestIssueSnapshotFingerprintCompatibility(t *testing.T) {
	snapshot := IssueSnapshot{
		Repository: "acme/widget",
		Number:     7,
		Title:      "Issue",
		Body:       "Body",
		State:      "open",
		URL:        "https://github.com/acme/widget/issues/7",
		UpdatedAt:  time.Date(2025, 1, 2, 3, 4, 5, 123000000, time.FixedZone("offset", 3600)),
	}
	if snapshot.UpdatedAt.Location() == time.UTC {
		t.Fatal("test timestamp must start in a non-UTC location")
	}
	// GitHub adapters normalize provider timestamps to UTC before fingerprinting.
	snapshot.UpdatedAt = snapshot.UpdatedAt.UTC()
	versionInput, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256(versionInput)
	if got, want := hex.EncodeToString(fingerprint[:]), "489fbcb2bb07b1d454999742b1a2e0f88a848998e76debccfde8d08454b7dc74"; got != want {
		t.Fatalf("snapshot fingerprint = %s, want %s", got, want)
	}
	if !strings.HasSuffix(snapshot.UpdatedAt.Format(time.RFC3339Nano), "Z") {
		t.Fatalf("normalized timestamp = %s, want UTC", snapshot.UpdatedAt)
	}
}
