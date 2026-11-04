package restserver

import (
	"strings"
	"testing"
)

func TestDecodeWorkerMappingFileAllowsOverlappingCertificatesForOnePrincipal(t *testing.T) {
	first, second := strings.Repeat("a", 64), strings.Repeat("b", 64)
	mappings, err := decodeWorkerMappingFile([]byte(`{"version":1,"mappings":[{"sha256":"` + first + `","principal_id":"worker-one"},{"sha256":"` + second + `","principal_id":"worker-one"}]}`))
	if err != nil || len(mappings) != 2 || mappings[first] != "worker-one" || mappings[second] != "worker-one" {
		t.Fatalf("overlap mapping decode=(%v,%v); want both fingerprints for worker-one", mappings, err)
	}
}

func TestDecodeWorkerMappingFileRequiresVersionAndUniqueFingerprints(t *testing.T) {
	valid := `{"version":1,"mappings":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-one"}]}`
	mappings, err := decodeWorkerMappingFile([]byte(valid))
	if err != nil || mappings[strings.Repeat("a", 64)] != "worker-one" {
		t.Fatalf("decodeWorkerMappingFile() = %v, %v", mappings, err)
	}
	for _, invalid := range []string{
		`{"version":2,"mappings":[]}`,
		`{"version":1,"mappings":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-one","extra":true}]}`,
		`{"version":1,"mappings":[{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-one"},{"sha256":"` + strings.Repeat("a", 64) + `","principal_id":"worker-two"}]}`,
		`{"version":1,"mappings":[]} {}`,
		`{"version":1,"version":1,"mappings":[]}`,
	} {
		if _, err := decodeWorkerMappingFile([]byte(invalid)); err == nil {
			t.Errorf("decodeWorkerMappingFile accepted %s", invalid)
		}
	}
}
