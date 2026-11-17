package coveragegate

import (
	"strings"
	"testing"
)

func TestParseProfilesCountsStatementsByPackage(t *testing.T) {
	profile := "mode: set\n" +
		"github.com/leehosanganson/factory/internal/api/handler.go:10.1,12.2 3 1\n" +
		"github.com/leehosanganson/factory/internal/api/handler.go:13.1,14.2 2 0\n" +
		"github.com/leehosanganson/factory/internal/jobs/store.go:10.1,11.2 4 1\n"
	got, err := ParseProfiles(strings.NewReader(profile), "github.com/leehosanganson/factory", []string{"internal/api", "internal/jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if want := (Counts{Covered: 3, Total: 5}); got["internal/api"] != want {
		t.Fatalf("API counts = %+v, want %+v", got["internal/api"], want)
	}
	if want := (Counts{Covered: 4, Total: 4}); got["internal/jobs"] != want {
		t.Fatalf("Jobs counts = %+v, want %+v", got["internal/jobs"], want)
	}
}

func TestParseProfilesRejectsMalformedOrMissingInput(t *testing.T) {
	for name, profile := range map[string]string{
		"empty":                   "mode: set\n",
		"malformed":               "mode: set\nnot a profile\n",
		"invalid statement count": "mode: set\ngithub.com/leehosanganson/factory/internal/api/handler.go:1.1,2.2 nope 1\n",
		"invalid execution count": "mode: set\ngithub.com/leehosanganson/factory/internal/api/handler.go:1.1,2.2 1 nope\n",
		"missing package":         "mode: set\ngithub.com/leehosanganson/factory/internal/jobs/store.go:1.1,2.2 1 1\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseProfiles(strings.NewReader(profile), "github.com/leehosanganson/factory", []string{"internal/api"}); err == nil {
				t.Fatal("ParseProfile() unexpectedly succeeded")
			}
		})
	}
}

func TestBaselineIsPinnedForEachKeyPackage(t *testing.T) {
	for _, packagePath := range []string{"internal/jobs", "internal/api", "internal/worker", "internal/server/runtime"} {
		if baseline, ok := Baseline(packagePath); !ok || baseline.Total == 0 {
			t.Errorf("Baseline(%q) = %+v, %t; want a non-empty baseline", packagePath, baseline, ok)
		}
	}
	if _, ok := Baseline("internal/unknown"); ok {
		t.Fatal("Baseline unexpectedly exists for an unknown package")
	}
}

func TestMeetsBaselineComparesRatiosExactly(t *testing.T) {
	baseline := Counts{Covered: 80, Total: 100}
	for _, tc := range []struct {
		name string
		got  Counts
		want bool
	}{
		{"same ratio", Counts{Covered: 80, Total: 100}, true},
		{"same ratio different totals", Counts{Covered: 40, Total: 50}, true},
		{"improved", Counts{Covered: 81, Total: 100}, true},
		{"regressed", Counts{Covered: 79, Total: 100}, false},
		{"new statements but same count", Counts{Covered: 80, Total: 101}, false},
		{"empty profile", Counts{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MeetsBaseline(tc.got, baseline); got != tc.want {
				t.Fatalf("MeetsBaseline(%+v, %+v) = %t, want %t", tc.got, baseline, got, tc.want)
			}
		})
	}
}
