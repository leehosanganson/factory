package factory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCanonicalTestPathResolvesSymlink(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	alias := filepath.Join(root, "alias")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}

	if got, want := canonicalTestPath(t, alias), canonicalTestPath(t, real); got != want {
		t.Fatalf("canonical test path for symlink = %q, want resolved target %q", got, want)
	}
}

func canonicalTestPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := canonicalPath(path)
	if err != nil {
		t.Fatalf("canonicalize test path %q: %v", path, err)
	}
	return resolved
}

func sameCanonicalTestPath(t *testing.T, left, right string) bool {
	t.Helper()
	return canonicalTestPath(t, left) == canonicalTestPath(t, right)
}
