package factory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvedTestPathResolvesSymlink(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	alias := filepath.Join(root, "alias")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}

	if got, want := resolvedTestPath(t, alias), resolvedTestPath(t, real); got != want {
		t.Fatalf("resolved test path for symlink = %q, want resolved target %q", got, want)
	}
}

func resolvedTestPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := resolvedPath(path)
	if err != nil {
		t.Fatalf("resolve test path %q: %v", path, err)
	}
	return resolved
}

func sameResolvedTestPath(t *testing.T, left, right string) bool {
	t.Helper()
	return resolvedTestPath(t, left) == resolvedTestPath(t, right)
}
