package main

import (
	"path/filepath"
	"testing"
)

func sameResolvedTestPath(t *testing.T, left, right string) bool {
	t.Helper()
	left, err := filepath.EvalSymlinks(left)
	if err != nil {
		t.Fatalf("resolve test path %q: %v", left, err)
	}
	right, err = filepath.EvalSymlinks(right)
	if err != nil {
		t.Fatalf("resolve test path %q: %v", right, err)
	}
	return left == right
}
