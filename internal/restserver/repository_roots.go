package restserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ValidateRepositoryRoots checks configured repository roots against the local
// filesystem and Git metadata. A future server startup path must call this before
// opening its listener; Config.Validate intentionally performs no process I/O.
func (c Config) ValidateRepositoryRoots() error {
	for alias, root := range c.Repositories {
		if err := validateRepositoryRoot(root); err != nil {
			return fmt.Errorf("repository %q is not a valid Git working tree root: %w", alias, err)
		}
	}
	return nil
}

const repositoryRootValidationTimeout = 10 * time.Second

func validateRepositoryRoot(root string) error {
	if !filepath.IsAbs(root) || strings.TrimSpace(root) == "" || strings.ContainsRune(root, 0) || filepath.Clean(root) != root {
		return errors.New("root must be an absolute canonical path")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return errors.New("root must be an existing directory")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil || resolvedRoot != root {
		return errors.New("root and its ancestors must not use symlinks")
	}

	ctx, cancel := context.WithTimeout(context.Background(), repositoryRootValidationTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--show-toplevel")
	var output bytes.Buffer
	command.Stdout = &output
	if err := command.Run(); err != nil {
		return errors.New("git could not verify the working tree")
	}
	gitRoot := strings.TrimSuffix(output.String(), "\n")
	if gitRoot == "" || strings.ContainsRune(gitRoot, '\n') {
		return errors.New("git returned an invalid working tree root")
	}
	resolvedGitRoot, err := filepath.EvalSymlinks(gitRoot)
	if err != nil || resolvedGitRoot != root {
		return errors.New("configured path is not the exact working tree root")
	}
	return nil
}
