package factory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultWorktreeParent = "../{repo}.worktrees"

func validateWorktreeParent(parent string, checkouts ...string) (string, error) {
	resolvedParent, err := resolvedPath(parent)
	if err != nil {
		return "", err
	}
	for _, checkout := range checkouts {
		resolvedCheckout, err := resolvedPath(checkout)
		if err != nil {
			return "", err
		}
		if isWithin(resolvedCheckout, resolvedParent) {
			return "", fmt.Errorf("worktree parent must be outside the target repository")
		}
	}
	return resolvedParent, nil
}

func resolveWorktreeParent(template, primaryCheckout string) (string, error) {
	if template == "" {
		template = defaultWorktreeParent
	}
	if strings.ContainsRune(template, 0) {
		return "", fmt.Errorf("worktree_parent contains NUL")
	}
	repository := filepath.Base(filepath.Clean(primaryCheckout))
	if repository == "." || repository == string(filepath.Separator) || repository == "" {
		return "", fmt.Errorf("cannot determine repository basename for worktree_parent")
	}
	parent := filepath.Clean(strings.ReplaceAll(template, "{repo}", repository))
	if !filepath.IsAbs(parent) {
		parent = filepath.Join(primaryCheckout, parent)
	}
	return filepath.Abs(parent)
}

func jobWorktreeName(slug, id string) (string, error) {
	if len(id) < 4 {
		return "", fmt.Errorf("job id must start with four hexadecimal characters")
	}
	for index, char := range id {
		validHex := char >= '0' && char <= '9' || char >= 'a' && char <= 'f' || char >= 'A' && char <= 'F'
		validComponent := validHex || char >= 'g' && char <= 'z' || char >= 'G' && char <= 'Z' || char == '-'
		if !validComponent || index < 4 && !validHex {
			return "", fmt.Errorf("job id must start with four hexadecimal characters and contain only letters, digits, or hyphens")
		}
	}
	return id[:4] + "-" + implementationJobSlug(slug), nil
}

func availableJobWorktreeName(parent, slug, id, registeredPath string) (string, error) {
	if _, err := jobWorktreeName(slug, id); err != nil {
		return "", err
	}
	suffix := "-" + implementationJobSlug(slug)
	for prefixLength := 4; prefixLength <= len(id); prefixLength++ {
		candidate := id[:prefixLength] + suffix
		path := filepath.Join(parent, candidate)
		if registeredPath != "" && path == registeredPath {
			return candidate, nil
		}
		if _, err := os.Lstat(path); os.IsNotExist(err) {
			return candidate, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect implementation worktree path: %w", err)
		}
	}
	return "", fmt.Errorf("implementation worktree path for job %s already exists", id)
}
