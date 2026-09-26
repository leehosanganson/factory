package factory

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//go:embed prompts/*.md
var embeddedPrompts embed.FS

var stages = []string{"requirements", "implement", "review", "fix", "document", "monitor"}

// LoadPrompt returns an external stage override when present, otherwise the embedded prompt.
func LoadPrompt(promptDir, stage string) (string, error) {
	if !validStage(stage) {
		return "", fmt.Errorf("unknown prompt stage %q", stage)
	}
	if promptDir != "" {
		overrides := []string{stage + ".md"}
		if stage == "monitor" {
			overrides = append(overrides, "babysit.md")
		}
		for _, name := range overrides {
			path := filepath.Join(promptDir, name)
			content, err := os.ReadFile(path)
			if err == nil {
				return strings.TrimSpace(string(content)), nil
			}
			if !os.IsNotExist(err) {
				return "", fmt.Errorf("read prompt override %s: %w", path, err)
			}
		}
	}
	content, err := embeddedPrompts.ReadFile("prompts/" + stage + ".md")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}

func validStage(stage string) bool {
	for _, candidate := range stages {
		if candidate == stage {
			return true
		}
	}
	return false
}
