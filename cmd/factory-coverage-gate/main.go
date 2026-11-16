package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/leehosanganson/factory/internal/coveragegate"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(stdout io.Writer) error {
	var root, profile string
	flag.StringVar(&root, "root", ".", "module root containing the coverage profile")
	flag.StringVar(&profile, "profile", "coverage.out", "Go coverage profile to evaluate")
	flag.Parse()

	absoluteProfile := profile
	if !filepath.IsAbs(absoluteProfile) {
		absoluteProfile = filepath.Join(root, profile)
	}
	file, err := os.Open(absoluteProfile)
	if err != nil {
		return fmt.Errorf("open Go coverage profile: %w", err)
	}
	defer file.Close()

	counts, err := coveragegate.ParseProfiles(file, modulePath, packagePaths)
	if err != nil {
		return fmt.Errorf("parse Go coverage profile: %w", err)
	}

	fmt.Fprintf(stdout, "Key Go package statement coverage (package-local tests; Linux baseline %s)\n", baselineRevision)
	for _, packagePath := range packagePaths {
		actual := counts[packagePath]
		baseline, ok := coveragegate.Baseline(packagePath)
		if !ok {
			return fmt.Errorf("no coverage baseline configured for %s", packagePath)
		}
		fmt.Fprintf(stdout, "%-28s %s (baseline %s)\n", packagePath, coveragegate.Percent(actual), coveragegate.Percent(baseline))
		if !coveragegate.MeetsBaseline(actual, baseline) {
			return fmt.Errorf("%s coverage %s is below baseline %s", packagePath, coveragegate.Percent(actual), coveragegate.Percent(baseline))
		}
	}
	return nil
}

const (
	modulePath       = "github.com/leehosanganson/factory"
	baselineRevision = "d8f51792084408045cffbb52fba20e073d3c08d2"
)

var packagePaths = []string{
	"internal/jobs",
	"internal/api",
	"internal/worker",
	"internal/server/runtime",
}
