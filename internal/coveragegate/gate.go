package coveragegate

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Baselines are statement counts from package-local coverage profiles at
// origin/main d8f51792084408045cffbb52fba20e073d3c08d2.
var baselines = map[string]Counts{
	"internal/jobs":           {Covered: 3465, Total: 7692},
	"internal/api":            {Covered: 1381, Total: 1708},
	"internal/worker":         {Covered: 374, Total: 450},
	"internal/server/runtime": {Covered: 559, Total: 954},
}

// Counts reports the number of Go statements covered in a package profile.
type Counts struct {
	Covered int
	Total   int
}

// Baseline returns the pinned coverage baseline for a package.
func Baseline(packagePath string) (Counts, bool) {
	baseline, ok := baselines[packagePath]
	return baseline, ok
}

// ParseProfiles reads go test -coverprofile output for the named module packages.
func ParseProfiles(reader io.Reader, modulePath string, packagePaths []string) (map[string]Counts, error) {
	counts := make(map[string]Counts, len(packagePaths))
	for _, packagePath := range packagePaths {
		counts[packagePath] = Counts{}
	}
	scanner := bufio.NewScanner(reader)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		if lineNumber == 1 && strings.HasPrefix(line, "mode: ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("coverage profile line %d: expected 3 fields", lineNumber)
		}
		file, _, ok := strings.Cut(fields[0], ":")
		if !ok {
			return nil, fmt.Errorf("coverage profile line %d: invalid source %q", lineNumber, fields[0])
		}
		packagePath := ""
		for _, candidate := range packagePaths {
			if strings.HasPrefix(file, modulePath+"/"+candidate+"/") {
				packagePath = candidate
				break
			}
		}
		if packagePath == "" {
			continue
		}
		statementCount, err := strconv.Atoi(fields[1])
		if err != nil || statementCount < 0 {
			return nil, fmt.Errorf("coverage profile line %d: invalid statement count %q", lineNumber, fields[1])
		}
		executions, err := strconv.Atoi(fields[2])
		if err != nil || executions < 0 {
			return nil, fmt.Errorf("coverage profile line %d: invalid execution count %q", lineNumber, fields[2])
		}
		packageCounts := counts[packagePath]
		packageCounts.Total += statementCount
		if executions > 0 {
			packageCounts.Covered += statementCount
		}
		counts[packagePath] = packageCounts
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, packagePath := range packagePaths {
		if counts[packagePath].Total == 0 {
			return nil, fmt.Errorf("coverage profile contains no statements for %s", packagePath)
		}
	}
	return counts, nil
}

// MeetsBaseline reports whether actual coverage is at least the baseline ratio.
func MeetsBaseline(actual, baseline Counts) bool {
	if actual.Total <= 0 || baseline.Total <= 0 || actual.Covered < 0 || baseline.Covered < 0 || actual.Covered > actual.Total || baseline.Covered > baseline.Total {
		return false
	}
	return int64(actual.Covered)*int64(baseline.Total) >= int64(baseline.Covered)*int64(actual.Total)
}

// Percent formats a statement coverage ratio for display.
func Percent(counts Counts) string {
	if counts.Total <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2f%%", 100*float64(counts.Covered)/float64(counts.Total))
}
