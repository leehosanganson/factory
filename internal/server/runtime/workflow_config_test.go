package runtime

import (
	"reflect"
	"testing"

	restserver "github.com/leehosanganson/factory/internal/server"
	"github.com/leehosanganson/factory/internal/workflow"
)

func TestNewWorkflowConfig(t *testing.T) {
	explicitConcurrency := 3
	tests := []struct {
		name         string
		config       restserver.Config
		wantChecks   [][]string
		wantParallel *workflow.ParallelImplementationConfig
	}{
		{
			name:       "defaults",
			config:     restserver.Config{VerificationChecks: [][]string{{"go", "test"}, {"make", "vet"}}},
			wantChecks: [][]string{{"go", "test"}, {"make", "vet"}},
		},
		{
			name: "parallel option omitted",
			config: restserver.Config{
				ParallelSubtasks: nil,
			},
		},
		{
			name: "parallel option disabled",
			config: restserver.Config{
				ParallelSubtasks: &restserver.ParallelSubtasksConfig{Enabled: false},
			},
		},
		{
			name: "parallel concurrency defaults",
			config: restserver.Config{
				ParallelSubtasks: &restserver.ParallelSubtasksConfig{Enabled: true},
			},
			wantParallel: &workflow.ParallelImplementationConfig{Enabled: true, MaxConcurrency: 4},
		},
		{
			name: "parallel concurrency is configured",
			config: restserver.Config{
				ParallelSubtasks: &restserver.ParallelSubtasksConfig{Enabled: true, MaxConcurrency: &explicitConcurrency},
			},
			wantParallel: &workflow.ParallelImplementationConfig{Enabled: true, MaxConcurrency: explicitConcurrency},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := newWorkflowConfig(test.config)
			if got.AutoPublish {
				t.Fatal("AutoPublish = true, want false")
			}
			if got.WorktreeParent != "" {
				t.Fatalf("WorktreeParent = %q, want empty", got.WorktreeParent)
			}
			if test.wantChecks == nil {
				if len(got.PipelineChecks) != 0 {
					t.Fatalf("PipelineChecks = %#v, want no checks", got.PipelineChecks)
				}
			} else if !reflect.DeepEqual(got.PipelineChecks, test.wantChecks) {
				t.Fatalf("PipelineChecks = %#v, want %#v", got.PipelineChecks, test.wantChecks)
			}
			if !reflect.DeepEqual(got.ParallelImplementation, test.wantParallel) {
				t.Fatalf("ParallelImplementation = %#v, want %#v", got.ParallelImplementation, test.wantParallel)
			}

			defaults := workflow.DefaultConfig()
			if got.Command != defaults.Command || !reflect.DeepEqual(got.Args, defaults.Args) || got.AgentTimeout != defaults.AgentTimeout {
				t.Fatalf("workflow defaults changed: got command=%q args=%#v timeout=%q", got.Command, got.Args, got.AgentTimeout)
			}
		})
	}
}

func TestNewWorkflowConfigClonesVerificationChecks(t *testing.T) {
	checks := [][]string{{"go", "test", "./..."}, {"make", "vet"}}
	got := newWorkflowConfig(restserver.Config{VerificationChecks: checks})

	got.PipelineChecks[0][0] = "changed"
	got.PipelineChecks[1] = append(got.PipelineChecks[1], "changed")
	if !reflect.DeepEqual(checks, [][]string{{"go", "test", "./..."}, {"make", "vet"}}) {
		t.Fatalf("source checks changed through workflow config: %#v", checks)
	}
}
