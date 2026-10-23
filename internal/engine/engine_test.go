package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestExecuteCompletesRun(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()

	var ran []string
	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "first", Fn: func(ctx context.Context) error { ran = append(ran, "first"); return nil }},
			{Name: "second", Fn: func(ctx context.Context) error { ran = append(ran, "second"); return nil }},
		},
	}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{Store: s, Reg: reg}
	runID, err := w.Execute(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(ran) != 2 || ran[0] != "first" || ran[1] != "second" {
		t.Fatalf("steps ran out of order: %v", ran)
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusCompleted {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	for _, st := range run.Steps {
		if st.Status != store.StepStatusCompleted {
			t.Fatalf("step %q not completed: %s", st.Name, st.Status)
		}
	}
}

func TestExecuteStopsOnStepFailure(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "ok", Fn: func(ctx context.Context) error { return nil }},
			{Name: "boom", Fn: func(ctx context.Context) error { return errors.New("boom") }},
			{Name: "never", Fn: func(ctx context.Context) error {
				t.Fatal("step after failure must not run")
				return nil
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := (&Worker{Store: s, Reg: reg}).Execute(context.Background(), "demo")
	if err == nil {
		t.Fatal("expected error from failed step")
	}

	run, err := s.GetRun(1)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusFailed {
		t.Fatalf("expected failed run, got %s", run.Status)
	}
	if run.Steps[0].Status != store.StepStatusCompleted ||
		run.Steps[1].Status != store.StepStatusFailed ||
		run.Steps[2].Status != store.StepStatusPending {
		t.Fatalf("unexpected step statuses: %+v", run.Steps)
	}
}

func TestExecuteUnknownWorkflow(t *testing.T) {
	s := openStore(t)
	_, err := (&Worker{Store: s, Reg: NewRegistry()}).Execute(context.Background(), "nope")
	if err == nil {
		t.Fatal("expected unknown-workflow error")
	}
}

func TestExecuteRunTimeout(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()
	if err := reg.Register(Workflow{
		Name: "slow",
		Steps: []Step{{Name: "stall", Fn: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	w := &Worker{Store: s, Reg: reg, RunTimeout: 50 * time.Millisecond}
	runID, err := w.Execute(context.Background(), "slow")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusFailed {
		t.Fatalf("expected failed run, got %s", run.Status)
	}
}
