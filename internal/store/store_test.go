package store

import (
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreateRunAndSteps(t *testing.T) {
	s := openTestStore(t)

	runID, err := s.CreateRun("deploy", []string{"fetch", "build"})
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Workflow != "deploy" || run.Status != RunStatusPending {
		t.Fatalf("unexpected run: %+v", run)
	}
	if len(run.Steps) != 2 || run.Steps[0].Name != "fetch" || run.Steps[0].Status != StepStatusPending {
		t.Fatalf("unexpected steps: %+v", run.Steps)
	}
	if _, err := s.GetRun(999); err == nil {
		t.Fatal("expected error for missing run")
	}
}

func TestStatusTransitionsPersist(t *testing.T) {
	s := openTestStore(t)

	runID, err := s.CreateRun("deploy", []string{"fetch"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetRunStatus(runID, RunStatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepStatus(runID, 0, StepStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepStatus(runID, 5, StepStatusCompleted); err == nil {
		t.Fatal("expected error for out-of-range step")
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != RunStatusRunning || run.Steps[0].Status != StepStatusCompleted {
		t.Fatalf("statuses not persisted: %+v", run)
	}
}

func TestRunNotFound(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.GetRun(42); err == nil {
		t.Fatal("expected not-found error")
	}
}
