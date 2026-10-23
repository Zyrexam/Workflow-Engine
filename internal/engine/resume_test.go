package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

func openEngineStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

type recorder struct {
	calls []string
}

func (r *recorder) step(name string, failN int) Step {
	return Step{Name: name, Fn: func(ctx context.Context) error {
		r.calls = append(r.calls, name)
		return nil
	}}
}

// TestResumeSkipsCompletedSteps: step0 completed in the store; resume must
// not call its Fn.
func TestResumeSkipsCompletedSteps(t *testing.T) {
	s := openEngineStore(t)
	reg := NewRegistry()
	rec := &recorder{}

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			rec.step("first", 0),
			rec.step("second", 0),
		},
	}); err != nil {
		t.Fatal(err)
	}

	runID, err := s.CreateRun("demo", []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, store.StepStatusCompleted, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 1, store.StepStatusPending, 0, ""); err != nil {
		t.Fatal(err)
	}

	_, err = (&Worker{Store: s, Reg: reg, Log: nil}).Resume(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusCompleted {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if len(rec.calls) != 1 || rec.calls[0] != "second" {
		t.Fatalf("resume ran wrong steps: %v", rec.calls)
	}
}

// TestResumeResolvesFromStore: fresh Worker (no shared memory), run marked
// completed through store only; resume finishes it.
func TestResumeResolvesFromStore(t *testing.T) {
	s := openEngineStore(t)
	reg := NewRegistry()
	rec := &recorder{}

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "first", Fn: func(ctx context.Context) error { return nil }},
			rec.step("second", 0),
		},
	}); err != nil {
		t.Fatal(err)
	}

	runID, err := s.CreateRun("demo", []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetRunStatus(runID, store.RunStatusRunning); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, store.StepStatusCompleted, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 1, store.StepStatusRunning, 2, "stale"); err != nil {
		t.Fatal(err)
	}

	// Fresh worker: no in-memory context. Stale "running" step2 must re-run.
	if _, err := (&Worker{Store: s, Reg: reg}).Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusCompleted || run.Steps[1].Status != store.StepStatusCompleted {
		t.Fatalf("resume did not complete run: %+v", run)
	}
	if got := run.Steps[1].LastError; got != "" {
		t.Fatalf("stale error not cleared: %q", got)
	}
}

func TestResumeCompletedIsNoop(t *testing.T) {
	s := openEngineStore(t)
	reg := NewRegistry()
	rec := &recorder{}

	if err := reg.Register(Workflow{
		Name:  "demo",
		Steps: []Step{rec.step("only", 0)},
	}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{Store: s, Reg: reg}
	runID, err := w.Execute(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Resume(context.Background(), runID)
	if !errors.Is(err, ErrRunCompleted) {
		t.Fatalf("expected ErrRunCompleted, got %v", err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("resume re-executed steps: %v", rec.calls)
	}
}

func TestResumeFreshTransitions(t *testing.T) {
	s := openEngineStore(t)
	reg := NewRegistry()

	var statuses []store.StepStatus
	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "restarted", Fn: func(ctx context.Context) error {
				run, err := s.GetRun(1)
				if err != nil {
					return err
				}
				statuses = append(statuses, run.Steps[0].Status)
				return nil
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	runID, err := s.CreateRun("demo", []string{"restarted"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, store.StepStatusFailed, 2, "crash mid-run"); err != nil {
		t.Fatal(err)
	}

	if _, err := (&Worker{Store: s, Reg: reg}).Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusCompleted {
		t.Fatalf("run: %+v", run)
	}
	if got := run.Steps[0].Attempts; got != 3 {
		t.Fatalf("attempts carried over should reach 3, got %d", got)
	}
	if run.Steps[0].LastError != "" {
		t.Fatalf("recovered step still has last error: %q", run.Steps[0].LastError)
	}
}

func TestResumeBackoffCancelMarksCorrectStep(t *testing.T) {
	s := openEngineStore(t)
	reg := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "done", Fn: func(ctx context.Context) error { return nil }},
			{Name: "target", Fn: func(ctx context.Context) error { return fmt.Errorf("boom") }},
			{Name: "later", Fn: func(ctx context.Context) error { t.Fatal("never reached"); return nil }},
		},
	}); err != nil {
		t.Fatal(err)
	}

	runID, err := s.CreateRun("demo", []string{"done", "target", "later"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, store.StepStatusCompleted, 1, ""); err != nil {
		t.Fatal(err)
	}

	pol := &RetryPolicy{
		MaxAttempts: 2,
		Backoff: func(attempt int) time.Duration { return 10 * time.Second },
		Sleep: func(ctx context.Context, d time.Duration) error {
			cancel()
			return SleepWithContext(ctx, d)
		},
		IsRetryable: IsRetryable,
	}
	// Regression: the sleep-cancel path once marked the slice index instead of
	// the absolute run index, failing "done" instead of "target".
	_, err = (&Worker{Store: s, Reg: reg, Retry: pol}).Resume(ctx, runID)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Steps[0].Status != store.StepStatusCompleted {
		t.Fatalf("wrong step marked failed: %+v", run.Steps[0])
	}
	if run.Steps[1].Status != store.StepStatusFailed || run.Steps[1].LastError == "" {
		t.Fatalf("target step not marked failed with error: %+v", run.Steps[1])
	}
}

func TestResumeUnknownRun(t *testing.T) {
	s := openEngineStore(t)
	_, err := (&Worker{Store: s, Reg: NewRegistry()}).Resume(context.Background(), 99)
	if err == nil {
		t.Fatal("expected error for unknown run")
	}
}

func TestResumeUnknownWorkflow(t *testing.T) {
	s := openEngineStore(t)
	runID, err := s.CreateRun("ghost", []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetRunStatus(runID, store.RunStatusRunning); err != nil {
		t.Fatal(err)
	}
	_, err = (&Worker{Store: s, Reg: NewRegistry()}).Resume(context.Background(), runID)
	if err == nil {
		t.Fatal("expected error for unregistered workflow")
	}
}

func TestResumeAllCompletedMarksRunCompleted(t *testing.T) {
	s := openEngineStore(t)
	reg := NewRegistry()
	rec := &recorder{}

	if err := reg.Register(Workflow{
		Name:  "demo",
		Steps: []Step{rec.step("only", 0)},
	}); err != nil {
		t.Fatal(err)
	}

	runID, err := s.CreateRun("demo", []string{"only"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, store.StepStatusCompleted, 1, ""); err != nil {
		t.Fatal(err)
	}

	// Crash after last step completed but before the run row was updated.
	if _, err := (&Worker{Store: s, Reg: reg}).Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusCompleted {
		t.Fatalf("run: %+v", run)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("steps re-ran despite all completed: %v", rec.calls)
	}
}
