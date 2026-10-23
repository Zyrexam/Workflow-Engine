package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

type permanentError struct{}

func (permanentError) Error() string   { return "permanent: validation failed" }
func (permanentError) Retryable() bool { return false }

type retryPolicyRecorder struct {
	sleeps []time.Duration
	fails  map[string]int
}

func newTestPolicy(rec *retryPolicyRecorder, maxAttempts int) *RetryPolicy {
	if rec.fails == nil {
		rec.fails = make(map[string]int)
	}
	return &RetryPolicy{
		MaxAttempts: maxAttempts,
		Backoff:     func(attempt int) time.Duration { return time.Duration(attempt) * time.Second },
		Sleep: func(ctx context.Context, d time.Duration) error {
			rec.sleeps = append(rec.sleeps, d)
			return nil
		},
		IsRetryable: IsRetryable,
	}
}

func countingStep(rec *retryPolicyRecorder, name string, failN int) Step {
	return Step{Name: name, Fn: func(ctx context.Context) error {
		rec.fails[name]++
		if rec.fails[name] <= failN {
			return fmt.Errorf("transient failure %d", rec.fails[name])
		}
		return nil
	}}
}

func TestExecuteRetriesThenSucceeds(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()
	rec := &retryPolicyRecorder{}

	if err := reg.Register(Workflow{
		Name:  "demo",
		Steps: []Step{countingStep(rec, "flaky", 1)},
	}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{Store: s, Reg: reg, Retry: newTestPolicy(rec, 3)}
	runID, err := w.Execute(context.Background(), "demo")
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
	if got := run.Steps[0].Attempts; got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
	if len(rec.sleeps) != 1 {
		t.Fatalf("expected 1 backoff sleep, got %d", len(rec.sleeps))
	}
}

func TestExecuteBackoffGrows(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()
	rec := &retryPolicyRecorder{}

	if err := reg.Register(Workflow{
		Name:  "demo",
		Steps: []Step{countingStep(rec, "flaky", 3)},
	}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{Store: s, Reg: reg, Retry: newTestPolicy(rec, 4)}
	if _, err := w.Execute(context.Background(), "demo"); err != nil {
		t.Fatal(err)
	}

	want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}
	for i, d := range want {
		if rec.sleeps[i] != d {
			t.Fatalf("sleep %d = %v, want %v", i, rec.sleeps[i], d)
		}
	}
	if run, err := s.GetRun(1); err != nil {
		t.Fatal(err)
	} else if run.Steps[0].Attempts != 4 {
		t.Fatalf("attempts = %d, want 4", run.Steps[0].Attempts)
	}
}

func TestExecuteExhaustsRetries(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()
	rec := &retryPolicyRecorder{}

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "always-fails", Fn: func(ctx context.Context) error { return fmt.Errorf("boom") }},
		},
	}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{Store: s, Reg: reg, Retry: newTestPolicy(rec, 3)}
	runID, err := w.Execute(context.Background(), "demo")
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusFailed || run.Steps[0].Status != store.StepStatusFailed {
		t.Fatalf("unexpected final state: %+v", run)
	}
	if got := run.Steps[0].Attempts; got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if got := run.Steps[0].LastError; got == "" {
		t.Fatal("last error not persisted")
	}
	if len(rec.sleeps) != 2 {
		t.Fatalf("expected 2 sleeps (after attempts 1, 2), got %d", len(rec.sleeps))
	}
}

func TestExecutePermanentErrorFailsFast(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()
	rec := &retryPolicyRecorder{}

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "bad", Fn: func(ctx context.Context) error { return permanentError{} }},
		},
	}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{Store: s, Reg: reg, Retry: newTestPolicy(rec, 3)}
	runID, err := w.Execute(context.Background(), "demo")
	if err == nil {
		t.Fatal("expected error from permanent failure")
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Steps[0].Status != store.StepStatusFailed || run.Steps[0].Attempts != 1 {
		t.Fatalf("permanent error should fail fast: %+v", run.Steps[0])
	}
	if len(rec.sleeps) != 0 {
		t.Fatalf("no sleep expected for permanent error, got %d", len(rec.sleeps))
	}
}

func TestExecuteRetryWrappedPermanentError(t *testing.T) {
	if IsRetryable(fmt.Errorf("wrapped: %w", permanentError{})) {
		t.Fatal("wrapped permanent error must not be retryable")
	}
	if !IsRetryable(fmt.Errorf("wrapped: %w", errors.New("unclassified"))) {
		t.Fatal("unclassified errors default to retryable")
	}
}

func TestBackoffRespectsContext(t *testing.T) {
	s := openStore(t)
	reg := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())

	if err := reg.Register(Workflow{
		Name: "demo",
		Steps: []Step{
			{Name: "always-fails", Fn: func(ctx context.Context) error { return fmt.Errorf("boom") }},
		},
	}); err != nil {
		t.Fatal(err)
	}

	pol := &RetryPolicy{
		MaxAttempts: 3,
		Backoff:     func(attempt int) time.Duration { return 10 * time.Second },
		Sleep: func(ctx context.Context, d time.Duration) error {
			cancel()
			return SleepWithContext(ctx, d)
		},
		IsRetryable: IsRetryable,
	}
	w := &Worker{Store: s, Reg: reg, Retry: pol}
	runID, err := w.Execute(ctx, "demo")
	if err == nil {
		t.Fatal("expected context error")
	}

	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.RunStatusFailed || run.Steps[0].Status != store.StepStatusFailed {
		t.Fatalf("canceled run state not persisted: %+v", run)
	}
	if got := run.Steps[0].LastError; got == "" {
		t.Fatal("cancellation error not persisted")
	}
}

func TestWorkerRequiresStoreAndRegistry(t *testing.T) {
	if _, err := (&Worker{}).Execute(context.Background(), "x"); err == nil {
		t.Fatal("expected error for nil store/registry")
	}
}
