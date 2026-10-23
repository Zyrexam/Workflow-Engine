package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

type StepFunc func(ctx context.Context) error

type Step struct {
	Name string
	Fn   StepFunc
}

type Workflow struct {
	Name  string
	Steps []Step
}

type Registry struct {
	workflows map[string]Workflow
}

func NewRegistry() *Registry {
	return &Registry{workflows: make(map[string]Workflow)}
}

func (r *Registry) Register(w Workflow) error {
	if w.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if len(w.Steps) == 0 {
		return fmt.Errorf("workflow %q has no steps", w.Name)
	}
	for _, st := range w.Steps {
		if st.Name == "" {
			return fmt.Errorf("workflow %q has a step with no name", w.Name)
		}
	}
	if _, exists := r.workflows[w.Name]; exists {
		return fmt.Errorf("workflow %q already registered", w.Name)
	}
	r.workflows[w.Name] = w
	return nil
}

func (r *Registry) Get(name string) (Workflow, bool) {
	w, ok := r.workflows[name]
	return w, ok
}

func (r *Registry) List() []string {
	names := make([]string, 0, len(r.workflows))
	for name := range r.workflows {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type Worker struct {
	Store      *store.Store
	Reg        *Registry
	Log        *slog.Logger
	Retry      *RetryPolicy
	RunTimeout time.Duration // 0 = no overall run deadline
}

func (w *Worker) validate() error {
	if w.Store == nil {
		return errors.New("engine: worker has no store")
	}
	if w.Reg == nil {
		return errors.New("engine: worker has no registry")
	}
	return nil
}

func (w *Worker) policy() *RetryPolicy {
	p := w.Retry
	if p == nil {
		p = &RetryPolicy{}
	}
	return p.withDefaults()
}

func (w *Worker) log() *slog.Logger {
	if w.Log == nil {
		return slog.Default()
	}
	return w.Log
}

// Execute starts a fresh run of the named workflow and drives every step
// through the retry policy.
func (w *Worker) Execute(ctx context.Context, name string) (int64, error) {
	if err := w.validate(); err != nil {
		return 0, err
	}
	if w.RunTimeout > 0 {
		c, cancel := context.WithTimeout(ctx, w.RunTimeout)
		defer cancel()
		ctx = c
	}
	log := w.log()

	wf, ok := w.Reg.Get(name)
	if !ok {
		return 0, fmt.Errorf("unknown workflow %q (registered: %v)", name, w.Reg.List())
	}

	stepNames := make([]string, len(wf.Steps))
	for i, st := range wf.Steps {
		stepNames[i] = st.Name
	}
	runID, err := w.Store.CreateRun(wf.Name, stepNames)
	if err != nil {
		return 0, err
	}
	log.Info("run started", "run", runID, "workflow", name)

	if err := w.Store.SetRunStatus(runID, store.RunStatusRunning); err != nil {
		return runID, err
	}
	if err := w.runSteps(ctx, log, runID, wf.Steps, make([]int, len(wf.Steps)), 0); err != nil {
		return runID, err
	}
	if err := w.Store.SetRunStatus(runID, store.RunStatusCompleted); err != nil {
		return runID, err
	}
	log.Info("run completed", "run", runID, "workflow", name)
	return runID, nil
}

// runSteps drives steps in order, starting each with attemptsBase[i] prior
// attempts recorded in the store; indexBase is the run's absolute index of
// steps[0] (0 for a fresh Execute, first unfinished step for Resume).
// Persisted progress writes through on every attempt transition. First
// failure that is not retryable (or that exhausts MaxAttempts) fails the
// run and aborts.
func (w *Worker) runSteps(ctx context.Context, log *slog.Logger, runID int64, steps []Step, attemptsBase []int, indexBase int) error {
	pol := w.policy()
	for i, st := range steps {
		runIndex := indexBase + i
		attempts := attemptsBase[i]
		for {
			attempts++
			if err := w.Store.SetStepProgress(runID, runIndex, store.StepStatusRunning, attempts, ""); err != nil {
				return err
			}
			stepErr := st.Fn(ctx)
			if stepErr == nil {
				if err := w.Store.SetStepProgress(runID, runIndex, store.StepStatusCompleted, attempts, ""); err != nil {
					return err
				}
				break
			}
			if err := w.Store.SetStepProgress(runID, runIndex, store.StepStatusRunning, attempts, stepErr.Error()); err != nil {
				return err
			}
			if !pol.IsRetryable(stepErr) || attempts >= pol.MaxAttempts {
				return w.failRun(log, runID, runIndex, st.Name, attempts, stepErr)
			}
			if sleepErr := pol.Sleep(ctx, pol.Backoff(attempts)); sleepErr != nil {
				return w.failRun(log, runID, runIndex, st.Name, attempts,
					fmt.Errorf("backoff canceled: %w", sleepErr))
			}
		}
	}
	return nil
}

func (w *Worker) failRun(log *slog.Logger, runID int64, stepIndex int, stepName string, attempts int, stepErr error) error {
	if err := w.Store.SetStepProgress(runID, stepIndex, store.StepStatusFailed, attempts, stepErr.Error()); err != nil {
		return err
	}
	if err := w.Store.SetRunStatus(runID, store.RunStatusFailed); err != nil {
		return err
	}
	log.Error("step failed", "run", runID, "step", stepName, "attempts", attempts, "error", stepErr)
	return fmt.Errorf("step %d %q: %w", stepIndex, stepName, stepErr)
}
