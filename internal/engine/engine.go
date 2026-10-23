package engine

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Zyrexam/workflowengine/internal/store"
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
	return names
}

type Worker struct {
	Store *store.Store
	Reg   *Registry
	Log   *slog.Logger
}

func (w *Worker) Execute(ctx context.Context, name string) (int64, error) {
	log := w.Log
	if log == nil {
		log = slog.Default()
	}

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

	for i, st := range wf.Steps {
		if err := w.Store.SetStepStatus(runID, i, store.StepStatusRunning); err != nil {
			return runID, err
		}
		if stepErr := st.Fn(ctx); stepErr != nil {
			if err := w.Store.SetStepStatus(runID, i, store.StepStatusFailed); err != nil {
				return runID, err
			}
			if err := w.Store.SetRunStatus(runID, store.RunStatusFailed); err != nil {
				return runID, err
			}
			log.Error("step failed", "run", runID, "step", st.Name, "error", stepErr)
			return runID, fmt.Errorf("step %d %q: %w", i, st.Name, stepErr)
		}
		if err := w.Store.SetStepStatus(runID, i, store.StepStatusCompleted); err != nil {
			return runID, err
		}
	}

	if err := w.Store.SetRunStatus(runID, store.RunStatusCompleted); err != nil {
		return runID, err
	}
	log.Info("run completed", "run", runID, "workflow", name)
	return runID, nil
}
