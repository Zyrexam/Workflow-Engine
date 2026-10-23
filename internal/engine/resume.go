package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/Zyrexam/Workflow-Engine/internal/store"
)

// ErrRunCompleted is returned by Resume when the run is already completed
// (a no-op was performed).
var ErrRunCompleted = errors.New("run already completed")

// Resume continues a run from persisted state only: every step recorded
// completed is skipped; pending/running/failed steps are re-run with the
// retry policy. A completed run is a no-op reported through ErrRunCompleted.
func (w *Worker) Resume(ctx context.Context, runID int64) (int64, error) {
	if err := w.validate(); err != nil {
		return 0, err
	}
	log := w.log()

	run, err := w.Store.GetRun(runID)
	if err != nil {
		return 0, err
	}
	if run.Status == store.RunStatusCompleted {
		return runID, ErrRunCompleted
	}
	if run.Status != store.RunStatusPending && run.Status != store.RunStatusRunning && run.Status != store.RunStatusFailed {
		return runID, fmt.Errorf("run %d status %q not resumable", runID, run.Status)
	}

	wf, ok := w.Reg.Get(run.Workflow)
	if !ok {
		return runID, fmt.Errorf("workflow %q of run %d is no longer registered (registered: %v)",
			run.Workflow, runID, w.Reg.List())
	}
	if len(wf.Steps) != len(run.Steps) {
		return runID, fmt.Errorf("workflow %q changed shape: %d steps registered, %d recorded for run %d",
			run.Workflow, len(wf.Steps), len(run.Steps), runID)
	}

	log.Info("resume started", "run", runID, "workflow", run.Workflow, "run_status", run.Status)
	if err := w.Store.SetRunStatus(runID, store.RunStatusRunning); err != nil {
		return runID, err
	}

	start := len(run.Steps)
	for i, st := range run.Steps {
		if st.Status != store.StepStatusCompleted {
			start = i
			break
		}
	}
	if start == len(run.Steps) {
		if err := w.Store.SetRunStatus(runID, store.RunStatusCompleted); err != nil {
			return runID, err
		}
		log.Info("run completed", "run", runID, "workflow", run.Workflow)
		return runID, nil
	}

	attemptsBase := make([]int, len(run.Steps))
	for i, st := range run.Steps {
		attemptsBase[i] = st.Attempts
	}
	for i := start; i < len(run.Steps); i++ {
		if err := w.Store.SetStepProgress(runID, i, store.StepStatusPending, attemptsBase[i], ""); err != nil {
			return runID, err
		}
	}

	if err := w.runSteps(ctx, log, runID, wf.Steps[start:], attemptsBase[start:], start); err != nil {
		return runID, err
	}
	if err := w.Store.SetRunStatus(runID, store.RunStatusCompleted); err != nil {
		return runID, err
	}
	log.Info("run completed", "run", runID, "workflow", run.Workflow)
	return runID, nil
}
