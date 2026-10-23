package store

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
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

func TestSetStepStatusPreservesProgress(t *testing.T) {
	s := openTestStore(t)

	runID, err := s.CreateRun("deploy", []string{"fetch"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, StepStatusRunning, 2, "boom once"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepStatus(runID, 0, StepStatusRunning); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Steps[0].Attempts != 2 || run.Steps[0].LastError != "boom once" {
		t.Fatalf("status-only update must not reset progress: %+v", run.Steps[0])
	}
}

func TestRunNotFound(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.GetRun(42); err == nil {
		t.Fatal("expected not-found error")
	}
}

func TestSetStepProgressPersists(t *testing.T) {
	s := openTestStore(t)

	runID, err := s.CreateRun("deploy", []string{"fetch"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, StepStatusRunning, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStepProgress(runID, 0, StepStatusRunning, 2, "boom: connection reset"); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	st := run.Steps[0]
	if st.Status != StepStatusRunning || st.Attempts != 2 || st.LastError != "boom: connection reset" {
		t.Fatalf("progress not persisted: %+v", st)
	}

	if err := s.SetStepProgress(runID, 0, StepStatusCompleted, 2, ""); err != nil {
		t.Fatal(err)
	}
	run, err = s.GetRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Steps[0].Status != StepStatusCompleted || run.Steps[0].LastError != "" {
		t.Fatalf("completion state wrong: %+v", run.Steps[0])
	}
}

func TestOpenMigratesOldSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "old.db")

	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`
CREATE TABLE runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	workflow TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE steps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id INTEGER NOT NULL REFERENCES runs(id),
	idx INTEGER NOT NULL,
	name TEXT NOT NULL,
	status TEXT NOT NULL
);
INSERT INTO runs (workflow, status) VALUES ('deploy', 'running');
INSERT INTO steps (run_id, idx, name, status) VALUES (1, 0, 'fetch', 'completed');
INSERT INTO steps (run_id, idx, name, status) VALUES (1, 1, 'build', 'running');
`); err != nil {
		t.Fatal(err)
	}
	old.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open with migration: %v", err)
	}
	defer s.Close()

	run, err := s.GetRun(1)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != RunStatusRunning || len(run.Steps) != 2 {
		t.Fatalf("data lost in migration: %+v", run)
	}
	if run.Steps[0].Status != StepStatusCompleted || run.Steps[0].Attempts != 0 {
		t.Fatalf("unexpected migrated step: %+v", run.Steps[0])
	}
	if err := s.SetStepProgress(1, 1, StepStatusRunning, 3, "boom"); err != nil {
		t.Fatalf("new columns not usable after migration: %v", err)
	}
}

func TestDuplicateStepIndexRejected(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.CreateRun("deploy", []string{"fetch", "build"}); err != nil {
		t.Fatal(err)
	}
	res, err := s.db.Exec(`INSERT INTO steps (run_id, idx, name, status) VALUES (1, 0, 'dup', 'pending')`)
	if err == nil {
		t.Fatalf("expected unique violation, got %d", func() int64 { n, _ := res.RowsAffected(); return n }())
	}
}
