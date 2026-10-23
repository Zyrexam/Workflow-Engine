package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type RunStatus string

const (
	RunStatusPending   RunStatus = "pending"
	RunStatusRunning   RunStatus = "running"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
)

type StepStatus string

const (
	StepStatusPending   StepStatus = "pending"
	StepStatusRunning   StepStatus = "running"
	StepStatusCompleted StepStatus = "completed"
	StepStatusFailed    StepStatus = "failed"
)

type Run struct {
	ID       int64
	Workflow string
	Status   RunStatus
	Steps    []StepRun
}

type StepRun struct {
	ID     int64
	RunID  int64
	Index  int
	Name   string
	Status StepStatus
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	workflow TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS steps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id INTEGER NOT NULL REFERENCES runs(id),
	idx INTEGER NOT NULL,
	name TEXT NOT NULL,
	status TEXT NOT NULL
);
`); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) CreateRun(workflow string, stepNames []string) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("create run: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.Exec(`INSERT INTO runs (workflow, status) VALUES (?, ?)`, workflow, RunStatusPending)
	if err != nil {
		return 0, fmt.Errorf("insert run: %w", err)
	}
	runID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("run id: %w", err)
	}
	for i, name := range stepNames {
		if _, err := tx.Exec(`INSERT INTO steps (run_id, idx, name, status) VALUES (?, ?, ?, ?)`,
			runID, i, name, StepStatusPending); err != nil {
			return 0, fmt.Errorf("insert step %q: %w", name, err)
		}
	}
	return runID, tx.Commit()
}

func (s *Store) SetRunStatus(runID int64, status RunStatus) error {
	res, err := s.db.Exec(`UPDATE runs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, status, runID)
	if err != nil {
		return fmt.Errorf("set run status: %w", err)
	}
	return requireRow("run", runID, res)
}

func (s *Store) SetStepStatus(runID int64, stepIndex int, status StepStatus) error {
	res, err := s.db.Exec(`UPDATE steps SET status = ? WHERE run_id = ? AND idx = ?`, status, runID, stepIndex)
	if err != nil {
		return fmt.Errorf("set step status: %w", err)
	}
	return requireRow("step", stepIndex, res)
}

func (s *Store) GetRun(runID int64) (Run, error) {
	run := Run{ID: runID}
	err := s.db.QueryRow(`SELECT workflow, status FROM runs WHERE id = ?`, runID).
		Scan(&run.Workflow, &run.Status)
	if err == sql.ErrNoRows {
		return run, fmt.Errorf("run %d not found", runID)
	}
	if err != nil {
		return run, fmt.Errorf("query run: %w", err)
	}
	rows, err := s.db.Query(`SELECT id, idx, name, status FROM steps WHERE run_id = ? ORDER BY idx`, runID)
	if err != nil {
		return run, fmt.Errorf("query steps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		st := StepRun{RunID: runID}
		var status StepStatus
		if err := rows.Scan(&st.ID, &st.Index, &st.Name, &status); err != nil {
			return run, fmt.Errorf("scan step: %w", err)
		}
		st.Status = status
		run.Steps = append(run.Steps, st)
	}
	return run, rows.Err()
}

func requireRow(kind string, id any, res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s %v not found", kind, id)
	}
	return nil
}
