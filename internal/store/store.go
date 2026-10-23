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
	ID        int64
	Workflow  string
	Status    RunStatus
	CreatedAt string
	UpdatedAt string
	Steps     []StepRun
}

type StepRun struct {
	ID        int64
	RunID     int64
	Index     int
	Name      string
	Status    StepStatus
	Attempts  int
	LastError string
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
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA busy_timeout=5000;`,
		`PRAGMA foreign_keys=ON;`,
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply %s: %w", pragma, err)
		}
	}
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func initSchema(db *sql.DB) error {
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	workflow TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	lease_token TEXT,
	lease_expires_at DATETIME
);
CREATE TABLE IF NOT EXISTS steps (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	idx INTEGER NOT NULL,
	name TEXT NOT NULL,
	status TEXT NOT NULL
);
`); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	cols, err := tableColumns(db, "steps")
	if err != nil {
		return err
	}
	migrations := []struct {
		column  string
		addDDL  string
		missing bool
	}{
		{column: "attempts", addDDL: `ALTER TABLE steps ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`},
		{column: "last_error", addDDL: `ALTER TABLE steps ADD COLUMN last_error TEXT`},
	}
	// run-level lease columns (migration for older DBs)
	runCols, err := tableColumns(db, "runs")
	if err != nil {
		return err
	}
	runMigrations := []struct {
		column string
		addDDL string
	}{
		{column: "lease_token", addDDL: `ALTER TABLE runs ADD COLUMN lease_token TEXT`},
		{column: "lease_expires_at", addDDL: `ALTER TABLE runs ADD COLUMN lease_expires_at DATETIME`},
	}
	for _, m := range runMigrations {
		if runCols[m.column] {
			continue
		}
		if _, err := db.Exec(m.addDDL); err != nil {
			return fmt.Errorf("migrate runs: add %s: %w", m.column, err)
		}
	}
	for _, m := range migrations {
		if cols[m.column] {
			continue
		}
		if _, err := db.Exec(m.addDDL); err != nil {
			return fmt.Errorf("migrate steps: add %s: %w", m.column, err)
		}
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_steps_run_order ON steps(run_id, idx)`); err != nil {
		return fmt.Errorf("init unique index: %w", err)
	}
	return nil
}

func tableColumns(db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, fmt.Errorf("table_info %s: %w", table, err)
	}
	defer rows.Close()
	cols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, fmt.Errorf("scan table_info: %w", err)
		}
		cols[name] = true
	}
	return cols, rows.Err()
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

func (s *Store) AcquireLease(runID int64) (bool, error) {
	token := fmt.Sprintf("%d", runID) // simple token; expiry is what matters
	res, err := s.db.Exec(`UPDATE runs SET lease_token = ?, lease_expires_at = datetime('now','+30 seconds'), updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND (lease_expires_at IS NULL OR lease_expires_at < CURRENT_TIMESTAMP)`,
		token, runID)
	if err != nil {
		return false, fmt.Errorf("acquire lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) ClaimRunForResume(runID int64) (bool, error) {
	token := fmt.Sprintf("resume-%d", runID)
	res, err := s.db.Exec(`UPDATE runs SET status = ?, lease_token = ?, lease_expires_at = datetime('now','+30 seconds'), updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND status IN (?, ?, ?) AND (lease_expires_at IS NULL OR lease_expires_at < CURRENT_TIMESTAMP)`,
		RunStatusRunning, token, runID, RunStatusPending, RunStatusRunning, RunStatusFailed)
	if err != nil {
		return false, fmt.Errorf("claim run: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) SetRunStatus(runID int64, status RunStatus) error {
	var res sql.Result
	var err error
	if status == RunStatusCompleted || status == RunStatusFailed {
		res, err = s.db.Exec(`UPDATE runs SET status = ?, updated_at = CURRENT_TIMESTAMP, lease_expires_at = CURRENT_TIMESTAMP WHERE id = ?`, status, runID)
	} else {
		res, err = s.db.Exec(`UPDATE runs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, status, runID)
	}
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

func (s *Store) SetStepProgress(runID int64, stepIndex int, status StepStatus, attempts int, lastError string) error {
	res, err := s.db.Exec(`UPDATE steps SET status = ?, attempts = MAX(attempts, ?), last_error = ? WHERE run_id = ? AND idx = ?`,
		status, attempts, lastError, runID, stepIndex)
	if err != nil {
		return fmt.Errorf("set step status: %w", err)
	}
	if err := requireRow("step", stepIndex, res); err != nil {
		return err
	}
	// heartbeat: extend the run lease only if one is held, so setup writes
	// (which never acquired a lease) don't masquerade as an active owner.
	_, _ = s.db.Exec(`UPDATE runs SET lease_expires_at = datetime('now','+30 seconds'), updated_at = CURRENT_TIMESTAMP WHERE id = ? AND lease_token IS NOT NULL`, runID)
	return nil
}

func (s *Store) ListRuns(status string, limit, offset int) ([]Run, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT id, workflow, status, created_at, updated_at FROM runs`
	args := []any{}
	if status != "" {
		q += ` WHERE status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.Workflow, &r.Status, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetRun(runID int64) (Run, error) {
	run := Run{ID: runID}
	err := s.db.QueryRow(`SELECT workflow, status, created_at, updated_at FROM runs WHERE id = ?`, runID).
		Scan(&run.Workflow, &run.Status, &run.CreatedAt, &run.UpdatedAt)
	if err == sql.ErrNoRows {
		return run, fmt.Errorf("run %d not found", runID)
	}
	if err != nil {
		return run, fmt.Errorf("query run: %w", err)
	}
	rows, err := s.db.Query(`SELECT id, idx, name, status, attempts, last_error FROM steps WHERE run_id = ? ORDER BY idx`, runID)
	if err != nil {
		return run, fmt.Errorf("query steps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		st := StepRun{RunID: runID}
		var status StepStatus
		var lastError sql.NullString
		if err := rows.Scan(&st.ID, &st.Index, &st.Name, &status, &st.Attempts, &lastError); err != nil {
			return run, fmt.Errorf("scan step: %w", err)
		}
		st.Status = status
		st.LastError = lastError.String
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
