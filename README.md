# Distributed Workflow Engine

A durable, resumable job orchestration engine written in Go. Define multi-step workflows as ordered Go functions, persist run/step state in SQLite, execute with retries and exponential backoff, and resume incomplete runs after a crash — all from a small CLI.

> Stack: Go, SQLite (pure-Go `modernc.org/sqlite`, no CGO), `log/slog`.

---

## Features

- **Ordered multi-step workflows** — declare `[]Step{Name, Fn}`; steps run strictly in order.
- **Durable state** — every run and step is persisted to SQLite as it progresses (`pending → running → completed/failed`).
- **Retries with exponential backoff + jitter** — per-step configurable budget; retryable vs. permanent error classification; context-aware waits.
- **Crash recovery** — `resume <run-id>` skips completed steps and continues from persisted state; completed runs are no-ops.
- **Overall run deadline** — optional `RunTimeout` aborts a run that exceeds its budget.
- **CLI** — `run`, `status`, `resume`, with distinct exit codes for usage vs. runtime errors.
- **Pure Go** — no C toolchain; cross-platform builds.

## Quick start

```bash
# build
go build ./...

# run the built-in "hello" workflow
go run ./cmd/workflow run hello

# inspect a run
go run ./cmd/workflow status 1

# resume after a crash
go run ./cmd/workflow resume 1
```

Override the database path with `-db`:

```bash
go run ./cmd/workflow -db /tmp/state.db run hello
```

## CLI reference

```
workflow [-db path] run <workflow-name>
workflow [-db path] status <run-id>
workflow [-db path] resume <run-id>
```

| Command  | Description                                              |
|----------|----------------------------------------------------------|
| `run`    | Start a new run of a registered workflow and execute it.  |
| `status` | Print a run and its per-step statuses from the store.     |
| `resume` | Resume an in-progress (or failed) run from persisted state.|

Exit codes: `0` success, `1` runtime failure, `2` bad invocation.

## Request / workflow flow

### 1. `run <workflow>`

```
┌──────────┐   parse args    ┌────────────┐
│  CLI     │ ──────────────► │  Registry  │  (lookup workflow by name)
└──────────┘                 └─────┬──────┘
                                  │ Workflow{Steps}
                                  ▼
                          ┌───────────────┐
                          │  Store        │  CreateRun: insert run (pending)
                          │  (SQLite)     │  + one row per step (pending)
                          └───────┬───────┘
                                  │ runID
                                  ▼
                          ┌───────────────┐
                          │  Worker       │  SetRunStatus(running)
                          │  Execute      │
                          └───────┬───────┘
                                  │
            ┌─────────────────────┼─────────────────────┐
            ▼                     ▼                     ▼
      step 0 (running)      step 1 (running)      step N (running)
       │ Fn(ctx)              │ Fn(ctx)              │ Fn(ctx)
       ├─ ok → completed     ├─ ok → completed      ├─ ok → completed
       └─ err → retry/backoff└─ err → retry/backoff └─ err → retry/backoff
                                  │
                                  ▼
                          SetRunStatus(completed | failed)
                                  │
                                  ▼
                          printRun(run)  → stdout
```

### 2. `status <run-id>`

```
CLI ──► ParseRunID ──► Store.GetRun(runID)
                          │
                          ├── runs row  (workflow, status)
                          └── steps rows (idx, name, status, attempts, last_error)
                                  │
                                  ▼
                          printRun(run)  → stdout
```

No execution happens; purely a read of persisted state.

### 3. `resume <run-id>` (crash recovery)

```
CLI ──► Store.GetRun(runID)
          │
          ├── status == completed  ──► ErrRunCompleted (no-op, report)
          ├── not resumable        ──► error
          └── pending/running/failed
                │
                ▼
        SetRunStatus(running)
                │
                ▼
        startIndex = first step whose status != completed
                │
                ├── all completed ──► SetRunStatus(completed) (no-op steps)
                │
                └── reset steps[start:] → pending (keep attempts)
                          │
                          ▼
                  runSteps(ctx, steps[start:], attemptsBase[start:], start)
                          │
                          ▼
                  SetRunStatus(completed | failed)
```

Recovery resolves the next step **purely from the store** — no in-memory state is assumed. Completed steps are never re-executed.

## Workflow simulation (example)

A two-step workflow that fails once then succeeds on retry:

```go
reg := engine.NewRegistry()
reg.Register(engine.Workflow{
    Name: "deploy",
    Steps: []engine.Step{
        {Name: "fetch", Fn: func(ctx context.Context) error {
            // idempotent fetch
            return nil
        }},
        {Name: "build", Fn: func(ctx context.Context) error {
            // may fail transiently
            return nil
        }},
    },
})

w := &engine.Worker{
    Store:      st,
    Reg:        reg,
    Retry:      engine.DefaultRetryPolicy(), // 3 attempts, 1s/2s/4s backoff (capped, jittered)
    RunTimeout: 5 * time.Minute,
}
runID, err := w.Execute(context.Background(), "deploy")
```

Simulated `status` output after a mid-run crash and `resume`:

```
Run 42 (deploy): completed
  [ 0] fetch            completed
  [ 1] build            completed
```

During execution (before completion):

```
Run 42 (deploy): running
  [ 0] fetch            completed
  [ 1] build            running
```

## Architecture

```
cmd/workflow/
  main.go        CLI: flags, arg dispatch, exit codes, printRun
  workflows.go   built-in workflow registrations

internal/engine/
  engine.go      Registry, Worker (Execute, runSteps, failRun, validate, policy)
  resume.go      Worker.Resume (crash recovery)
  retry.go       RetryPolicy, IsRetryable, DefaultBackoff, SleepWithContext
  parse.go       ParseRunID
  *_test.go

internal/store/
  store.go       SQLite store: Open, CreateRun, SetRunStatus, SetStepStatus,
                 SetStepProgress, GetRun, Close + schema migration
  *_test.go
```

Layering: `main` wires dependencies; `engine` owns orchestration; `store` owns persistence. Anything under `internal/` is import-protected.

## Data model

```sql
CREATE TABLE runs (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  workflow   TEXT NOT NULL,
  status     TEXT NOT NULL,   -- pending/running/completed/failed
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE steps (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id     INTEGER NOT NULL REFERENCES runs(id),
  idx        INTEGER NOT NULL,   -- 0-based position in declared order
  name       TEXT NOT NULL,
  status     TEXT NOT NULL,      -- pending/running/completed/failed
  attempts   INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);
```

- `CreateRun` inserts the run + all steps in one transaction (all-or-nothing).
- Row-per-step created up front: executing a step is an `UPDATE`, never an `INSERT`.
- Schema is migrated on `Open` (new columns/indexes added to existing DBs).

## Retry & backoff

- Per-step configurable budget (default 3 attempts, cap ~8–10).
- Exponential backoff: `1s × 2^(attempt-1)`, capped at 30s, with full jitter.
- Backoff waits respect `context.Context` (cancellable mid-backoff).
- Retryable vs. permanent: errors implementing `Retryable() bool` are classified; unclassified errors default to retryable.
- Step functions must be **idempotent** — retries re-run the entire step.
- Attempt count and last error persisted per step; `status` reflects progress mid-retry.

## Crash recovery

- Resolve the next step purely from persisted store state (no in-memory assumptions).
- Skip every step recorded `completed`; re-run `pending`/`running`/`failed` from the pickup point.
- Resuming a `completed` run is a no-op (`ErrRunCompleted`).
- A step left `running` after a crash is re-run (only `completed` is skipped).

## Testing

```bash
go test ./...          # all tests
go test -race ./...    # with race detector
go vet ./...           # static checks
go build ./...         # compile
```

Tests use `t.TempDir()` for isolated SQLite files and injectable backoff/sleep so retry tests run in milliseconds.