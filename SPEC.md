# Workflow Engine — Spec

## Goal
Durable, resumable job orchestration: define multi-step workflows, persist state, execute with retries and timeouts, resume after crash.

## MVP
- Workflow defined as ordered steps (Go functions) or JSON config (`-config`), with per-step and per-workflow timeouts.
- SQLite state store: workflow run + step status, attempts, last error, lease/heartbeat, created/updated timestamps.
- Worker executes steps; retry with exponential backoff + jitter; atomic run claim; overall run deadline.
- Resume incomplete runs on restart (skip completed, re-run pending/running/failed).
- CLI: `run`, `status`, `list`, `resume` (with `-timeout`, `-config`, `-status`, `-limit`, `-offset`).

## Non-goals (v1)
- Distributed workers, exactly-once, UI, cron triggers, multi-tenant.

## Stack
Go, SQLite (modernc, pure Go), slog.

## Milestones
1. Single-step run persisted to SQLite. (done)
2. Multi-step sequential execution. (done)
3. Retry + backoff. (done)
4. Crash recovery on startup. (done)

## Success criteria
Kill process mid-workflow; restart resumes from last completed step.