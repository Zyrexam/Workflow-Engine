# Workflow Engine — Spec

## Goal
Durable, resumable job orchestration: define multi-step workflows, persist state, execute with retries and timeouts, resume after crash.

## MVP
- Workflow defined as ordered steps (Go functions) or JSON config (`-config`).
- SQLite state store: workflow run + step status.
- Worker executes steps; retry with exponential backoff + jitter.
- Per-step and per-workflow timeouts; overall run deadline.
- Resume incomplete runs on restart (atomic claim, skip completed).
- CLI: `run`, `status`, `list`, `resume`.

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