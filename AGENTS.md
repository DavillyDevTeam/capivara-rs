# AGENTS.md — capivara

Instructions for humans and coding agents working on this repository.

## Project

- **Crate:** `capivara` — Rust job/worker **library** (not a CLI product).
- **Topology:** enqueue → broker → worker → optional result backend (Celery-like *system shape*).
- **API direction:** typed `Task` trait (`register::<T>()`, `send::<T>(&args)`), JSON serde payloads, optional results via `JobId` + `get_result`.
- **Not goals (v0):** Celery protocol interop, pickle, shipping functions over the wire, universal installable worker for third-party code.

## Layout (target)

```text
src/
  lib.rs app.rs job.rs error.rs task.rs registry.rs
  broker/  result/  worker/
tests/
```

## Commands

```bash
cargo test
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

CI runs fmt check, clippy (`-D warnings`), and tests on every PR.

## Pull requests

- Branch from `main`, open a PR (direct pushes to `main` should be blocked by protection).
- Prefer **squash** merge.
- Keep changes focused; explain design tradeoffs in the PR body.
- Do not commit secrets or proprietary data.

## Style

- No `unsafe` unless explicitly justified (crate forbids it by default).
- Library errors: structured (`thiserror`); keep public API small and documented.
- Prefer tests that run **without Redis** for core logic (memory backends).
