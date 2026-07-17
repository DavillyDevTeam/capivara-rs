# AGENTS.md — capivara

Instructions for humans and coding agents working on this repository.

## Project

- **Crate:** `capivara` — Rust job/worker **library** (not a CLI product).
- **Topology:** enqueue → broker → worker → optional result backend (Celery-like *system shape*).
- **API:** typed `Task` trait (`register::<T>()`, `send::<T>(&args)`), JSON serde payloads, optional results via `JobId` + `get_result`.
- **Not goals (v0):** Celery protocol interop, pickle, shipping functions over the wire, universal installable worker for third-party code.

## Layout

```text
src/
  lib.rs app.rs job.rs error.rs task.rs registry.rs metrics.rs
  metrics_http.rs   # behind feature metrics-http
  broker/  result/  worker/
docs/
  ARCHITECTURE.md  guarantees.md
tests/
  memory_roundtrip.rs  metrics_record.rs  metrics_http.rs (feature metrics-http)
```

## Commands

```bash
cargo test
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

CI runs fmt check, clippy (`-D warnings`), and tests on every PR.

## Pull requests

- Branch from `main`, open a PR (direct pushes to `main` are blocked).
- Prefer **squash** merge.
- Keep changes focused; explain design tradeoffs in the PR body.
- Do not commit secrets or proprietary data.

## Style

- No `unsafe` (crate forbids it).
- Library errors: `thiserror` (`CapivaraError` / `TaskError`).
- Prefer tests that run **without Redis** for core logic (memory backends).
- `Task::run` uses native async-in-trait; `Broker` / `ResultBackend` use `async_trait` for `dyn` object safety.
