# capivara

> **Under construction — not production-released.**  
> Version **`0.0.1`** with **`publish = false`** (Cargo will refuse `cargo publish`).  
> APIs may change until a formal release is announced.

**capivara** is a Rust-idiomatic **job / worker library** with a Celery-like *topology*
(enqueue → broker → worker → optional results). It is **not** a Celery clone and **not**
a universal CLI that runs arbitrary remote code.

| | |
|---|---|
| **Package** | `capivara` (repo: [`capivara-rs`](https://github.com/DavillyDevTeam/capivara-rs)) |
| **Org** | [DavillyDevTeam](https://github.com/DavillyDevTeam) |
| **License** | MIT OR Apache-2.0 |
| **Status** | M2 (in progress): DLQ + terminal Failure + producer idempotency; M1 Memory + Redis + concurrency |

## What works today (M0–M2)

- Typed **`Task`** trait (`NAME`, `Args`, `Output`, native async `run`)
- **`App`**: `register` / `send` / `send_with_idempotency_key` / `run_worker` / `get_result`
  - optional `with_result_backend`, `with_default_queue`
  - `broker()` for shared broker access / advanced raw `Job` enqueue
  - worker policy: `with_lease` (default **30s**), `with_concurrency` (default **4**),
    `RetryPolicy` via `with_retry_policy` (defaults: **max_attempts 3**, **base_delay 1s**,
    **max_delay 15m**, **equal jitter** on);
    convenience: `with_max_attempts`, `with_nack_delay` (sets `base_delay` only)
  - **producer idempotency**: optional key on enqueue returns the existing `JobId` (no
    duplicate queue entry). For safe **producer retries** only — **at-least-once** still
    applies for in-flight worker crashes; tasks should remain idempotent
- **`MemoryBroker`** + optional **`MemoryResultBackend`**
  - **Single-process only** — not shared across OS processes; not a distributed queue
- Optional **`RedisBroker`** + **`RedisResultBackend`** (`redis` feature)
  - LIST + lease, Lua claim/ack/nack, delayed requeue
  - **lease recover-on-claim**, **claim tokens** (late ack/nack/dead_letter cannot steal a newer claim)
  - results as `{prefix}result:{id}` STRING JSON with **24h TTL**
- Worker concurrency: Tokio tasks limited by a semaphore (default **4**)
- Worker retry policy: task `Err` / panic → **no intermediate Failure store** →
  `nack(RequeueAfter)` with **exponential backoff + equal jitter** until
  `max_attempts`, then `dead_letter(reason)` and store terminal `Failure` only if
  ownership was confirmed (unknown task is always terminal DLQ; lost-lease settle
  is non-fatal and does **not** write Failure)
- Per-queue **dead-letter list** (`Broker::dead_letter` / `list_dead`); job body kept for inspect; **no replay** in M2
- Claim-scoped ownership: each claim issues a `ClaimToken` required by `ack`/`nack`/`dead_letter`
- Panic isolation at the task boundary (worker keeps going)
- Results: `send` → `JobId`; `get_result` only if a backend is configured
  (stores **Success** and **terminal Failure only**); errors clearly if no backend / missing id
- CI: fmt, clippy, tests (least-privilege permissions + concurrency)
- Dependabot for Cargo / Actions; secret scanning enabled on the repo

## Features

| Feature | Default | What it enables |
|---|---|---|
| *(none)* | yes | `MemoryBroker` / `MemoryResultBackend` |
| `redis` | **opt-in** | `RedisBroker` + `RedisResultBackend` (multi-process capable) |

```toml
capivara = { version = "0.0.1", features = ["redis"] }
```

Redis integration tests (`cargo test --features redis`) use **testcontainers** when
`REDIS_URL` is unset. For local runs without a working testcontainers Docker socket:

```bash
docker run -d --rm -p 6379:6379 docker.io/library/redis:7-alpine
REDIS_URL=redis://127.0.0.1:6379/ cargo test --features redis
```

## Multi-process (Redis)

Producer and worker are separate OS processes that share Redis:

1. Both connect with the **same** `RedisConfig` (`url` + `prefix`).
2. Producer: `RedisBroker` (+ optional `RedisResultBackend` if it will call `get_result`).
3. Worker: same `RedisBroker` + same `RedisResultBackend`, `register` the same task types, then `run_worker` (or a long-running loop around it).
4. At-least-once delivery: a crashed worker’s claim expires (default lease **30s**); **recover-on-claim** requeues the job. Tasks should be **idempotent**. Failures retry with exponential `nack` delay (`RetryPolicy`: base **1s**, max **15m**, equal jitter) up to `max_attempts` (default **3**), then dead-letter; panics count as failures. Intermediate retries do not store `JobResult::Failure`.
5. **Producer retries**: use `App::send_with_idempotency_key` (or set `Job.idempotency_key` on raw enqueue). The broker maps `key → JobId` (Memory: HashMap; Redis: `{prefix}idempotency:{key}` SET NX, body-first Lua) and returns the existing id on collision. Keys are **global per broker prefix** — include task/queue in the key string when needed. Empty keys are rejected. This does **not** cancel at-least-once worker redelivery after a crash mid-execution.

```rust
// Producer process
use capivara::{App, RedisBroker, RedisConfig, RedisResultBackend, Task, /* ... */};

let config = RedisConfig::new("redis://127.0.0.1/").with_prefix("myapp:");
let broker = RedisBroker::connect(config.clone()).await?;
let results = RedisResultBackend::connect(config).await?;
let app = App::new(broker).with_result_backend(results);
// register, send, get_result ...
```

```rust
// Worker process — same url, prefix, and Task impls
let app = App::new(broker)
    .with_result_backend(results)
    .with_concurrency(4); // default; clamp ≥ 1
app.register::<MyTask>().await?;
app.run_worker(None).await?;
```

## Not yet

- DLQ replay / redrive API
- Proc-macro or `app.task("name", fn)` sugar
- crates.io publish (`publish = false` until then)

## Quick example

This is a **library** example. Your app needs **Tokio** (async runtime) and **serde**
for task args; `serde_json` is already pulled in by capivara for payloads.

```rust
use capivara::{App, JobResult, MemoryBroker, MemoryResultBackend, Task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddArgs { x: i32, y: i32 }

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AddResult { sum: i32 }

struct Add;

impl Task for Add {
    const NAME: &'static str = "add";
    type Args = AddArgs;
    type Output = AddResult;

    async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        Ok(AddResult { sum: args.x + args.y })
    }
}

#[tokio::main]
async fn main() -> capivara::Result<()> {
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await?;
    let id = app.send::<Add>(&AddArgs { x: 2, y: 3 }).await?;
    // `None` = drain the in-memory queue; `Some(n)` = process at most n jobs
    app.run_worker(None).await?;
    match app.get_result(id).await? {
        JobResult::Success { payload } => {
            let out: AddResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.sum, 5);
        }
        JobResult::Failure { message } => panic!("{message}"),
    }
    Ok(())
}
```

## Development

```bash
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Git hooks (recommended):

```bash
pre-commit install
pre-commit run --all-files
```

## Security

See [SECURITY.md](SECURITY.md) — report vulnerabilities via GitHub Private Vulnerability Reporting
(not public issues).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Prefer pull requests against `main` (branch protection
requires CI). Merged feature branches are deleted on GitHub; keep local branches if you want.
