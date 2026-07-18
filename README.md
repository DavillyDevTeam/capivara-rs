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
| **Status** | **M2 complete** (RetryPolicy, DLQ, terminal Failure, producer idempotency, guarantees docs); M1 Memory + Redis + concurrency |

## What works today (M0–M2)

- Typed **`Task`** trait (`NAME`, `Args`, `Output`, native async `run`)
- **`App`**: `register` / `send` / `send_with_idempotency_key` / `run_worker` / `get_result`
  - optional `with_result_backend`, `with_default_queue`
  - `broker()` for shared broker access / advanced raw `Job` enqueue
  - worker policy: `with_lease` (default **30s**), `with_concurrency` (default **4**),
    `RetryPolicy` via `with_retry_policy` (defaults: **max_attempts 3**, **base_delay 1s**,
    **max_delay 15m**, **equal jitter** on);
    convenience: `with_max_attempts`, `with_nack_delay` (sets `base_delay` only)
- **`MemoryBroker`** + optional **`MemoryResultBackend`**
  - **Single-process only** — not shared across OS processes; not a distributed queue
- Optional **`RedisBroker`** + **`RedisResultBackend`** (`redis` feature)
  - LIST + lease, Lua claim/ack/nack/dead_letter, delayed requeue
  - **lease recover-on-claim**, **claim tokens** (late ack/nack/dead_letter cannot steal a newer claim)
  - results as `{prefix}result:{id}` STRING JSON with **24h TTL**
- Worker concurrency: Tokio tasks limited by a semaphore (default **4**)
- Claim-scoped ownership: each claim issues a `ClaimToken` required by `ack`/`nack`/`dead_letter`
- Panic isolation at the task boundary (worker keeps going)
- CI: fmt, clippy, tests (least-privilege permissions + concurrency)
- Dependabot for Cargo / Actions; secret scanning enabled on the repo

**Delivery, retries, DLQ, results, and idempotency** are spelled out below —
see [Delivery guarantees & failure modes](#delivery-guarantees--failure-modes)
and [docs/guarantees.md](docs/guarantees.md).

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

## Delivery guarantees & failure modes

Capivara is intentionally **not** exactly-once. Design for **at-least-once**
execution and **idempotent** task handlers. Deeper rationale lives in
[docs/guarantees.md](docs/guarantees.md).

### At-least-once delivery

1. Worker **claims** a job under a lease (default **30s** via `App::with_lease`).
2. Each claim gets a **`ClaimToken`**. `ack` / `nack` / `dead_letter` succeed only
   when the token still matches the active claim (late settle after recovery cannot
   steal a newer claim).
3. If the worker crashes or never settles, the lease expires. The next
   **`claim`** recovers expired leases (**recover-on-claim**) and may **redeliver**
   the same job (new token, `attempts` incremented).
4. **Implication:** side effects in `Task::run` may run more than once. Handlers
   should be idempotent (or safe under duplicate work).
5. Crash **after** storing `Success` and **before** `ack` can also redeliver: the
   result backend may be **rewritten** (another Success, or even terminal Failure).
   Visible Success is not a durable “done” bit until the claim is settled with no
   further redelivery—prefer task-side idempotency (and/or external side-effect
   de-dupe) if you need stronger outcome guarantees.

### Terminal Failure only

| Outcome | Broker settle | Result backend (if configured) |
|---|---|---|
| Handler **success** | `ack` | `JobResult::Success` |
| Handler **Err** / panic, `attempts < max_attempts` | `nack(RequeueAfter { delay })` | **nothing** — `get_result` stays `ResultNotFound` |
| Handler **Err** / panic, `attempts >= max_attempts` | `dead_letter(reason)` | `JobResult::Failure` **only if** dead_letter confirmed ownership |
| **Unknown** task name | `dead_letter(...)` (always terminal) | `Failure` only if ownership confirmed |
| Lost lease on settle (`JobNotFound`) | non-fatal; drain continues | **no** Failure write (avoids non-terminal Failure) |

**`JobResult::Failure` means terminal** (exhausted retries / DLQ, or unknown task) —
not “this attempt failed.” Polling `get_result` during retries will see
`ResultNotFound` until success or terminal failure.

### Dead-letter queue (DLQ)

- Per-queue inspect API: `Broker::list_dead(&queue) -> Vec<DeadLetter>` (public fields
  `job`, `reason`).
- Terminal path calls `Broker::dead_letter(id, claim_token, reason)`; job body is
  retained for debugging.
- **No replay / redrive API in M2** — inspect only. Operators re-enqueue manually if needed.
- Redis keys: `{prefix}q:{queue}:dead` LIST of ids; `{prefix}job:{id}:dead_reason`;
  job body kept (no TTL in M2). Memory: in-process per-queue dead list.

### Producer `idempotency_key`

- `App::send_with_idempotency_key::<T>(&args, key)` or set `Job.idempotency_key` on raw enqueue.
- Broker maps `key → JobId`. On a seen key, returns the **existing** id and does **not**
  create a second queue entry (even if the first job is in-flight, done, or dead-lettered).
- Memory: `HashMap`; Redis: `{prefix}idempotency:{key}` SET NX (body-first Lua; no TTL in M2).
- **Key scope:** global per broker (Memory process / Redis `prefix`) — not namespaced by
  task name or queue. Include task/queue in the key string when needed (e.g. `"add:order-42"`).
  Empty / whitespace-only keys are rejected (`EmptyIdempotencyKey`).
- **Scope:** safe **producer** retries only (network blip, client timeout before seeing the id).
- **Does not** make workers exactly-once. Lease recovery can still redeliver a claimed job;
  task handlers must remain idempotent.

### `RetryPolicy` defaults

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | **3** | Max claim attempts before terminal DLQ |
| `base_delay` | **1s** | Delay after attempt 1 (`base * 2^(attempt-1)`) |
| `max_delay` | **15m** | Cap on raw exponential delay (before jitter) |
| `jitter` | **true** | Equal jitter: delay ∈ ≈ `[raw/2, raw]` |

Configure with `App::with_retry_policy`, or convenience `with_max_attempts` /
`with_nack_delay` (sets `base_delay` only). Public constants:
`DEFAULT_MAX_ATTEMPTS`, `DEFAULT_BASE_DELAY`, `DEFAULT_MAX_DELAY`.

A single `run_worker` drain uses non-blocking claim and **does not sleep** for nack
delays, so under default policy it will **not** exhaust retries in one call. Re-invoke
after the delay (tests do multi-pass with sleep), or run a continuous claim loop in
production workers.

### Optional results (fire-and-forget)

- No result backend (`App::new(broker)` only) → worker never stores outcomes;
  `get_result` returns `CapivaraError::NoResultBackend`. This is intentional
  fire-and-forget.
- With a backend: `send` still returns `JobId`; `get_result` reads Success or
  terminal Failure; missing id → `ResultNotFound`.
- The result backend is **not** a monotonic commit log: crash between Success store
  and `ack` can redeliver and rewrite the stored result (see at-least-once above).

## Multi-process (Redis)

Producer and worker are separate OS processes that share Redis:

1. Both connect with the **same** `RedisConfig` (`url` + `prefix`).
2. Producer: `RedisBroker` (+ optional `RedisResultBackend` if it will call `get_result`).
3. Worker: same `RedisBroker` + same `RedisResultBackend`, `register` the same task types, then `run_worker` (or a long-running loop around it).
4. Delivery, retries, DLQ, and producer keys follow the guarantees above.

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
- Metrics / full tracing suite (M3+)

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
