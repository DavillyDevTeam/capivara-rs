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
| **Status** | **M3 complete**; M4 multi-broker path (Broker matrix + experimental RabbitMQ spike); Memory + Redis + experimental Rabbit |

## Architecture

Celery-like *topology* (not Celery protocol). Deeper map: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

```mermaid
flowchart LR
  subgraph Producer
    P[App::send]
  end

  subgraph Broker["Broker"]
    MB[(MemoryBroker)]
    RB[(RedisBroker)]
    QB[(RabbitBroker experimental)]
  end

  subgraph Worker
    W[claim → handle → ack / nack / DLQ]
  end

  subgraph Results["Optional ResultBackend"]
    MR[(MemoryResultBackend)]
    RR[(RedisResultBackend)]
  end

  P --> MB
  P --> RB
  P --> QB
  MB --> W
  RB --> W
  QB --> W
  W --> MR
  W --> RR
  P -.->|get_result| MR
  P -.->|get_result| RR
```

| | Memory (default) | Redis (`redis` feature) | RabbitMQ (`rabbitmq`, **experimental**) |
|---|---|---|---|
| **Process model** | Single process only | Multi-process (shared `url` + `prefix`) | Multi-process (shared AMQP URL + prefix) |
| **Broker** | `MemoryBroker` | `RedisBroker` (LIST + lease, Lua) | `RabbitBroker` (lapin; **no** timed lease) |
| **Results** | `MemoryResultBackend` | `RedisResultBackend` (JSON STRING, 24h TTL) | Use Memory/Redis results; no Rabbit result backend |
| **Use** | Unit tests, in-process apps | Producers + workers across machines | Spike / multi-worker experiments only |

Omit a result backend for **fire-and-forget**. Delivery is **at-least-once** — see guarantees below.

**Broker capability matrix** (enqueue, claim+block, lease/recover, delayed nack, DLQ,
`list_dead`, producer idempotency — Memory vs Redis vs experimental Rabbit; Kafka not planned):
[docs/BROKER.md](docs/BROKER.md).

## What works today (M0–M4 partial)

- Typed **`Task`** trait (`NAME`, `Args`, `Output`, native async `run`)
- **Sync / blocking handlers** — [`SyncTask`](src/task_sync.rs) (blanket `Task` via
  `spawn_blocking`) and [`run_blocking`](src/task_sync.rs) for hybrid async bodies;
  see [Blocking / sync tasks](#blocking--sync-tasks) and `examples/sync_task.rs`
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
- **`tracing`** spans on core paths (`capivara.enqueue`, `claim`, `handle`, `ack`, `nack`,
  `dead_letter`, `get_result`) — library emits only; apps install a subscriber
- **`metrics`** facade counters/histograms (Prometheus-ready names); optional
  **`metrics-http`** scrape server (`GET /metrics` on `127.0.0.1:9090` by default)
- CI: fmt, clippy, tests (least-privilege permissions + concurrency)
- Dependabot for Cargo / Actions; secret scanning enabled on the repo

**Delivery, retries, DLQ, results, and idempotency** are spelled out below —
see [Failure modes in 10 minutes](#failure-modes-in-10-minutes),
[Delivery guarantees & failure modes](#delivery-guarantees--failure-modes),
[docs/guarantees.md](docs/guarantees.md), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md),
and [docs/BROKER.md](docs/BROKER.md) (capability matrix).

## Failure modes in 10 minutes

Capivara is **at-least-once**, not exactly-once. Design handlers for duplicate work.
Full rationale: [docs/guarantees.md](docs/guarantees.md).

| What you see | What happened | What to do |
|---|---|---|
| **Same job runs twice** | Lease expired before `ack`/`nack`/`dead_letter`; recover-on-claim redelivered with a **new** `ClaimToken` and `attempts++`. Late settle from the first claim loses (`JobNotFound`) and does not steal the newer claim. | Make `Task::run` **idempotent** (or external de-dupe). Prefer short work relative to `with_lease` (default **30s**). Producer `idempotency_key` only de-dupes **enqueue**, not worker redelivery. |
| **`get_result` → `ResultNotFound` mid-flight** | Intermediate retries **do not** store Failure. No result yet means “still pending / retrying / never stored,” not “failed forever.” | Poll until `Success` or terminal `Failure`, or treat missing as in-progress when the job is still live. Without a result backend you always get `NoResultBackend`. |
| **Job on DLQ / `list_dead`** | Attempts exhausted (`attempts >= max_attempts`, default **3**) or **unknown task name**. Body + reason retained for inspect. | Inspect `Broker::list_dead(&queue)`. **No redrive API in M2/M3** — re-enqueue manually if needed. Terminal `JobResult::Failure` is written only if `dead_letter` confirmed ownership. |
| **Metrics `status="dead"`** | `capivara_jobs_completed_total{status="dead"}` — claim was **dead-lettered** (terminal). Contrast: `status="failure"` means **nacked for retry**, not terminal. | Alert on `dead` (and DLQ depth) for real give-ups; do not treat `failure` as permanent. Lost-lease races do **not** increment completion counters. |

Other sharp edges worth one line each:

- Crash **after** storing `Success` and **before** `ack` can redeliver and **rewrite** the stored result.
- One `run_worker` drain does **not** sleep for nack delays — re-invoke after delay, or run a continuous loop in production.
- Redis `capivara_queue_depth` is **not** updated on the hot path (no `LLEN` under load); Memory updates pending length.

## Features

| Feature | Default | What it enables |
|---|---|---|
| *(none)* | yes | `MemoryBroker` / `MemoryResultBackend` |
| `redis` | **opt-in** | `RedisBroker` + `RedisResultBackend` (multi-process capable) |
| `rabbitmq` | **opt-in, experimental** | `RabbitBroker` (lapin). **Not production-ready** — see [docs/BROKER.md](docs/BROKER.md) gaps |
| `metrics-http` | **opt-in** | Prometheus scrape HTTP server (`capivara::metrics_http`) |

```toml
capivara = { version = "0.0.1", features = ["redis"] }
# experimental RabbitMQ broker (not Redis parity; see docs/BROKER.md):
# capivara = { version = "0.0.1", features = ["rabbitmq"] }
# optional scrape endpoint (pulls metrics-exporter-prometheus + hyper):
# capivara = { version = "0.0.1", features = ["metrics-http"] }
```

Default features stay free of Redis and lapin.

Redis integration tests (`cargo test --features redis`) use **testcontainers** when
`REDIS_URL` is unset. For local runs without a working testcontainers Docker socket:

```bash
docker run -d --rm -p 6379:6379 docker.io/library/redis:7-alpine
REDIS_URL=redis://127.0.0.1:6379/ cargo test --features redis
```

RabbitMQ integration tests (`cargo test --features rabbitmq`) use **testcontainers**
when `RABBITMQ_URL` / `AMQP_URL` is unset:

```bash
docker run -d --rm -p 5672:5672 docker.io/library/rabbitmq:3.8-management
RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/%2f cargo test --features rabbitmq
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

## Observability

How-to for apps embedding capivara. Structural notes: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#observability-surfaces).

Capivara is a **library**: it emits spans and metrics through facades. **Your binary**
installs the tracing subscriber and/or metrics recorder (or uses the optional scrape helper).

### Tracing (subscriber in your binary)

Always-on [`tracing`](https://docs.rs/tracing) dependency. Spans on core paths:

| Span name | Where |
|---|---|
| `capivara.enqueue` | `App::send` / `send_with_idempotency_key` |
| `capivara.claim` | Worker claim loop (only when a job is claimed; empty polls are silent) |
| `capivara.handle` | Per-job process (handler + settle); duration is full processing, not pure handler CPU |
| `capivara.ack` | Successful settle |
| `capivara.nack` | Retry requeue settle |
| `capivara.dead_letter` | Terminal DLQ settle |
| `capivara.get_result` | `App::get_result` |

Common fields (when available): `job.id`, `task.name`, `queue`, `attempt`.
Payloads and secrets (e.g. Redis URL passwords) are **never** logged.

Raw `App::broker().enqueue` is intentionally **uninstrumented** (escape hatch) — prefer
`App::send` / `send_with_idempotency_key` if you want `capivara.enqueue` spans. Handler
futures spawned for panic isolation re-attach the current `capivara.handle` span so
app-authored spans inside `Task::run` still parent correctly.

```toml
# Cargo.toml of your binary (not required by the library):
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
// In main, before App / worker work:
tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();
```

```bash
RUST_LOG=capivara=info,info cargo run
# or quieter: RUST_LOG=capivara=debug,warn
```

### Metrics (facade + optional scrape)

Always-on [`metrics`](https://docs.rs/metrics) facade. The core library does **not**
install a global recorder. Wire an exporter yourself, or enable **`metrics-http`**.

| Metric | Type | Labels |
|---|---|---|
| `capivara_jobs_enqueued_total` | counter | `queue`, `task_name` |
| `capivara_jobs_completed_total` | counter | `queue`, `task_name`, `status` |
| `capivara_job_duration_seconds` | histogram | `task_name` |
| `capivara_claim_wait_seconds` | histogram | `queue` |
| `capivara_queue_depth` | gauge | `queue` (best-effort) |

`status` values on `capivara_jobs_completed_total`:

| Label | Meaning |
|---|---|
| `success` | Handler succeeded; claim **acked** |
| `failure` | Handler failed this attempt; claim **nacked for retry** (not terminal) |
| `dead` | Claim **dead-lettered** (terminal: max attempts / unknown task) |

Recorded only when settle confirms claim ownership (lost-lease races do not inflate counters).
**Never** label by `job_id`. Constants: [`capivara::metrics`](src/metrics.rs).

**Queue depth:** `MemoryBroker` updates from in-process pending length. Redis does **not**
`LLEN` on the claim/enqueue hot path — sample out-of-band if needed.

#### Optional scrape endpoint (`metrics-http`)

```toml
capivara = { version = "0.0.1", features = ["metrics-http"] }
```

```rust
// Requires a Tokio runtime. Call once at process startup.
use capivara::metrics_http;

#[tokio::main]
async fn main() -> capivara::Result<()> {
    // Default: 127.0.0.1:9090 — scrape GET /metrics
    let _metrics = metrics_http::serve()?;
    // Or: metrics_http::start_metrics_server("127.0.0.1:9100".parse()?)?;

    // ... build App, register tasks, run_worker ...
    Ok(())
}
```

```bash
curl -s http://127.0.0.1:9090/metrics | head
cargo test --features metrics-http
```

- Installs the **global** Prometheus recorder (only one per process; a second install errors).
- Prometheus text exposition (scrape `GET /metrics`; exporter accepts any path).
- Default bind: **`127.0.0.1:9090`** ([`metrics_http::DEFAULT_BIND`](src/metrics_http.rs)).
- **v0 security:** no authentication. Keep loopback or isolate the scrape network;
  put TLS/auth at a reverse proxy if you expose it.

## Not yet

- **RabbitMQ production polish** — experimental `RabbitBroker` exists (`rabbitmq` feature)
  but is **not** Redis parity (no timed lease/recover, process-local claim tokens /
  idempotency, best-effort `list_dead`); see [docs/BROKER.md](docs/BROKER.md)
- **Kafka** is **not planned**
- DLQ replay / redrive API
- Proc-macro or `app.task("name", fn)` sugar
- crates.io publish — stay at **`0.0.1`** / **`publish = false`** until the maintainer
  agrees a **`0.1.0`** release (discuss after M3; do not publish unilaterally)

## Blocking / sync tasks

Workers run on Tokio. **Do not** call `std::thread::sleep`, blocking `std::fs`, or
heavy CPU loops directly inside `async fn Task::run` — that stalls the runtime and
hurts concurrency.

Two DX helpers (no proc-macro):

| Helper | When to use |
|---|---|
| **`SyncTask`** | Entire handler is sync/blocking. Implement `fn run(...) -> Result<...>` (no `async`). A blanket `Task` impl runs it with `tokio::task::spawn_blocking`. `register::<T>()` / `send::<T>()` work unchanged. |
| **`run_blocking(f, args).await`** | Hand-written async `Task::run` that only needs a blocking section (or a one-off closure). |

```rust
use capivara::{SyncTask, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct WorkArgs { n: u64 }

#[derive(Serialize, Deserialize)]
struct WorkOut { sum: u64 }

struct BlockingWork;

impl SyncTask for BlockingWork {
    const NAME: &'static str = "blocking_work";
    type Args = WorkArgs;
    type Output = WorkOut;

    fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        // Runs on the blocking pool — sleep / CPU / sync I/O are OK.
        let sum: u64 = (0..args.n).sum();
        Ok(WorkOut { sum })
    }
}
// app.register::<BlockingWork>().await?;
// app.send::<BlockingWork>(&WorkArgs { n: 100 }).await?;
```

Full runnable sample: `cargo run --example sync_task`.

**Note:** both `Task` and `SyncTask` expose `run`. To call the sync body directly,
use UFCS: `<T as SyncTask>::run(args)`. Prefer `register` / `send` for normal use.

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
