# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### M4 multi-broker path (in progress)

Stabilize the shared `Broker` contract and land an **experimental** RabbitMQ spike.
**Kafka is not planned.** Package remains **`0.0.1`** with **`publish = false`**.

### Added

#### Experimental RabbitMQ broker spike (M4-2)

- Opt-in Cargo feature **`rabbitmq`**: [`RabbitBroker`] / [`RabbitConfig`] via
  [lapin](https://docs.rs/lapin) (default features stay free of lapin).
- Implements `Broker`: enqueue (JSON publish), claim (`basic_get` + poll for
  `block_for`), ack / nack / dead_letter with process-local claim-token → delivery
  ownership, DLQ side queue `{prefix}{queue}:dead`, delayed nack via TTL+DLX hop on
  `{prefix}{queue}:delayed`, best-effort `list_dead`.
- **Honest gaps** (not Redis parity) documented in [`docs/BROKER.md`](docs/BROKER.md):
  no timed lease/recover (`lease` ignored), process-local claim tokens and
  `idempotency_key`, best-effort `list_dead`, no hot-path queue-depth metric.
  Multi-process workers are broker-native (shared AMQP URL + prefix).
- Integration tests `tests/rabbitmq_broker.rs` (testcontainers RabbitMQ image, or
  `RABBITMQ_URL` / `AMQP_URL`); worker happy path with `MemoryResultBackend`.

#### Broker capability matrix (M4-1)

- [`docs/BROKER.md`](docs/BROKER.md): frozen capability matrix for **Memory** vs **Redis**
  (enqueue, claim+block, lease/recover, delayed nack, DLQ, `list_dead`, producer
  `idempotency_key`, multi-process, queue-depth metric) plus **RabbitMQ** experimental
  row and explicit **Kafka not planned**.
- `Broker` trait / module docs: multi-broker contract, settle rules, claim loop order
  (recover → promote delayed → claim); no API breaks.
- README links to the matrix; Rabbit called out as experimental with gaps.

### M3 observability suite (complete)

End-to-end observability for the library shape: **`tracing`** spans on lifecycle paths,
Prometheus-ready **`metrics`** facade counters/histograms, and an optional **`metrics-http`**
scrape endpoint. Architecture + failure-mode docs close the milestone. Package remains
**`0.0.1`** with **`publish = false`**.

**Release note:** discuss a formal **`0.1.0`** crates.io release **with the maintainer**
before publishing. Do not flip `publish = true` or bump to 0.1.0 unilaterally after M3.

### Added

#### Architecture + observability docs (M3-4)

- README **Architecture** mermaid (producer → Memory/Redis broker → worker → optional results).
- README **Failure modes in 10 minutes**: lease-expire double-run, intermediate
  `ResultNotFound`, DLQ inspect-only, metrics `status=dead` vs `failure`.
- README **Observability** how-to: tracing subscriber + `RUST_LOG`; metrics table;
  `metrics-http` `serve` / `start_metrics_server` snippet and security notes.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): topology, components, claim/lease
  sequence, observability surfaces, non-goals.
- Status line marks **M3 complete**.

#### Prometheus scrape endpoint (M3-3)

- Optional Cargo feature **`metrics-http`**: installs a Prometheus recorder and serves
  a scrape HTTP endpoint via [`metrics-exporter-prometheus`](https://docs.rs/metrics-exporter-prometheus)
  (HTTP listener only; no push-gateway).
- Public API: [`capivara::metrics_http::serve`] / `start_metrics_server(SocketAddr) -> JoinHandle<()>`
  (requires a Tokio runtime). Default bind **`127.0.0.1:9090`**.
- Documents **no auth** in v0, loopback default, and **one global recorder** per process.
- Integration test `tests/metrics_http.rs` (ephemeral loopback port) behind the feature.
- Default builds do **not** pull hyper / the exporter.

#### Metrics via `metrics` facade (M3-2)

- Always-on [`metrics`](https://docs.rs/metrics) dependency (facade only; no recorder/HTTP forced).
- Counters/histograms (Prometheus-ready names; never label by `job_id`):
  - `capivara_jobs_enqueued_total{queue,task_name}`
  - `capivara_jobs_completed_total{queue,task_name,status=success|failure|dead}`
  - `capivara_job_duration_seconds{task_name}`
  - `capivara_claim_wait_seconds{queue}`
  - `capivara_queue_depth{queue}` — best-effort; **Memory** updates from pending length;
    **Redis does not LLEN** on the hot path
- Wired at `App` enqueue and Worker claim/completion paths (`src/metrics.rs` helpers).
- Unit + integration tests with `metrics-util` `DebuggingRecorder`.
- README **Observability → Metrics** note.

#### Tracing spans (M3-1)

- Always-on [`tracing`](https://docs.rs/tracing) dependency (facade only; no subscriber forced).
- Spans on core paths: `capivara.enqueue`, `capivara.claim`, `capivara.handle`,
  `capivara.ack`, `capivara.nack`, `capivara.dead_letter`, `capivara.get_result`.
- Fields when available: `job.id`, `task.name`, `queue`, `attempt` (no payloads/secrets).
- Instrumented at App/Worker orchestration so Memory and Redis share the same coverage.
- README **Observability** section: install a subscriber + `RUST_LOG` example.

### M2 reliability suite (complete)

End-to-end reliability for the library shape: shared **RetryPolicy**, per-queue **DLQ**,
**terminal-only Failure**, producer **idempotency_key**, and explicit **delivery guarantees**
docs. Behavior remains **at-least-once** (lease + recover-on-claim may redeliver); tasks
should be **idempotent**. Optional result backend → fire-and-forget when unset.

### Added

#### Documentation (M2-4)

- README section **Delivery guarantees & failure modes**: at-least-once, terminal Failure
  only, DLQ `list_dead` (no replay), producer `idempotency_key` scope, `RetryPolicy`
  defaults, optional results / fire-and-forget.
- [`docs/guarantees.md`](docs/guarantees.md): architecture decision notes aligned with
  actual APIs (`Broker`, `RetryPolicy`, `JobResult`, claim tokens).

#### Producer idempotency (M2-3)

- Optional `Job.idempotency_key` (`#[serde(default)]`) and `App::send_with_idempotency_key`.
  On enqueue with a key, Memory/Redis return the existing `JobId` if the key was already
  seen (no duplicate queue entry). Redis uses `{prefix}idempotency:{key}` SET NX (Lua: job
  body SET first, then NX, then LPUSH; NX loss deletes the orphan body). Keys are global
  per broker/prefix (caller should namespace by task/queue when needed). Empty/whitespace
  keys → `EmptyIdempotencyKey`. **At-least-once still applies** for in-flight worker
  crashes — the key is for safe producer retries only.

#### Dead-letter + terminal Failure (M2-2)

- **Per-queue dead-letter queue (DLQ)**: `Broker::dead_letter(id, claim_token, reason)` and
  `Broker::list_dead(queue)` (inspect only; **no replay** in M2). Job body retained.
  - Memory: in-process per-queue dead list with reason.
  - Redis: `{prefix}q:{queue}:dead` LIST of job ids; `{prefix}job:{id}:dead_reason`; job body kept (no TTL in M2).
- Public type `DeadLetter { job, reason }`.

#### RetryPolicy (M2-1)

- **`RetryPolicy`** (shared across Memory/Redis worker paths): exponential backoff with optional
  **equal jitter**. Defaults: `max_attempts` **3**, `base_delay` **1s**, `max_delay` **15m**,
  `jitter` **true**. Worker nack delay is `delay_for_attempt(job.attempts)`.
- `App::with_retry_policy`; `with_max_attempts` / `with_nack_delay` remain as convenience
  mutators (`with_nack_delay` sets `base_delay` only).
- Public defaults: `DEFAULT_MAX_ATTEMPTS`, `DEFAULT_BASE_DELAY`, `DEFAULT_MAX_DELAY`.

#### M1 and earlier (retained)

- **`RedisResultBackend`** (`redis` feature): `{prefix}result:{id}` STRING JSON `JobResult`,
  default TTL **24h** (`EX 86400`); shares [`RedisConfig`] with the broker.
- `CapivaraError::ResultBackend` for result-backend I/O (distinct from `Broker`).
- Worker **concurrency** via Tokio `Semaphore` (default **4**); `App::with_concurrency` (clamped ≥ 1).
- Redis integration: full roundtrip with `RedisBroker` + `RedisResultBackend`; concurrency smoke.
- Memory concurrency smoke test (several jobs with concurrency 4).
- README multi-process notes (producer + worker, same prefix).
- Lease **recover-on-claim** for `RedisBroker` (Lua) and `MemoryBroker` (expired `in_flight` → pending).
- **Claim tokens**: lease member `{queue}\x1f{id}\x1f{token}`; `ack`/`nack`/`dead_letter` require matching token so late settle cannot steal a reclaimed claim.
- Redis claim **atomically INCRs** `{prefix}attempts:{id}` with lease (attempt counter independent of body JSON).
- Worker treats `JobNotFound` on settle as non-fatal (drain continues after lost lease).
- Worker delayed-nack policy: task `Err`/panic retries via `nack(RequeueAfter)` until
  `max_attempts` (default **3**), then terminal **dead_letter**. Defaults: lease **30s**.
- `App::with_lease` / `with_max_attempts` (clamped ≥ 1) / `with_nack_delay` for worker policy.
- Optional Cargo feature `redis` with `RedisBroker` + `RedisResultBackend` (LIST + lease, Lua claim/ack/nack/dead_letter, delayed requeue).
- Extended `Broker` trait: `claim(queues, lease, block_for)`, `nack(RequeueAfter)`; `ClaimedJob`.
- testcontainers Redis integration tests (`tests/redis_broker.rs`), with `REDIS_URL` override.
- Typed `Task` trait with native async `run`, `App::register` / `send` / `run_worker` / `get_result`.
- `MemoryBroker` and optional `MemoryResultBackend` (in-process; success and terminal failure stored).
- Worker panic isolation via `tokio::spawn` join errors.
- Integration tests for success, task error, panic isolation, missing result backend,
  bad JSON payload, `max_jobs`, `ResultNotFound`, fire-and-forget drain, unknown task name,
  DLQ / terminal-only Failure, retry-then-success, producer idempotency, and `with_default_queue`.
- `App::broker()` for shared broker access (tests / raw `Job` injection).
- Repository skeleton: dual MIT OR Apache-2.0 license, README (WIP), security policy,
  contributing guide, GitHub Actions CI (fmt, clippy, test), Dependabot config.
- CI least-privilege `permissions` and `concurrency` cancel-in-progress.

### Changed

- **Terminal-only `JobResult::Failure`**: intermediate retries no longer store Failure;
  only max-attempts exhaustion and unknown-task outcomes write Failure (if a result backend is set),
  and only **after** a successful `dead_letter` (lost-lease races skip Failure so it stays ≈ terminal).
- Terminal outcomes use `dead_letter` (not bare `ack`) so failed jobs are inspectable on the DLQ.
- Nack requeue delay is no longer a fixed **5s**; it follows [`RetryPolicy`] exponential
  schedule (base **1s**, cap **15m**, equal jitter on by default).
- Worker drain is concurrent (default 4 in-flight); claim tokens remain per-job.
- Redis lease ZSET members use `{queue}\x1f{id}\x1f{token}`; delayed remains `{queue}\x1f{id}`.
- Package version set to **`0.0.1`** with **`publish = false`** until a real release.
- Apache-2.0 license appendix copyright filled in for Duarte Mainart Tecnologia e Publicidade LTDA.
- README status marks **M2 complete** for the reliability suite; multi-process notes cross-link
  the guarantees section instead of duplicating policy text.
- README status marks **M3 complete** for the observability suite (tracing, metrics facade,
  optional scrape) and links architecture / failure-mode docs.
