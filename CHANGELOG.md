# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Producer idempotency key**: optional `Job.idempotency_key` (`#[serde(default)]`) and
  `App::send_with_idempotency_key`. On enqueue with a key, Memory/Redis return the existing
  `JobId` if the key was already seen (no duplicate queue entry). Redis uses
  `{prefix}idempotency:{key}` SET NX (Lua with SET job + LPUSH). **At-least-once still
  applies** for in-flight worker crashes — the key is for safe producer retries only.
- **Per-queue dead-letter queue (DLQ)**: `Broker::dead_letter(id, claim_token, reason)` and
  `Broker::list_dead(queue)` (inspect only; **no replay** in M2). Job body retained.
  - Memory: in-process per-queue dead list with reason.
  - Redis: `{prefix}q:{queue}:dead` LIST of job ids; `{prefix}job:{id}:dead_reason`; job body kept (no TTL in M2).
- Public type `DeadLetter { job, reason }`.
- **`RetryPolicy`** (shared across Memory/Redis worker paths): exponential backoff with optional
  **equal jitter**. Defaults: `max_attempts` **3**, `base_delay` **1s**, `max_delay` **15m**,
  `jitter` **true**. Worker nack delay is `delay_for_attempt(job.attempts)`.
- `App::with_retry_policy`; `with_max_attempts` / `with_nack_delay` remain as convenience
  mutators (`with_nack_delay` sets `base_delay` only).
- Public defaults: `DEFAULT_MAX_ATTEMPTS`, `DEFAULT_BASE_DELAY`, `DEFAULT_MAX_DELAY`.
- **`RedisResultBackend`** (`redis` feature): `{prefix}result:{id}` STRING JSON `JobResult`,
  default TTL **24h** (`EX 86400`); shares [`RedisConfig`] with the broker.
- `CapivaraError::ResultBackend` for result-backend I/O (distinct from `Broker`).
- Worker **concurrency** via Tokio `Semaphore` (default **4**); `App::with_concurrency` (clamped ≥ 1).
- Redis integration: full roundtrip with `RedisBroker` + `RedisResultBackend`; concurrency smoke.
- Memory concurrency smoke test (several jobs with concurrency 4).
- README multi-process notes (producer + worker, same prefix) and at-least-once / idempotency guidance.
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
  DLQ / terminal-only Failure, retry-then-success, and `with_default_queue`.
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
