# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`RedisResultBackend`** (`redis` feature): `{prefix}result:{id}` STRING JSON `JobResult`,
  default TTL **24h** (`EX 86400`); shares [`RedisConfig`] with the broker.
- `CapivaraError::ResultBackend` for result-backend I/O (distinct from `Broker`).
- Worker **concurrency** via Tokio `Semaphore` (default **4**); `App::with_concurrency` (clamped ≥ 1).
- Redis integration: full roundtrip with `RedisBroker` + `RedisResultBackend`; concurrency smoke.
- Memory concurrency smoke test (several jobs with concurrency 4).
- README multi-process notes (producer + worker, same prefix) and at-least-once / idempotency guidance.
- Lease **recover-on-claim** for `RedisBroker` (Lua) and `MemoryBroker` (expired `in_flight` → pending).
- **Claim tokens**: lease member `{queue}\x1f{id}\x1f{token}`; `ack`/`nack` require matching token so late settle cannot steal a reclaimed claim.
- Redis claim **atomically INCRs** `{prefix}attempts:{id}` with lease (attempt counter independent of body JSON).
- Worker treats `JobNotFound` on settle as non-fatal (drain continues after lost lease).
- Worker delayed-nack policy: task `Err`/panic retries via `nack(RequeueAfter)` until
  `max_attempts` (default **3**), then terminal `ack`. Defaults: lease **30s**, nack delay **5s**.
- `App::with_lease` / `with_max_attempts` (clamped ≥ 1) / `with_nack_delay` for worker policy.
- Optional Cargo feature `redis` with `RedisBroker` + `RedisResultBackend` (LIST + lease, Lua claim/ack/nack, delayed requeue).
- Extended `Broker` trait: `claim(queues, lease, block_for)`, `nack(RequeueAfter)`; `ClaimedJob`.
- testcontainers Redis integration tests (`tests/redis_broker.rs`), with `REDIS_URL` override.
- Typed `Task` trait with native async `run`, `App::register` / `send` / `run_worker` / `get_result`.
- `MemoryBroker` and optional `MemoryResultBackend` (in-process; success and failure stored).
- Worker panic isolation via `tokio::spawn` join errors.
- Integration tests for success, task error, panic isolation, missing result backend,
  bad JSON payload, `max_jobs`, `ResultNotFound`, fire-and-forget drain, unknown task name,
  and `with_default_queue`.
- `App::broker()` for shared broker access (tests / raw `Job` injection).
- Repository skeleton: dual MIT OR Apache-2.0 license, README (WIP), security policy,
  contributing guide, GitHub Actions CI (fmt, clippy, test), Dependabot config.
- CI least-privilege `permissions` and `concurrency` cancel-in-progress.

### Changed

- Worker drain is concurrent (default 4 in-flight); claim tokens remain per-job.
- Redis lease ZSET members use `{queue}\x1f{id}\x1f{token}`; delayed remains `{queue}\x1f{id}`.
- Package version set to **`0.0.1`** with **`publish = false`** until a real release.
- Apache-2.0 license appendix copyright filled in for Duarte Mainart Tecnologia e Publicidade LTDA.
