# Delivery guarantees & architecture notes

This document records **what capivara promises today** (M0–**M4 complete**)
and the design decisions behind those promises. For a shorter operator-facing summary,
see the README sections [Failure modes in 10 minutes](../README.md#failure-modes-in-10-minutes)
and [Delivery guarantees & failure modes](../README.md#delivery-guarantees--failure-modes).
Structural map: [ARCHITECTURE.md](ARCHITECTURE.md). Broker capability matrix (Memory /
Redis / experimental Rabbit `rabbitmq` feature): [BROKER.md](BROKER.md).

APIs referenced: `App`, `Broker`, `RetryPolicy`, `JobResult`, `DeadLetter`,
`ClaimToken`, `send_with_idempotency_key`, `SyncTask`, `run_blocking`.

**Scope of the guarantees below:** the **Memory** and **Redis** backends meet the
full lease / recover-on-claim / claim-token bar. The experimental **Rabbit** spike
implements the `Broker` trait surface for a worker happy path but **opts out** of
several rows (no timed lease, process-local tokens and `idempotency_key`,
ack-then-republish settle crash window) — treat those sections as Memory/Redis
truth and read [BROKER.md](BROKER.md) before relying on Rabbit.

---

## 1. At-least-once delivery (not exactly-once)

### Promise

A job that is enqueued will be **claimed at least once** under normal broker
operation. After a claim, if the worker does not `ack` / `nack` / `dead_letter`
before the **lease** expires, **recover-on-claim** may put the job back on the
pending queue. Another worker (or the same process after restart) can claim it
again with a **new** `ClaimToken` and an incremented `Job.attempts`.

### Why lease + recover-on-claim

- Classic visibility timeout (Celery/SQS-style): crash safety without requiring
  a separate “reaper” process in M1/M2.
- Recovery runs on the claim path (Memory and Redis) so an idle system does not
  need a background timer to unstick work; the next claimer reclaims expired
  leases first, then promotes delayed jobs, then claims.

### Claim tokens

Settle methods require the token issued at claim time:

- Redis lease ZSET member: `{queue}\x1f{id}\x1f{token}`
- A late `ack`/`nack`/`dead_letter` after recovery fails with `JobNotFound` (or
  equivalent lost-ownership path) instead of mutating a newer claim.

Workers treat lost-lease settle as **non-fatal** so a drain can continue when
another claim already owns the job.

### Task author implication

**Write idempotent handlers** (or accept duplicate side effects). Producer
`idempotency_key` does **not** change this (see §4).

### Success store before ack (result rewrite)

The success path is **store Success → `ack`** (see §2). That ordering is intentional
for visibility of outcomes, but it is not a two-phase commit:

- Crash **after** `store(Success)` and **before** `ack` leaves the lease in place.
- Lease expiry → recover-on-claim → redelivery can run the handler again.
- A later claim may **overwrite** the stored result (another `Success`, or terminal
  `Failure` if retries exhaust). The result backend is **not** a monotonic commit log.

**Implication:** a visible `JobResult::Success` is not a durable “done” bit until the
claim is settled and no further redelivery can occur. Prefer task-side idempotency
and/or external side-effect de-dupe if you need stronger outcome guarantees.

---

## 2. Terminal Failure only

### Promise

When a result backend is configured:

| When | Stored result |
|---|---|
| Handler returns `Ok` | `JobResult::Success { payload }` after encode |
| Handler `Err` / panic and `attempts < max_attempts` | **No store** — `get_result` → `ResultNotFound` |
| Handler `Err` / panic and `attempts >= max_attempts` | `JobResult::Failure` **only after** successful `dead_letter` |
| Unknown task name | Terminal: `dead_letter`, then `Failure` if ownership held |
| `dead_letter` loses the race (`JobNotFound`) | **No** Failure write |

Without a backend, nothing is stored; see §6.

### Why not intermediate Failure

Storing Failure on every failed attempt would make `get_result` ambiguous:

- Callers could not tell “still retrying” from “give up.”
- A lost-lease race after writing Failure could leave a Failure while another
  claim still runs (or later succeeds).

So **Failure ≈ terminal outcome** (exhausted retries / DLQ / unknown task),
aligned with inspecting the dead-letter list.

### Order of operations on terminal path

1. `dead_letter(id, claim_token, reason)` — move body to per-queue DLQ, clear claim.
2. If step 1 confirmed ownership → optional `ResultBackend::store(Failure)`.
3. If step 1 lost ownership → skip Failure so the backend does not claim terminal
   while another claim may still be in flight.

Success path stores Success (if backend), then `ack`. Crash between those steps can
redeliver and rewrite results (see §1 “Success store before ack”).

---

## 3. Dead-letter queue (DLQ)

### Promise

- **Per queue:** `Broker::list_dead(&QueueName) -> Vec<DeadLetter>`.
- Each `DeadLetter { job, reason }` retains the job body for inspect.
- Terminal worker outcomes use `dead_letter` (not bare `ack`) so failed jobs are
  inspectable.
- **No replay / redrive API in M2.** No automatic re-enqueue from DLQ.

### Storage sketch

| Backend | Dead list | Reason | Body |
|---|---|---|---|
| Memory | In-process per-queue list | stored with entry | full `Job` |
| Redis | `{prefix}q:{queue}:dead` LIST of ids | `{prefix}job:{id}:dead_reason` | job key retained (no TTL in M2) |
| Rabbit (experimental) | `{prefix}{queue}:dead` AMQP queue | `x-capivara-dead-reason` header | JSON job body; `list_dead` best-effort |

### Why inspect-only first

Replay needs policy (who may redrive, attempt reset, duplicate vs new id,
idempotency interaction). M2 ships durable inspect + terminal results; redrive
is deferred.

---

## 4. Producer `idempotency_key` (not worker exactly-once)

### Promise

- Optional `Job.idempotency_key` (`#[serde(default)]` → `None` for old JSON).
- `App::send_with_idempotency_key` or raw enqueue with the field set.
- If the key was already recorded on that broker, `enqueue` returns the
  **existing** `JobId` and does **not** push a second pending entry.
- Applies even if the first job is in-flight, completed, or dead-lettered
  (simple key → id map; no TTL in M2).

### What it is for

Safe **producer** retries: client timeout, reconnect, “did my send land?”

### What it is not

- Not worker de-duplication after lease recovery.
- Not a substitute for idempotent `Task::run`.
- Not a distributed lock across different Redis prefixes or Memory processes.
- Not scoped by task name or queue unless the caller embeds those in the key
  string (global per Memory process / Redis `prefix`). Same string on two tasks
  → first job wins; second payload is discarded.

### Storage sketch

| Backend | Map |
|---|---|
| Memory | `HashMap<String, JobId>` under broker mutex |
| Redis | `{prefix}idempotency:{key}` STRING, Lua job body SET first then SET NX + LPUSH; NX loss DEL orphan body |
| Rabbit (experimental) | **Process-local** map only; recorded **after** successful publish — not multi-process safe ([BROKER.md](BROKER.md)) |

---

## 5. `RetryPolicy` (shared worker path)

Defaults (also `RetryPolicy::default()` / public constants):

| Field | Default |
|---|---|
| `max_attempts` | `3` (`DEFAULT_MAX_ATTEMPTS`) |
| `base_delay` | `1s` (`DEFAULT_BASE_DELAY`) |
| `max_delay` | `15m` (`DEFAULT_MAX_DELAY`) |
| `jitter` | `true` (equal jitter) |

Delay after a failed claim with attempt count `attempt` (1-based on `Job.attempts`
after claim):

```text
raw = min(max_delay, base_delay * 2^(attempt.saturating_sub(1)))
```

- `jitter == false` → delay is exactly `raw`.
- `jitter == true` → equal jitter in ≈ `[raw/2, raw]` (integer nanosecond math;
  see `src/retry.rs`).

Worker wiring:

- Intermediate failure → `nack(RequeueAfter { delay: policy.delay_for_attempt(attempts) })`.
- `attempts >= max_attempts` → terminal DLQ path (§2–3).
- `App::with_retry_policy` for full control; `with_max_attempts` / `with_nack_delay`
  are conveniences (`with_nack_delay` only sets `base_delay`).
- `max_attempts` values below 1 are clamped to 1.

Lease default remains **30s** (`App::with_lease`) — orthogonal to nack delay.

### Single `run_worker` drain vs delayed retries

`App::run_worker` / `Worker::run` claim with **non-blocking** `block_for` (`Duration::ZERO`)
and **do not sleep** for nack delays. Delayed jobs are only claimable after the delay
elapses and a later claim loop promotes them.

Under default policy (base **1s**, jitter on → ≈ 0.5–1s+), **one** `run_worker(None)`
call will **not** exhaust retries for a failing job. Operators and tests must either:

- re-invoke `run_worker` after the delay (integration tests multi-pass with sleep), or
- run a continuous claim loop (production multi-process worker).

---

## 6. Optional results (fire-and-forget)

### Promise

- `App::new(broker)` — no result backend → fire-and-forget. Worker never calls
  `store`. `get_result` → `CapivaraError::NoResultBackend`.
- `with_result_backend(...)` enables `get_result`. Missing id →
  `CapivaraError::ResultNotFound`.
- Redis results: `{prefix}result:{id}` STRING JSON, default TTL **24h**
  (`DEFAULT_RESULT_TTL`).

`send` / `send_with_idempotency_key` always return `JobId` whether or not a
backend exists; the id is only useful for results when a backend is attached.

When a backend is present, treat stored results as **best-effort visibility**, not a
durable commit log: store-then-ack Success can be rewritten after redelivery (§1).

---

## 7. Metrics vs delivery outcomes

Completion counter `capivara_jobs_completed_total` labels (recorded only when settle
confirms claim ownership):

| `status` | Delivery meaning |
|---|---|
| `success` | Handler `Ok`; claim **acked** |
| `failure` | Handler Err/panic this attempt; claim **nacked for retry** — **not** terminal |
| `dead` | Claim **dead-lettered** (max attempts / unknown task) — terminal path |

Do not treat metrics `failure` as permanent give-up; pair alerts on `dead` with
`list_dead` / terminal `JobResult::Failure`. Lost-lease `JobNotFound` on settle does
**not** increment the counter (same for all three statuses).

---

## 8. Explicit non-goals (current milestones)

- Celery wire protocol / pickle / shipping functions over the wire.
- Exactly-once execution end-to-end.
- DLQ automatic replay.
- Cross-process Memory broker.
- Idempotency key TTL / eviction policy (M2: permanent map entry per key).
- **Kafka** broker (not planned).
- Rabbit production readiness / Redis lease+idempotency parity (spike only).
- **`#[task]` proc-macro** (optional M4-4 skipped; use `Task` / `SyncTask`).
- Unilateral crates.io publish — stay **`0.0.1`** / **`publish = false`** until the
  maintainer agrees a formal **0.1.0** (post-M3 discussion still open; no post-M4
  unilateral 0.1.x / 0.2.0 either).

When any of these change, update this file, [ARCHITECTURE.md](ARCHITECTURE.md),
[BROKER.md](BROKER.md), and the README guarantees section together so docs stay
aligned with APIs.
