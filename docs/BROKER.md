# Broker capability matrix

This document **freezes** the [`Broker`](../src/broker/mod.rs) contract that Memory
and Redis implement today, and records what a future experimental RabbitMQ backend
must match (or explicitly opt out of). Delivery promises (at-least-once, claim
tokens, terminal Failure) live in [guarantees.md](guarantees.md); structural
topology lives in [ARCHITECTURE.md](ARCHITECTURE.md).

**Milestone context:** M0–M3 shipped Memory + Redis end-to-end. **M4** starts by
stabilizing this matrix before an experimental Rabbit spike. **Kafka is not planned.**

---

## Trait surface (stable for multi-broker path)

All production-shaped backends implement `async_trait` [`Broker`](../src/broker/mod.rs)
so `App` can hold `dyn Broker`:

| Method | Role |
|---|---|
| `enqueue(job) -> JobId` | Accept a job; honor optional `Job.idempotency_key` (safe producer retries) |
| `claim(queues, lease, block_for) -> Option<ClaimedJob>` | Recover expired leases → promote delayed → claim; optional block wait |
| `ack(id, claim_token)` | Successful settle; drop lease / in-flight; discard ready body (Redis) |
| `nack(id, claim_token, NackAction)` | Retry path: delayed requeue (`RequeueAfter { delay }`) |
| `dead_letter(id, claim_token, reason)` | Terminal path: per-queue DLQ + clear claim; keep body for inspect |
| `list_dead(queue) -> Vec<DeadLetter>` | Inspect-only DLQ listing (oldest first); **no replay / redrive API** |

Supporting types (public, shared):

| Type | Role |
|---|---|
| `ClaimToken` | Opaque ownership per claim; late settle cannot steal a recovered claim |
| `ClaimedJob { job, claim_token }` | Claim result |
| `NackAction::RequeueAfter { delay }` | Delayed nack (only variant today) |
| `DeadLetter { job, reason }` | Inspect payload for DLQ entries |

Worker policy (`RetryPolicy`, lease duration, concurrency) sits **above** the
broker trait and must work the same for every backend that implements the matrix.

---

## Capability matrix

Legend:

| Mark | Meaning |
|---|---|
| **yes** | Supported and covered by tests / docs |
| **partial** | Present with documented limitations |
| **no** | Not implemented |
| **spike** | Planned experimental next (gaps expected) |
| **—** | Out of scope / not planned |

| Capability | Memory | Redis (`redis` feature) | RabbitMQ (next, experimental) | Kafka |
|---|---|---|---|---|
| **enqueue** | **yes** — in-process FIFO pending | **yes** — LIST pending + job STRING | **spike** — publish to queue | **—** |
| **claim + block** | **yes** — mutex + sleep loop; `block_for=0` non-blocking | **yes** — RPOP + poll/sleep; `block_for=0` non-blocking | **spike** — basic get / consumer; blocking model TBD | **—** |
| **lease / recover-on-claim** | **yes** — `in_flight` + `lease_until`; recover before claim | **yes** — ZSET lease + Lua recover; claim tokens in member | **spike** — likely **gap**: classic AMQP ack/nack ≠ timed lease; document if emulated | **—** |
| **claim tokens** | **yes** | **yes** — `{queue}\x1f{id}\x1f{token}` | **spike** — map to delivery tag or synthetic token | **—** |
| **delayed nack** | **yes** — in-process delayed list, promote on claim | **yes** — delayed ZSET, promote on claim | **spike** — likely **gap** without delayed exchange / TTL+DLX plugin story | **—** |
| **DLQ (`dead_letter`)** | **yes** — per-queue in-process list | **yes** — `{prefix}q:{queue}:dead` LIST | **spike** — dead-letter exchange or side queue | **—** |
| **`list_dead` (inspect)** | **yes** | **yes** | **spike** — may be limited / admin-only | **—** |
| **producer idempotency_key** | **yes** — `HashMap` key → `JobId` | **yes** — `{prefix}idempotency:{key}` SET NX (Lua) | **spike** — may require app-level or Redis map sidecar | **—** |
| **multi-process / multi-worker** | **no** — single process only | **yes** — shared `url` + `prefix` | **spike** — yes (broker-native) | **—** |
| **queue depth metric** | **yes** — pending length after enqueue/claim/nack | **no** on hot path (no `LLEN` under load) | **spike** — TBD | **—** |

### Semantic guarantees (all **yes** backends)

When a cell is **yes**, the backend must preserve:

1. **At-least-once** delivery under lease expiry + recover-on-claim (not exactly-once).
2. **Token-checked settle** — `ack` / `nack` / `dead_letter` succeed only when
   `claim_token` matches the active claim; late settle after recover fails without
   stealing the newer claim.
3. **Idempotency scope** — `idempotency_key` de-dupes **enqueue** only; worker
   redelivery after crash still possible; handlers stay idempotent.
4. **DLQ inspect-only** — no automatic redrive / replay API in current milestones.
5. **Claim loop order** — recover expired leases → promote due delayed jobs → claim.

Memory and Redis both meet this bar today. Rabbit spike must either meet it or
document each gap in this matrix when that PR lands.

---

## Backend notes

### Memory (`MemoryBroker`)

- Default backend; no feature flag.
- Single-process: not shared across OS processes.
- Ideal for unit tests and in-process apps.
- Cheap `capivara_queue_depth` updates from pending length.

### Redis (`RedisBroker`, feature `redis`)

- Multi-process producers + workers with the same `RedisConfig` (`url`, `prefix`).
- LIST + lease ZSET + delayed ZSET; claim/ack/nack/dead_letter/recover/promote use **Lua**.
- Key layout documented on the type (`src/broker/redis_broker.rs`).
- Does **not** update `capivara_queue_depth` on the hot path (avoid `LLEN` under load).

### RabbitMQ (next PR — experimental)

- Planned as opt-in feature (e.g. `rabbitmq` + `lapin`), **not** a second default.
- Expected first spike: enqueue / claim / ack / nack path enough to run a worker.
- **Likely capability gaps** vs this matrix: timed **lease/recover**, **delayed nack**,
  and possibly producer **idempotency_key** without a sidecar store.
- Gaps must be called out in this matrix when the spike merges; do not silently
  claim parity with Redis.
- Production readiness is **not** a goal of the first Rabbit PR.

### Kafka

- **Not planned.** Capivara’s worker model (claim + lease + delayed requeue + DLQ
  inspect) maps poorly to Kafka consumer groups without a large custom layer.
- If priorities change, open a design discussion before any implementation PR.

---

## What this freeze is (and is not)

**Is:**

- The method set and settle semantics Memory/Redis already share.
- A checklist for any new `Broker` impl (especially Rabbit).
- Docs + light trait documentation only in M4-1 (no Rabbit code in that PR).

**Is not:**

- A promise of Celery protocol interop or Kombu wire compatibility.
- Exactly-once execution.
- DLQ redrive / automatic replay.
- Kafka support.
- Breaking API churn for cosmetic renames — prefer additive docs and helper types.

When a backend changes capabilities, update **this file**, [guarantees.md](guarantees.md),
and the README link in the same PR.
