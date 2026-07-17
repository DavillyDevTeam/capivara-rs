# Broker capability matrix

This document **freezes** the [`Broker`](../src/broker/mod.rs) contract that Memory
and Redis implement today, and records what the experimental RabbitMQ backend
matches or **explicitly opts out of**. Delivery promises (at-least-once, claim
tokens, terminal Failure) live in [guarantees.md](guarantees.md); structural
topology lives in [ARCHITECTURE.md](ARCHITECTURE.md).

**Milestone context:** M0–M3 shipped Memory + Redis end-to-end. **M4** freezes this
matrix and lands an **experimental** Rabbit spike (`rabbitmq` feature). **Kafka is
not planned.**

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
| **spike** | Experimental (gaps expected; not production) |
| **—** | Out of scope / not planned |

| Capability | Memory | Redis (`redis` feature) | RabbitMQ (`rabbitmq`, **experimental**) | Kafka |
|---|---|---|---|---|
| **enqueue** | **yes** — in-process FIFO pending | **yes** — LIST pending + job STRING | **spike** — `basic_publish` JSON job to `{prefix}{queue}` | **—** |
| **claim + block** | **yes** — mutex + sleep loop; `block_for=0` non-blocking | **yes** — RPOP + poll/sleep; `block_for=0` non-blocking | **spike** — `basic_get` + poll sleep for `block_for`; `0` = non-blocking | **—** |
| **lease / recover-on-claim** | **yes** — `in_flight` + `lease_until`; recover before claim | **yes** — ZSET lease + Lua recover; claim tokens in member | **no** — **`lease` ignored**; unacked until settle or channel/connection drop (then Rabbit redelivers). No Redis-style timed lease ZSET | **—** |
| **claim tokens** | **yes** | **yes** — `{queue}\x1f{id}\x1f{token}` | **partial** — process-local `JobId → (ClaimToken, Acker)`; delivery-tag ownership only in the claiming process | **—** |
| **delayed nack** | **yes** — in-process delayed list, promote on claim | **yes** — delayed ZSET, promote on claim | **partial** — ack original + publish to `{prefix}{queue}:delayed` with per-message TTL; DLX hop back to ready. Mixed TTLs may reorder | **—** |
| **DLQ (`dead_letter`)** | **yes** — per-queue in-process list | **yes** — `{prefix}q:{queue}:dead` LIST | **spike** — ack + publish to `{prefix}{queue}:dead` with reason header | **—** |
| **`list_dead` (inspect)** | **yes** | **yes** | **partial** — best-effort `basic_get` + requeue (cap 256); not a durable admin API | **—** |
| **producer idempotency_key** | **yes** — `HashMap` key → `JobId` | **yes** — `{prefix}idempotency:{key}` SET NX (Lua) | **partial** — **process-local** map only (not multi-process; no Redis sidecar) | **—** |
| **multi-process / multi-worker** | **no** — single process only | **yes** — shared `url` + `prefix` | **yes** — broker-native competing consumers / `basic_get` (shared URL + prefix) | **—** |
| **queue depth metric** | **yes** — pending length after enqueue/claim/nack | **no** on hot path (no `LLEN` under load) | **no** — not updated on hot path | **—** |

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

Memory and Redis both meet this bar today. Rabbit (`rabbitmq` feature) is an
**experimental spike**: it implements the trait surface enough for a worker happy
path but **does not** meet every semantic row above — see the matrix and the
Rabbit section below. Do not treat Rabbit as parity with Redis.

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

### RabbitMQ (`RabbitBroker`, feature `rabbitmq`) — **experimental**

- Opt-in only (`lapin` + Tokio executor/reactor traits). **Not** a second default;
  default builds stay free of lapin.
- Enough for enqueue → claim → worker → ack / nack / dead_letter happy path
  (integration tests behind `cargo test --features rabbitmq`; testcontainers or
  `RABBITMQ_URL` / `AMQP_URL`).
- Queue layout (`prefix` default `capivara:`):
  - `{prefix}{queue}` — ready work (JSON job)
  - `{prefix}{queue}:dead` — DLQ (reason in `x-capivara-dead-reason` header)
  - `{prefix}{queue}:delayed` — TTL hop with DLX back to ready
- **Documented gaps (not Redis parity):**
  1. **No timed lease / recover-on-claim** — `lease` is ignored. Unacked messages
     stay with the consumer until settle or connection/channel close (then Rabbit
     redelivers). Crash recovery is connection-drop redelivery, not lease ZSET.
  2. **Delayed nack** uses ack + publish to `:delayed` with per-message TTL + DLX;
     not Redis delayed ZSET. Mixed TTLs on one delay queue can reorder.
  3. **Claim tokens** are process-local (delivery tag / `Acker` map). Correct
     multi-worker *delivery* is broker-native; late-settle token checks do not
     span processes.
  4. **Producer `idempotency_key`** is process-local only (no shared store).
  5. **`list_dead`** is best-effort inspect (`basic_get` + requeue, capped).
  6. No hot-path `capivara_queue_depth` updates.
- Production readiness is **not** a goal of this spike.

### Kafka

- **Not planned.** Capivara’s worker model (claim + lease + delayed requeue + DLQ
  inspect) maps poorly to Kafka consumer groups without a large custom layer.
- If priorities change, open a design discussion before any implementation PR.

---

## What this freeze is (and is not)

**Is:**

- The method set and settle semantics Memory/Redis already share.
- A checklist for any new `Broker` impl (especially Rabbit).
- Honest gap documentation for the experimental Rabbit spike.

**Is not:**

- A promise of Celery protocol interop or Kombu wire compatibility.
- Exactly-once execution.
- DLQ redrive / automatic replay.
- Kafka support.
- Rabbit production readiness or Redis lease/idempotency parity.
- Breaking API churn for cosmetic renames — prefer additive docs and helper types.

When a backend changes capabilities, update **this file**, [guarantees.md](guarantees.md),
and the README link in the same PR.
