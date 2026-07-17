//! Experimental RabbitMQ broker (feature `rabbitmq`, via [lapin]).
//!
//! # Status
//!
//! **Spike / not production-ready.** Implements enough of [`Broker`] to run a
//! worker happy path. Capability gaps vs Memory/Redis are documented in
//! `docs/BROKER.md` and restated below.
//!
//! # Queue layout (`prefix` configurable, default `capivara:`)
//!
//! - `{prefix}{queue}` — ready work (JSON [`Job`])
//! - `{prefix}{queue}:dead` — dead-letter inspect queue
//! - `{prefix}{queue}:delayed` — TTL hop: per-message `expiration` + DLX back to
//!   the ready queue (delayed nack)
//!
//! # Claim tokens
//!
//! Ownership is process-local: claim maps `JobId → (ClaimToken, Acker, Job)`.
//! `ack` / `nack` / `dead_letter` succeed only when the token matches the local
//! entry. AMQP delivery tags are channel-scoped; tokens do not transfer across
//! processes. Multi-worker safety is broker-native (competing consumers /
//! `basic_get`); token checking only protects the claiming process from late settle.
//!
//! # Capability gaps (honest)
//!
//! - **Lease / recover-on-claim:** `lease` is **ignored**. Unacked messages stay
//!   with the consumer until settle or channel/connection close (then Rabbit
//!   redelivers). There is no Redis-style timed lease ZSET.
//! - **Delayed nack:** implemented via TTL + DLX hop on `:delayed` (not an
//!   in-process delayed list / Redis delayed ZSET). Mixed TTLs on one delay
//!   queue can reorder; fine for a spike.
//! - **Settle is ack-then-republish:** `nack` / `dead_letter` ack the original
//!   delivery first, then publish the updated body. A crash (or channel death)
//!   between those steps **drops the job** with no redelivery — weaker than
//!   Memory/Redis atomic settle and weaker than leaving the message unacked.
//! - **Producer `idempotency_key`:** process-local map only (not multi-process).
//!   The key is recorded **only after a successful publish** so a failed
//!   enqueue does not poison retries. Concurrent same-key enqueues in one
//!   process may still rare-race double-publish.
//! - **Persistence:** queues are durable and publishes use `delivery_mode = 2`
//!   (persistent), but **publisher confirms are not enabled** (`confirm_select`
//!   is not called); the publish future’s second `.await` is typically
//!   `NotRequested`. Do not treat this spike as crash-durable enqueue.
//! - **`list_dead`:** best-effort `basic_get` + requeue; not a durable admin API.
//! - **Queue depth metric:** not updated on the hot path.

use crate::broker::{Broker, ClaimToken, ClaimedJob, DeadLetter, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use async_trait::async_trait;
use lapin::acker::Acker;
use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// AMQP header used when publishing to the dead-letter queue.
const DEAD_REASON_HEADER: &str = "x-capivara-dead-reason";

/// AMQP delivery mode: persistent (survives broker restart when queues are durable).
const DELIVERY_MODE_PERSISTENT: u8 = 2;

/// Connection / queue-namespace settings for [`RabbitBroker`].
#[derive(Debug, Clone)]
pub struct RabbitConfig {
    /// AMQP URL, e.g. `amqp://guest:guest@127.0.0.1:5672/%2f`.
    pub url: String,
    /// Queue name prefix, e.g. `capivara:` (include trailing separator if you want).
    pub prefix: String,
}

impl RabbitConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: "capivara:".into(),
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

struct InFlight {
    token: ClaimToken,
    acker: Acker,
    job: Job,
}

/// Experimental RabbitMQ [`Broker`] (feature `rabbitmq`).
///
/// See module docs and `docs/BROKER.md` for gaps vs Memory/Redis.
pub struct RabbitBroker {
    channel: Channel,
    prefix: String,
    /// Claimed-but-not-settled jobs in **this** process.
    in_flight: Mutex<HashMap<uuid::Uuid, InFlight>>,
    /// Logical queue names already declared (ready + dead + delayed).
    declared: Mutex<HashSet<String>>,
    /// Process-local producer idempotency (not multi-process).
    idempotency: Mutex<HashMap<String, JobId>>,
}

impl RabbitBroker {
    /// Connect with Tokio executor/reactor (required for lapin 2.x under Tokio).
    pub async fn connect(config: RabbitConfig) -> Result<Self> {
        let options = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        let conn = Connection::connect(&config.url, options)
            .await
            .map_err(|e| CapivaraError::Broker(format!("rabbitmq connect: {e}")))?;
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| CapivaraError::Broker(format!("rabbitmq channel: {e}")))?;

        Ok(Self {
            channel,
            prefix: config.prefix,
            in_flight: Mutex::new(HashMap::new()),
            declared: Mutex::new(HashSet::new()),
            idempotency: Mutex::new(HashMap::new()),
        })
    }

    fn ready_name(&self, queue: &str) -> String {
        format!("{}{queue}", self.prefix)
    }

    fn dead_name(&self, queue: &str) -> String {
        format!("{}{queue}:dead", self.prefix)
    }

    fn delayed_name(&self, queue: &str) -> String {
        format!("{}{queue}:delayed", self.prefix)
    }

    /// Declare ready / dead / delayed queues for a logical Capivara queue name.
    async fn ensure_topology(&self, queue: &str) -> Result<()> {
        {
            let declared = self.declared.lock().await;
            if declared.contains(queue) {
                return Ok(());
            }
        }

        let ready = self.ready_name(queue);
        let dead = self.dead_name(queue);
        let delayed = self.delayed_name(queue);

        let durable = QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        };

        self.channel
            .queue_declare(&ready, durable, FieldTable::default())
            .await
            .map_err(|e| CapivaraError::Broker(format!("declare ready {ready}: {e}")))?;

        self.channel
            .queue_declare(&dead, durable, FieldTable::default())
            .await
            .map_err(|e| CapivaraError::Broker(format!("declare dead {dead}: {e}")))?;

        // Delayed hop: messages expire (per-message TTL) then DLX to default
        // exchange with routing key = ready queue name.
        let mut delayed_args = FieldTable::default();
        delayed_args.insert(
            ShortString::from("x-dead-letter-exchange"),
            AMQPValue::LongString("".into()),
        );
        delayed_args.insert(
            ShortString::from("x-dead-letter-routing-key"),
            AMQPValue::LongString(ready.clone().into()),
        );
        self.channel
            .queue_declare(&delayed, durable, delayed_args)
            .await
            .map_err(|e| CapivaraError::Broker(format!("declare delayed {delayed}: {e}")))?;

        self.declared.lock().await.insert(queue.to_string());
        Ok(())
    }

    async fn publish_json(
        &self,
        routing_key: &str,
        job: &Job,
        props: BasicProperties,
    ) -> Result<()> {
        let body = serde_json::to_vec(job)?;
        // Persistent bodies + durable queues reduce broker-restart loss; we still
        // do not call confirm_select, so the second await is usually NotRequested.
        let props = props
            .with_content_type(ShortString::from("application/json"))
            .with_delivery_mode(DELIVERY_MODE_PERSISTENT);
        self.channel
            .basic_publish(
                "",
                routing_key,
                BasicPublishOptions::default(),
                &body,
                props,
            )
            .await
            .map_err(|e| CapivaraError::Broker(format!("publish {routing_key}: {e}")))?
            .await
            .map_err(|e| CapivaraError::Broker(format!("publish confirm {routing_key}: {e}")))?;
        Ok(())
    }

    /// Take in-flight entry only if `claim_token` matches.
    async fn take_owned(&self, id: &JobId, claim_token: &ClaimToken) -> Result<InFlight> {
        let mut guard = self.in_flight.lock().await;
        match guard.get(&id.0) {
            Some(entry) if &entry.token == claim_token => {
                Ok(guard.remove(&id.0).expect("just checked"))
            }
            Some(_) => Err(CapivaraError::JobNotFound { id: id.to_string() }),
            None => Err(CapivaraError::JobNotFound { id: id.to_string() }),
        }
    }

    async fn try_claim_one(&self, queue: &str) -> Result<Option<ClaimedJob>> {
        self.ensure_topology(queue).await?;
        let ready = self.ready_name(queue);

        let msg = self
            .channel
            .basic_get(&ready, BasicGetOptions { no_ack: false })
            .await
            .map_err(|e| CapivaraError::Broker(format!("basic_get {ready}: {e}")))?;

        let Some(msg) = msg else {
            return Ok(None);
        };

        let mut job: Job = serde_json::from_slice(&msg.data)
            .map_err(|e| CapivaraError::Broker(format!("job json from {ready}: {e}")))?;
        // Mirror Redis/Memory: attempts is claim count (1-based after first claim).
        job.attempts = job.attempts.saturating_add(1);

        let claim_token = ClaimToken::new();
        let acker = msg.delivery.acker.clone();
        let job_id = job.id;

        self.in_flight.lock().await.insert(
            job_id.0,
            InFlight {
                token: claim_token.clone(),
                acker,
                job: job.clone(),
            },
        );

        Ok(Some(ClaimedJob { job, claim_token }))
    }
}

#[async_trait]
impl Broker for RabbitBroker {
    async fn enqueue(&self, job: Job) -> Result<JobId> {
        // Process-local idempotency only (documented gap vs Redis multi-process map).
        // Lookup before publish; record **only after** a successful publish so a
        // failed enqueue does not return Ok(existing) on retry without a queue body.
        // Concurrent same-key enqueues may rare-race double-publish (spike tradeoff).
        if let Some(ref key) = job.idempotency_key {
            let map = self.idempotency.lock().await;
            if let Some(existing) = map.get(key) {
                return Ok(*existing);
            }
        }

        let queue = job.queue.as_str().to_string();
        self.ensure_topology(&queue).await?;
        let ready = self.ready_name(&queue);
        let id = job.id;
        self.publish_json(&ready, &job, BasicProperties::default())
            .await?;

        if let Some(ref key) = job.idempotency_key {
            let mut map = self.idempotency.lock().await;
            // First successful publisher to finish wins the map entry.
            map.entry(key.clone()).or_insert(id);
        }
        Ok(id)
    }

    async fn claim(
        &self,
        queues: &[QueueName],
        _lease: Duration,
        block_for: Duration,
    ) -> Result<Option<ClaimedJob>> {
        // `lease` intentionally unused: no timed lease recover (AMQP unacked /
        // connection-drop redelivery instead). See module docs / BROKER.md.
        let queue_names: Vec<String> = if queues.is_empty() {
            vec![QueueName::default().as_str().to_string()]
        } else {
            queues.iter().map(|q| q.as_str().to_string()).collect()
        };

        let deadline = if block_for.is_zero() {
            None
        } else {
            Some(Instant::now() + block_for)
        };

        loop {
            for q in &queue_names {
                if let Some(claimed) = self.try_claim_one(q).await? {
                    return Ok(Some(claimed));
                }
            }

            match deadline {
                None => return Ok(None),
                Some(d) if Instant::now() >= d => return Ok(None),
                Some(d) => {
                    let remaining = d.saturating_duration_since(Instant::now());
                    let sleep = remaining.min(Duration::from_millis(50));
                    if sleep.is_zero() {
                        return Ok(None);
                    }
                    tokio::time::sleep(sleep).await;
                }
            }
        }
    }

    async fn ack(&self, id: &JobId, claim_token: &ClaimToken) -> Result<()> {
        let entry = self.take_owned(id, claim_token).await?;
        entry
            .acker
            .ack(BasicAckOptions::default())
            .await
            .map_err(|e| CapivaraError::Broker(format!("ack {id}: {e}")))?;
        Ok(())
    }

    async fn nack(&self, id: &JobId, claim_token: &ClaimToken, action: NackAction) -> Result<()> {
        let entry = self.take_owned(id, claim_token).await?;
        let NackAction::RequeueAfter { delay } = action;
        let job = entry.job;
        let queue = job.queue.as_str().to_string();
        self.ensure_topology(&queue).await?;

        // Ack-then-republish so the body carries updated `attempts`.
        // (basic_nack requeue=true would restore the pre-claim body and stall attempts.)
        // Crash window: if we die after ack and before publish, the job is lost
        // (documented gap vs Memory/Redis atomic settle).
        entry
            .acker
            .ack(BasicAckOptions::default())
            .await
            .map_err(|e| CapivaraError::Broker(format!("nack ack {id}: {e}")))?;

        if delay.is_zero() {
            let ready = self.ready_name(&queue);
            self.publish_json(&ready, &job, BasicProperties::default())
                .await?;
        } else {
            let delayed = self.delayed_name(&queue);
            // Per-message TTL (ms) as short string; DLX routes to ready queue.
            let exp_ms = delay.as_millis().min(u128::from(u32::MAX)) as u32;
            let props =
                BasicProperties::default().with_expiration(ShortString::from(exp_ms.to_string()));
            self.publish_json(&delayed, &job, props).await?;
        }
        Ok(())
    }

    async fn dead_letter(&self, id: &JobId, claim_token: &ClaimToken, reason: &str) -> Result<()> {
        let entry = self.take_owned(id, claim_token).await?;
        let job = entry.job;
        let queue = job.queue.as_str().to_string();
        self.ensure_topology(&queue).await?;

        // Ack-then-publish to `:dead`. Same crash-between-steps loss window as nack.
        entry
            .acker
            .ack(BasicAckOptions::default())
            .await
            .map_err(|e| CapivaraError::Broker(format!("dead_letter ack {id}: {e}")))?;

        let mut headers = FieldTable::default();
        headers.insert(
            ShortString::from(DEAD_REASON_HEADER),
            AMQPValue::LongString(reason.to_string().into()),
        );
        let props = BasicProperties::default().with_headers(headers);
        let dead = self.dead_name(&queue);
        self.publish_json(&dead, &job, props).await?;
        Ok(())
    }

    async fn list_dead(&self, queue: &QueueName) -> Result<Vec<DeadLetter>> {
        let q = queue.as_str();
        self.ensure_topology(q).await?;
        let dead = self.dead_name(q);

        // Best-effort inspect: basic_get (no auto-ack), then requeue each.
        // Cap to avoid unbounded loops on huge DLQs in a spike. Order is
        // approximate (AMQP requeue placement is not a stable list API).
        const MAX: usize = 256;
        let mut held: Vec<(Acker, DeadLetter)> = Vec::new();

        for _ in 0..MAX {
            let msg = self
                .channel
                .basic_get(&dead, BasicGetOptions { no_ack: false })
                .await
                .map_err(|e| CapivaraError::Broker(format!("list_dead get {dead}: {e}")))?;
            let Some(msg) = msg else {
                break;
            };

            let acker = msg.delivery.acker.clone();
            let job: Job = match serde_json::from_slice(&msg.data) {
                Ok(j) => j,
                Err(_) => {
                    // Keep unreadable bodies on the queue; do not drop them.
                    let _ = acker
                        .nack(BasicNackOptions {
                            requeue: true,
                            multiple: false,
                        })
                        .await;
                    continue;
                }
            };

            let reason = msg
                .properties
                .headers()
                .as_ref()
                .and_then(|h| h.inner().get(DEAD_REASON_HEADER))
                .and_then(|v| match v {
                    AMQPValue::LongString(s) => {
                        Some(String::from_utf8_lossy(s.as_bytes()).into_owned())
                    }
                    AMQPValue::ShortString(s) => Some(s.to_string()),
                    _ => None,
                })
                .unwrap_or_else(|| "unknown".into());

            held.push((acker, DeadLetter { job, reason }));
        }

        let mut out = Vec::with_capacity(held.len());
        // Requeue reverse so FIFO head tends to stay oldest-first (best-effort).
        for (acker, dl) in held.into_iter().rev() {
            acker
                .nack(BasicNackOptions {
                    requeue: true,
                    multiple: false,
                })
                .await
                .map_err(|e| CapivaraError::Broker(format!("list_dead requeue: {e}")))?;
            out.push(dl);
        }
        out.reverse();
        Ok(out)
    }
}
