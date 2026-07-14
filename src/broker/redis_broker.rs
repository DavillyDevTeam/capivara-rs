//! Redis LIST + lease broker (feature `redis`).
//!
//! Celery analogy: Redis as Kombu transport with a visibility/lease window.
//!
//! # Key layout (prefix configurable)
//!
//! - `{prefix}q:{queue}:pending` — LIST of job id strings (ready)
//! - `{prefix}job:{id}` — STRING JSON [`Job`]
//! - `{prefix}lease` — ZSET score = lease expiry (unix ms), member = job id
//! - `{prefix}delayed` — ZSET score = available_at (unix ms), member = job id
//!
//! Claim is a **Lua** script so RPOP + lease update is atomic.
//! Blocking claim polls Lua until `block_for` elapses (safe; no BRPOP race).

use crate::broker::{Broker, ClaimedJob, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Connection / key-namespace settings for [`RedisBroker`].
#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    /// Key prefix, e.g. `capivara:` (include trailing separator if you want).
    pub prefix: String,
}

impl RedisConfig {
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

pub struct RedisBroker {
    conn: ConnectionManager,
    prefix: String,
    claim_script: Script,
    ack_script: Script,
    nack_script: Script,
    promote_script: Script,
}

impl RedisBroker {
    /// Connect using a redis URL (`redis://127.0.0.1/`).
    pub async fn connect(config: RedisConfig) -> Result<Self> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        // KEYS[1] = pending list, KEYS[2] = lease zset
        // ARGV[1] = job key prefix (e.g. capivara:job:), ARGV[2] = lease expiry ms
        // Returns job JSON or nil
        let claim_script = Script::new(
            r#"
            local id = redis.call('RPOP', KEYS[1])
            if not id then
              return nil
            end
            local job_key = ARGV[1] .. id
            local body = redis.call('GET', job_key)
            if not body then
              return nil
            end
            redis.call('ZADD', KEYS[2], ARGV[2], id)
            return body
            "#,
        );

        // KEYS[1] = lease zset, ARGV[1] = job key, ARGV[2] = id
        let ack_script = Script::new(
            r#"
            local removed = redis.call('ZREM', KEYS[1], ARGV[2])
            redis.call('DEL', ARGV[1])
            return removed
            "#,
        );

        // KEYS[1]=lease, KEYS[2]=pending or delayed
        // ARGV[1]=job key, ARGV[2]=id, ARGV[3]=mode ('pending'|'delayed'), ARGV[4]=score if delayed
        let nack_script = Script::new(
            r#"
            local id = ARGV[2]
            redis.call('ZREM', KEYS[1], id)
            local body = redis.call('GET', ARGV[1])
            if not body then
              return 0
            end
            if ARGV[3] == 'pending' then
              redis.call('LPUSH', KEYS[2], id)
            else
              redis.call('ZADD', KEYS[2], ARGV[4], id)
            end
            return 1
            "#,
        );

        // KEYS[1]=delayed zset, KEYS[2]=pending list, ARGV[1]=now_ms
        let promote_script = Script::new(
            r#"
            local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            for _, id in ipairs(ids) do
              redis.call('ZREM', KEYS[1], id)
              redis.call('LPUSH', KEYS[2], id)
            end
            return #ids
            "#,
        );

        Ok(Self {
            conn,
            prefix: config.prefix,
            claim_script,
            ack_script,
            nack_script,
            promote_script,
        })
    }

    fn pending_key(&self, queue: &str) -> String {
        format!("{}q:{}:pending", self.prefix, queue)
    }

    fn job_key(&self, id: &JobId) -> String {
        format!("{}job:{}", self.prefix, id)
    }

    fn job_key_prefix(&self) -> String {
        format!("{}job:", self.prefix)
    }

    fn lease_key(&self) -> String {
        format!("{}lease", self.prefix)
    }

    fn delayed_key(&self) -> String {
        format!("{}delayed", self.prefix)
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn promote_delayed(&self, queues: &[QueueName]) -> Result<()> {
        let mut conn = self.conn.clone();
        let now = Self::now_ms().to_string();
        let queue_names: Vec<String> = if queues.is_empty() {
            vec![QueueName::default().as_str().to_string()]
        } else {
            queues.iter().map(|q| q.as_str().to_string()).collect()
        };
        // Promote into each queue's pending — delayed members are job ids; job has queue field.
        // For M1 simplicity: delayed jobs are always pushed to the first requested queue's pending,
        // or we store "queue\0id" — better: on nack we know queue from job body.
        // Delayed ZSET member = job id only; on promote LPUSH to job's queue from GET body.
        // So promote script needs to be smarter — promote in Rust for PR-A clarity:

        let delayed_key = self.delayed_key();
        let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&delayed_key)
            .arg("-inf")
            .arg(&now)
            .query_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        for id in ids {
            let job_key = format!("{}job:{}", self.prefix, id);
            let body: Option<String> = redis::cmd("GET")
                .arg(&job_key)
                .query_async(&mut conn)
                .await
                .map_err(|e| CapivaraError::Broker(e.to_string()))?;
            let Some(body) = body else {
                let _: i32 = redis::cmd("ZREM")
                    .arg(&delayed_key)
                    .arg(&id)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| CapivaraError::Broker(e.to_string()))?;
                continue;
            };
            let job: Job = serde_json::from_str(&body)?;
            let pending = self.pending_key(job.queue.as_str());
            // Only promote if that queue is in the claim filter (or filter empty/default).
            let allowed = queue_names.iter().any(|q| q == job.queue.as_str());
            if !allowed {
                continue;
            }
            let _: i32 = redis::cmd("ZREM")
                .arg(&delayed_key)
                .arg(&id)
                .query_async(&mut conn)
                .await
                .map_err(|e| CapivaraError::Broker(e.to_string()))?;
            let _: i32 = redis::cmd("LPUSH")
                .arg(&pending)
                .arg(&id)
                .query_async(&mut conn)
                .await
                .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        }

        // silence unused script for now (used as documentation / future)
        let _ = &self.promote_script;
        Ok(())
    }

    async fn try_claim_one(&self, queue: &str, lease: Duration) -> Result<Option<ClaimedJob>> {
        let mut conn = self.conn.clone();
        let pending = self.pending_key(queue);
        let lease_key = self.lease_key();
        let expiry = Self::now_ms() + lease.as_millis() as u64;

        let body: Option<String> = self
            .claim_script
            .key(&pending)
            .key(&lease_key)
            .arg(self.job_key_prefix())
            .arg(expiry)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        let Some(body) = body else {
            return Ok(None);
        };
        let mut job: Job = serde_json::from_str(&body)?;
        // Persist incremented attempts
        job.attempts = job.attempts.saturating_add(1);
        let job_key = self.job_key(&job.id);
        let updated = serde_json::to_string(&job)?;
        let _: () = redis::cmd("SET")
            .arg(&job_key)
            .arg(updated)
            .query_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        Ok(Some(ClaimedJob { job }))
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn enqueue(&self, job: Job) -> Result<JobId> {
        let id = job.id;
        let mut conn = self.conn.clone();
        let job_key = self.job_key(&id);
        let pending = self.pending_key(job.queue.as_str());
        let body = serde_json::to_string(&job)?;

        // SET job + LPUSH pending (pipeline)
        redis::pipe()
            .cmd("SET")
            .arg(&job_key)
            .arg(&body)
            .cmd("LPUSH")
            .arg(&pending)
            .arg(id.to_string())
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        Ok(id)
    }

    async fn claim(
        &self,
        queues: &[QueueName],
        lease: Duration,
        block_for: Duration,
    ) -> Result<Option<ClaimedJob>> {
        let queue_names: Vec<String> = if queues.is_empty() {
            vec![QueueName::default().as_str().to_string()]
        } else {
            queues.iter().map(|q| q.as_str().to_string()).collect()
        };

        let deadline = Instant::now() + block_for;
        loop {
            self.promote_delayed(queues).await?;

            for q in &queue_names {
                if let Some(claimed) = self.try_claim_one(q, lease).await? {
                    return Ok(Some(claimed));
                }
            }

            if block_for.is_zero() || Instant::now() >= deadline {
                return Ok(None);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let slice = remaining.min(Duration::from_millis(50));
            tokio::time::sleep(slice).await;
        }
    }

    async fn ack(&self, id: &JobId) -> Result<()> {
        let mut conn = self.conn.clone();
        let removed: i32 = self
            .ack_script
            .key(self.lease_key())
            .arg(self.job_key(id))
            .arg(id.to_string())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        if removed == 0 {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        Ok(())
    }

    async fn nack(&self, id: &JobId, action: NackAction) -> Result<()> {
        let mut conn = self.conn.clone();
        // Need job body for queue name
        let job_key = self.job_key(id);
        let body: Option<String> = redis::cmd("GET")
            .arg(&job_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        let Some(body) = body else {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        };
        let job: Job = serde_json::from_str(&body)?;

        match action {
            NackAction::RequeueAfter { delay } => {
                if delay.is_zero() {
                    let pending = self.pending_key(job.queue.as_str());
                    let ok: i32 = self
                        .nack_script
                        .key(self.lease_key())
                        .key(&pending)
                        .arg(&job_key)
                        .arg(id.to_string())
                        .arg("pending")
                        .arg(0)
                        .invoke_async(&mut conn)
                        .await
                        .map_err(|e| CapivaraError::Broker(e.to_string()))?;
                    if ok == 0 {
                        return Err(CapivaraError::JobNotFound { id: id.to_string() });
                    }
                } else {
                    let score = Self::now_ms() + delay.as_millis() as u64;
                    let delayed = self.delayed_key();
                    let ok: i32 = self
                        .nack_script
                        .key(self.lease_key())
                        .key(&delayed)
                        .arg(&job_key)
                        .arg(id.to_string())
                        .arg("delayed")
                        .arg(score)
                        .invoke_async(&mut conn)
                        .await
                        .map_err(|e| CapivaraError::Broker(e.to_string()))?;
                    if ok == 0 {
                        return Err(CapivaraError::JobNotFound { id: id.to_string() });
                    }
                }
            }
        }
        Ok(())
    }
}
