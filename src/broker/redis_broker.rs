//! Redis LIST + lease broker (feature `redis`).
//!
//! Celery analogy: Redis as Kombu transport with a visibility/lease window.
//!
//! # Key layout (prefix configurable)
//!
//! - `{prefix}q:{queue}:pending` — LIST of job id strings (ready)
//! - `{prefix}job:{id}` — STRING JSON [`Job`]
//! - `{prefix}lease` — ZSET score = lease expiry (unix ms), member = `{queue}\x1f{id}`
//! - `{prefix}delayed` — ZSET score = available_at (unix ms), member = `{queue}\x1f{id}`
//!
//! Claim / promote / recover / ack paths use **Lua** so multi-worker races cannot
//! double-enqueue or delete unowned job bodies.

use crate::broker::{Broker, ClaimedJob, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Unit separator used in lease/delayed ZSET members: `{queue}\x1f{job_id}`.
const MEMBER_SEP: char = '\u{001f}';

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
    /// Atomically: for each due delayed member, ZREM then LPUSH only if we won ZREM.
    promote_script: Script,
    /// Atomically: for each expired lease member, ZREM then LPUSH only if we won ZREM.
    recover_script: Script,
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
        // ARGV[1] = job key prefix, ARGV[2] = lease expiry ms, ARGV[3] = queue name
        // Lease member format: "{queue}\x1f{id}" (same as delayed) so recoverer needs no JSON.
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
            local sep = string.char(0x1f)
            local member = ARGV[3] .. sep .. id
            redis.call('ZADD', KEYS[2], ARGV[2], member)
            return body
            "#,
        );

        // Only delete the job body if we actually held the lease.
        // KEYS[1] = lease zset, ARGV[1] = job key, ARGV[2] = lease member
        let ack_script = Script::new(
            r#"
            local removed = redis.call('ZREM', KEYS[1], ARGV[2])
            if removed == 1 then
              redis.call('DEL', ARGV[1])
            end
            return removed
            "#,
        );

        // KEYS[1]=lease, KEYS[2]=pending or delayed
        // ARGV[1]=job key, ARGV[2]=lease member, ARGV[3]=mode ('pending'|'delayed'),
        // ARGV[4]=score if delayed, ARGV[5]=delayed member if delayed,
        // ARGV[6]=pending list member (bare job id) if pending
        let nack_script = Script::new(
            r#"
            local member = ARGV[2]
            local removed = redis.call('ZREM', KEYS[1], member)
            if removed == 0 then
              return 0
            end
            local body = redis.call('GET', ARGV[1])
            if not body then
              return 0
            end
            if ARGV[3] == 'pending' then
              redis.call('LPUSH', KEYS[2], ARGV[6])
            else
              redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
            end
            return 1
            "#,
        );

        // KEYS[1]=delayed, ARGV[1]=now_ms, ARGV[2]=pending key prefix ("{prefix}q:")
        // Delayed ZSET member format: "{queue}\x1f{job_id}" so we never need cjson.
        let promote_script = Script::new(
            r#"
            local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            local promoted = 0
            local sep = string.char(0x1f)
            for _, member in ipairs(members) do
              if redis.call('ZREM', KEYS[1], member) == 1 then
                local pos = string.find(member, sep, 1, true)
                if pos then
                  local q = string.sub(member, 1, pos - 1)
                  local id = string.sub(member, pos + 1)
                  local pending = ARGV[2] .. q .. ':pending'
                  redis.call('LPUSH', pending, id)
                  promoted = promoted + 1
                end
              end
            end
            return promoted
            "#,
        );

        // KEYS[1]=lease, ARGV[1]=now_ms, ARGV[2]=pending key prefix ("{prefix}q:")
        // Lease member format matches delayed: "{queue}\x1f{job_id}".
        let recover_script = Script::new(
            r#"
            local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            local recovered = 0
            local sep = string.char(0x1f)
            for _, member in ipairs(members) do
              if redis.call('ZREM', KEYS[1], member) == 1 then
                local pos = string.find(member, sep, 1, true)
                if pos then
                  local q = string.sub(member, 1, pos - 1)
                  local id = string.sub(member, pos + 1)
                  local pending = ARGV[2] .. q .. ':pending'
                  redis.call('LPUSH', pending, id)
                  recovered = recovered + 1
                end
              end
            end
            return recovered
            "#,
        );

        Ok(Self {
            conn,
            prefix: config.prefix,
            claim_script,
            ack_script,
            nack_script,
            promote_script,
            recover_script,
        })
    }

    fn pending_key(&self, queue: &str) -> String {
        format!("{}q:{}:pending", self.prefix, queue)
    }

    fn pending_key_prefix(&self) -> String {
        // "{prefix}q:" so script can build "{prefix}q:{queue}:pending"
        format!("{}q:", self.prefix)
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

    fn lease_member(queue: &str, id: &JobId) -> String {
        format!("{queue}{MEMBER_SEP}{id}")
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Reclaim jobs whose lease expired (worker crashed / stuck without ack/nack).
    async fn recover_expired_leases(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i32 = self
            .recover_script
            .key(self.lease_key())
            .arg(Self::now_ms())
            .arg(self.pending_key_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        Ok(())
    }

    /// Promote due delayed jobs. Queue filter is applied after promote by claim
    /// (jobs land on their own queue's pending list).
    async fn promote_delayed(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i32 = self
            .promote_script
            .key(self.delayed_key())
            .arg(Self::now_ms())
            .arg(self.pending_key_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
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
            .arg(queue)
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

    /// Load job body for id; used by ack/nack to build lease member from queue.
    async fn get_job(&self, id: &JobId) -> Result<Job> {
        let mut conn = self.conn.clone();
        let job_key = self.job_key(id);
        let body: Option<String> = redis::cmd("GET")
            .arg(&job_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        let Some(body) = body else {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        };
        Ok(serde_json::from_str(&body)?)
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

        // Atomic SET + LPUSH so a crash cannot leave a list entry without a body
        // (or orphan body only — SET-first still preferred).
        redis::pipe()
            .atomic()
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
            // Recover expired leases before promoting delayed so reclaimed jobs
            // are claimable in this same pass.
            self.recover_expired_leases().await?;
            self.promote_delayed().await?;

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
        let job = self.get_job(id).await?;
        let member = Self::lease_member(job.queue.as_str(), id);
        let mut conn = self.conn.clone();
        let removed: i32 = self
            .ack_script
            .key(self.lease_key())
            .arg(self.job_key(id))
            .arg(member)
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
        let job_key = self.job_key(id);
        let job = self.get_job(id).await?;
        let lease_member = Self::lease_member(job.queue.as_str(), id);

        match action {
            NackAction::RequeueAfter { delay } => {
                if delay.is_zero() {
                    let pending = self.pending_key(job.queue.as_str());
                    let ok: i32 = self
                        .nack_script
                        .key(self.lease_key())
                        .key(&pending)
                        .arg(&job_key)
                        .arg(&lease_member)
                        .arg("pending")
                        .arg(0)
                        .arg("")
                        .arg(id.to_string())
                        .invoke_async(&mut conn)
                        .await
                        .map_err(|e| CapivaraError::Broker(e.to_string()))?;
                    if ok == 0 {
                        return Err(CapivaraError::JobNotFound { id: id.to_string() });
                    }
                } else {
                    let score = Self::now_ms() + delay.as_millis() as u64;
                    let delayed = self.delayed_key();
                    let delayed_member = Self::lease_member(job.queue.as_str(), id);
                    let ok: i32 = self
                        .nack_script
                        .key(self.lease_key())
                        .key(&delayed)
                        .arg(&job_key)
                        .arg(&lease_member)
                        .arg("delayed")
                        .arg(score)
                        .arg(&delayed_member)
                        .arg("")
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
