//! Redis LIST + lease broker (feature `redis`).
//!
//! Celery analogy: Redis as Kombu transport with a visibility/lease window.
//!
//! # Key layout (prefix configurable)
//!
//! - `{prefix}q:{queue}:pending` — LIST of job id strings (ready)
//! - `{prefix}q:{queue}:dead` — LIST of job id strings (dead-lettered; no TTL in M2)
//! - `{prefix}job:{id}` — STRING JSON [`Job`] (kept after dead-letter for inspect)
//! - `{prefix}job:{id}:dead_reason` — STRING reason when dead-lettered
//! - `{prefix}attempts:{id}` — STRING integer attempt counter (INCR on claim)
//! - `{prefix}lease` — ZSET score = lease expiry (unix ms),
//!   member = `{queue}\x1f{id}\x1f{token}`
//! - `{prefix}delayed` — ZSET score = available_at (unix ms), member = `{queue}\x1f{id}`
//! - `{prefix}idempotency:{key}` — STRING job id (SET NX; producer retry dedupe; no TTL in M2)
//!
//! Claim / promote / recover / ack / dead_letter paths use **Lua** so multi-worker
//! races cannot double-enqueue or delete unowned job bodies. Claim tokens ensure
//! late ack/nack/dead_letter after recover cannot steal a newer claim. Enqueue with
//! an idempotency key uses Lua: **job body SET first**, then map SET NX, then LPUSH.
//! On NX loss the orphan body is `DEL`'d and the winner id is returned — so a
//! mid-script error cannot leave a map entry without a body (silent lost enqueue).

use crate::broker::{Broker, ClaimToken, ClaimedJob, DeadLetter, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Unit separator used in lease/delayed ZSET members.
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
    /// Drop lease, push id onto dead list, store reason, drop attempts; keep job body.
    dead_letter_script: Script,
    /// Atomically: for each due delayed member, ZREM then LPUSH only if we won ZREM.
    promote_script: Script,
    /// Atomically: for each expired lease member, ZREM then LPUSH only if we won ZREM.
    recover_script: Script,
    /// Body SET first, then SET NX map; on win LPUSH pending; on lose DEL orphan body + return winner.
    enqueue_idempotent_script: Script,
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
        // ARGV[1] = job key prefix, ARGV[2] = lease expiry ms,
        // ARGV[3] = queue, ARGV[4] = claim token, ARGV[5] = attempts key prefix
        // Lease member: "{queue}\x1f{id}\x1f{token}"
        // Returns {body, attempts} or nil.
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
            local attempts = redis.call('INCR', ARGV[5] .. id)
            local sep = string.char(0x1f)
            local member = ARGV[3] .. sep .. id .. sep .. ARGV[4]
            redis.call('ZADD', KEYS[2], ARGV[2], member)
            return {body, attempts}
            "#,
        );

        // Only settle if this claim's exact lease member still exists.
        // KEYS[1] = lease zset
        // ARGV[1] = job key, ARGV[2] = lease member, ARGV[3] = attempts key
        let ack_script = Script::new(
            r#"
            local removed = redis.call('ZREM', KEYS[1], ARGV[2])
            if removed == 1 then
              redis.call('DEL', ARGV[1])
              redis.call('DEL', ARGV[3])
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

        // KEYS[1]=lease, KEYS[2]=dead list
        // ARGV[1]=job key, ARGV[2]=lease member, ARGV[3]=job id,
        // ARGV[4]=reason key, ARGV[5]=reason, ARGV[6]=attempts key
        // Clears claim, appends to dead list, stores reason; keeps job body.
        let dead_letter_script = Script::new(
            r#"
            local removed = redis.call('ZREM', KEYS[1], ARGV[2])
            if removed == 0 then
              return 0
            end
            local body = redis.call('GET', ARGV[1])
            if not body then
              return 0
            end
            redis.call('LPUSH', KEYS[2], ARGV[3])
            redis.call('SET', ARGV[4], ARGV[5])
            redis.call('DEL', ARGV[6])
            return 1
            "#,
        );

        // KEYS[1]=delayed
        // ARGV[1]=now_ms, ARGV[2]=pending key prefix, ARGV[3]=job key prefix,
        // ARGV[4]=attempts key prefix
        // Delayed member: "{queue}\x1f{job_id}"
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
                  local body = redis.call('GET', ARGV[3] .. id)
                  if body then
                    local pending = ARGV[2] .. q .. ':pending'
                    redis.call('LPUSH', pending, id)
                    promoted = promoted + 1
                  else
                    -- Orphan delayed entry without body: drop attempts counter too.
                    redis.call('DEL', ARGV[4] .. id)
                  end
                end
              end
            end
            return promoted
            "#,
        );

        // KEYS[1]=lease
        // ARGV[1]=now_ms, ARGV[2]=pending key prefix, ARGV[3]=job key prefix,
        // ARGV[4]=attempts key prefix
        // Lease member: "{queue}\x1f{id}\x1f{token}" — parse queue+id, ignore token.
        // Only LPUSH when job body still exists (mirrors nack).
        let recover_script = Script::new(
            r#"
            local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            local recovered = 0
            local sep = string.char(0x1f)
            for _, member in ipairs(members) do
              if redis.call('ZREM', KEYS[1], member) == 1 then
                local pos1 = string.find(member, sep, 1, true)
                if pos1 then
                  local pos2 = string.find(member, sep, pos1 + 1, true)
                  local q = string.sub(member, 1, pos1 - 1)
                  local id
                  if pos2 then
                    id = string.sub(member, pos1 + 1, pos2 - 1)
                  else
                    id = string.sub(member, pos1 + 1)
                  end
                  local body = redis.call('GET', ARGV[3] .. id)
                  if body then
                    local pending = ARGV[2] .. q .. ':pending'
                    redis.call('LPUSH', pending, id)
                    recovered = recovered + 1
                  else
                    -- Orphan lease without body: drop attempts counter too.
                    redis.call('DEL', ARGV[4] .. id)
                  end
                end
              end
            end
            return recovered
            "#,
        );

        // KEYS[1]=idempotency key, KEYS[2]=job key, KEYS[3]=pending list
        // ARGV[1]=job id string, ARGV[2]=job body JSON
        // Body first so a mid-script OOM/error after map NX cannot leave a map
        // entry with no job body (producer retry would return that id → silent
        // lost enqueue). On NX loss DEL the orphan body and return the winner.
        // Returns the winning job id (existing or newly enqueued).
        let enqueue_idempotent_script = Script::new(
            r#"
            redis.call('SET', KEYS[2], ARGV[2])
            local set = redis.call('SET', KEYS[1], ARGV[1], 'NX')
            if not set then
              redis.call('DEL', KEYS[2])
              return redis.call('GET', KEYS[1])
            end
            redis.call('LPUSH', KEYS[3], ARGV[1])
            return ARGV[1]
            "#,
        );

        Ok(Self {
            conn,
            prefix: config.prefix,
            claim_script,
            ack_script,
            nack_script,
            dead_letter_script,
            promote_script,
            recover_script,
            enqueue_idempotent_script,
        })
    }

    fn pending_key(&self, queue: &str) -> String {
        format!("{}q:{}:pending", self.prefix, queue)
    }

    fn dead_key(&self, queue: &str) -> String {
        format!("{}q:{}:dead", self.prefix, queue)
    }

    fn pending_key_prefix(&self) -> String {
        format!("{}q:", self.prefix)
    }

    fn job_key(&self, id: &JobId) -> String {
        format!("{}job:{}", self.prefix, id)
    }

    fn job_key_prefix(&self) -> String {
        format!("{}job:", self.prefix)
    }

    fn dead_reason_key(&self, id: &JobId) -> String {
        format!("{}job:{}:dead_reason", self.prefix, id)
    }

    fn attempts_key(&self, id: &JobId) -> String {
        format!("{}attempts:{}", self.prefix, id)
    }

    fn attempts_key_prefix(&self) -> String {
        format!("{}attempts:", self.prefix)
    }

    fn lease_key(&self) -> String {
        format!("{}lease", self.prefix)
    }

    fn delayed_key(&self) -> String {
        format!("{}delayed", self.prefix)
    }

    fn idempotency_key(&self, key: &str) -> String {
        format!("{}idempotency:{}", self.prefix, key)
    }

    /// Lease ZSET member: `{queue}\x1f{id}\x1f{token}`.
    fn lease_member(queue: &str, id: &JobId, token: &ClaimToken) -> String {
        format!("{queue}{MEMBER_SEP}{id}{MEMBER_SEP}{token}")
    }

    /// Delayed ZSET member (no token): `{queue}\x1f{id}`.
    fn delayed_member(queue: &str, id: &JobId) -> String {
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
            .arg(self.job_key_prefix())
            .arg(self.attempts_key_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        Ok(())
    }

    /// Promote due delayed jobs onto their queue pending lists.
    async fn promote_delayed(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: i32 = self
            .promote_script
            .key(self.delayed_key())
            .arg(Self::now_ms())
            .arg(self.pending_key_prefix())
            .arg(self.job_key_prefix())
            .arg(self.attempts_key_prefix())
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
        let claim_token = ClaimToken::new();

        // Redis returns a multi-bulk {body, attempts} or null.
        let result: Option<(String, i64)> = self
            .claim_script
            .key(&pending)
            .key(&lease_key)
            .arg(self.job_key_prefix())
            .arg(expiry)
            .arg(queue)
            .arg(claim_token.as_str())
            .arg(self.attempts_key_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        let Some((body, attempts)) = result else {
            return Ok(None);
        };
        let mut job: Job = serde_json::from_str(&body)?;
        // Authoritative attempt count from atomic INCR (may differ from body field).
        job.attempts = if attempts < 0 { 0 } else { attempts as u32 };

        // Best-effort mirror attempts into the job body for observability.
        // Retry logic trusts the INCR counter, not this write.
        if let Ok(updated) = serde_json::to_string(&job) {
            let job_key = self.job_key(&job.id);
            let _: redis::RedisResult<()> = redis::cmd("SET")
                .arg(&job_key)
                .arg(updated)
                .query_async(&mut conn)
                .await;
        }

        Ok(Some(ClaimedJob { job, claim_token }))
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

        // Producer idempotency: body first, then SET NX map; return existing on collision.
        if let Some(ref key) = job.idempotency_key {
            if key.trim().is_empty() {
                return Err(CapivaraError::EmptyIdempotencyKey);
            }
            let idemp = self.idempotency_key(key);
            let returned: String = self
                .enqueue_idempotent_script
                .key(&idemp)
                .key(&job_key)
                .key(&pending)
                .arg(id.to_string())
                .arg(&body)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| CapivaraError::Broker(e.to_string()))?;
            let parsed = uuid::Uuid::parse_str(&returned).map_err(|e| {
                CapivaraError::Broker(format!("invalid idempotency job id {returned}: {e}"))
            })?;
            return Ok(JobId(parsed));
        }

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

    async fn ack(&self, id: &JobId, claim_token: &ClaimToken) -> Result<()> {
        let job = self.get_job(id).await?;
        let member = Self::lease_member(job.queue.as_str(), id, claim_token);
        let mut conn = self.conn.clone();
        let removed: i32 = self
            .ack_script
            .key(self.lease_key())
            .arg(self.job_key(id))
            .arg(member)
            .arg(self.attempts_key(id))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        if removed == 0 {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        Ok(())
    }

    async fn nack(&self, id: &JobId, claim_token: &ClaimToken, action: NackAction) -> Result<()> {
        let mut conn = self.conn.clone();
        let job_key = self.job_key(id);
        let job = self.get_job(id).await?;
        let lease_member = Self::lease_member(job.queue.as_str(), id, claim_token);

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
                    let delayed_member = Self::delayed_member(job.queue.as_str(), id);
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

    async fn dead_letter(&self, id: &JobId, claim_token: &ClaimToken, reason: &str) -> Result<()> {
        let job = self.get_job(id).await?;
        let lease_member = Self::lease_member(job.queue.as_str(), id, claim_token);
        let dead = self.dead_key(job.queue.as_str());
        let mut conn = self.conn.clone();
        let ok: i32 = self
            .dead_letter_script
            .key(self.lease_key())
            .key(&dead)
            .arg(self.job_key(id))
            .arg(&lease_member)
            .arg(id.to_string())
            .arg(self.dead_reason_key(id))
            .arg(reason)
            .arg(self.attempts_key(id))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;
        if ok == 0 {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        Ok(())
    }

    async fn list_dead(&self, queue: &QueueName) -> Result<Vec<DeadLetter>> {
        let mut conn = self.conn.clone();
        let dead = self.dead_key(queue.as_str());
        // Dead list uses LPUSH (newest at head); reverse so oldest is first.
        let ids: Vec<String> = redis::cmd("LRANGE")
            .arg(&dead)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::Broker(e.to_string()))?;

        let mut out = Vec::with_capacity(ids.len());
        for id_str in ids.into_iter().rev() {
            let id = JobId(uuid::Uuid::parse_str(&id_str).map_err(|e| {
                CapivaraError::Broker(format!("invalid dead-letter job id {id_str}: {e}"))
            })?);
            let body: Option<String> = redis::cmd("GET")
                .arg(self.job_key(&id))
                .query_async(&mut conn)
                .await
                .map_err(|e| CapivaraError::Broker(e.to_string()))?;
            let Some(body) = body else {
                // Orphan dead id without body — skip but keep listing others.
                continue;
            };
            let job: Job = serde_json::from_str(&body)?;
            let reason: String = redis::cmd("GET")
                .arg(self.dead_reason_key(&id))
                .query_async::<Option<String>>(&mut conn)
                .await
                .map_err(|e| CapivaraError::Broker(e.to_string()))?
                .unwrap_or_default();
            out.push(DeadLetter { job, reason });
        }
        Ok(out)
    }
}
