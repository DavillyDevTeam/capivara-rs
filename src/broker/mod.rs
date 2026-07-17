//! Broker abstraction — put/get jobs.
//!
//! Celery analogy: Kombu / Redis transport.
//! - M0/default: [`memory::MemoryBroker`] (single-process).
//! - Feature `redis`: [`redis_broker::RedisBroker`] (multi-process capable).

mod memory;
#[cfg(feature = "redis")]
mod redis_broker;

pub use memory::MemoryBroker;
#[cfg(feature = "redis")]
pub use redis_broker::{RedisBroker, RedisConfig};

use crate::error::Result;
use crate::job::{Job, JobId, QueueName};
use async_trait::async_trait;
use std::fmt;
use std::time::Duration;
use uuid::Uuid;

/// Opaque claim / delivery token. Ack and nack must present the token issued
/// at claim time so a late settle cannot steal a newer claim after lease recovery.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClaimToken(Uuid);

impl ClaimToken {
    /// Fresh random token for a new claim.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Token string form (stable for Redis lease members).
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl Default for ClaimToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ClaimToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A job successfully claimed from the broker, with claim-scoped ownership.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job: Job,
    /// Must be passed to [`Broker::ack`] / [`Broker::nack`] for this claim.
    pub claim_token: ClaimToken,
}

/// What to do when a worker cannot complete a job (M1: delayed requeue).
#[derive(Debug, Clone)]
pub enum NackAction {
    /// Put the job aside until `delay` elapses, then make it claimable again.
    ///
    /// Both Redis and Memory honor `delay` (Memory uses an in-process delayed
    /// list; Redis uses a delayed ZSET promoted on claim).
    RequeueAfter { delay: Duration },
}

/// Transport for job messages.
///
/// Uses `async_trait` so `dyn Broker` stays object-safe for `App`.
#[async_trait]
pub trait Broker: Send + Sync {
    /// Enqueue a job; returns its id.
    async fn enqueue(&self, job: Job) -> Result<JobId>;

    /// Claim the next available job from any of `queues`.
    ///
    /// - `lease`: how long the worker may hold the job before recover-on-claim
    ///   is allowed to return it to pending.
    /// - `block_for`: max time to wait for a job. Zero means non-blocking
    ///   (return `Ok(None)` immediately if empty).
    ///
    /// On each claim loop iteration brokers recover expired leases, then promote
    /// due delayed jobs, then try to claim.
    ///
    /// The returned [`ClaimedJob::claim_token`] must be used for subsequent
    /// `ack` / `nack` of this claim.
    async fn claim(
        &self,
        queues: &[QueueName],
        lease: Duration,
        block_for: Duration,
    ) -> Result<Option<ClaimedJob>>;

    /// Complete this claim attempt: drop the lease / in-flight entry.
    ///
    /// Does **not** imply the task handler succeeded — the worker may `ack`
    /// after storing a failure result when attempts are exhausted (or for
    /// unknown tasks). Succeeds only when `claim_token` matches the active claim.
    async fn ack(&self, id: &JobId, claim_token: &ClaimToken) -> Result<()>;

    /// Negative-ack: requeue according to `action` (no DLQ in M1).
    ///
    /// Succeeds only when `claim_token` matches the active claim.
    async fn nack(&self, id: &JobId, claim_token: &ClaimToken, action: NackAction) -> Result<()>;
}
