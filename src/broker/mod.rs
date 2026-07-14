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
use std::time::Duration;

/// A job successfully claimed from the broker (may carry lease metadata later).
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job: Job,
}

/// What to do when a worker cannot complete a job (M1: delayed requeue).
#[derive(Debug, Clone)]
pub enum NackAction {
    /// Put the job aside until `delay` elapses, then make it claimable again.
    ///
    /// Redis uses a delayed ZSET; Memory requeues immediately in PR-A
    /// (full delayed memory behavior can match Redis later).
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
    /// - `lease`: how long the worker may hold the job before a recoverer (PR-B)
    ///   is allowed to steal it. Brokers should record this even if recoverer
    ///   is not running yet.
    /// - `block_for`: max time to wait for a job. Zero means non-blocking
    ///   (return `Ok(None)` immediately if empty).
    async fn claim(
        &self,
        queues: &[QueueName],
        lease: Duration,
        block_for: Duration,
    ) -> Result<Option<ClaimedJob>>;

    /// Acknowledge successful processing (remove from in-flight / lease).
    async fn ack(&self, id: &JobId) -> Result<()>;

    /// Negative-ack: requeue according to `action` (no DLQ in M1).
    async fn nack(&self, id: &JobId, action: NackAction) -> Result<()>;
}
