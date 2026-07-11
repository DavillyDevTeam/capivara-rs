//! Broker abstraction — put/get jobs.
//!
//! Celery analogy: Kombu / Redis transport. M0 ships only [`memory::MemoryBroker`].

mod memory;

pub use memory::MemoryBroker;

use crate::error::Result;
use crate::job::{Job, JobId};
use async_trait::async_trait;

/// Transport for job messages.
///
/// Uses `async_trait` so `dyn Broker` stays object-safe for `App`.
/// Task handlers themselves use native async (see [`crate::Task`]).
#[async_trait]
pub trait Broker: Send + Sync {
    /// Enqueue a job; returns its id.
    async fn enqueue(&self, job: Job) -> Result<JobId>;

    /// Claim the next available job, if any.
    async fn claim(&self) -> Result<Option<Job>>;

    /// Acknowledge successful processing (M0: remove from in-flight tracking if any).
    async fn ack(&self, id: &JobId) -> Result<()>;
}
