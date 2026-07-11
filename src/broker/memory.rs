//! In-process FIFO broker for tests and local development.
//!
//! **Not multi-process:** a `MemoryBroker` is not shared across OS processes.
//! Use Redis (M1+) for real distributed workers.

use crate::broker::Broker;
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct MemoryBroker {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    pending: VecDeque<Job>,
    /// Jobs claimed but not yet acked (simple in-flight set).
    in_flight: HashMap<uuid::Uuid, Job>,
}

impl MemoryBroker {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Broker for MemoryBroker {
    async fn enqueue(&self, job: Job) -> Result<JobId> {
        let id = job.id;
        let mut guard = self.inner.lock().await;
        guard.pending.push_back(job);
        Ok(id)
    }

    async fn claim(&self) -> Result<Option<Job>> {
        let mut guard = self.inner.lock().await;
        let Some(mut job) = guard.pending.pop_front() else {
            return Ok(None);
        };
        job.attempts = job.attempts.saturating_add(1);
        guard.in_flight.insert(job.id.0, job.clone());
        Ok(Some(job))
    }

    async fn ack(&self, id: &JobId) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if guard.in_flight.remove(&id.0).is_none() {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        Ok(())
    }
}
