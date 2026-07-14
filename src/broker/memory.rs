//! In-process FIFO broker for tests and local development.
//!
//! **Not multi-process:** a `MemoryBroker` is not shared across OS processes.
//! Use [`super::RedisBroker`] (feature `redis`) for real distributed workers.

use crate::broker::{Broker, ClaimedJob, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct MemoryBroker {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// queue name -> pending jobs
    pending: HashMap<String, VecDeque<Job>>,
    in_flight: HashMap<uuid::Uuid, InFlight>,
    /// delayed requeue (minimal; PR-B aligns Redis semantics)
    delayed: Vec<(Instant, Job)>,
}

struct InFlight {
    job: Job,
    #[allow(dead_code)]
    lease_until: Instant,
}

impl MemoryBroker {
    pub fn new() -> Self {
        Self::default()
    }

    fn promote_delayed(inner: &mut Inner) {
        let now = Instant::now();
        let mut keep = Vec::new();
        for (when, job) in inner.delayed.drain(..) {
            if when <= now {
                let q = job.queue.as_str().to_string();
                inner.pending.entry(q).or_default().push_back(job);
            } else {
                keep.push((when, job));
            }
        }
        inner.delayed = keep;
    }
}

#[async_trait]
impl Broker for MemoryBroker {
    async fn enqueue(&self, job: Job) -> Result<JobId> {
        let id = job.id;
        let mut guard = self.inner.lock().await;
        let q = job.queue.as_str().to_string();
        guard.pending.entry(q).or_default().push_back(job);
        Ok(id)
    }

    async fn claim(
        &self,
        queues: &[QueueName],
        lease: Duration,
        block_for: Duration,
    ) -> Result<Option<ClaimedJob>> {
        let deadline = Instant::now() + block_for;
        loop {
            {
                let mut guard = self.inner.lock().await;
                Self::promote_delayed(&mut guard);

                let queue_names: Vec<String> = if queues.is_empty() {
                    vec![QueueName::default().as_str().to_string()]
                } else {
                    queues.iter().map(|q| q.as_str().to_string()).collect()
                };

                for q in &queue_names {
                    if let Some(deque) = guard.pending.get_mut(q) {
                        if let Some(mut job) = deque.pop_front() {
                            job.attempts = job.attempts.saturating_add(1);
                            let lease_until = Instant::now() + lease;
                            guard.in_flight.insert(
                                job.id.0,
                                InFlight {
                                    job: job.clone(),
                                    lease_until,
                                },
                            );
                            return Ok(Some(ClaimedJob { job }));
                        }
                    }
                }
            }

            // Non-blocking or timed out.
            if block_for.is_zero() || Instant::now() >= deadline {
                return Ok(None);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let slice = remaining.min(Duration::from_millis(20));
            tokio::time::sleep(slice).await;
        }
    }

    async fn ack(&self, id: &JobId) -> Result<()> {
        let mut guard = self.inner.lock().await;
        if guard.in_flight.remove(&id.0).is_none() {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        Ok(())
    }

    async fn nack(&self, id: &JobId, action: NackAction) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let Some(InFlight { job, .. }) = guard.in_flight.remove(&id.0) else {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        };
        match action {
            NackAction::RequeueAfter { delay } => {
                if delay.is_zero() {
                    let q = job.queue.as_str().to_string();
                    guard.pending.entry(q).or_default().push_back(job);
                } else {
                    guard.delayed.push((Instant::now() + delay, job));
                }
            }
        }
        Ok(())
    }
}
