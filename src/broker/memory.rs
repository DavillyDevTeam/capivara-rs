//! In-process FIFO broker for tests and local development.
//!
//! **Not multi-process:** a `MemoryBroker` is not shared across OS processes.
//! Use [`super::RedisBroker`] (feature `redis`) for real distributed workers.
//!
//! Dead-lettered jobs are kept in an in-process per-queue list with reason
//! (inspect via [`Broker::list_dead`]; no replay in M2).
//!
//! Producer [`Job::idempotency_key`] values are stored in an in-process map
//! (`key → JobId`) under the broker mutex.
//!
//! Best-effort metrics: updates `capivara_queue_depth` from in-process pending
//! length after enqueue/claim/nack. Cheap (no network). Redis does not do this
//! on the hot path (LLEN is documented as costly).

use crate::broker::{Broker, ClaimToken, ClaimedJob, DeadLetter, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use crate::metrics;
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
    /// delayed requeue; promoted on claim when due
    delayed: Vec<(Instant, Job)>,
    /// queue name -> dead-lettered jobs (oldest first)
    dead: HashMap<String, Vec<DeadLetter>>,
    /// producer idempotency_key → first JobId (safe producer retries)
    idempotency: HashMap<String, JobId>,
}

struct InFlight {
    job: Job,
    lease_until: Instant,
    token: ClaimToken,
}

impl MemoryBroker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Move jobs whose lease expired back to pending (worker crash / no ack).
    fn recover_expired(inner: &mut Inner) {
        let now = Instant::now();
        let expired: Vec<uuid::Uuid> = inner
            .in_flight
            .iter()
            .filter(|(_, f)| f.lease_until <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(InFlight { job, .. }) = inner.in_flight.remove(&id) {
                let q = job.queue.as_str().to_string();
                inner.pending.entry(q).or_default().push_back(job);
            }
        }
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

    /// Pending (ready) depth only — not delayed or in-flight.
    fn pending_depth(inner: &Inner, queue: &str) -> usize {
        inner.pending.get(queue).map(|d| d.len()).unwrap_or(0)
    }

    fn record_pending_depth(inner: &Inner, queue: &str) {
        metrics::set_queue_depth(queue, Self::pending_depth(inner, queue));
    }
}

#[async_trait]
impl Broker for MemoryBroker {
    async fn enqueue(&self, job: Job) -> Result<JobId> {
        if let Some(ref key) = job.idempotency_key {
            if key.trim().is_empty() {
                return Err(CapivaraError::EmptyIdempotencyKey);
            }
        }
        let mut guard = self.inner.lock().await;
        // Producer idempotency: same key → existing JobId, no second queue entry.
        // Map insert + pending push are under one mutex (atomic for this process).
        let id = job.id;
        if let Some(ref key) = job.idempotency_key {
            if let Some(existing) = guard.idempotency.get(key) {
                return Ok(*existing);
            }
            guard.idempotency.insert(key.clone(), id);
        }
        let q = job.queue.as_str().to_string();
        guard.pending.entry(q.clone()).or_default().push_back(job);
        Self::record_pending_depth(&guard, &q);
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
                // Recover expired leases before promoting delayed so reclaimed
                // jobs are claimable in this same pass.
                Self::recover_expired(&mut guard);
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
                            let claim_token = ClaimToken::new();
                            guard.in_flight.insert(
                                job.id.0,
                                InFlight {
                                    job: job.clone(),
                                    lease_until,
                                    token: claim_token.clone(),
                                },
                            );
                            Self::record_pending_depth(&guard, q);
                            return Ok(Some(ClaimedJob { job, claim_token }));
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

    async fn ack(&self, id: &JobId, claim_token: &ClaimToken) -> Result<()> {
        let mut guard = self.inner.lock().await;
        match guard.in_flight.get(&id.0) {
            Some(flight) if &flight.token == claim_token => {
                guard.in_flight.remove(&id.0);
                Ok(())
            }
            _ => Err(CapivaraError::JobNotFound { id: id.to_string() }),
        }
    }

    async fn nack(&self, id: &JobId, claim_token: &ClaimToken, action: NackAction) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let matches = guard
            .in_flight
            .get(&id.0)
            .is_some_and(|f| &f.token == claim_token);
        if !matches {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        let Some(InFlight { job, .. }) = guard.in_flight.remove(&id.0) else {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        };
        match action {
            NackAction::RequeueAfter { delay } => {
                if delay.is_zero() {
                    let q = job.queue.as_str().to_string();
                    guard.pending.entry(q.clone()).or_default().push_back(job);
                    Self::record_pending_depth(&guard, &q);
                } else {
                    guard.delayed.push((Instant::now() + delay, job));
                }
            }
        }
        Ok(())
    }

    async fn dead_letter(&self, id: &JobId, claim_token: &ClaimToken, reason: &str) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let matches = guard
            .in_flight
            .get(&id.0)
            .is_some_and(|f| &f.token == claim_token);
        if !matches {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        }
        let Some(InFlight { job, .. }) = guard.in_flight.remove(&id.0) else {
            return Err(CapivaraError::JobNotFound { id: id.to_string() });
        };
        let q = job.queue.as_str().to_string();
        guard.dead.entry(q).or_default().push(DeadLetter {
            job,
            reason: reason.to_string(),
        });
        Ok(())
    }

    async fn list_dead(&self, queue: &QueueName) -> Result<Vec<DeadLetter>> {
        let guard = self.inner.lock().await;
        Ok(guard.dead.get(queue.as_str()).cloned().unwrap_or_default())
    }
}
