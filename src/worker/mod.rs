//! Worker loop: claim → run → store result? → ack or delayed nack.
//!
//! Celery analogy: the consumer process. With Redis this can run in another
//! process than the producer; with Memory it stays in-process.

use crate::broker::{Broker, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::QueueName;
use crate::registry::Registry;
use crate::result::{JobResult, ResultBackend};
use std::sync::Arc;
use std::time::Duration;

/// Default claim lease (visibility timeout).
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
/// Non-blocking claim so `run_worker` drains without sleeping when empty.
pub const DEFAULT_BLOCK: Duration = Duration::ZERO;
/// Attempts before terminal ack (claim increments attempts, so attempt 3 is last).
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Delay before a failed job becomes claimable again.
pub const DEFAULT_NACK_DELAY: Duration = Duration::from_secs(5);

/// Process jobs until the broker is empty (or a limit is hit).
pub struct Worker {
    pub registry: Arc<Registry>,
    pub broker: Arc<dyn Broker>,
    pub results: Option<Arc<dyn ResultBackend>>,
    /// Queues to claim from (empty → `default` only).
    pub queues: Vec<QueueName>,
    /// Claim lease duration.
    pub lease: Duration,
    /// Max claim attempts before terminal ack on failure.
    pub max_attempts: u32,
    /// Delay used for [`NackAction::RequeueAfter`] on retryable failures.
    pub nack_delay: Duration,
}

impl Worker {
    /// Run until no more jobs can be claimed (non-blocking), or `max_jobs` is hit.
    ///
    /// Policy:
    /// - Success → store Success (if backend) → ack
    /// - Err / panic → store Failure (if backend) → nack(RequeueAfter) while
    ///   `attempts < max_attempts`, else terminal ack
    /// - Unknown task → store Failure (if backend) → terminal ack (not retryable)
    pub async fn run(&self, max_jobs: Option<usize>) -> Result<usize> {
        let mut processed = 0usize;
        let queues = if self.queues.is_empty() {
            vec![QueueName::default()]
        } else {
            self.queues.clone()
        };

        loop {
            if max_jobs.is_some_and(|m| processed >= m) {
                break;
            }
            let Some(claimed) = self
                .broker
                .claim(&queues, self.lease, DEFAULT_BLOCK)
                .await?
            else {
                break;
            };
            let job = claimed.job;
            let attempts = job.attempts;
            let job_id = job.id;

            let handler = match self.registry.get(&job.task_name) {
                Ok(h) => h,
                Err(e) => {
                    if let Some(backend) = &self.results {
                        backend
                            .store(
                                &job_id,
                                JobResult::Failure {
                                    message: e.to_string(),
                                },
                            )
                            .await?;
                    }
                    // Unknown task is terminal — not a retryable handler failure.
                    self.broker.ack(&job_id).await?;
                    processed += 1;
                    continue;
                }
            };

            let payload = job.payload;

            // Isolate panics: spawn catches unwind as JoinError.
            let join = tokio::spawn(async move { handler(payload).await });

            let outcome = match join.await {
                Ok(Ok(bytes)) => JobResult::Success { payload: bytes },
                Ok(Err(CapivaraError::TaskFailed { message })) => JobResult::Failure { message },
                Ok(Err(e)) => JobResult::Failure {
                    message: e.to_string(),
                },
                Err(join_err) => JobResult::Failure {
                    message: format!("task panicked: {join_err}"),
                },
            };

            let is_success = matches!(outcome, JobResult::Success { .. });

            if let Some(backend) = &self.results {
                backend.store(&job_id, outcome).await?;
            }

            if is_success {
                self.broker.ack(&job_id).await?;
            } else if attempts < self.max_attempts {
                self.broker
                    .nack(
                        &job_id,
                        NackAction::RequeueAfter {
                            delay: self.nack_delay,
                        },
                    )
                    .await?;
            } else {
                // Terminal failure after max attempts.
                self.broker.ack(&job_id).await?;
            }

            processed += 1;
        }
        Ok(processed)
    }
}
