//! Worker loop: claim → run → store result? → ack or delayed nack.
//!
//! Celery analogy: the consumer process. With Redis this can run in another
//! process than the producer; with Memory it stays in-process.
//!
//! Concurrency: up to N jobs run as concurrent Tokio tasks (default 4),
//! limited by a [`tokio::sync::Semaphore`].

use crate::broker::{Broker, ClaimToken, ClaimedJob, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{JobId, QueueName};
use crate::registry::Registry;
use crate::result::{JobResult, ResultBackend};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

/// Default claim lease (visibility timeout).
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
/// Non-blocking claim so `run_worker` drains without sleeping when empty.
pub const DEFAULT_BLOCK: Duration = Duration::ZERO;
/// Attempts before terminal ack (claim increments attempts, so attempt 3 is last).
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Delay before a failed job becomes claimable again.
pub const DEFAULT_NACK_DELAY: Duration = Duration::from_secs(5);
/// Default max concurrent in-flight jobs per worker drain.
pub const DEFAULT_CONCURRENCY: usize = 4;

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
    /// Max concurrent in-flight jobs (clamped to ≥ 1).
    pub concurrency: usize,
}

impl Worker {
    /// Run until no more jobs can be claimed (non-blocking), or `max_jobs` is hit.
    ///
    /// Policy:
    /// - Success → store Success (if backend) → ack
    /// - Err / panic → store Failure (if backend) → nack(RequeueAfter) while
    ///   `attempts < max_attempts`, else terminal ack
    /// - Unknown task → store Failure (if backend) → terminal ack (not retryable)
    ///
    /// Lost-lease `JobNotFound` from ack/nack is non-fatal (another claim may
    /// already own the job after recover); the drain continues.
    ///
    /// Concurrency: claims while under `concurrency` and under `max_jobs`; each
    /// claimed job keeps its own [`ClaimToken`]. When the broker is empty and
    /// nothing is in-flight, exit. When `max_jobs` is reached, wait for
    /// in-flight work then exit.
    pub async fn run(&self, max_jobs: Option<usize>) -> Result<usize> {
        let concurrency = self.concurrency.max(1);
        let sem = Arc::new(Semaphore::new(concurrency));
        let queues = if self.queues.is_empty() {
            vec![QueueName::default()]
        } else {
            self.queues.clone()
        };

        let mut processed = 0usize;
        let mut in_flight: JoinSet<Result<()>> = JoinSet::new();
        let mut stop_claiming = false;
        let mut first_err: Option<CapivaraError> = None;

        loop {
            // Reap completed work without blocking when we can still claim.
            while let Some(joined) = poll_join_next(&mut in_flight) {
                match joined {
                    Ok(Ok(())) => processed += 1,
                    Ok(Err(e)) => {
                        processed += 1;
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                        stop_claiming = true;
                    }
                    Err(join_err) => {
                        // Outer task panicked (should be rare — handler panics
                        // are isolated inside process_one).
                        processed += 1;
                        if first_err.is_none() {
                            first_err = Some(CapivaraError::TaskPanicked {
                                message: join_err.to_string(),
                            });
                        }
                        stop_claiming = true;
                    }
                }
            }

            if first_err.is_some() {
                break;
            }

            if max_jobs.is_some_and(|m| processed >= m) {
                break;
            }

            let slots_used = in_flight.len();
            let under_max = max_jobs.is_none_or(|m| processed + slots_used < m);

            if stop_claiming || !under_max {
                if in_flight.is_empty() {
                    break;
                }
                // Wait for one in-flight job so we can re-check limits.
                if let Some(joined) = in_flight.join_next().await {
                    match joined {
                        Ok(Ok(())) => processed += 1,
                        Ok(Err(e)) => {
                            processed += 1;
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                            stop_claiming = true;
                        }
                        Err(join_err) => {
                            processed += 1;
                            if first_err.is_none() {
                                first_err = Some(CapivaraError::TaskPanicked {
                                    message: join_err.to_string(),
                                });
                            }
                            stop_claiming = true;
                        }
                    }
                }
                continue;
            }

            // Try to acquire a concurrency permit without blocking.
            let permit = match Arc::clone(&sem).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // At concurrency limit — wait for one job to finish.
                    if let Some(joined) = in_flight.join_next().await {
                        match joined {
                            Ok(Ok(())) => processed += 1,
                            Ok(Err(e)) => {
                                processed += 1;
                                if first_err.is_none() {
                                    first_err = Some(e);
                                }
                                stop_claiming = true;
                            }
                            Err(join_err) => {
                                processed += 1;
                                if first_err.is_none() {
                                    first_err = Some(CapivaraError::TaskPanicked {
                                        message: join_err.to_string(),
                                    });
                                }
                                stop_claiming = true;
                            }
                        }
                    }
                    continue;
                }
            };

            match self.broker.claim(&queues, self.lease, DEFAULT_BLOCK).await {
                Ok(Some(claimed)) => {
                    let broker = Arc::clone(&self.broker);
                    let results = self.results.clone();
                    let registry = Arc::clone(&self.registry);
                    let max_attempts = self.max_attempts;
                    let nack_delay = self.nack_delay;
                    in_flight.spawn(async move {
                        let _permit: OwnedSemaphorePermit = permit;
                        process_one(broker, results, registry, claimed, max_attempts, nack_delay)
                            .await
                    });
                }
                Ok(None) => {
                    // Release unused permit; if nothing in-flight, drain is done.
                    drop(permit);
                    if in_flight.is_empty() {
                        break;
                    }
                    // Wait for in-flight; a nack(delay=0) may requeue work.
                    if let Some(joined) = in_flight.join_next().await {
                        match joined {
                            Ok(Ok(())) => processed += 1,
                            Ok(Err(e)) => {
                                processed += 1;
                                if first_err.is_none() {
                                    first_err = Some(e);
                                }
                                stop_claiming = true;
                            }
                            Err(join_err) => {
                                processed += 1;
                                if first_err.is_none() {
                                    first_err = Some(CapivaraError::TaskPanicked {
                                        message: join_err.to_string(),
                                    });
                                }
                                stop_claiming = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    drop(permit);
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    stop_claiming = true;
                }
            }
        }

        // Drain remaining in-flight work.
        while let Some(joined) = in_flight.join_next().await {
            match joined {
                Ok(Ok(())) => processed += 1,
                Ok(Err(e)) => {
                    processed += 1;
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(join_err) => {
                    processed += 1;
                    if first_err.is_none() {
                        first_err = Some(CapivaraError::TaskPanicked {
                            message: join_err.to_string(),
                        });
                    }
                }
            }
        }

        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(processed)
    }

    /// Ack; treat lost ownership as non-fatal so the drain keeps going.
    async fn settle_ack(
        broker: &Arc<dyn Broker>,
        id: &JobId,
        claim_token: &ClaimToken,
    ) -> Result<()> {
        match broker.ack(id, claim_token).await {
            Ok(()) => Ok(()),
            Err(CapivaraError::JobNotFound { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Nack; treat lost ownership as non-fatal so the drain keeps going.
    async fn settle_nack(
        broker: &Arc<dyn Broker>,
        id: &JobId,
        claim_token: &ClaimToken,
        action: NackAction,
    ) -> Result<()> {
        match broker.nack(id, claim_token, action).await {
            Ok(()) => Ok(()),
            Err(CapivaraError::JobNotFound { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Non-blocking poll of the next completed join (None if none ready).
fn poll_join_next(
    set: &mut JoinSet<Result<()>>,
) -> Option<std::result::Result<Result<()>, tokio::task::JoinError>> {
    if set.is_empty() {
        return None;
    }
    // Prefer try_join_next when available (tokio 1.40+); fall back to
    // zero-timeout biased poll via join_next only when something might be ready.
    // `try_join_next` is on JoinSet since tokio 1.40.
    set.try_join_next()
}

/// Run a single claimed job: handler → store → ack/nack with this claim's token.
async fn process_one(
    broker: Arc<dyn Broker>,
    results: Option<Arc<dyn ResultBackend>>,
    registry: Arc<Registry>,
    claimed: ClaimedJob,
    max_attempts: u32,
    nack_delay: Duration,
) -> Result<()> {
    let job = claimed.job;
    let claim_token = claimed.claim_token;
    let attempts = job.attempts;
    let job_id = job.id;

    let handler = match registry.get(&job.task_name) {
        Ok(h) => h,
        Err(e) => {
            if let Some(backend) = &results {
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
            Worker::settle_ack(&broker, &job_id, &claim_token).await?;
            return Ok(());
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

    if let Some(backend) = &results {
        backend.store(&job_id, outcome).await?;
    }

    if is_success {
        Worker::settle_ack(&broker, &job_id, &claim_token).await?;
    } else if attempts < max_attempts {
        Worker::settle_nack(
            &broker,
            &job_id,
            &claim_token,
            NackAction::RequeueAfter { delay: nack_delay },
        )
        .await?;
    } else {
        // Terminal failure after max attempts.
        Worker::settle_ack(&broker, &job_id, &claim_token).await?;
    }

    Ok(())
}
