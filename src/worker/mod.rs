//! Worker loop: claim → run → store result? → ack / delayed nack / dead_letter.
//!
//! Celery analogy: the consumer process. With Redis this can run in another
//! process than the producer; with Memory it stays in-process.
//!
//! Concurrency: up to N jobs run as concurrent Tokio tasks (default 4),
//! limited by a [`tokio::sync::Semaphore`].
//!
//! **Result policy:** `JobResult::Failure` is stored only on **terminal**
//! outcomes after a successful `dead_letter` (max attempts exhausted or unknown
//! task). Intermediate retries and lost-lease races leave the result backend
//! empty (`ResultNotFound`).

use crate::broker::{Broker, ClaimToken, ClaimedJob, NackAction};
use crate::error::{CapivaraError, Result};
use crate::job::{JobId, QueueName};
use crate::registry::Registry;
use crate::result::{JobResult, ResultBackend};
use crate::retry::RetryPolicy;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::Instrument;

/// Default claim lease (visibility timeout).
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
/// Non-blocking claim so `run_worker` drains without sleeping when empty.
pub const DEFAULT_BLOCK: Duration = Duration::ZERO;
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
    /// Retry / nack requeue policy (max attempts + exponential backoff delay).
    pub retry_policy: RetryPolicy,
    /// Max concurrent in-flight jobs (clamped to ≥ 1).
    pub concurrency: usize,
}

impl Worker {
    /// Run until no more jobs can be claimed (non-blocking), or `max_jobs` is hit.
    ///
    /// Policy:
    /// - Success → store Success (if backend) → ack
    /// - Err / panic, `attempts < max_attempts` → **no** result store →
    ///   nack(RequeueAfter { delay: policy.delay_for_attempt(attempts) })
    /// - Err / panic, `attempts >= max_attempts` → dead_letter(reason), then
    ///   store Failure **only if** dead_letter confirmed ownership
    /// - Unknown task → dead_letter("unknown task…"), then store Failure only
    ///   if dead_letter confirmed ownership
    ///
    /// Nack delay is computed via [`RetryPolicy::delay_for_attempt`] from the
    /// job's current claim count (`Job.attempts`).
    ///
    /// Lost-lease `JobNotFound` from ack/nack/dead_letter is non-fatal (another
    /// claim may already own the job after recover); the drain continues and
    /// Failure is **not** stored when dead_letter loses the race (keeps
    /// `JobResult::Failure` ≈ terminal).
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
        let mut first_err: Option<CapivaraError> = None;

        loop {
            // Reap completed work without blocking when we can still claim.
            while let Some(joined) = in_flight.try_join_next() {
                account_join(joined, &mut processed, &mut first_err);
            }

            if first_err.is_some() {
                break;
            }

            if max_jobs.is_some_and(|m| processed >= m) {
                break;
            }

            let slots_used = in_flight.len();
            let under_max = max_jobs.is_none_or(|m| processed + slots_used < m);

            if !under_max {
                if in_flight.is_empty() {
                    break;
                }
                // Wait for one in-flight job so we can re-check limits.
                if let Some(joined) = in_flight.join_next().await {
                    account_join(joined, &mut processed, &mut first_err);
                }
                continue;
            }

            // Try to acquire a concurrency permit without blocking.
            let permit = match Arc::clone(&sem).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // At concurrency limit — wait for one job to finish.
                    // Empty JoinSet + exhausted semaphore is an invariant break
                    // (would busy-loop otherwise).
                    if in_flight.is_empty() {
                        first_err = Some(CapivaraError::Internal {
                            message: "semaphore exhausted with no in-flight jobs (permit leak)"
                                .into(),
                        });
                        break;
                    }
                    if let Some(joined) = in_flight.join_next().await {
                        account_join(joined, &mut processed, &mut first_err);
                    }
                    continue;
                }
            };

            // Span only on a real claim: empty Ok(None) polls are common in drain
            // and would flood INFO with Empty fields. Fields are set at creation
            // (not late-recorded after the instrumented future ends).
            match self.broker.claim(&queues, self.lease, DEFAULT_BLOCK).await {
                Ok(Some(claimed)) => {
                    let claim_span = tracing::info_span!(
                        "capivara.claim",
                        job.id = %claimed.job.id,
                        task.name = claimed.job.task_name.as_str(),
                        queue = claimed.job.queue.as_str(),
                        attempt = claimed.job.attempts,
                    );
                    let _enter = claim_span.enter();

                    let broker = Arc::clone(&self.broker);
                    let results = self.results.clone();
                    let registry = Arc::clone(&self.registry);
                    let retry_policy = self.retry_policy;
                    in_flight.spawn(async move {
                        let _permit: OwnedSemaphorePermit = permit;
                        process_one(broker, results, registry, claimed, retry_policy).await
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
                        account_join(joined, &mut processed, &mut first_err);
                    }
                }
                Err(e) => {
                    drop(permit);
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }

        // Drain remaining in-flight work.
        while let Some(joined) = in_flight.join_next().await {
            account_join(joined, &mut processed, &mut first_err);
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
        task_name: &str,
        queue: &str,
        attempt: u32,
    ) -> Result<()> {
        let span = tracing::info_span!(
            "capivara.ack",
            job.id = %id,
            task.name = task_name,
            queue = queue,
            attempt = attempt,
        );
        async {
            match broker.ack(id, claim_token).await {
                Ok(()) => Ok(()),
                Err(CapivaraError::JobNotFound { .. }) => Ok(()),
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await
    }

    /// Nack; treat lost ownership as non-fatal so the drain keeps going.
    async fn settle_nack(
        broker: &Arc<dyn Broker>,
        id: &JobId,
        claim_token: &ClaimToken,
        action: NackAction,
        task_name: &str,
        queue: &str,
        attempt: u32,
    ) -> Result<()> {
        let span = tracing::info_span!(
            "capivara.nack",
            job.id = %id,
            task.name = task_name,
            queue = queue,
            attempt = attempt,
        );
        async {
            match broker.nack(id, claim_token, action).await {
                Ok(()) => Ok(()),
                Err(CapivaraError::JobNotFound { .. }) => Ok(()),
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await
    }

    /// Dead-letter this claim.
    ///
    /// Returns `Ok(true)` if ownership was confirmed and the job was moved to
    /// the DLQ. Returns `Ok(false)` if the claim was already lost (`JobNotFound`)
    /// so the caller can skip storing terminal Failure. Other broker errors
    /// propagate.
    async fn settle_dead_letter(
        broker: &Arc<dyn Broker>,
        id: &JobId,
        claim_token: &ClaimToken,
        reason: &str,
        task_name: &str,
        queue: &str,
        attempt: u32,
    ) -> Result<bool> {
        let span = tracing::info_span!(
            "capivara.dead_letter",
            job.id = %id,
            task.name = task_name,
            queue = queue,
            attempt = attempt,
        );
        async {
            match broker.dead_letter(id, claim_token, reason).await {
                Ok(()) => Ok(true),
                Err(CapivaraError::JobNotFound { .. }) => Ok(false),
                Err(e) => Err(e),
            }
        }
        .instrument(span)
        .await
    }
}

/// Record a finished JoinSet task: always increments `processed`; stores the
/// first infrastructure / outer-panic error for later return.
fn account_join(
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
    processed: &mut usize,
    first_err: &mut Option<CapivaraError>,
) {
    *processed += 1;
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            if first_err.is_none() {
                *first_err = Some(e);
            }
        }
        Err(join_err) => {
            // Outer task panicked (should be rare — handler panics are isolated
            // inside process_one).
            if first_err.is_none() {
                *first_err = Some(CapivaraError::TaskPanicked {
                    message: join_err.to_string(),
                });
            }
        }
    }
}

/// Run a single claimed job: handler → ack/nack/dead_letter → (maybe) store.
async fn process_one(
    broker: Arc<dyn Broker>,
    results: Option<Arc<dyn ResultBackend>>,
    registry: Arc<Registry>,
    claimed: ClaimedJob,
    retry_policy: RetryPolicy,
) -> Result<()> {
    let job = claimed.job;
    let claim_token = claimed.claim_token;
    let attempts = job.attempts;
    let job_id = job.id;
    let task_name = job.task_name.clone();
    let queue = job.queue.as_str().to_string();

    let handle_span = tracing::info_span!(
        "capivara.handle",
        job.id = %job_id,
        task.name = task_name.as_str(),
        queue = queue.as_str(),
        attempt = attempts,
    );

    async {
        let handler = match registry.get(&task_name) {
            Ok(h) => h,
            Err(e) => {
                let message = e.to_string();
                // Unknown task is terminal — dead-letter first so Failure ≈ ownership.
                let reason = format!("unknown task: {task_name}");
                let owned = Worker::settle_dead_letter(
                    &broker,
                    &job_id,
                    &claim_token,
                    &reason,
                    &task_name,
                    &queue,
                    attempts,
                )
                .await?;
                if owned {
                    if let Some(backend) = &results {
                        backend
                            .store(&job_id, JobResult::Failure { message })
                            .await?;
                    }
                }
                return Ok(());
            }
        };

        let payload = job.payload;

        // Isolate panics: spawn catches unwind as JoinError.
        // Propagate `capivara.handle` so spans inside Task::run parent correctly
        // (tokio::spawn would otherwise drop the tracing context).
        let handle_ctx = tracing::Span::current();
        let join = tokio::spawn(async move { handler(payload).await }.instrument(handle_ctx));

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

        match outcome {
            JobResult::Success { payload } => {
                if let Some(backend) = &results {
                    backend
                        .store(&job_id, JobResult::Success { payload })
                        .await?;
                }
                Worker::settle_ack(&broker, &job_id, &claim_token, &task_name, &queue, attempts)
                    .await?;
            }
            JobResult::Failure { message } => {
                if attempts < retry_policy.max_attempts {
                    // Intermediate retry: do **not** store Failure.
                    let delay = retry_policy.delay_for_attempt(attempts);
                    Worker::settle_nack(
                        &broker,
                        &job_id,
                        &claim_token,
                        NackAction::RequeueAfter { delay },
                        &task_name,
                        &queue,
                        attempts,
                    )
                    .await?;
                } else {
                    // Terminal: dead_letter first; store Failure only if we still own
                    // the claim (avoids Failure while another claim may still run).
                    let owned = Worker::settle_dead_letter(
                        &broker,
                        &job_id,
                        &claim_token,
                        &message,
                        &task_name,
                        &queue,
                        attempts,
                    )
                    .await?;
                    if owned {
                        if let Some(backend) = &results {
                            backend
                                .store(&job_id, JobResult::Failure { message })
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
    .instrument(handle_span)
    .await
}
