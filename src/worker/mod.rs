//! Worker loop: claim → run → store result? → ack.
//!
//! Celery analogy: the consumer process, but M0 runs **in-process** (same as tests).

use crate::broker::Broker;
use crate::error::{CapivaraError, Result};
use crate::registry::Registry;
use crate::result::{JobResult, ResultBackend};
use std::sync::Arc;

/// Process jobs until the broker is empty (or a limit is hit).
pub struct Worker {
    pub registry: Arc<Registry>,
    pub broker: Arc<dyn Broker>,
    pub results: Option<Arc<dyn ResultBackend>>,
}

impl Worker {
    /// Run until no more jobs can be claimed, or `max_jobs` is reached if set.
    pub async fn run(&self, max_jobs: Option<usize>) -> Result<usize> {
        let mut processed = 0usize;
        loop {
            if max_jobs.is_some_and(|m| processed >= m) {
                break;
            }
            let Some(job) = self.broker.claim().await? else {
                break;
            };

            let handler = match self.registry.get(&job.task_name) {
                Ok(h) => h,
                Err(e) => {
                    if let Some(backend) = &self.results {
                        backend
                            .store(
                                &job.id,
                                JobResult::Failure {
                                    message: e.to_string(),
                                },
                            )
                            .await?;
                    }
                    self.broker.ack(&job.id).await?;
                    processed += 1;
                    continue;
                }
            };

            let job_id = job.id;
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

            if let Some(backend) = &self.results {
                backend.store(&job_id, outcome).await?;
            }

            self.broker.ack(&job_id).await?;
            processed += 1;
        }
        Ok(processed)
    }
}
