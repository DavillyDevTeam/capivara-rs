//! Application facade: register tasks, send jobs, run workers, get results.
//!
//! Celery analogy: the `Celery` app object — without import-time magic.

use crate::broker::Broker;
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use crate::registry::Registry;
use crate::result::{JobResult, ResultBackend};
use crate::retry::RetryPolicy;
use crate::task::Task;
use crate::worker::{DEFAULT_CONCURRENCY, DEFAULT_LEASE, Worker};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::Instrument;

/// Capivara application: registry + broker + optional result backend.
pub struct App {
    registry: Arc<Mutex<Registry>>,
    broker: Arc<dyn Broker>,
    results: Option<Arc<dyn ResultBackend>>,
    default_queue: QueueName,
    lease: Duration,
    retry_policy: RetryPolicy,
    concurrency: usize,
}

impl App {
    /// Build an app with a broker and no result backend (fire-and-forget).
    pub fn new(broker: impl Broker + 'static) -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::new())),
            broker: Arc::new(broker),
            results: None,
            default_queue: QueueName::default(),
            lease: DEFAULT_LEASE,
            retry_policy: RetryPolicy::default(),
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Attach a result backend (enables [`Self::get_result`]).
    pub fn with_result_backend(mut self, backend: impl ResultBackend + 'static) -> Self {
        self.results = Some(Arc::new(backend));
        self
    }

    pub fn with_default_queue(mut self, queue: impl Into<QueueName>) -> Self {
        self.default_queue = queue.into();
        self
    }

    /// Claim lease duration passed to the worker (default 30s).
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Full retry / nack requeue policy (exponential backoff + jitter).
    ///
    /// `max_attempts` below 1 is clamped to 1.
    pub fn with_retry_policy(mut self, mut policy: RetryPolicy) -> Self {
        policy.max_attempts = policy.max_attempts.max(1);
        self.retry_policy = policy;
        self
    }

    /// Max claim attempts before a failure is terminal (default 3).
    ///
    /// Values below 1 are clamped to 1 (every claim is at least one attempt).
    /// Convenience for [`RetryPolicy::max_attempts`]; use
    /// [`Self::with_retry_policy`] for full control.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.retry_policy.max_attempts = max_attempts.max(1);
        self
    }

    /// Sets [`RetryPolicy::base_delay`] used for exponential backoff (default 1s).
    ///
    /// For simple configs this approximates a fixed nack delay when
    /// `max_attempts` is small and jitter is disabled; full control (cap,
    /// jitter, base) is via [`Self::with_retry_policy`].
    pub fn with_nack_delay(mut self, nack_delay: Duration) -> Self {
        self.retry_policy.base_delay = nack_delay;
        self
    }

    /// Max concurrent in-flight jobs in [`Self::run_worker`] (default 4).
    ///
    /// Values below 1 are clamped to 1.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Shared broker handle (same instance the app uses for `send` / worker).
    ///
    /// Useful for tests and advanced injection (e.g. enqueue a raw [`Job`] that
    /// bypasses typed [`Self::send`]). Future broker-side validation on the
    /// Redis/send path (if any) will not apply to raw [`Broker::enqueue`] via
    /// this escape hatch.
    pub fn broker(&self) -> Arc<dyn Broker> {
        Arc::clone(&self.broker)
    }

    /// Register a typed task. Duplicate [`Task::NAME`] is an error.
    pub async fn register<T: Task>(&self) -> Result<()> {
        let mut reg = self.registry.lock().await;
        reg.register::<T>()
    }

    /// Enqueue a job for `T` with typed arguments.
    pub async fn send<T: Task>(&self, args: &T::Args) -> Result<JobId> {
        self.send_inner::<T>(args, None).await
    }

    /// Enqueue with a producer **idempotency key** for safe producer retries.
    ///
    /// If the same key was already enqueued on this broker, returns the existing
    /// [`JobId`] and does **not** create a second queue entry. This applies even
    /// when the first job is still in-flight, completed, or dead-lettered.
    ///
    /// **At-least-once still applies** for in-flight worker crashes: the key only
    /// dedupes producer-side retries, not worker redelivery after lease recovery.
    /// Tasks should still be written to tolerate duplicate execution.
    ///
    /// Keys are **global per broker** (not scoped by task name or queue). Callers
    /// that need isolation should embed task/queue in the key string (e.g.
    /// `"add:order-42"`). Empty / whitespace-only keys return
    /// [`CapivaraError::EmptyIdempotencyKey`].
    pub async fn send_with_idempotency_key<T: Task>(
        &self,
        args: &T::Args,
        key: impl Into<String>,
    ) -> Result<JobId> {
        let key = key.into();
        if key.trim().is_empty() {
            return Err(CapivaraError::EmptyIdempotencyKey);
        }
        self.send_inner::<T>(args, Some(key)).await
    }

    async fn send_inner<T: Task>(
        &self,
        args: &T::Args,
        idempotency_key: Option<String>,
    ) -> Result<JobId> {
        {
            let reg = self.registry.lock().await;
            if !reg.contains(T::NAME) {
                return Err(CapivaraError::TaskNotRegistered {
                    name: T::NAME.to_string(),
                });
            }
        }
        let payload = serde_json::to_vec(args)?;
        let mut job = Job::new(T::NAME, payload);
        job.queue = self.default_queue.clone();
        job.idempotency_key = idempotency_key;

        let queue = job.queue.as_str().to_string();
        let span = tracing::info_span!(
            "capivara.enqueue",
            job.id = %job.id,
            task.name = T::NAME,
            queue = queue.as_str(),
            attempt = job.attempts,
        );
        let id = async move { self.broker.enqueue(job).await }
            .instrument(span)
            .await?;
        crate::metrics::record_enqueued(&queue, T::NAME);
        Ok(id)
    }

    /// Fetch a stored result. Errors if no backend is configured.
    pub async fn get_result(&self, id: JobId) -> Result<JobResult> {
        let span = tracing::info_span!("capivara.get_result", job.id = %id);
        async move {
            let Some(backend) = &self.results else {
                return Err(CapivaraError::NoResultBackend);
            };
            match backend.get(&id).await? {
                Some(r) => Ok(r),
                None => Err(CapivaraError::ResultNotFound { id: id.to_string() }),
            }
        }
        .instrument(span)
        .await
    }

    /// Process pending jobs in-process until the queue is empty (or `max_jobs`).
    ///
    /// Uses configured lease / [`RetryPolicy`] / concurrency. Delayed nacks are
    /// not waited on in a single drain pass — call again after the delay for
    /// retries.
    pub async fn run_worker(&self, max_jobs: Option<usize>) -> Result<usize> {
        let registry = {
            let guard = self.registry.lock().await;
            Arc::new(guard.clone())
        };

        let worker = Worker {
            registry,
            broker: Arc::clone(&self.broker),
            results: self.results.clone(),
            queues: vec![self.default_queue.clone()],
            lease: self.lease,
            retry_policy: self.retry_policy,
            concurrency: self.concurrency,
        };
        worker.run(max_jobs).await
    }
}
