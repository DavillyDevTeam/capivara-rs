//! Application facade: register tasks, send jobs, run workers, get results.
//!
//! Celery analogy: the `Celery` app object — without import-time magic.

use crate::broker::Broker;
use crate::error::{CapivaraError, Result};
use crate::job::{Job, JobId, QueueName};
use crate::registry::Registry;
use crate::result::{JobResult, ResultBackend};
use crate::task::Task;
use crate::worker::{DEFAULT_LEASE, DEFAULT_MAX_ATTEMPTS, DEFAULT_NACK_DELAY, Worker};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Capivara application: registry + broker + optional result backend.
pub struct App {
    registry: Arc<Mutex<Registry>>,
    broker: Arc<dyn Broker>,
    results: Option<Arc<dyn ResultBackend>>,
    default_queue: QueueName,
    lease: Duration,
    max_attempts: u32,
    nack_delay: Duration,
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
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            nack_delay: DEFAULT_NACK_DELAY,
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

    /// Max claim attempts before a failure is terminal (default 3).
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Delay before requeue after a retryable failure (default 5s).
    pub fn with_nack_delay(mut self, nack_delay: Duration) -> Self {
        self.nack_delay = nack_delay;
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
        self.broker.enqueue(job).await
    }

    /// Fetch a stored result. Errors if no backend is configured.
    pub async fn get_result(&self, id: JobId) -> Result<JobResult> {
        let Some(backend) = &self.results else {
            return Err(CapivaraError::NoResultBackend);
        };
        match backend.get(&id).await? {
            Some(r) => Ok(r),
            None => Err(CapivaraError::ResultNotFound { id: id.to_string() }),
        }
    }

    /// Process pending jobs in-process until the queue is empty (or `max_jobs`).
    ///
    /// Uses configured lease / max_attempts / nack_delay. Delayed nacks are not
    /// waited on in a single drain pass — call again after the delay for retries.
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
            max_attempts: self.max_attempts,
            nack_delay: self.nack_delay,
        };
        worker.run(max_jobs).await
    }
}
