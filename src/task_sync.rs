//! Sync / blocking task adapters.
//!
//! Capivara's worker is async (Tokio). CPU-bound or blocking I/O work should not
//! run directly on the async runtime. This module provides:
//!
//! - [`SyncTask`] — define a **synchronous** handler; a blanket [`Task`] impl runs
//!   it on the blocking pool via [`tokio::task::spawn_blocking`].
//! - [`run_blocking`] — call a sync closure from an async [`Task::run`] body.
//!
//! Prefer these over `std::thread::sleep` / blocking I/O inside plain `async fn run`.

use crate::error::TaskError;
use crate::task::Task;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Synchronous unit of work that participates in the typed task model.
///
/// Implement this instead of [`Task`] when the handler is blocking or CPU-bound.
/// Registration is unchanged: `app.register::<T>()` and `app.send::<T>(&args)` work
/// because every [`SyncTask`] gets a blanket [`Task`] impl that schedules
/// [`SyncTask::run`] on Tokio's blocking thread pool.
///
/// # Example
///
/// ```
/// use capivara::{App, JobResult, MemoryBroker, MemoryResultBackend, SyncTask, TaskError};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct HeavyArgs { n: u32 }
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq)]
/// struct HeavyOut { sum: u64 }
///
/// struct HeavyWork;
///
/// impl SyncTask for HeavyWork {
///     const NAME: &'static str = "heavy_work";
///     type Args = HeavyArgs;
///     type Output = HeavyOut;
///
///     fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
///         // Blocking / CPU work is fine here — it runs on spawn_blocking.
///         let sum: u64 = (0..args.n as u64).sum();
///         Ok(HeavyOut { sum })
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> capivara::Result<()> {
/// let app = App::new(MemoryBroker::new())
///     .with_result_backend(MemoryResultBackend::new());
/// app.register::<HeavyWork>().await?;
/// let id = app.send::<HeavyWork>(&HeavyArgs { n: 10 }).await?;
/// app.run_worker(None).await?;
/// match app.get_result(id).await? {
///     JobResult::Success { payload } => {
///         let out: HeavyOut = serde_json::from_slice(&payload).unwrap();
///         assert_eq!(out.sum, 45);
///     }
///     JobResult::Failure { message } => panic!("{message}"),
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Name clash with [`Task::run`]
///
/// Both traits expose `run`. Prefer UFCS when calling the sync body directly:
/// `<T as SyncTask>::run(args)`. Worker / `register` / `send` use the [`Task`] path.
pub trait SyncTask: Send + Sync + 'static {
    /// Wire / registry name (must be unique per `App`). Same rules as [`Task::NAME`].
    const NAME: &'static str;

    /// Deserialized from the job payload (JSON).
    type Args: Serialize + DeserializeOwned + Send + 'static;

    /// Serialized into the optional result backend on success.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// Execute the task on a blocking thread (via the blanket [`Task`] impl).
    fn run(args: Self::Args) -> Result<Self::Output, TaskError>;
}

impl<T: SyncTask> Task for T {
    const NAME: &'static str = <T as SyncTask>::NAME;
    type Args = <T as SyncTask>::Args;
    type Output = <T as SyncTask>::Output;

    async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        run_blocking(<T as SyncTask>::run, args).await
    }
}

/// Run a synchronous function on Tokio's blocking pool.
///
/// Use inside a hand-written async [`Task::run`] when only part of the work is
/// blocking, or when you prefer not to implement [`SyncTask`]:
///
/// ```ignore
/// async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
///     capivara::run_blocking(|a| {
///         // std::fs, CPU, etc.
///         Ok(compute(a))
///     }, args).await
/// }
/// ```
///
/// Panics inside `f` surface as [`TaskError`] (worker treats them like handler
/// failures and may retry / dead-letter per policy). Cancellation of the
/// `spawn_blocking` join also becomes a [`TaskError`].
pub async fn run_blocking<A, O, F>(f: F, args: A) -> Result<O, TaskError>
where
    A: Send + 'static,
    O: Send + 'static,
    F: FnOnce(A) -> Result<O, TaskError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || f(args))
        .await
        .map_err(|e| {
            if e.is_panic() {
                TaskError::new(format!("blocking task panicked: {e}"))
            } else {
                TaskError::new(format!("blocking task join error: {e}"))
            }
        })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_blocking_executes_on_blocking_pool() {
        let start = Instant::now();
        let out = run_blocking(
            |ms| {
                std::thread::sleep(Duration::from_millis(ms));
                Ok::<_, TaskError>(ms)
            },
            30u64,
        )
        .await
        .unwrap();
        assert_eq!(out, 30);
        assert!(start.elapsed() >= Duration::from_millis(25));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_blocking_maps_sync_error() {
        let err = run_blocking(|()| Err::<(), _>(TaskError::new("nope")), ())
            .await
            .unwrap_err();
        assert_eq!(err.message, "nope");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_blocking_maps_panic() {
        let err = run_blocking(
            |()| -> Result<(), TaskError> {
                panic!("kaboom");
            },
            (),
        )
        .await
        .unwrap_err();
        assert!(
            err.message.contains("blocking task panicked"),
            "unexpected message: {}",
            err.message
        );
    }

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Unit;

    struct Sleepy;

    impl SyncTask for Sleepy {
        const NAME: &'static str = "sleepy_unit";
        type Args = Unit;
        type Output = Unit;

        fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
            std::thread::sleep(Duration::from_millis(20));
            Ok(Unit)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sync_task_blanket_impl_runs() {
        let start = Instant::now();
        // Task path (async) should complete via spawn_blocking.
        let out = <Sleepy as Task>::run(Unit).await.unwrap();
        assert_eq!(out, Unit);
        assert!(start.elapsed() >= Duration::from_millis(15));
    }
}
