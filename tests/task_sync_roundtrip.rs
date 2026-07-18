//! End-to-end SyncTask + run_blocking via memory broker/results.

use capivara::{
    App, JobResult, MemoryBroker, MemoryResultBackend, SyncTask, Task, TaskError, run_blocking,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SleepArgs {
    ms: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SleepOut {
    slept_ms: u64,
}

struct BlockingSleep;

impl SyncTask for BlockingSleep {
    const NAME: &'static str = "blocking_sleep";
    type Args = SleepArgs;
    type Output = SleepOut;

    fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        std::thread::sleep(Duration::from_millis(args.ms));
        Ok(SleepOut { slept_ms: args.ms })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_task_roundtrip_with_blocking_sleep() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<BlockingSleep>().await.unwrap();

    let start = Instant::now();
    let id = app
        .send::<BlockingSleep>(&SleepArgs { ms: 40 })
        .await
        .unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);
    assert!(
        start.elapsed() >= Duration::from_millis(35),
        "expected blocking sleep to take wall time"
    );

    match app.get_result(id).await.unwrap() {
        JobResult::Success { payload } => {
            let out: SleepOut = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out, SleepOut { slept_ms: 40 });
        }
        JobResult::Failure { message } => panic!("unexpected failure: {message}"),
    }
}

/// Async Task that uses `run_blocking` for the heavy part only.
struct Hybrid;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct HybridArgs {
    n: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct HybridOut {
    sum: u64,
}

static HYBRID_RAN_BLOCKING: AtomicBool = AtomicBool::new(false);

impl Task for Hybrid {
    const NAME: &'static str = "hybrid_blocking";
    type Args = HybridArgs;
    type Output = HybridOut;

    async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        run_blocking(
            |n| {
                HYBRID_RAN_BLOCKING.store(true, Ordering::SeqCst);
                // Cheap CPU work + a short sleep so we know the pool ran.
                std::thread::sleep(Duration::from_millis(15));
                Ok(HybridOut {
                    sum: (0..n).sum::<u64>(),
                })
            },
            args.n,
        )
        .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_blocking_inside_async_task_roundtrip() {
    HYBRID_RAN_BLOCKING.store(false, Ordering::SeqCst);
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Hybrid>().await.unwrap();

    let id = app.send::<Hybrid>(&HybridArgs { n: 20 }).await.unwrap();
    assert_eq!(app.run_worker(None).await.unwrap(), 1);
    assert!(HYBRID_RAN_BLOCKING.load(Ordering::SeqCst));

    match app.get_result(id).await.unwrap() {
        JobResult::Success { payload } => {
            let out: HybridOut = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.sum, (0..20u64).sum::<u64>());
        }
        JobResult::Failure { message } => panic!("unexpected failure: {message}"),
    }
}

struct SyncFails;

#[derive(Serialize, Deserialize)]
struct Empty;

impl SyncTask for SyncFails {
    const NAME: &'static str = "sync_fails";
    type Args = Empty;
    type Output = Empty;

    fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        Err(TaskError::new("sync boom"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_task_failure_is_terminal_after_retries() {
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(1);
    app.register::<SyncFails>().await.unwrap();

    let id = app.send::<SyncFails>(&Empty).await.unwrap();
    let _ = app.run_worker(None).await.unwrap();

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.contains("sync boom"), "{message}"),
        JobResult::Success { .. } => panic!("expected failure"),
    }
}
