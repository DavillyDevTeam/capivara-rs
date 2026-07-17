//! Blocking / CPU-bound handler via [`capivara::SyncTask`].
//!
//! Run with: `cargo run --example sync_task`

use capivara::{App, JobResult, MemoryBroker, MemoryResultBackend, SyncTask, TaskError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct WorkArgs {
    /// Milliseconds of blocking sleep (simulates CPU / sync I/O).
    sleep_ms: u64,
    /// Simple CPU-ish work so the example is not only sleep.
    n: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct WorkOut {
    sum: u64,
}

struct BlockingWork;

impl SyncTask for BlockingWork {
    const NAME: &'static str = "blocking_work";
    type Args = WorkArgs;
    type Output = WorkOut;

    fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        // This body runs on Tokio's blocking pool — it must not use `.await`.
        std::thread::sleep(Duration::from_millis(args.sleep_ms));
        let sum: u64 = (0..args.n).sum();
        Ok(WorkOut { sum })
    }
}

#[tokio::main]
async fn main() -> capivara::Result<()> {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<BlockingWork>().await?;

    let id = app
        .send::<BlockingWork>(&WorkArgs {
            sleep_ms: 10,
            n: 100,
        })
        .await?;
    app.run_worker(None).await?;

    match app.get_result(id).await? {
        JobResult::Success { payload } => {
            let out: WorkOut = serde_json::from_slice(&payload).unwrap();
            println!("blocking_work finished: sum = {}", out.sum);
            assert_eq!(out.sum, (0..100u64).sum::<u64>());
        }
        JobResult::Failure { message } => panic!("task failed: {message}"),
    }
    Ok(())
}
