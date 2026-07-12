//! In-process roundtrips: register → send → worker → get_result.
//!
//! These prove the Celery-like *topology* without Redis.

use capivara::{
    App, CapivaraError, Job, JobId, JobResult, MemoryBroker, MemoryResultBackend, Task, TaskError,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct AddArgs {
    x: i32,
    y: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct AddResult {
    sum: i32,
}

struct Add;

impl Task for Add {
    const NAME: &'static str = "add";
    type Args = AddArgs;
    type Output = AddResult;

    async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        Ok(AddResult {
            sum: args.x + args.y,
        })
    }
}

struct Fails;

#[derive(Serialize, Deserialize)]
struct Empty;

impl Task for Fails {
    const NAME: &'static str = "fails";
    type Args = Empty;
    type Output = Empty;

    async fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        Err(TaskError::new("boom"))
    }
}

struct Panics;

impl Task for Panics {
    const NAME: &'static str = "panics";
    type Args = Empty;
    type Output = Empty;

    async fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        panic!("intentional test panic");
    }
}

#[tokio::test]
async fn success_roundtrip_with_results() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await.unwrap();

    let id = app.send::<Add>(&AddArgs { x: 2, y: 40 }).await.unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);

    match app.get_result(id).await.unwrap() {
        JobResult::Success { payload } => {
            let out: AddResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out, AddResult { sum: 42 });
        }
        JobResult::Failure { message } => panic!("unexpected failure: {message}"),
    }
}

#[tokio::test]
async fn task_err_stores_failure() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Fails>().await.unwrap();

    let id = app.send::<Fails>(&Empty).await.unwrap();
    app.run_worker(None).await.unwrap();

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.contains("boom")),
        JobResult::Success { .. } => panic!("expected failure"),
    }
}

#[tokio::test]
async fn panic_is_isolated_and_second_job_runs() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Panics>().await.unwrap();
    app.register::<Add>().await.unwrap();

    let panic_id = app.send::<Panics>(&Empty).await.unwrap();
    let add_id = app.send::<Add>(&AddArgs { x: 1, y: 1 }).await.unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 2);

    match app.get_result(panic_id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.to_lowercase().contains("panic")),
        JobResult::Success { .. } => panic!("expected panic failure"),
    }

    match app.get_result(add_id).await.unwrap() {
        JobResult::Success { payload } => {
            let out: AddResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.sum, 2);
        }
        JobResult::Failure { message } => panic!("add failed: {message}"),
    }
}

#[tokio::test]
async fn get_result_without_backend_errors() {
    let app = App::new(MemoryBroker::new());
    app.register::<Add>().await.unwrap();
    let id = app.send::<Add>(&AddArgs { x: 1, y: 2 }).await.unwrap();
    app.run_worker(None).await.unwrap();

    let err = app.get_result(id).await.unwrap_err();
    assert!(matches!(err, CapivaraError::NoResultBackend));
}

#[tokio::test]
async fn duplicate_register_errors() {
    let app = App::new(MemoryBroker::new());
    app.register::<Add>().await.unwrap();
    let err = app.register::<Add>().await.unwrap_err();
    assert!(matches!(err, CapivaraError::TaskAlreadyRegistered { .. }));
}

#[tokio::test]
async fn send_unregistered_errors() {
    let app = App::new(MemoryBroker::new());
    let err = app.send::<Add>(&AddArgs { x: 1, y: 2 }).await.unwrap_err();
    assert!(matches!(err, CapivaraError::TaskNotRegistered { .. }));
}

#[tokio::test]
async fn bad_json_payload_stores_failure() {
    // Worker deserialize path: job name matches a registered task, payload is not JSON.
    // CapivaraError::Serialize is used for both encode and decode (serde_json::Error).
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await.unwrap();

    let job = Job::new(Add::NAME, b"this-is-not-json".to_vec());
    let id = job.id;
    app.broker().enqueue(job).await.unwrap();

    app.run_worker(None).await.unwrap();

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => {
            // serde_json error text is implementation-defined; require a clear failure.
            assert!(
                message.to_lowercase().contains("serde")
                    || message.to_lowercase().contains("json")
                    || message.to_lowercase().contains("expected")
                    || message.contains("EOF")
                    || message.contains("key must be"),
                "unexpected message: {message}"
            );
        }
        JobResult::Success { .. } => panic!("expected deserialize failure"),
    }
}

#[tokio::test]
async fn max_jobs_limits_processing() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await.unwrap();

    let _a = app.send::<Add>(&AddArgs { x: 1, y: 1 }).await.unwrap();
    let _b = app.send::<Add>(&AddArgs { x: 2, y: 2 }).await.unwrap();
    let _c = app.send::<Add>(&AddArgs { x: 3, y: 3 }).await.unwrap();

    let n = app.run_worker(Some(2)).await.unwrap();
    assert_eq!(n, 2, "worker should stop after max_jobs");

    // One job still pending on the shared broker.
    let leftover = app.broker().claim().await.unwrap();
    assert!(leftover.is_some(), "third job should still be claimable");
    app.broker().ack(&leftover.unwrap().id).await.unwrap();
}

#[tokio::test]
async fn result_not_found_for_unknown_or_unprocessed_id() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await.unwrap();

    // Completely unknown id.
    let err = app.get_result(JobId::new()).await.unwrap_err();
    assert!(matches!(err, CapivaraError::ResultNotFound { .. }));

    // Enqueued but not yet processed.
    let id = app.send::<Add>(&AddArgs { x: 1, y: 2 }).await.unwrap();
    let err = app.get_result(id).await.unwrap_err();
    assert!(matches!(err, CapivaraError::ResultNotFound { .. }));
}

#[tokio::test]
async fn fire_and_forget_worker_acks_without_stuck_job() {
    // No result backend: worker must still claim+ack so the queue drains.
    let app = App::new(MemoryBroker::new());
    app.register::<Add>().await.unwrap();

    let id = app.send::<Add>(&AddArgs { x: 9, y: 1 }).await.unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);

    // Queue empty / nothing in-flight to claim.
    assert!(app.broker().claim().await.unwrap().is_none());

    // Results API correctly refuses without a backend.
    let err = app.get_result(id).await.unwrap_err();
    assert!(matches!(err, CapivaraError::NoResultBackend));
}

#[tokio::test]
async fn unknown_task_name_on_claimed_job_is_failed_and_acked() {
    // Bypass send(): raw job with a name that was never registered.
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await.unwrap();

    let job = Job::new("no.such.task", b"{}".to_vec());
    let id = job.id;
    app.broker().enqueue(job).await.unwrap();

    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => {
            assert!(
                message.contains("no.such.task") || message.contains("not registered"),
                "unexpected: {message}"
            );
        }
        JobResult::Success { .. } => panic!("expected registry miss failure"),
    }

    // Not stuck in-flight.
    assert!(app.broker().claim().await.unwrap().is_none());
}

#[tokio::test]
async fn with_default_queue_is_applied_on_send() {
    let app = App::new(MemoryBroker::new()).with_default_queue("emails");
    app.register::<Add>().await.unwrap();

    let id = app.send::<Add>(&AddArgs { x: 0, y: 0 }).await.unwrap();
    let job = app.broker().claim().await.unwrap().expect("job enqueued");
    assert_eq!(job.id, id);
    assert_eq!(job.queue.as_str(), "emails");
    app.broker().ack(&job.id).await.unwrap();
}
