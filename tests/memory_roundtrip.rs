//! In-process roundtrips: register → send → worker → get_result.
//!
//! These prove the Celery-like *topology* without Redis.

use capivara::{App, CapivaraError, JobResult, MemoryBroker, MemoryResultBackend, Task, TaskError};
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
