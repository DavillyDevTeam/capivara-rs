//! In-process roundtrips: register → send → worker → get_result.
//!
//! These prove the Celery-like *topology* without Redis.

use capivara::{
    App, Broker, CapivaraError, Job, JobId, JobResult, MemoryBroker, MemoryResultBackend,
    QueueName, Task, TaskError,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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

/// Tracks in-flight PeakProbe handlers for concurrency bounds assertions.
static PEAK_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static PEAK_MAX: AtomicUsize = AtomicUsize::new(0);

struct PeakProbe;

impl Task for PeakProbe {
    const NAME: &'static str = "peak_probe";
    type Args = Empty;
    type Output = Empty;

    async fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        let n = PEAK_IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK_MAX.fetch_max(n, Ordering::SeqCst);
        // Hold the slot long enough for other concurrent claims to overlap.
        tokio::time::sleep(Duration::from_millis(80)).await;
        PEAK_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        Ok(Empty)
    }
}

#[tokio::test]
async fn concurrency_processes_several_jobs() {
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_concurrency(4);
    app.register::<Add>().await.unwrap();

    let mut ids = Vec::new();
    for i in 0..8 {
        ids.push(app.send::<Add>(&AddArgs { x: i, y: i * 10 }).await.unwrap());
    }

    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 8);

    for (i, id) in ids.into_iter().enumerate() {
        match app.get_result(id).await.unwrap() {
            JobResult::Success { payload } => {
                let out: AddResult = serde_json::from_slice(&payload).unwrap();
                let i = i as i32;
                assert_eq!(out.sum, i + i * 10);
            }
            JobResult::Failure { message } => panic!("job {i}: {message}"),
        }
    }
}

#[tokio::test]
async fn concurrency_bounds_in_flight_handlers() {
    // Reset peak counters (tests may run in parallel across processes, not threads).
    PEAK_IN_FLIGHT.store(0, Ordering::SeqCst);
    PEAK_MAX.store(0, Ordering::SeqCst);

    const LIMIT: usize = 4;
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_concurrency(LIMIT);
    app.register::<PeakProbe>().await.unwrap();

    for _ in 0..8 {
        app.send::<PeakProbe>(&Empty).await.unwrap();
    }

    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 8);

    let peak = PEAK_MAX.load(Ordering::SeqCst);
    assert!(
        peak <= LIMIT,
        "peak in-flight handlers {peak} exceeded concurrency {LIMIT}"
    );
    assert!(
        peak >= 2,
        "expected overlapping work under concurrency (peak was {peak})"
    );
}

#[tokio::test]
async fn task_err_stores_failure() {
    // Single-shot terminal failure for result storage assertion.
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(1);
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
    // max_attempts=1 so panic is terminal in one drain (retry policy covered elsewhere).
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(1);
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
    // max_attempts=1 so a single drain ends in terminal ack (retry policy covered elsewhere).
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(1);
    app.register::<Add>().await.unwrap();

    let job = Job::new(Add::NAME, b"this-is-not-json".to_vec());
    let id = job.id;
    app.broker().enqueue(job).await.unwrap();

    app.run_worker(None).await.unwrap();

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => {
            // CapivaraError::Serialize Display prefix is stable; not raw serde_json text.
            assert!(
                message.contains("JSON serde error"),
                "unexpected message: {message}"
            );
        }
        JobResult::Success { .. } => panic!("expected deserialize failure"),
    }

    // Job must be acked (not stuck in-flight).
    assert!(
        app.broker()
            .claim(
                &[],
                std::time::Duration::from_secs(30),
                std::time::Duration::ZERO
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn max_jobs_limits_processing() {
    let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await.unwrap();

    let a = app.send::<Add>(&AddArgs { x: 1, y: 1 }).await.unwrap();
    let b = app.send::<Add>(&AddArgs { x: 2, y: 2 }).await.unwrap();
    let c = app.send::<Add>(&AddArgs { x: 3, y: 3 }).await.unwrap();

    let n = app.run_worker(Some(2)).await.unwrap();
    assert_eq!(n, 2, "worker should stop after max_jobs");

    // First two processed successfully; third never ran.
    match app.get_result(a).await.unwrap() {
        JobResult::Success { .. } => {}
        JobResult::Failure { message } => panic!("first job failed: {message}"),
    }
    match app.get_result(b).await.unwrap() {
        JobResult::Success { .. } => {}
        JobResult::Failure { message } => panic!("second job failed: {message}"),
    }
    let err = app.get_result(c).await.unwrap_err();
    assert!(
        matches!(err, CapivaraError::ResultNotFound { .. }),
        "third job should not have a result: {err:?}"
    );

    // One job still pending on the shared broker — must be the third.
    let leftover = app
        .broker()
        .claim(
            &[],
            std::time::Duration::from_secs(30),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
    let leftover = leftover.expect("third job should still be claimable");
    assert_eq!(
        leftover.job.id, c,
        "leftover job should be the unprocessed third"
    );
    app.broker()
        .ack(&leftover.job.id, &leftover.claim_token)
        .await
        .unwrap();
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
    assert!(
        app.broker()
            .claim(
                &[],
                std::time::Duration::from_secs(30),
                std::time::Duration::ZERO
            )
            .await
            .unwrap()
            .is_none()
    );

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
    assert!(
        app.broker()
            .claim(
                &[],
                std::time::Duration::from_secs(30),
                std::time::Duration::ZERO
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn unknown_task_name_without_backend_still_acks() {
    // Registry miss with no result backend must still claim+ack (fire-and-forget drain).
    let app = App::new(MemoryBroker::new());
    // no result backend
    let job = Job::new("ghost", b"{}".to_vec());
    app.broker().enqueue(job).await.unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);
    assert!(
        app.broker()
            .claim(
                &[],
                std::time::Duration::from_secs(30),
                std::time::Duration::ZERO
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn with_default_queue_is_applied_on_send() {
    let app = App::new(MemoryBroker::new()).with_default_queue("emails");
    app.register::<Add>().await.unwrap();

    let id = app.send::<Add>(&AddArgs { x: 0, y: 0 }).await.unwrap();
    let job = app
        .broker()
        .claim(
            &[QueueName::new("emails")],
            std::time::Duration::from_secs(30),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
        .expect("job enqueued");
    assert_eq!(job.job.id, id);
    assert_eq!(job.job.queue.as_str(), "emails");
    app.broker()
        .ack(&job.job.id, &job.claim_token)
        .await
        .unwrap();
}

#[tokio::test]
async fn lease_expires_then_reclaimed() {
    let broker = MemoryBroker::new();
    let mut job = Job::new("add", br#"{"x":1,"y":2}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let short_lease = std::time::Duration::from_millis(80);
    let claimed = broker
        .claim(
            &[QueueName::default()],
            short_lease,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
        .expect("first claim");
    assert_eq!(claimed.job.id, id);
    assert_eq!(claimed.job.attempts, 1);

    // Do not ack — wait past lease, then reclaim.
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    let again = broker
        .claim(
            &[QueueName::default()],
            short_lease,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
        .expect("reclaimed after lease expiry");
    assert_eq!(again.job.id, id);
    assert_eq!(again.job.attempts, 2);
    broker.ack(&id, &again.claim_token).await.unwrap();
}

#[tokio::test]
async fn worker_retries_failure_then_terminal_ack() {
    use std::time::{Duration, Instant};

    let nack_delay = Duration::from_millis(50);
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(3)
        .with_nack_delay(nack_delay);
    app.register::<Fails>().await.unwrap();

    let id = app.send::<Fails>(&Empty).await.unwrap();

    // Drain until 3 attempts with a generous deadline (avoids tight sleep races).
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut total = 0usize;
    while total < 3 && Instant::now() < deadline {
        total += app.run_worker(None).await.unwrap();
        if total >= 3 {
            break;
        }
        // Wait at least the nack delay for delayed requeue to become claimable.
        tokio::time::sleep(nack_delay + Duration::from_millis(30)).await;
    }
    assert_eq!(total, 3, "should process 3 attempts before deadline");

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.contains("boom")),
        JobResult::Success { .. } => panic!("expected failure"),
    }

    // Terminal ack — not claimable.
    assert!(
        app.broker()
            .claim(&[], Duration::from_secs(30), Duration::ZERO)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn worker_retries_panic_then_terminal_ack() {
    use std::time::{Duration, Instant};

    let nack_delay = Duration::from_millis(50);
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(3)
        .with_nack_delay(nack_delay);
    app.register::<Panics>().await.unwrap();

    let id = app.send::<Panics>(&Empty).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut total = 0usize;
    while total < 3 && Instant::now() < deadline {
        total += app.run_worker(None).await.unwrap();
        if total >= 3 {
            break;
        }
        tokio::time::sleep(nack_delay + Duration::from_millis(30)).await;
    }
    assert_eq!(total, 3);

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.to_lowercase().contains("panic")),
        JobResult::Success { .. } => panic!("expected panic failure"),
    }

    assert!(
        app.broker()
            .claim(&[], Duration::from_secs(30), Duration::ZERO)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn late_ack_after_recover_does_not_steal_new_claim() {
    use std::time::Duration;

    let broker = MemoryBroker::new();
    let mut job = Job::new("add", br#"{"x":1,"y":2}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let short_lease = Duration::from_millis(60);
    let claim_a = broker
        .claim(&[QueueName::default()], short_lease, Duration::ZERO)
        .await
        .unwrap()
        .expect("claim A");
    assert_eq!(claim_a.job.id, id);
    assert_eq!(claim_a.job.attempts, 1);
    let token_a = claim_a.claim_token;

    // Expire lease and let recover requeue, then claim B.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let claim_b = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_millis(200),
        )
        .await
        .unwrap()
        .expect("claim B after recover");
    assert_eq!(claim_b.job.id, id);
    assert_eq!(claim_b.job.attempts, 2);
    assert_ne!(token_a, claim_b.claim_token);

    // Late ack from A must fail and must not destroy B's claim.
    let err = broker.ack(&id, &token_a).await.unwrap_err();
    assert!(matches!(err, CapivaraError::JobNotFound { .. }));

    // B still owns the claim and can settle.
    broker.ack(&id, &claim_b.claim_token).await.unwrap();

    assert!(
        broker
            .claim(
                &[QueueName::default()],
                Duration::from_secs(30),
                Duration::ZERO
            )
            .await
            .unwrap()
            .is_none()
    );
}
