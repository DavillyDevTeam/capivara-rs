//! Redis broker smoke tests (feature `redis`).
//!
//! Prefer `REDIS_URL` if set (e.g. local Docker). Otherwise start Redis via
//! testcontainers (needs a working Docker socket — standard on GHA).

use capivara::{
    App, Broker, Job, JobResult, MemoryResultBackend, NackAction, QueueName, RedisBroker,
    RedisConfig, Task, TaskError,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PingArgs {
    msg: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PingResult {
    echo: String,
}

struct Ping;

impl Task for Ping {
    const NAME: &'static str = "ping";
    type Args = PingArgs;
    type Output = PingResult;

    async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        Ok(PingResult { echo: args.msg })
    }
}

/// Hold a container so it is not dropped while tests run.
enum RedisGuard {
    Env,
    #[allow(dead_code)]
    Container(Box<testcontainers::ContainerAsync<testcontainers_modules::redis::Redis>>),
}

async fn redis_url() -> (RedisGuard, String) {
    if let Ok(url) = std::env::var("REDIS_URL") {
        return (RedisGuard::Env, url);
    }

    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::Redis;

    let container = Redis::default()
        .start()
        .await
        .expect(
            "start redis via testcontainers (Docker required), or set REDIS_URL=redis://127.0.0.1:6379/",
        );
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(6379).await.expect("port 6379");
    let url = format!("redis://{host}:{port}/");
    (RedisGuard::Container(Box::new(container)), url)
}

#[tokio::test]
async fn redis_enqueue_claim_ack() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_test:"))
        .await
        .expect("connect");

    let mut job = Job::new("ping", br#"{"msg":"hi"}"#.to_vec());
    job.queue = QueueName::new("default");
    let id = broker.enqueue(job).await.unwrap();

    let claimed = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_secs(2),
        )
        .await
        .unwrap()
        .expect("should claim");
    assert_eq!(claimed.job.id, id);
    assert_eq!(claimed.job.task_name, "ping");
    assert_eq!(claimed.job.attempts, 1);

    broker.ack(&id).await.unwrap();

    let none = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn redis_app_roundtrip_with_memory_results() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_app:"))
        .await
        .unwrap();
    let app = App::new(broker).with_result_backend(MemoryResultBackend::new());
    app.register::<Ping>().await.unwrap();

    let id = app
        .send::<Ping>(&PingArgs {
            msg: "hello".into(),
        })
        .await
        .unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);

    match app.get_result(id).await.unwrap() {
        JobResult::Success { payload } => {
            let out: PingResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.echo, "hello");
        }
        JobResult::Failure { message } => panic!("{message}"),
    }
}

#[tokio::test]
async fn redis_nack_requeue_immediate() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_nack:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"x"}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let claimed = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.job.id, id);

    broker
        .nack(
            &id,
            NackAction::RequeueAfter {
                delay: Duration::ZERO,
            },
        )
        .await
        .unwrap();

    let again = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("requeued");
    assert_eq!(again.job.id, id);
    assert_eq!(again.job.attempts, 2);
    broker.ack(&id).await.unwrap();
}

#[tokio::test]
async fn redis_nack_delayed_then_promoted() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_delay:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"later"}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let claimed = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.job.id, id);

    broker
        .nack(
            &id,
            NackAction::RequeueAfter {
                delay: Duration::from_millis(250),
            },
        )
        .await
        .unwrap();

    // Not claimable immediately.
    let none = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert!(none.is_none(), "job should still be delayed");

    tokio::time::sleep(Duration::from_millis(350)).await;

    let again = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("promoted after delay");
    assert_eq!(again.job.id, id);
    broker.ack(&id).await.unwrap();
}

#[tokio::test]
async fn redis_lease_expires_then_reclaimed() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_lease:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"lease"}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let short_lease = Duration::from_millis(200);
    let claimed = broker
        .claim(&[QueueName::default()], short_lease, Duration::ZERO)
        .await
        .unwrap()
        .expect("first claim");
    assert_eq!(claimed.job.id, id);
    assert_eq!(claimed.job.attempts, 1);

    // Do not ack — wait past lease, then reclaim via recover-on-claim.
    tokio::time::sleep(Duration::from_millis(350)).await;

    let again = broker
        .claim(&[QueueName::default()], short_lease, Duration::from_secs(1))
        .await
        .unwrap()
        .expect("reclaimed after lease expiry");
    assert_eq!(again.job.id, id);
    assert_eq!(again.job.attempts, 2);
    broker.ack(&id).await.unwrap();
}

struct AlwaysFails;

impl Task for AlwaysFails {
    const NAME: &'static str = "always_fails";
    type Args = PingArgs;
    type Output = PingResult;

    async fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        Err(TaskError::new("always fails"))
    }
}

#[tokio::test]
async fn redis_worker_retries_then_terminal() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_retry:"))
        .await
        .unwrap();

    let app = App::new(broker)
        .with_result_backend(MemoryResultBackend::new())
        .with_max_attempts(3)
        .with_nack_delay(Duration::from_millis(80));
    app.register::<AlwaysFails>().await.unwrap();

    let id = app
        .send::<AlwaysFails>(&PingArgs {
            msg: "retry-me".into(),
        })
        .await
        .unwrap();

    let mut total = 0usize;
    for _ in 0..15 {
        total += app.run_worker(None).await.unwrap();
        if total >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    assert_eq!(total, 3, "three attempts then terminal");

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.contains("always fails")),
        JobResult::Success { .. } => panic!("expected failure"),
    }

    // Not claimable after terminal ack.
    let none = app
        .broker()
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert!(none.is_none());
}
