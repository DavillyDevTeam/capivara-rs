//! RabbitMQ broker smoke tests (feature `rabbitmq`, **experimental**).
//!
//! Prefer `RABBITMQ_URL` or `AMQP_URL` if set (e.g. local Docker). Otherwise start
//! RabbitMQ via testcontainers (needs a working Docker socket — standard on GHA).

use capivara::{
    App, Broker, CapivaraError, Job, JobResult, MemoryResultBackend, NackAction, QueueName,
    RabbitBroker, RabbitConfig, Task, TaskError,
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
enum RabbitGuard {
    Env,
    #[allow(dead_code)]
    Container(Box<testcontainers::ContainerAsync<testcontainers_modules::rabbitmq::RabbitMq>>),
}

async fn rabbit_url() -> (RabbitGuard, String) {
    if let Ok(url) = std::env::var("RABBITMQ_URL").or_else(|_| std::env::var("AMQP_URL")) {
        return (RabbitGuard::Env, url);
    }

    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::rabbitmq::RabbitMq;

    let container = RabbitMq::default().start().await.expect(
        "start rabbitmq via testcontainers (Docker required), or set RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/%2f",
    );
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5672).await.expect("port 5672");
    // guest/guest is the image default; %2f = default vhost "/"
    let url = format!("amqp://guest:guest@{host}:{port}/%2f");
    (RabbitGuard::Container(Box::new(container)), url)
}

#[tokio::test]
async fn rabbit_enqueue_claim_ack() {
    let (_guard, url) = rabbit_url().await;
    let broker = RabbitBroker::connect(RabbitConfig::new(url).with_prefix("capivara_test:"))
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

    broker.ack(&id, &claimed.claim_token).await.unwrap();

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
async fn rabbit_app_roundtrip_with_memory_results() {
    let (_guard, url) = rabbit_url().await;
    let broker = RabbitBroker::connect(RabbitConfig::new(url).with_prefix("capivara_app:"))
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
async fn rabbit_nack_requeue_preserves_attempts() {
    let (_guard, url) = rabbit_url().await;
    let broker = RabbitBroker::connect(RabbitConfig::new(url).with_prefix("capivara_nack:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"retry"}"#.to_vec());
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
        .expect("claim 1");
    assert_eq!(claimed.job.attempts, 1);

    broker
        .nack(
            &id,
            &claimed.claim_token,
            NackAction::RequeueAfter {
                delay: Duration::ZERO,
            },
        )
        .await
        .unwrap();

    let claimed2 = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_secs(2),
        )
        .await
        .unwrap()
        .expect("claim 2 after nack");
    assert_eq!(claimed2.job.id, id);
    assert_eq!(
        claimed2.job.attempts, 2,
        "attempts must survive re-publish nack"
    );
    broker.ack(&id, &claimed2.claim_token).await.unwrap();
}

#[tokio::test]
async fn rabbit_dead_letter_and_list_dead() {
    let (_guard, url) = rabbit_url().await;
    let broker = RabbitBroker::connect(RabbitConfig::new(url).with_prefix("capivara_dlq:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"dead"}"#.to_vec());
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
        .expect("claim");
    broker
        .dead_letter(&id, &claimed.claim_token, "test terminal")
        .await
        .unwrap();

    let dead = broker.list_dead(&QueueName::default()).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].job.id, id);
    assert_eq!(dead[0].reason, "test terminal");

    // list_dead is inspect-only: entry should still be listable.
    let dead2 = broker.list_dead(&QueueName::default()).await.unwrap();
    assert_eq!(dead2.len(), 1);
}

#[tokio::test]
async fn rabbit_wrong_claim_token_is_job_not_found() {
    let (_guard, url) = rabbit_url().await;
    let broker = RabbitBroker::connect(RabbitConfig::new(url).with_prefix("capivara_token:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"tok"}"#.to_vec());
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
        .expect("claim");

    let wrong = capivara::ClaimToken::new();
    let err = broker.ack(&id, &wrong).await.unwrap_err();
    assert!(
        matches!(err, CapivaraError::JobNotFound { .. }),
        "expected JobNotFound, got {err:?}"
    );

    // Correct token still works.
    broker.ack(&id, &claimed.claim_token).await.unwrap();
}

#[tokio::test]
async fn rabbit_process_local_idempotency_key() {
    let (_guard, url) = rabbit_url().await;
    let broker = RabbitBroker::connect(RabbitConfig::new(url).with_prefix("capivara_idemp:"))
        .await
        .unwrap();
    let app = App::new(broker).with_result_backend(MemoryResultBackend::new());
    app.register::<Ping>().await.unwrap();

    let args = PingArgs { msg: "once".into() };
    let id1 = app
        .send_with_idempotency_key::<Ping>(&args, "invoice-rabbit-1")
        .await
        .unwrap();
    let id2 = app
        .send_with_idempotency_key::<Ping>(&args, "invoice-rabbit-1")
        .await
        .unwrap();
    assert_eq!(id1, id2, "same process key must return same JobId");

    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1, "duplicate key must not double-queue in-process");
}
