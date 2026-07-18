//! Redis broker smoke tests (feature `redis`).
//!
//! Prefer `REDIS_URL` if set (e.g. local Docker). Otherwise start Redis via
//! testcontainers (needs a working Docker socket — standard on GHA).

use capivara::{
    App, Broker, CapivaraError, DEFAULT_RESULT_TTL, Job, JobResult, MemoryResultBackend,
    NackAction, QueueName, RedisBroker, RedisConfig, RedisResultBackend, RetryPolicy, Task,
    TaskError,
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
async fn redis_producer_idempotency_key_dedupes() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_idemp:"))
        .await
        .unwrap();
    let app = App::new(broker).with_result_backend(MemoryResultBackend::new());
    app.register::<Ping>().await.unwrap();

    let args = PingArgs { msg: "once".into() };
    let id1 = app
        .send_with_idempotency_key::<Ping>(&args, "invoice-7")
        .await
        .unwrap();
    let id2 = app
        .send_with_idempotency_key::<Ping>(&args, "invoice-7")
        .await
        .unwrap();
    assert_eq!(id1, id2, "same key must return the same JobId");

    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1, "duplicate key must not double-queue");

    match app.get_result(id1).await.unwrap() {
        JobResult::Success { payload } => {
            let out: PingResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.echo, "once");
        }
        JobResult::Failure { message } => panic!("{message}"),
    }

    // Nothing left pending.
    assert!(
        app.broker()
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

#[tokio::test]
async fn redis_app_roundtrip_with_redis_results() {
    let (_guard, url) = redis_url().await;
    let prefix = "capivara_redis_results:";
    let config = RedisConfig::new(url).with_prefix(prefix);
    let broker = RedisBroker::connect(config.clone()).await.unwrap();
    let results = RedisResultBackend::connect(config).await.unwrap();
    let app = App::new(broker).with_result_backend(results);
    app.register::<Ping>().await.unwrap();

    let id = app
        .send::<Ping>(&PingArgs {
            msg: "redis-result".into(),
        })
        .await
        .unwrap();
    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 1);

    match app.get_result(id).await.unwrap() {
        JobResult::Success { payload } => {
            let out: PingResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.echo, "redis-result");
        }
        JobResult::Failure { message } => panic!("{message}"),
    }
}

#[tokio::test]
async fn redis_result_key_has_24h_ttl() {
    let (_guard, url) = redis_url().await;
    let prefix = "capivara_redis_ttl:";
    let config = RedisConfig::new(url.clone()).with_prefix(prefix);
    let broker = RedisBroker::connect(config.clone()).await.unwrap();
    let results = RedisResultBackend::connect(config).await.unwrap();
    let app = App::new(broker).with_result_backend(results);
    app.register::<Ping>().await.unwrap();

    let id = app
        .send::<Ping>(&PingArgs {
            msg: "ttl-check".into(),
        })
        .await
        .unwrap();
    app.run_worker(None).await.unwrap();
    // Confirm result is readable through the backend.
    assert!(matches!(
        app.get_result(id).await.unwrap(),
        JobResult::Success { .. }
    ));

    // Direct Redis TTL on the result key (default EX 86400).
    let client = redis::Client::open(url.as_str()).expect("redis client");
    let mut conn = redis::aio::ConnectionManager::new(client)
        .await
        .expect("redis conn");
    // Key layout is part of the public contract: `{prefix}result:{id}`.
    let key = format!("{prefix}result:{id}");
    let ttl: i64 = redis::cmd("TTL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .expect("TTL");
    let default_secs = DEFAULT_RESULT_TTL.as_secs() as i64;
    assert!(
        ttl > default_secs - 60 && ttl <= default_secs,
        "expected TTL near {default_secs}s, got {ttl} (key={key})"
    );
}

#[tokio::test]
async fn redis_concurrency_smoke() {
    let (_guard, url) = redis_url().await;
    let prefix = "capivara_redis_conc:";
    let config = RedisConfig::new(url).with_prefix(prefix);
    let broker = RedisBroker::connect(config.clone()).await.unwrap();
    let results = RedisResultBackend::connect(config).await.unwrap();
    let app = App::new(broker)
        .with_result_backend(results)
        .with_concurrency(4);
    app.register::<Ping>().await.unwrap();

    let mut ids = Vec::new();
    for i in 0..8 {
        let id = app
            .send::<Ping>(&PingArgs {
                msg: format!("c{i}"),
            })
            .await
            .unwrap();
        ids.push(id);
    }

    let n = app.run_worker(None).await.unwrap();
    assert_eq!(n, 8);

    for (i, id) in ids.into_iter().enumerate() {
        match app.get_result(id).await.unwrap() {
            JobResult::Success { payload } => {
                let out: PingResult = serde_json::from_slice(&payload).unwrap();
                assert_eq!(out.echo, format!("c{i}"));
            }
            JobResult::Failure { message } => panic!("job {i}: {message}"),
        }
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
            &claimed.claim_token,
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
    broker.ack(&id, &again.claim_token).await.unwrap();
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
            &claimed.claim_token,
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
    broker.ack(&id, &again.claim_token).await.unwrap();
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
    broker.ack(&id, &again.claim_token).await.unwrap();
}

#[tokio::test]
async fn redis_late_ack_after_recover_does_not_steal_new_claim() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_late_ack:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"steal"}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let short_lease = Duration::from_millis(200);
    let claim_a = broker
        .claim(&[QueueName::default()], short_lease, Duration::ZERO)
        .await
        .unwrap()
        .expect("claim A");
    assert_eq!(claim_a.job.id, id);
    assert_eq!(claim_a.job.attempts, 1);
    let token_a = claim_a.claim_token;

    // Expire lease; recover-on-claim requeues; B claims with a new token.
    tokio::time::sleep(Duration::from_millis(350)).await;
    let claim_b = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_secs(1),
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
async fn redis_late_dead_letter_after_recover_does_not_steal_new_claim() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_late_dlq:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"dlq-steal"}"#.to_vec());
    job.queue = QueueName::default();
    let id = broker.enqueue(job).await.unwrap();

    let short_lease = Duration::from_millis(200);
    let claim_a = broker
        .claim(&[QueueName::default()], short_lease, Duration::ZERO)
        .await
        .unwrap()
        .expect("claim A");
    assert_eq!(claim_a.job.id, id);
    let token_a = claim_a.claim_token;

    // Expire lease; recover-on-claim requeues; B claims with a new token.
    tokio::time::sleep(Duration::from_millis(350)).await;
    let claim_b = broker
        .claim(
            &[QueueName::default()],
            Duration::from_secs(30),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .expect("claim B after recover");
    assert_eq!(claim_b.job.id, id);
    assert_ne!(token_a, claim_b.claim_token);

    // Late dead_letter from A must fail and must not append a DLQ entry.
    let err = broker
        .dead_letter(&id, &token_a, "stale claim A")
        .await
        .unwrap_err();
    assert!(matches!(err, CapivaraError::JobNotFound { .. }));
    let dead = broker.list_dead(&QueueName::default()).await.unwrap();
    assert!(
        dead.is_empty(),
        "late dead_letter must not write DLQ: {dead:?}"
    );

    // B still owns the claim and can dead-letter.
    broker
        .dead_letter(&id, &claim_b.claim_token, "owned by B")
        .await
        .unwrap();
    let dead = broker.list_dead(&QueueName::default()).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].job.id, id);
    assert_eq!(dead[0].reason, "owned by B");

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
async fn redis_worker_retries_then_dead_letter_terminal_only() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_retry:"))
        .await
        .unwrap();

    // Deterministic policy: no jitter so sleep matches exact exponential delays.
    let base_delay = Duration::from_millis(80);
    let policy = RetryPolicy {
        max_attempts: 3,
        base_delay,
        max_delay: Duration::from_secs(60),
        jitter: false,
    };
    let app = App::new(broker)
        .with_result_backend(MemoryResultBackend::new())
        .with_retry_policy(policy);
    app.register::<AlwaysFails>().await.unwrap();

    let id = app
        .send::<AlwaysFails>(&PingArgs {
            msg: "retry-me".into(),
        })
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut total = 0usize;
    while total < 3 && std::time::Instant::now() < deadline {
        total += app.run_worker(None).await.unwrap();
        if total < 3 {
            let err = app.get_result(id).await.unwrap_err();
            assert!(
                matches!(err, CapivaraError::ResultNotFound { .. }),
                "intermediate attempt {total} must not store Failure: {err:?}"
            );
        }
        if total >= 3 {
            break;
        }
        let wait = policy.delay_for_attempt(total as u32) + Duration::from_millis(40);
        tokio::time::sleep(wait).await;
    }
    assert_eq!(total, 3, "three attempts then terminal");

    match app.get_result(id).await.unwrap() {
        JobResult::Failure { message } => assert!(message.contains("always fails")),
        JobResult::Success { .. } => panic!("expected failure"),
    }

    // Not claimable after terminal dead-letter; body retained on DLQ.
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

    let dead = app.broker().list_dead(&QueueName::default()).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].job.id, id);
    assert!(dead[0].reason.contains("always fails"));
}

#[tokio::test]
async fn redis_dead_letter_keeps_job_body_for_inspect() {
    let (_guard, url) = redis_url().await;
    let broker = RedisBroker::connect(RedisConfig::new(url).with_prefix("capivara_dlq:"))
        .await
        .unwrap();

    let mut job = Job::new("ping", br#"{"msg":"dead"}"#.to_vec());
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
        .expect("claim");
    assert_eq!(claimed.job.id, id);

    broker
        .dead_letter(&id, &claimed.claim_token, "manual poison")
        .await
        .unwrap();

    let dead = broker.list_dead(&QueueName::default()).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].job.id, id);
    assert_eq!(dead[0].job.task_name, "ping");
    assert_eq!(dead[0].reason, "manual poison");
    assert_eq!(dead[0].job.payload, br#"{"msg":"dead"}"#);

    // Not reclaimable from pending.
    assert!(
        broker
            .claim(
                &[QueueName::default()],
                Duration::from_secs(30),
                Duration::ZERO,
            )
            .await
            .unwrap()
            .is_none()
    );
}
