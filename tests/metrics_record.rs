//! End-to-end: App/Worker emit metrics via the `metrics` facade.
//!
//! Uses `metrics-util` DebuggingRecorder + a current-thread runtime so the
//! thread-local recorder covers async work (including `tokio::spawn`).

use capivara::metrics::{
    CLAIM_WAIT_SECONDS, JOB_DURATION_SECONDS, JOBS_COMPLETED_TOTAL, JOBS_ENQUEUED_TOTAL,
    QUEUE_DEPTH,
};
use capivara::{App, MemoryBroker, MemoryResultBackend, RetryPolicy, Task, TaskError};
use metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use metrics_util::{CompositeKey, MetricKind};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Empty;

struct OkTask;
impl Task for OkTask {
    const NAME: &'static str = "metrics_ok";
    type Args = Empty;
    type Output = Empty;
    async fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        Ok(Empty)
    }
}

struct FailTask;
impl Task for FailTask {
    const NAME: &'static str = "metrics_fail";
    type Args = Empty;
    type Output = Empty;
    async fn run(_args: Self::Args) -> Result<Self::Output, TaskError> {
        Err(TaskError::new("nope"))
    }
}

fn counter_value(
    snap: &[(
        CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )],
    name: &str,
    want_labels: &[(&str, &str)],
) -> Option<u64> {
    for (ck, _, _, val) in snap {
        if ck.kind() != MetricKind::Counter || ck.key().name() != name {
            continue;
        }
        let labels = ck.key().labels();
        let ok = want_labels
            .iter()
            .all(|(k, v)| labels.clone().any(|l| l.key() == *k && l.value() == *v));
        if ok {
            if let DebugValue::Counter(n) = val {
                return Some(*n);
            }
        }
    }
    None
}

fn has_histogram(
    snap: &[(
        CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )],
    name: &str,
) -> bool {
    snap.iter().any(|(ck, _, _, val)| {
        ck.kind() == MetricKind::Histogram
            && ck.key().name() == name
            && matches!(val, DebugValue::Histogram(vs) if !vs.is_empty())
    })
}

#[test]
fn success_path_records_enqueue_complete_duration_claim_and_depth() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let app = App::new(MemoryBroker::new()).with_result_backend(MemoryResultBackend::new());
            app.register::<OkTask>().await.unwrap();
            app.send::<OkTask>(&Empty).await.unwrap();
            let n = app.run_worker(None).await.unwrap();
            assert_eq!(n, 1);
        });
    });

    let snap = snapshotter.snapshot().into_vec();

    assert_eq!(
        counter_value(
            &snap,
            JOBS_ENQUEUED_TOTAL,
            &[("queue", "default"), ("task_name", "metrics_ok")]
        ),
        Some(1)
    );
    assert_eq!(
        counter_value(
            &snap,
            JOBS_COMPLETED_TOTAL,
            &[
                ("queue", "default"),
                ("task_name", "metrics_ok"),
                ("status", "success")
            ]
        ),
        Some(1)
    );
    assert!(
        has_histogram(&snap, JOB_DURATION_SECONDS),
        "expected job duration histogram"
    );
    assert!(
        has_histogram(&snap, CLAIM_WAIT_SECONDS),
        "expected claim wait histogram"
    );

    // Memory best-effort depth should have been set at least once.
    let saw_depth = snap
        .iter()
        .any(|(ck, _, _, _)| ck.kind() == MetricKind::Gauge && ck.key().name() == QUEUE_DEPTH);
    assert!(saw_depth, "expected MemoryBroker queue_depth gauge");

    // Cardinality policy: never label by job_id.
    for (ck, _, _, _) in &snap {
        for l in ck.key().labels() {
            assert_ne!(l.key(), "job_id");
        }
    }
}

#[test]
fn failure_retry_then_dead_records_status_labels() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    with_local_recorder(&recorder, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let app = App::new(MemoryBroker::new()).with_retry_policy(RetryPolicy {
                max_attempts: 2,
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                jitter: false,
            });
            app.register::<FailTask>().await.unwrap();
            app.send::<FailTask>(&Empty).await.unwrap();

            // delay=0: same drain reclaims after nack → attempt1 failure + attempt2 dead.
            let n = app.run_worker(None).await.unwrap();
            assert_eq!(n, 2);
        });
    });

    let snap = snapshotter.snapshot().into_vec();

    assert_eq!(
        counter_value(
            &snap,
            JOBS_ENQUEUED_TOTAL,
            &[("queue", "default"), ("task_name", "metrics_fail")]
        ),
        Some(1)
    );
    assert_eq!(
        counter_value(
            &snap,
            JOBS_COMPLETED_TOTAL,
            &[
                ("queue", "default"),
                ("task_name", "metrics_fail"),
                ("status", "failure")
            ]
        ),
        Some(1)
    );
    assert_eq!(
        counter_value(
            &snap,
            JOBS_COMPLETED_TOTAL,
            &[
                ("queue", "default"),
                ("task_name", "metrics_fail"),
                ("status", "dead")
            ]
        ),
        Some(1)
    );
}
