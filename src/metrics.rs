//! Prometheus-ready metrics via the [`metrics`] facade.
//!
//! Capivara emits counters, histograms, and best-effort gauges. It does **not**
//! install a global recorder or HTTP scrape endpoint (see M3-3). Applications
//! choose an exporter (e.g. `metrics-exporter-prometheus`).
//!
//! # Metric names
//!
//! | Name | Type | Labels |
//! |---|---|---|
//! | `capivara_jobs_enqueued_total` | counter | `queue`, `task_name` |
//! | `capivara_jobs_completed_total` | counter | `queue`, `task_name`, `status` |
//! | `capivara_job_duration_seconds` | histogram | `task_name` |
//! | `capivara_claim_wait_seconds` | histogram | `queue` |
//! | `capivara_queue_depth` | gauge | `queue` (best-effort) |
//!
//! **Never** label by `job_id` (cardinality explosion).
//!
//! # Status values (`capivara_jobs_completed_total`)
//!
//! Recorded only when settle confirms claim ownership (lost-lease `JobNotFound`
//! is a no-op and does **not** increment the counter — same for all three):
//!
//! - `success` — handler succeeded and claim was settled with `ack`
//! - `failure` — handler failed/panicked and claim was `nack`ed for retry
//! - `dead` — claim was moved to the dead-letter queue (terminal)
//!
//! # Queue depth
//!
//! [`crate::MemoryBroker`] updates `capivara_queue_depth` from in-process pending
//! length (cheap). Redis does **not** call `LLEN` on the hot path (costly under
//! load); install a separate sampler if you need Redis depth.

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use std::sync::Once;
use std::time::Instant;

/// Jobs accepted through [`crate::App::send`] / `send_with_idempotency_key`.
pub const JOBS_ENQUEUED_TOTAL: &str = "capivara_jobs_enqueued_total";
/// Per-claim settlement outcomes (see module docs for `status` values).
pub const JOBS_COMPLETED_TOTAL: &str = "capivara_jobs_completed_total";
/// Wall time of a single claimed job handle (handler + settle).
pub const JOB_DURATION_SECONDS: &str = "capivara_job_duration_seconds";
/// Time spent waiting in `Broker::claim` (including empty non-blocking polls).
pub const CLAIM_WAIT_SECONDS: &str = "capivara_claim_wait_seconds";
/// Best-effort ready-queue depth (Memory only today).
pub const QUEUE_DEPTH: &str = "capivara_queue_depth";

static DESCRIBED: Once = Once::new();

/// Describe metrics once so exporters can show metadata before first emission.
pub fn ensure_described() {
    DESCRIBED.call_once(|| {
        describe_counter!(
            JOBS_ENQUEUED_TOTAL,
            "Jobs accepted by App enqueue (includes idempotent re-sends that return an existing id)."
        );
        describe_counter!(
            JOBS_COMPLETED_TOTAL,
            "Claim settlements: status=success|failure|dead (see capivara::metrics docs)."
        );
        describe_histogram!(
            JOB_DURATION_SECONDS,
            Unit::Seconds,
            "Wall time to handle one claimed job (handler + result store + settle)."
        );
        describe_histogram!(
            CLAIM_WAIT_SECONDS,
            Unit::Seconds,
            "Duration of a single Broker::claim call."
        );
        describe_gauge!(
            QUEUE_DEPTH,
            "Best-effort pending queue depth. Memory updates this; Redis does not LLEN on the hot path."
        );
    });
}

/// Outcome label for [`JOBS_COMPLETED_TOTAL`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionStatus {
    /// Handler succeeded; claim acked.
    Success,
    /// Handler failed this attempt; claim nacked for retry.
    Failure,
    /// Claim dead-lettered (terminal).
    Dead,
}

impl CompletionStatus {
    /// Prometheus `status` label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Dead => "dead",
        }
    }
}

/// Increment [`JOBS_ENQUEUED_TOTAL`].
pub fn record_enqueued(queue: &str, task_name: &str) {
    ensure_described();
    counter!(
        JOBS_ENQUEUED_TOTAL,
        "queue" => queue.to_owned(),
        "task_name" => task_name.to_owned()
    )
    .increment(1);
}

/// Increment [`JOBS_COMPLETED_TOTAL`] for a claim settlement.
pub fn record_completed(queue: &str, task_name: &str, status: CompletionStatus) {
    ensure_described();
    counter!(
        JOBS_COMPLETED_TOTAL,
        "queue" => queue.to_owned(),
        "task_name" => task_name.to_owned(),
        "status" => status.as_str()
    )
    .increment(1);
}

/// Record [`JOB_DURATION_SECONDS`] from `started` until now.
pub fn record_job_duration(task_name: &str, started: Instant) {
    ensure_described();
    histogram!(JOB_DURATION_SECONDS, "task_name" => task_name.to_owned())
        .record(started.elapsed().as_secs_f64());
}

/// Record [`CLAIM_WAIT_SECONDS`] from `started` until now.
pub fn record_claim_wait(queue: &str, started: Instant) {
    ensure_described();
    histogram!(CLAIM_WAIT_SECONDS, "queue" => queue.to_owned())
        .record(started.elapsed().as_secs_f64());
}

/// Set best-effort [`QUEUE_DEPTH`] for `queue`.
pub fn set_queue_depth(queue: &str, depth: usize) {
    ensure_described();
    gauge!(QUEUE_DEPTH, "queue" => queue.to_owned()).set(depth as f64);
}

/// RAII helper: records [`JOB_DURATION_SECONDS`] on drop.
pub struct JobDurationTimer {
    task_name: String,
    started: Instant,
}

impl JobDurationTimer {
    pub fn start(task_name: impl Into<String>) -> Self {
        ensure_described();
        Self {
            task_name: task_name.into(),
            started: Instant::now(),
        }
    }
}

impl Drop for JobDurationTimer {
    fn drop(&mut self) {
        record_job_duration(&self.task_name, self.started);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};

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

    #[test]
    fn record_enqueued_and_completed_emit_counters() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            record_enqueued("default", "add");
            record_completed("default", "add", CompletionStatus::Success);
            record_completed("default", "add", CompletionStatus::Failure);
            record_completed("default", "fails", CompletionStatus::Dead);
        });

        let snap = snapshotter.snapshot().into_vec();
        assert_eq!(
            counter_value(
                &snap,
                JOBS_ENQUEUED_TOTAL,
                &[("queue", "default"), ("task_name", "add")]
            ),
            Some(1)
        );
        assert_eq!(
            counter_value(
                &snap,
                JOBS_COMPLETED_TOTAL,
                &[
                    ("queue", "default"),
                    ("task_name", "add"),
                    ("status", "success")
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
                    ("task_name", "add"),
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
                    ("task_name", "fails"),
                    ("status", "dead")
                ]
            ),
            Some(1)
        );
    }

    #[test]
    fn histograms_and_gauge_record() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            let start = Instant::now();
            record_job_duration("add", start);
            record_claim_wait("default", start);
            set_queue_depth("default", 3);
            let _t = JobDurationTimer::start("add");
            drop(_t);
        });

        let snap = snapshotter.snapshot().into_vec();
        let mut saw_duration = false;
        let mut saw_claim = false;
        let mut saw_depth = false;
        for (ck, _, _, val) in &snap {
            match (ck.kind(), ck.key().name(), val) {
                (MetricKind::Histogram, JOB_DURATION_SECONDS, DebugValue::Histogram(vs)) => {
                    assert!(!vs.is_empty());
                    saw_duration = true;
                }
                (MetricKind::Histogram, CLAIM_WAIT_SECONDS, DebugValue::Histogram(vs)) => {
                    assert!(!vs.is_empty());
                    saw_claim = true;
                }
                (MetricKind::Gauge, QUEUE_DEPTH, DebugValue::Gauge(g)) => {
                    assert!((g.0 - 3.0).abs() < f64::EPSILON);
                    saw_depth = true;
                }
                _ => {}
            }
        }
        assert!(saw_duration, "expected job duration histogram");
        assert!(saw_claim, "expected claim wait histogram");
        assert!(saw_depth, "expected queue depth gauge");

        // Ensure job_id is never used as a key name/label in our helpers.
        for (ck, _, _, _) in &snap {
            assert_ne!(ck.key().name(), "job_id");
            for l in ck.key().labels() {
                assert_ne!(l.key(), "job_id");
            }
        }
    }
}
