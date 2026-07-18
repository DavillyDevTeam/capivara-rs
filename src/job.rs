//! Job envelope — what sits on the broker.
//!
//! Celery analogy: the *message* on the queue (task name + serialized body),
//! not the Python function itself.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique id for an enqueued job (and optional result lookup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Queue name newtype — avoids mixing arbitrary strings with task names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueueName(String);

impl QueueName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for QueueName {
    fn default() -> Self {
        Self::new("default")
    }
}

impl From<&str> for QueueName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for QueueName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Wire job stored on a [`crate::Broker`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub queue: QueueName,
    /// Stable task type name (`Task::NAME`).
    pub task_name: String,
    /// JSON-encoded `Task::Args`.
    pub payload: Vec<u8>,
    pub attempts: u32,
    /// Optional producer-side dedupe key. When set on enqueue, the broker returns
    /// the existing [`JobId`] if the key was seen before (no duplicate queue entry).
    ///
    /// Keys are **global per broker** (Memory process / Redis key `prefix`), not
    /// namespaced by `task_name` or `queue`. Include the task name (and queue if
    /// needed) in the key string when uniqueness must not collide across tasks.
    /// Empty / whitespace-only keys are rejected with
    /// [`crate::CapivaraError::EmptyIdempotencyKey`].
    ///
    /// `#[serde(default)]` so older job JSON without this field deserializes as `None`.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

impl Job {
    pub fn new(task_name: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            id: JobId::new(),
            queue: QueueName::default(),
            task_name: task_name.into(),
            payload,
            attempts: 0,
            idempotency_key: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_without_idempotency_field_deserializes() {
        let id = JobId::new();
        // Legacy wire form (pre-M2-3) — no idempotency_key field.
        let json = format!(
            r#"{{"id":"{}","queue":"default","task_name":"add","payload":[],"attempts":0}}"#,
            id.0
        );
        let job: Job = serde_json::from_str(&json).expect("legacy job json");
        assert_eq!(job.id, id);
        assert_eq!(job.idempotency_key, None);
    }
}
