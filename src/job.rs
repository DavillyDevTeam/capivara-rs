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
}

impl Job {
    pub fn new(task_name: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            id: JobId::new(),
            queue: QueueName::default(),
            task_name: task_name.into(),
            payload,
            attempts: 0,
        }
    }
}
