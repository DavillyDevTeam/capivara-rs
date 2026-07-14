//! Library error types.
//!
//! Celery analogy: exceptions raised by tasks / broker failures — but we use
//! a single structured error enum instead of free-form exceptions.

use thiserror::Error;

/// Errors returned by the capivara library (broker, registry, results, worker).
#[derive(Debug, Error)]
pub enum CapivaraError {
    #[error("task `{name}` is already registered")]
    TaskAlreadyRegistered { name: String },

    #[error("task `{name}` is not registered")]
    TaskNotRegistered { name: String },

    #[error("no result backend configured; cannot get_result")]
    NoResultBackend,

    #[error("result for job `{id}` not found")]
    ResultNotFound { id: String },

    #[error("job `{id}` not found in broker")]
    JobNotFound { id: String },

    /// JSON encode/decode failure (name kept short; covers both directions).
    #[error("JSON serde error: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("broker error: {0}")]
    Broker(String),

    #[error("task failed: {message}")]
    TaskFailed { message: String },

    #[error("task panicked: {message}")]
    TaskPanicked { message: String },
}

/// Convenience alias for library results.
pub type Result<T> = std::result::Result<T, CapivaraError>;

/// Error type tasks return from [`crate::Task::run`].
///
/// Converted into a stored failure and/or worker failure path.
#[derive(Debug, Error, Clone)]
#[error("{message}")]
pub struct TaskError {
    pub message: String,
}

impl TaskError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<&str> for TaskError {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TaskError {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}
