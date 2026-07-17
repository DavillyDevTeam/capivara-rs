//! Optional result backend.
//!
//! Celery analogy: result backend — store success **and** failure; producer
//! uses `JobId` + `get_result` (not a fake blocking `AsyncResult` without config).

mod memory;
#[cfg(feature = "redis")]
mod redis_result;

pub use memory::MemoryResultBackend;
#[cfg(feature = "redis")]
pub use redis_result::{DEFAULT_RESULT_TTL, RedisResultBackend};

use crate::error::Result;
use crate::job::JobId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Stored outcome for a job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobResult {
    Success { payload: Vec<u8> },
    Failure { message: String },
}

#[async_trait]
pub trait ResultBackend: Send + Sync {
    async fn store(&self, id: &JobId, result: JobResult) -> Result<()>;
    async fn get(&self, id: &JobId) -> Result<Option<JobResult>>;
}
