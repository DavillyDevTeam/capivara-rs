//! Redis result backend (feature `redis`).
//!
//! Celery analogy: Redis as result backend — STRING keys with TTL.
//!
//! # Key layout (prefix from [`crate::broker::RedisConfig`])
//!
//! - `{prefix}result:{id}` — STRING JSON [`JobResult`], default TTL 24h
//!
//! # Wire shape (M1)
//!
//! Values are `serde_json` of [`JobResult`]. For
//! [`JobResult::Success`], `payload: Vec<u8>` is encoded as a **JSON array of
//! numbers** (e.g. `[123, 34, 101, ...]`), not base64. That is correct and
//! round-trips, but is bulky for large binary outputs. Changing the encoding
//! later would be a breaking wire change for any external Redis consumers.

use crate::broker::RedisConfig;
use crate::error::{CapivaraError, Result};
use crate::job::JobId;
use crate::result::{JobResult, ResultBackend};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use std::time::Duration;

/// Default result key TTL (24 hours).
pub const DEFAULT_RESULT_TTL: Duration = Duration::from_secs(86_400);

/// Redis-backed [`ResultBackend`]: multi-process capable when producer and worker
/// share the same [`RedisConfig`] url + prefix.
pub struct RedisResultBackend {
    conn: ConnectionManager,
    prefix: String,
    ttl: Duration,
}

impl RedisResultBackend {
    /// Connect using the same [`RedisConfig`] as [`crate::broker::RedisBroker`].
    pub async fn connect(config: RedisConfig) -> Result<Self> {
        Self::connect_with_ttl(config, DEFAULT_RESULT_TTL).await
    }

    /// Connect with a custom result key TTL.
    pub async fn connect_with_ttl(config: RedisConfig, ttl: Duration) -> Result<Self> {
        let client =
            redis::Client::open(config.url.as_str()).map_err(|e| CapivaraError::ResultBackend {
                message: e.to_string(),
            })?;
        let conn =
            ConnectionManager::new(client)
                .await
                .map_err(|e| CapivaraError::ResultBackend {
                    message: e.to_string(),
                })?;
        Ok(Self {
            conn,
            prefix: config.prefix,
            ttl: if ttl.is_zero() {
                DEFAULT_RESULT_TTL
            } else {
                ttl
            },
        })
    }

    /// Full Redis key for a job result (`{prefix}result:{id}`).
    pub fn result_key(&self, id: &JobId) -> String {
        format!("{}result:{}", self.prefix, id)
    }
}

#[async_trait]
impl ResultBackend for RedisResultBackend {
    async fn store(&self, id: &JobId, result: JobResult) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = self.result_key(id);
        let body = serde_json::to_string(&result)?;
        let ttl_secs = self.ttl.as_secs().max(1);
        redis::cmd("SET")
            .arg(&key)
            .arg(&body)
            .arg("EX")
            .arg(ttl_secs)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CapivaraError::ResultBackend {
                message: e.to_string(),
            })?;
        Ok(())
    }

    async fn get(&self, id: &JobId) -> Result<Option<JobResult>> {
        let mut conn = self.conn.clone();
        let key = self.result_key(id);
        let body: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CapivaraError::ResultBackend {
                message: e.to_string(),
            })?;
        match body {
            None => Ok(None),
            Some(s) => Ok(Some(serde_json::from_str(&s)?)),
        }
    }
}
