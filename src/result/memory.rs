use crate::error::Result;
use crate::job::JobId;
use crate::result::{JobResult, ResultBackend};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct MemoryResultBackend {
    inner: Mutex<HashMap<uuid::Uuid, JobResult>>,
}

impl MemoryResultBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ResultBackend for MemoryResultBackend {
    async fn store(&self, id: &JobId, result: JobResult) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.insert(id.0, result);
        Ok(())
    }

    async fn get(&self, id: &JobId) -> Result<Option<JobResult>> {
        let guard = self.inner.lock().await;
        Ok(guard.get(&id.0).cloned())
    }
}
