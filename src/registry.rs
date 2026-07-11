//! Task registry: string name → type-erased async handler.
//!
//! Celery analogy: the in-process map filled by `@app.task` imports, but
//! registration is **explicit** via [`crate::App::register`].

use crate::error::{CapivaraError, Result, TaskError};
use crate::task::Task;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Erased handler: JSON bytes in → JSON bytes out (or error).
pub(crate) type ErasedHandler =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>> + Send + Sync>;

#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<&'static str, ErasedHandler>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: Task>(&mut self) -> Result<()> {
        if self.handlers.contains_key(T::NAME) {
            return Err(CapivaraError::TaskAlreadyRegistered {
                name: T::NAME.to_string(),
            });
        }
        self.handlers.insert(T::NAME, erase::<T>());
        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Result<ErasedHandler> {
        self.handlers
            .get(name)
            .cloned()
            .ok_or_else(|| CapivaraError::TaskNotRegistered {
                name: name.to_string(),
            })
    }

    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }
}

fn erase<T: Task>() -> ErasedHandler {
    Arc::new(|payload: Vec<u8>| {
        Box::pin(async move {
            let args: T::Args =
                serde_json::from_slice(&payload).map_err(CapivaraError::Serialize)?;
            let output = T::run(args)
                .await
                .map_err(|e: TaskError| CapivaraError::TaskFailed { message: e.message })?;
            serde_json::to_vec(&output).map_err(CapivaraError::Serialize)
        })
    })
}
