//! Typed task definition (option B).
//!
//! Celery analogy: the `@app.task` *callable*, expressed as a Rust type with
//! associated argument/output types instead of a free function + decorator.

use crate::error::TaskError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;

/// A unit of work apps define and workers execute.
///
/// # Example
///
/// ```ignore
/// struct Add;
///
/// impl Task for Add {
///     const NAME: &'static str = "add";
///     type Args = AddArgs;
///     type Output = AddResult;
///
///     async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
///         Ok(AddResult { sum: args.x + args.y })
///     }
/// }
/// ```
pub trait Task: Send + Sync + 'static {
    /// Wire / registry name (must be unique per `App`).
    const NAME: &'static str;

    /// Deserialized from the job payload (JSON).
    type Args: Serialize + DeserializeOwned + Send + 'static;

    /// Serialized into the optional result backend on success.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// Execute the task.
    ///
    /// Native `async fn` in trait (edition 2024) — no `async-trait` on tasks.
    fn run(args: Self::Args) -> impl Future<Output = Result<Self::Output, TaskError>> + Send;
}
