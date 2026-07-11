//! Capivara — Rust-idiomatic job / worker library.
//!
//! **Status:** early development; APIs may change before a formal `0.1.0` release
//! announcement. Suitable for experimentation and learning.
//!
//! # Topology (Celery-like system shape)
//!
//! ```text
//! register Task types → send::<T>(&args) → Broker → Worker → optional ResultBackend
//! ```
//!
//! # Example
//!
//! ```
//! use capivara::{
//!     App, JobResult, MemoryBroker, MemoryResultBackend, Task, TaskError,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct AddArgs { x: i32, y: i32 }
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct AddResult { sum: i32 }
//!
//! struct Add;
//!
//! impl Task for Add {
//!     const NAME: &'static str = "add";
//!     type Args = AddArgs;
//!     type Output = AddResult;
//!
//!     async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
//!         Ok(AddResult { sum: args.x + args.y })
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> capivara::Result<()> {
//! let app = App::new(MemoryBroker::new())
//!     .with_result_backend(MemoryResultBackend::new());
//! app.register::<Add>().await?;
//! let id = app.send::<Add>(&AddArgs { x: 2, y: 3 }).await?;
//! app.run_worker(None).await?;
//! match app.get_result(id).await? {
//!     JobResult::Success { payload } => {
//!         let out: AddResult = serde_json::from_slice(&payload).unwrap();
//!         assert_eq!(out.sum, 5);
//!     }
//!     JobResult::Failure { message } => panic!("{message}"),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Design notes
//!
//! - **Typed tasks** (`impl Task`) — not stringly `app.task("name", fn)`.
//! - **Optional results** — `get_result` errors if no backend is configured.
//! - **Memory backends** are for tests / single-process demos; Redis comes later.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

mod app;
mod broker;
mod error;
mod job;
mod registry;
mod result;
mod task;
mod worker;

pub use app::App;
pub use broker::{Broker, MemoryBroker};
pub use error::{CapivaraError, Result, TaskError};
pub use job::{Job, JobId, QueueName};
pub use result::{JobResult, MemoryResultBackend, ResultBackend};
pub use task::Task;
