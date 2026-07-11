//! Capivara — Rust-idiomatic job / worker library.
//!
//! **Status:** under construction; **not released.** The public API (typed
//! [`Task`](crate) trait, brokers, optional results) will land in subsequent
//! milestones. Do not depend on this crate in production yet.
//!
//! # Intended shape (preview)
//!
//! - Define work units as types implementing a `Task` trait (name + args + output).
//! - `register` / `send` / in-process or Redis-backed workers.
//! - Optional result backend (`JobId` + `get_result`).
//!
//! Celery provides the *topology* mental model (enqueue → broker → worker →
//! optional results); the API is intentionally idiomatic Rust, not a Celery clone.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles_and_links() {
        // Placeholder until Task / App surface lands.
        assert_eq!(2 + 2, 4);
    }
}
