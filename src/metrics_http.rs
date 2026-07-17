//! Optional Prometheus scrape endpoint (`metrics-http` feature).
//!
//! Installs a global [`metrics`] recorder via
//! [`metrics_exporter_prometheus`] and serves Prometheus text exposition over
//! HTTP (any path; conventionally scrape `GET /metrics`).
//!
//! # Security (v0)
//!
//! - **No authentication** on the scrape endpoint.
//! - Default bind is **loopback only** ([`DEFAULT_BIND`] = `127.0.0.1:9090`).
//! - Binding to a non-loopback address exposes process metrics to the network
//!   without auth — use network isolation (firewall, private scrape network)
//!   or terminate TLS/auth at a reverse proxy if you must scrape remotely.
//!
//! # Global recorder
//!
//! The [`metrics`] facade allows **only one** global recorder per process.
//! [`start_metrics_server`] / [`serve`] install that recorder. A second call
//! (or installing another global exporter) returns
//! [`CapivaraError::MetricsHttp`]. Prefer calling once at process startup.
//!
//! Must be invoked from within a Tokio runtime (the exporter is spawned on the
//! current handle).
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(feature = "metrics-http")]
//! # {
//! use capivara::metrics_http;
//!
//! #[tokio::main]
//! async fn main() -> capivara::Result<()> {
//!     // Optional: override bind; default is 127.0.0.1:9090
//!     let _handle = metrics_http::serve()?;
//!     // ... run workers ...
//!     Ok(())
//! }
//! # }
//! ```

use crate::error::{CapivaraError, Result};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::task::JoinHandle;

/// Default scrape bind address: loopback, port **9090**.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9090);

/// Install the Prometheus recorder and start the HTTP scrape server on `addr`.
///
/// Returns a [`JoinHandle`] for the exporter future (runs until the process
/// exits or the handle is aborted). Scrape with Prometheus at
/// `http://{addr}/metrics` (any path works with the exporter).
///
/// # Errors
///
/// - Bind failure (address in use, permission denied, …)
/// - Global recorder already installed
/// - Called outside a Tokio runtime
pub fn start_metrics_server(addr: SocketAddr) -> Result<JoinHandle<()>> {
    let runtime = tokio::runtime::Handle::try_current().map_err(|_| CapivaraError::MetricsHttp {
        message: "start_metrics_server requires a Tokio runtime (call from async context or after runtime enter)".into(),
    })?;

    let _enter = runtime.enter();
    let (recorder, exporter) = PrometheusBuilder::new()
        .with_http_listener(addr)
        .build()
        .map_err(|e| CapivaraError::MetricsHttp {
            message: e.to_string(),
        })?;

    ::metrics::set_global_recorder(recorder).map_err(|e| CapivaraError::MetricsHttp {
        message: format!("failed to install global metrics recorder: {e}"),
    })?;

    // Ensure HELP/TYPE metadata is registered with the installed recorder.
    crate::metrics::ensure_described();

    // Map exporter Result into unit so the public handle is JoinHandle<()>.
    Ok(runtime.spawn(async move {
        if let Err(err) = exporter.await {
            // Exporter only ends on fatal accept/serve failure; surface via tracing.
            tracing::error!(?err, "capivara metrics HTTP exporter stopped");
        }
    }))
}

/// Start the scrape server on [`DEFAULT_BIND`] (`127.0.0.1:9090`).
///
/// See [`start_metrics_server`] for security and global-recorder notes.
pub fn serve() -> Result<JoinHandle<()>> {
    start_metrics_server(DEFAULT_BIND)
}
