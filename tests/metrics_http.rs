//! Integration: `metrics-http` scrape endpoint on an ephemeral loopback port.
//!
//! Uses a multi-thread runtime so the exporter task can accept while the test
//! issues a blocking-style scrape (or sleeps).

use capivara::metrics::{self, JOBS_ENQUEUED_TOTAL};
use capivara::metrics_http;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local_addr")
}

fn http_get(addr: SocketAddr, path: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes())?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body)?;
    String::from_utf8(body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

async fn wait_for_scrape(addr: SocketAddr) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut last_err = None;
    while tokio::time::Instant::now() < deadline {
        match tokio::task::spawn_blocking(move || http_get(addr, "/metrics")).await {
            Ok(Ok(body)) if body.contains("HTTP/") => return body,
            Ok(Ok(body)) => last_err = Some(format!("unexpected body: {body}")),
            Ok(Err(e)) => last_err = Some(e.to_string()),
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("scrape never became ready: {last_err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scrape_endpoint_exposes_enqueued_counter() {
    let addr = free_loopback_addr();
    let handle = metrics_http::start_metrics_server(addr).expect("start metrics server");

    // Emit a labeled counter through the global recorder.
    metrics::record_enqueued("default", "metrics_http_probe");

    let body = wait_for_scrape(addr).await;
    assert!(body.contains("200"), "expected HTTP 200, got:\n{body}");
    assert!(
        body.contains(JOBS_ENQUEUED_TOTAL),
        "expected {JOBS_ENQUEUED_TOTAL} in scrape body:\n{body}"
    );
    assert!(
        body.contains("metrics_http_probe"),
        "expected task_name label value in scrape:\n{body}"
    );

    handle.abort();
}
