# capivara

> **Under construction — not production-released.**  
> Version **`0.0.1`** with **`publish = false`** (Cargo will refuse `cargo publish`).  
> APIs may change until a formal release is announced.

**capivara** is a Rust-idiomatic **job / worker library** with a Celery-like *topology*
(enqueue → broker → worker → optional results). It is **not** a Celery clone and **not**
a universal CLI that runs arbitrary remote code.

| | |
|---|---|
| **Package** | `capivara` (repo: [`capivara-rs`](https://github.com/DavillyDevTeam/capivara-rs)) |
| **Org** | [DavillyDevTeam](https://github.com/DavillyDevTeam) |
| **License** | MIT OR Apache-2.0 |
| **Status** | M0: in-process memory backends only |

## What works today (M0)

- Typed **`Task`** trait (`NAME`, `Args`, `Output`, native async `run`)
- **`App`**: `register` / `send` / `run_worker` / `get_result`
  - optional `with_result_backend`, `with_default_queue`
  - `broker()` for shared broker access / advanced raw `Job` enqueue
- **`MemoryBroker`** + optional **`MemoryResultBackend`**
  - **Single-process only** — not shared across OS processes; not a distributed queue
- Panic isolation at the task boundary (worker keeps going)
- Results: `send` → `JobId`; `get_result` only if a backend is configured
  (stores **success and failure**); errors clearly if no backend / missing id
- CI: fmt, clippy, tests (least-privilege permissions + concurrency)
- Dependabot for Cargo / Actions; secret scanning enabled on the repo

## Not yet

- Redis / multi-process workers  
- Retries, DLQ, leases / recoverer  
- Proc-macro or `app.task("name", fn)` sugar  
- crates.io publish (`publish = false` until then)

## Quick example

This is a **library** example. Your app needs **Tokio** (async runtime) and **serde**
for task args; `serde_json` is already pulled in by capivara for payloads.

```rust
use capivara::{App, JobResult, MemoryBroker, MemoryResultBackend, Task, TaskError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AddArgs { x: i32, y: i32 }

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AddResult { sum: i32 }

struct Add;

impl Task for Add {
    const NAME: &'static str = "add";
    type Args = AddArgs;
    type Output = AddResult;

    async fn run(args: Self::Args) -> Result<Self::Output, TaskError> {
        Ok(AddResult { sum: args.x + args.y })
    }
}

#[tokio::main]
async fn main() -> capivara::Result<()> {
    let app = App::new(MemoryBroker::new())
        .with_result_backend(MemoryResultBackend::new());
    app.register::<Add>().await?;
    let id = app.send::<Add>(&AddArgs { x: 2, y: 3 }).await?;
    // `None` = drain the in-memory queue; `Some(n)` = process at most n jobs
    app.run_worker(None).await?;
    match app.get_result(id).await? {
        JobResult::Success { payload } => {
            let out: AddResult = serde_json::from_slice(&payload).unwrap();
            assert_eq!(out.sum, 5);
        }
        JobResult::Failure { message } => panic!("{message}"),
    }
    Ok(())
}
```

## Development

```bash
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Git hooks (recommended):

```bash
pre-commit install
pre-commit run --all-files
```

## Security

See [SECURITY.md](SECURITY.md) — report vulnerabilities via GitHub Private Vulnerability Reporting
(not public issues).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Prefer pull requests against `main` (branch protection
requires CI). Merged feature branches are deleted on GitHub; keep local branches if you want.
