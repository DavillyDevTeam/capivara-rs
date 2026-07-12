# capivara

> **Under construction — not production-released.**  
> Version **`0.0.1`** with **`publish = false`** (will not publish to crates.io by accident).  
> APIs may change until a formal release is announced.

**capivara** is a Rust-idiomatic **job / worker library** with a Celery-like *topology*
(enqueue → broker → worker → optional results). It is **not** a Celery clone and **not**
a universal CLI that runs arbitrary remote code.

| | |
|---|---|
| **Package** | `capivara` (repo: `capivara-rs`) |
| **Org** | [DavillyDevTeam](https://github.com/DavillyDevTeam) |
| **License** | MIT OR Apache-2.0 |

## What works today (M0)

- Typed **`Task`** trait (`NAME`, `Args`, `Output`, async `run`)
- **`App::register` / `send` / `run_worker` / `get_result`**
- **`MemoryBroker`** + optional **`MemoryResultBackend`** (in-process only)
- Panic isolation at the task boundary
- CI: fmt, clippy, tests

## Not yet

- Redis / multi-process workers  
- Retries, DLQ, leases  
- Proc-macro / `app.task("name", fn)` sugar  

## Quick example

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
pre-commit install   # optional
pre-commit run --all-files
```

## Security

See [SECURITY.md](SECURITY.md) — use GitHub Private Vulnerability Reporting.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
