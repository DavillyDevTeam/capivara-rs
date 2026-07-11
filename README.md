# capivara

> **Under construction — not released.**  
> Do not use this crate in production yet. APIs will change without notice until `0.1.0`.

**capivara** is a Rust-idiomatic **job / worker library** with a Celery-like *topology*
(enqueue → broker → worker → optional results), not a Celery clone or a universal CLI.

| | |
|---|---|
| **Package** | `capivara` (this repo: `capivara-rs`) |
| **Org** | [DavillyDevTeam](https://github.com/DavillyDevTeam) |
| **License** | MIT OR Apache-2.0 |

## Status (M0)

- [x] Public repository, dual license, CI skeleton  
- [ ] Typed `Task` trait, in-memory broker & results, worker loop (next PR)  
- [ ] Redis broker (later milestone)  

## Intended use (preview)

Applications depend on this library, define work as types implementing `Task`,
register them, enqueue typed payloads, and run a worker in **their** binary.

```text
impl Task for Add { const NAME: &str = "add"; type Args = ...; type Output = ...; async fn run(...) }
app.register::<Add>()?;
let id = app.send::<Add>(&args).await?;
// optional: app.get_result(id).await?
```

## Development

```bash
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Install git hooks (optional, recommended):

```bash
pre-commit install
pre-commit run --all-files
```

## Security

See [SECURITY.md](SECURITY.md). Please use GitHub Private Vulnerability Reporting.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
