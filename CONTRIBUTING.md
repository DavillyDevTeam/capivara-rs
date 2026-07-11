# Contributing to capivara

Thanks for your interest. The project is early; small, well-tested changes are best.

## Workflow

1. Create a branch from `main` (org members can push branches to this repo).
2. Make your changes with tests where reasonable.
3. Ensure checks pass locally (see below).
4. Open a pull request against `main`.
5. PRs are **squash-merged** after CI is green.

## Local checks

```bash
cargo test --all-targets
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Pre-commit (optional but recommended)

This repo includes a [pre-commit](https://pre-commit.com/) config that mirrors CI lint:

```bash
pre-commit install
pre-commit run --all-files
```

## Design notes

- Prefer extending the library API over adding a catch-all CLI.
- Core behavior should stay testable with **in-memory** broker/results when possible.
- See [AGENTS.md](AGENTS.md) for agent-oriented project norms.
