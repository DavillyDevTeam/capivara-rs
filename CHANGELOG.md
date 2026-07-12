# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Typed `Task` trait with native async `run`, `App::register` / `send` / `run_worker` / `get_result`.
- `MemoryBroker` and optional `MemoryResultBackend` (in-process; success and failure stored).
- Worker panic isolation via `tokio::spawn` join errors.
- Integration tests for success, task error, panic isolation, missing result backend,
  bad JSON payload, `max_jobs`, `ResultNotFound`, fire-and-forget drain, unknown task name,
  and `with_default_queue`.
- `App::broker()` for shared broker access (tests / raw `Job` injection).
- Repository skeleton: dual MIT OR Apache-2.0 license, README (WIP), security policy,
  contributing guide, GitHub Actions CI (fmt, clippy, test), Dependabot config.
- CI least-privilege `permissions` and `concurrency` cancel-in-progress.

### Changed

- Package version set to **`0.0.1`** with **`publish = false`** until a real release.
- Apache-2.0 license appendix copyright filled in for Duarte Mainart Tecnologia e Publicidade LTDA.
