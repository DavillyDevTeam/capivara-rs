# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Typed `Task` trait with native async `run`, `App::register` / `send` / `run_worker` / `get_result`.
- `MemoryBroker` and optional `MemoryResultBackend` (in-process; success and failure stored).
- Worker panic isolation via `tokio::spawn` join errors.
- Integration tests for success, task error, panic isolation, and missing result backend.
- Repository skeleton: dual MIT OR Apache-2.0 license, README (WIP), security policy,
  contributing guide, and GitHub Actions CI (fmt, clippy, test).
