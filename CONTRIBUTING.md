# Contributing to RaftKV

Thank you for your interest in contributing to RaftKV! This document outlines
the process for contributing to the project.

## Development Environment

### Prerequisites

- Go 1.24.0 or later
- Docker and Docker Compose (for integration testing)
- Git

### Setup

```bash
git clone https://github.com/raftkv/raftkv-v1.git
cd raftkv-v1
go build ./...
go test . -skip TestRealGRPCConnectivity -count=1
```

## Contribution Workflow

### 1. Fork and Branch

```bash
git checkout -b feature/your-feature-name
```

Use descriptive branch names:
- `feature/` for new features
- `fix/` for bug fixes
- `docs/` for documentation changes
- `refactor/` for code refactoring

### 2. Code Standards

- **Language**: Go 1.24+ (idiomatic Go, `gofmt` compliant)
- **Naming**: `snake_case` for files, `CamelCase` for exported identifiers
- **Comments**: Code comments in English; commit messages in English
- **No comments unless necessary**: Prefer self-documenting code
- **Testing**: All new features must include unit tests
- **No secrets**: Never hardcode keys, passwords, or credentials (R7 red line)

### 3. Testing

Before submitting a PR, ensure all tests pass:

```bash
# Build
go build ./...

# Unit tests (exclude gRPC connectivity test that needs a running server)
go test . -skip TestRealGRPCConnectivity -count=1

# Go vet
go vet ./...
```

### 4. Regression Gate

The project maintains a 10-line regression gate (`tests/contracts/regression.yaml`).
Your PR must not break any existing regression lines.

### 5. Commit Messages

Follow the existing commit message style:

```
<type>: <short description>

<optional longer description>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

### 6. Pull Request

- PRs should be focused and atomic (one logical change per PR)
- Include a clear description of what changed and why
- Reference any related issues
- Ensure CI passes (build + test + vet)

## Architecture Overview

RaftKV is a Raft-based key-value store with the following key components:

- **Raft consensus engine** (`raft.go`, `raft_pipeline.go`): Leader election,
  log replication, snapshot, log compaction
- **WAL storage** (`raft_storage.go`): SM4-encrypted write-ahead log
- **gRPC server** (`grpc_server.go`): Inter-node communication
- **HTTP API** (`main.go`): Client-facing REST API
- **Chaos injection** (`cmd/chaos_injector/`): Testing tool for fault injection

### Key Design Decisions

- **Proto files are frozen** (RL-07): `protoc` is not available in the build
  environment. Proto-generated files (`raftkv.pb.go`) cannot be regenerated.
  Do not modify `.proto` files or `.pb.go` files.
- **SM4 encryption**: WAL is encrypted using Chinese national standard SM4 cipher.
  Key is loaded from environment variable `SM4_KEY` (fail-closed).
- **Snapshot transport**: Uses HTTP chunked transfer (not gRPC streaming) due to
  proto limitations.

## Audit and Governance

The project maintains an audit case library (`docs/governance/AUDIT.md`) with 26
cases. Contributors should be aware of:

- **Red lines**: 13+4 enforced constraints that must not be violated
- **Regression gate**: 10 lines that must stay green
- **Long-running tests**: Soak tests (≥1h) must pass before closing any batch
- **No "beautification" of failures**: Test failures must be investigated, not
  relabeled as "known trade-offs"

## License

By contributing, you agree that your contributions will be licensed under the
GNU Affero General Public License v3 (AGPLv3) (see [LICENSE](LICENSE)).