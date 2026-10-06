# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Version Strategy

- **v1.0.0**: First stable public release. API is considered stable; breaking
  changes will follow semantic versioning.
- **0.x releases**: Pre-stable API. Breaking changes allowed in minor bumps.
- **Internal history**: This project underwent internal iteration prior to
  open-sourcing. Open-source versioning starts from v0.5.0.

## [v1.0.0] - 2026-10-06

### Added

- `WAL_PATH` environment variable now documented and set in all Docker Compose files
- `deploy.env.example` rewritten with full variable documentation and neutral placeholders
- Quickstart deployment (`examples/docker-compose-quickstart.yml`) with read-only mode
- 503 response with guidance text when writes are rejected in degraded (Fail-Open) mode
- README dual-path documentation: "90-Second Experience" (quickstart) and "Production Deployment" (fail-closed)
- Docker memory recommendation (≥16 GB, 32 GB for production)

### Changed

- **Image name unified** to `raftkit-gateway:v1` across all compose files and documentation
- **Version badge** updated from v0.5.1 to v1.0.0
- **Internal designations purged**: all internal project names and personal identifiers
  replaced with neutral terms
- `deploy.env.example` image name updated from `raftkv-v26:ci-gate` to `raftkit-gateway:v1`
- `deploy_up.sh` / `deploy_verify.sh` default image updated to `raftkit-gateway:v1`
- **Fingerprint prefix updated** from the legacy internal anchor prefix to
  `raftkv-anchor:` in `GetMachineFingerprint()` — license files must be re-signed to bind
  to the new fingerprint. The embedded RSA public key in `license_guard.go` has been
  regenerated accordingly. **Developer note**: any future change to the fingerprint prefix
  or computation MUST be accompanied by re-signing all license files with the corresponding
  private key; otherwise Fail-Closed startup will reject all nodes

### Fixed

- `/raft/propose` now correctly returns 503 in degraded (Fail-Open) mode instead of
  allowing writes through
- All Docker Compose files now set `WAL_PATH`, preventing `log.Fatal` on startup

## [v0.5.0] - 2026-09-15

### Added

- Raft consensus engine with leader election and log replication
  - Pre-vote protocol to prevent election storms
  - Batch sync with incremental catch-up (no full retransmission)
  - Pipeline AppendEntries for overlapped quorum waits
- Snapshot subsystem with log compaction
  - Snapshot threshold: 50,000 entries
  - Minimum snapshot interval: 10s
  - Chunk size: 1MB for HTTP-based InstallSnapshot
  - Snapshot throttle: rate 100/s, burst 10
- SM4 (Chinese national standard) encrypted WAL storage
  - CTR mode encryption with configurable key via environment variable
  - Fail-closed: startup fails if SM4 key is missing or invalid
- HTTP API for KV operations (GET/PUT/DELETE)
  - Leader routing with automatic redirect
  - Client retry with exponential backoff
- mTLS support for inter-node communication
  - X.509 v3 certificates with SANs (Go 1.25+ compatible)
  - CA-based certificate generation script
- Observability endpoints
  - `/raft/stats` - Raft state metrics
  - `/metrics` - Prometheus-format metrics
  - `/health` - Health check endpoint
- Chaos injection tool (`cmd/chaos_injector`)
  - Node kill/restart, network partition, disk full injection
  - Composite scenario support
- 5-node Docker Compose deployment with port mapping overlay

### Performance

All TPS values measured with concurrency=128, write ratio=20%, 5-node cluster.
See [TPS Benchmark Table](#tps-benchmark) in README for full conditions.

- **pre-Raft baseline**: TPS=841 (30min, 75,687 write failures, P99≈3,100ms)
  - Baseline before Raft consensus was implemented. Write path had no consensus,
    resulting in 95% success rate with 75,687 complete write failures.
- **post-Raft (30min smoke)**: TPS=10,579 (0 failures, P99=67.71ms, 100% success)
  - After introducing Raft consensus (election + log replication).
  - vs pre-Raft baseline: 12.57x TPS improvement, 45.8x P99 improvement.
- **post-Raft+snapshot (1h soak)**: TPS=14,770 (0 failures, P99=40.46ms, 100% success)
  - After adding snapshot subsystem + log compaction.
  - vs pre-Raft baseline: 17.57x TPS improvement.
  - vs post-Raft smoke baseline: 1.40x TPS improvement (note: 30min→1h, snapshot enabled).

### Security

- SM4 key loaded from environment variable (fail-closed)
- mTLS for inter-node gRPC communication
- Idempotency tokens for client request deduplication
- No hardcoded secrets in source code

### Known Limitations

- `protoc` not available in build environment; proto-generated files (`raftkv.pb.go`,
  `raftkv_grpc.pb.go`) retain original proto package name (RL-07).
  gRPC service paths use `/raftkv.RaftService/*` — this is a proto-level identifier,
  not a security concern.
- `pprof` not integrated into gateway binary; performance profiling uses
  container-internal endpoint at `127.0.0.1:9600/debug/pprof`.
- `TestRealGRPCConnectivity` requires a running gRPC server; excluded from
  automated test runs via `-skip` flag.

### Infrastructure

- Go module: `raftkv` (renamed from internal module for open-source release)
- Go version: 1.24.0+
- Regression gate: 10 lines (REG-1 through REG-10)
- Audit case library: 26 cases (AUDIT-001 through AUDIT-026)
- Red lines: 13+4 enforced constraints