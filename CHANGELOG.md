# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Version Strategy

- **0.x releases**: API allows breaking changes. Minor version bumps may include
  incompatible API modifications.
- **v1.0.0 upgrade condition**: First production-grade deployment completed +
  API maintains zero breaking changes for one full quarter (3 months).
  v1.0.0 will be separately decided at that time.
- **Internal history**: This project underwent internal iteration v1.x through
  v2.4 prior to open-sourcing. Open-source versioning starts from v0.5.0.
  Internal version numbers are not used externally.

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