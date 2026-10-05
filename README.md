# RaftKV

[![License: AGPLv3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8.svg)](https://go.dev/)
[![Version](https://img.shields.io/badge/version-v0.5.1-orange.svg)](CHANGELOG.md)

RaftKV is a distributed key-value store built on a Raft consensus engine with
SM4-encrypted WAL storage, snapshot-based log compaction, and pipeline-optimized
log replication. It provides both HTTP and gRPC interfaces, mTLS for inter-node
communication, and a built-in chaos injection toolkit for resilience testing.

> **License**: This software is licensed under the **GNU Affero General Public
> License v3 (AGPLv3)**. Network use of modified versions requires source code
> disclosure. For commercial licensing or proprietary use, contact the project
> owner.

---

## Table of Contents

- [Key Features](#key-features)
- [TPS Benchmark](#tps-benchmark)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [HTTP API Reference](#http-api-reference)
- [Testing](#testing)
- [Deployment](#deployment)
- [Architecture](#architecture)
- [Known Limitations](#known-limitations)
- [Contributing](#contributing)
- [License](#license)

---

## Key Features

- **Raft consensus**: Leader election with pre-vote protocol, log replication
  with pipeline batching and group commit
- **Snapshot subsystem**: Log compaction at threshold 50,000 entries, 1MB chunked
  InstallSnapshot, throttled at rate 100/s burst 10
- **SM4 encrypted WAL**: Chinese national standard SM4 in CTR mode, fail-closed
  if key is missing or invalid
- **mTLS inter-node**: X.509 v3 certificates with SANs for gRPC communication
- **Idempotency tokens**: Client request deduplication for exactly-once semantics
- **Observability**: Prometheus metrics, Raft stats, latency decomposition, health
  probes (liveness + readiness)
- **Chaos engineering**: Built-in chaos injector for node kill/restart, network
  partition, disk full, and composite scenarios
- **Membership change**: Dynamic cluster add/remove via HTTP API

---

## TPS Benchmark

> **Rule OSS-EXPR-01**: All TPS values are annotated with their measurement
> conditions. No bare TPS numbers are used in this document.

All measurements use a 5-node cluster with concurrency=128, write ratio=20%.

| TPS | Batch | Raft State | Duration | Snapshot | Write Failures | Success | P99 Latency | Cluster Status |
|-----|-------|------------|----------|----------|---------------|---------|-------------|----------------|
| **841** | batch33-S | pre-Raft baseline | 30min | none | 75,687 (all failed) | 95.00% | 3,100ms | Bleeding: no consensus on write path |
| **10,579** | batch34-S | post-Raft (election + log replication) | 30min smoke | none | 0 | 100.00% | 67.71ms | Healthy: 5-node consensus |
| **14,770** | batch35-S | post-Raft + snapshot + log compaction | 1h soak | 53 snapshots | 0 | 100.00% | 40.46ms | Healthy: clean single-process soak |

### Improvement Summary

| Transition | TPS Multiplier | Baseline | P99 Improvement | Confidence |
|------------|---------------|----------|-----------------|------------|
| 841 → 10,579 | **12.57x** | pre-Raft bleeding baseline (841) | 45.8x (3,100ms → 67.71ms) | High — same c=128/write=20%/30min, only variable is Raft |
| 10,579 → 14,770 | **1.40x** | post-Raft smoke baseline (10,579) | 1.67x (67.71ms → 40.46ms) | Medium — 30min→1h, snapshot enabled |
| 841 → 14,770 | **17.57x** | pre-Raft bleeding baseline (841) | 76.7x (3,100ms → 40.46ms) | Composite of above two transitions |

### Snapshot Subsystem Details (1h soak)

- Snapshot threshold: 50,000 entries
- Minimum snapshot interval: 10s
- Chunk size: 1MB
- 53 snapshots taken in 1h, 0 WAL errors
- Snapshot archive size: ~75MB (gzip compressed)

### Fault Tolerance

- **kill leader**: election recovery ≤ 7.6s, zero data loss (see verification report)

---

## Quick Start

### Prerequisites

- Go 1.24.0 or later
- Docker and Docker Compose (for multi-node deployment)

### Build

```bash
go build ./...
```

### Run a Single Node

```bash
# SM4 key: 16 bytes as 32-char hex (TEST KEY ONLY - do not use in production)
export SM4_KEY="726166746b765f736d34746573743031"
# Demo mode: allows startup without a license (read-only)
export LICENSE_FAIL_MODE=open

go run . -id node-1 -port 9500 -http 9000
```

### Run a 3-Node Cluster (Local)

```bash
# Terminal 1
go run . -id node-1 -port 9500 -http 9001 \
  -peers node-2=localhost:9501,node-3=localhost:9502

# Terminal 2
go run . -id node-2 -port 9501 -http 9002 \
  -peers node-1=localhost:9500,node-3=localhost:9502

# Terminal 3
go run . -id node-3 -port 9502 -http 9003 \
  -peers node-1=localhost:9500,node-2=localhost:9501
```

### Run a 5-Node Cluster (Docker Compose)

```bash
docker compose -p deploy5 \
  -f tests/deploy/docker-compose-5node.yml \
  -f tests/deploy/docker-compose-5node-ports.yml \
  -f tests/deploy/docker-compose-5node-batch16.yml \
  --env-file tests/deploy/deploy.env up -d
```

### Basic Operations

```bash
# Find the leader: query /raft/status on each node until you find "leader"
curl http://localhost:9001/raft/status   # node-1 (HTTP port 9001)
curl http://localhost:9002/raft/status   # node-2 (HTTP port 9002)
# The leader's response contains "state":"Leader" — use that node's port below.
# If node-1 is not leader, replace 9001 with the leader's port.

# Propose a command (must target the leader)
curl -X POST http://localhost:9001/raft/propose \
  -d '{"key":"hello","value":"world"}'
# Response: {"success":true,"index":1}

# Read by log index (any node, not just the leader)
curl http://localhost:9001/raft/get?index=1
# Response: {"found":true,"index":1,"term":1,"command":"eyJrZXkiOiJoZWxsbyIsInZhbHVlIjoid29ybGQifQ=="}
# The command field is Base64-encoded — decode to recover the original body:
#   echo "eyJrZXkiOiJoZWxsbyIsInZhbHVlIjoid29ybGQifQ==" | base64 -d
#   → {"key":"hello","value":"world"}

# Check cluster health
curl http://localhost:9001/health/live
curl http://localhost:9001/health/ready

# View Raft status
curl http://localhost:9001/raft/status
```

> **Command field encoding**: The `/raft/propose` body is raw bytes. The
> `command` field in the `/raft/get` response is always Base64-encoded
> (Go `[]byte` JSON marshalling). To recover the original body, decode:
>
> ```bash
> # Send raw bytes (including binary) directly as the body
> curl -X POST http://localhost:9001/raft/propose --data-binary @file.bin
> # Response: {"success":true,"index":2}
>
> # Read back — command is Base64 of the bytes you sent
> curl http://localhost:9001/raft/get?index=2
> # Response: {"found":true,"index":2,"term":1,"command":"<base64>"}
> # Decode: echo "<base64>" | base64 -d > recovered.bin
> ```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node-1` | Unique node identifier |
| `GRPC_PORT` | `9500` | gRPC service port |
| `HTTP_PORT` | `9000` | HTTP API port |
| `HTTP_BIND` | `127.0.0.1` | HTTP bind address |
| `PEERS` | (empty) | Peer list: `id1=host1:port1,id2=host2:port2` |
| `SM4_KEY` | (required) | SM4 encryption key (16 bytes as 32-char hex, fail-closed) |
| `LICENSE_FAIL_MODE` | `closed` | `closed` = fail-closed, `open` = degraded read-only |
| `RSA_PRIVATE_KEY_PATH` | (env) | Path to RSA private key for license verification |

See [`raftkv.env.default`](raftkv.env.default) for the full configuration template.

### SM4 Key

The SM4 key must be a 32-character hex-encoded string representing 16 bytes.
If `SM4_KEY` is missing or invalid, the process exits immediately with a
non-zero exit code (fail-closed).

```bash
# Example: hex-encoded 16-byte key
export SM4_KEY="726166746b765f736d34746573743031"
```

---

## HTTP API Reference

### KV Operations

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/raft/propose` | Propose a command (leader only). Body: raw bytes. Returns `{"success":true,"index":N}`. Optional `?idem_token=<token>` for exactly-once semantics. |
| `GET` | `/raft/get?index=<N>` | Read log entry at index (any node). Returns `{"found":true,"index":N,"term":T,"command":"..."}`. |
| `GET` | `/raft/entry?index=<N>` | Read committed entry (batch22, F3 liveness verification). Returns `{"index":N,"term":T,"value":"...","commit_index":C}`. Supports `&count=<C>` for batch reads. |

### Raft Internals

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/raft/status` | Raft state (term, leader, committed index) |
| `GET` | `/raft/stats` | Detailed Raft metrics |
| `GET` | `/raft/election_metrics` | Election statistics |
| `POST` | `/raft/pre_vote` | Pre-vote RPC |
| `POST` | `/raft/install-snapshot` | InstallSnapshot RPC |
| `POST` | `/raft/install-snapshot-chunk` | Chunked InstallSnapshot |

### Cluster Management

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/cluster/add` | Add a new node |
| `POST` | `/cluster/remove` | Remove a node |
| `GET` | `/cluster/members` | List cluster members |

### Observability

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe |
| `GET` | `/metrics` | Prometheus-format metrics |
| `GET` | `/raft/stats` | Raft state metrics |
| `GET` | `/pipeline/stats` | Pipeline batching stats |
| `GET` | `/wal/stats` | WAL statistics |
| `GET` | `/replay/stats` | Log replay stats |
| `GET` | `/latency/stats` | Latency statistics |
| `GET` | `/latency/metrics` | Latency Prometheus metrics |
| `GET` | `/latency/decomp` | Latency decomposition |
| `GET` | `/idem/stats` | Idempotency token stats |
| `GET` | `/sm3/status` | SM3 integrity status |
| `GET` | `/license/status` | License status |

### gRPC Service

gRPC service paths use the `/raftkv.RaftService/*` prefix (proto-level
identifier, see [Known Limitations](#known-limitations)).

---

## Testing

### Unit Tests

```bash
go test . -skip TestRealGRPCConnectivity -count=1
```

`TestRealGRPCConnectivity` requires a running gRPC server and is excluded from
automated runs via the `-skip` flag.

### Regression Gate

The regression gate enforces 10 lines (REG-1 through REG-10):

```bash
python3 tests/contracts/regression_gate.py
```

Configuration: [`tests/contracts/regression.yaml`](tests/contracts/regression.yaml)

### Chaos Testing

```bash
# Inject node kill
go run ./cmd/chaos_injector -action kill -target node-2

# Inject network partition
go run ./cmd/chaos_injector -action partition -targets node-1,node-2

# Composite scenario
go run ./cmd/chaos_injector -scenario composite_scenario.json
```

---

## Deployment

### Docker

```bash
# Build image
docker build -t raftkv:latest .

# Run single container
docker run -d --name raft-node-1 \
  -p 9000:9000 -p 9500:9500 \
  -e NODE_ID=node-1 \
  -e SM4_KEY="your-16-byte-key" \
  raftkv:latest
```

### 5-Node Docker Compose

See [`tests/deploy/`](tests/deploy/) for the full 5-node deployment with
port mapping overlay and batch16 configuration.

### systemd

A systemd service template is provided at [`raftkv_cluster.service`](raftkv_cluster.service).
A daemon management script is at [`raftkv_daemon.sh`](raftkv_daemon.sh).

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    HTTP API Layer                     │
│  /raft/propose  /raft/get  /cluster/*  /metrics  /health│
├─────────────────────────────────────────────────────┤
│                   Auth Middleware                     │
│              (mTLS + License Guard)                   │
├─────────────────────────────────────────────────────┤
│                  Raft Consensus                       │
│  ┌──────────┐  ┌────────────┐  ┌──────────────────┐ │
│  │  Leader   │  │  Pre-Vote  │  │  Log Replication  │ │
│  │  Election │  │  Protocol  │  │  (Pipeline+Batch) │ │
│  └──────────┘  └────────────┘  └──────────────────┘ │
├─────────────────────────────────────────────────────┤
│                 Snapshot Subsystem                    │
│  Threshold: 50K  |  Chunk: 1MB  |  Throttle: 100/s   │
├─────────────────────────────────────────────────────┤
│                   WAL Storage                         │
│            SM4-CTR Encrypted + SM3 Integrity          │
├─────────────────────────────────────────────────────┤
│              gRPC Inter-Node Transport                │
│              (mTLS, InstallSnapshot)                  │
└─────────────────────────────────────────────────────┘
```

Key source files:

| File | Responsibility |
|------|---------------|
| `main.go` | HTTP endpoints, startup, signal handling |
| `raft.go` | Raft state machine (leader election, log replication) |
| `raft_node.go` | Node lifecycle and peer management |
| `raft_storage.go` | Storage layer with snapshot support |
| `raft_wal.go` | WAL implementation with SM4 encryption |
| `grpc_server.go` | gRPC server (AppendEntries, RequestVote, InstallSnapshot) |
| `membership.go` | Dynamic cluster membership changes |
| `sm3_integrity.go` | SM3 hash-based integrity verification |
| `cmd/chaos_injector/` | Chaos engineering toolkit |

Full architecture document: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

---

## Test Environment & Fault Domain

| Aspect | Description |
|--------|-------------|
| Test environment | Single-machine containerized deployment (Docker) |
| Fault domain | Unisolated (same host / same disk / same NIC) |
| Covered | Process-level faults (container/process termination) |
| Not covered | Node-level / datacenter-level fault tolerance (requires multi-host environment, tested separately) |

### Test Coverage & Assertions

The test suite comprises **30 core test cases** across **34 assertions**, covering:
- Raft consensus (leader election, log replication, snapshot, pre-vote)
- WAL storage (SM4 encryption, integrity verification, compaction)
- gRPC/HTTP interfaces (request handling, mTLS, pipeline)
- Chaos injection (23 fault scenarios across 6 categories)

Test execution: `go test ./... -skip TestRealGRPCConnectivity`

> **口径行**: 30项核心用例 / 34项断言 / 10线回归门 / 26例审计库

---

## Known Limitations

- **proto package name**: `protoc` is not available in the build environment.
  The generated files (`proto/raftkv.pb.go`, `proto/raftkv_grpc.pb.go`)
  retain the original proto package name `raftkv`. gRPC service paths use
  `/raftkv.RaftService/*` — this is a proto-level identifier, not a security
  concern. Renaming requires `protoc` to regenerate the `.pb.go` files. (RL-07)
- **pprof**: Not integrated into the gateway binary. Performance profiling uses
  the container-internal endpoint at `127.0.0.1:9600/debug/pprof`.
- **TestRealGRPCConnectivity**: Requires a running gRPC server; excluded from
  automated test runs via the `-skip` flag.
- **API stability**: v0.5.0 is a 0.x release. Breaking API changes may occur
  in minor version bumps. v1.0.0 will be tagged after the first production
  deployment with one quarter of zero breaking API changes.

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, code standards,
testing requirements, and the contribution workflow.

All contributions must pass the 10-line regression gate and the 26-case audit
library before merging.

---

## License

Licensed under the GNU Affero General Public License v3 (AGPLv3). See
[`LICENSE`](LICENSE) for the full license text.