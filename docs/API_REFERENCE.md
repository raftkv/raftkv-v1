# API Reference

RaftKV exposes both an HTTP API and a gRPC service for cluster operations,
management, and observability.

---

## HTTP API

All HTTP endpoints accept and return JSON unless otherwise noted.

### KV Operations

#### POST /raft/propose

Propose a command (raw bytes body). Must be sent to the leader node; follower
nodes will return a redirect to the leader. Optional `?idem_token=<token>`
query parameter for exactly-once semantics.

**Request:** Raw bytes (sent as the request body).

**Response (200):**

```json
{
  "success": true,
  "index": 42
}
```

**Response (503):** Degraded read-only mode (license fail-open) or in-flight cap exceeded.

**Response (307):** Redirect to leader node (if sent to a follower).

---

#### GET /raft/get?index=\<N\>

Read a log entry by index. Can be served by any node (linearizable read).

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | int64 | yes | The log index to read |

**Response (200):**

```json
{
  "found": true,
  "index": 42,
  "term": 5,
  "command": "base64-encoded-bytes"
}
```

The `command` field is Base64-encoded (Go `[]byte` JSON marshalling). Decode
to recover the original propose body.

**Response (200, not found):**

```json
{
  "found": false
}
```

**Response (400):** Missing or invalid `index` parameter.

---

#### GET /raft/entry?index=\<N\>

Read a committed entry for F3 liveness verification. Supports `&count=<C>`
for batch reads.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `index` | int64 | no | The log index (defaults to commit_index) |
| `count` | int64 | no | Batch count (returns a list of entries) |

**Response (200, single):**

```json
{
  "index": 42,
  "term": 5,
  "value": "entry-value",
  "commit_index": 42
}
```

---

### Raft Internals

#### GET /raft/status

Returns the current Raft state of the node.

**Response:**

```json
{
  "id": "node-1",
  "state": "Leader",
  "term": 5,
  "leader_id": "node-1",
  "commit_index": 42,
  "last_applied": 42,
  "log_count": 42,
  "peer_count": 4,
  "voted_for": "node-1"
}
```

The `state` field is one of: `Follower`, `Candidate`, `Leader`, `PreCandidate`.

---

#### GET /raft/stats

Returns detailed Raft metrics including message counts, timing, and
election statistics.

---

#### GET /raft/election_metrics

Returns election-specific metrics: total elections, election timeouts,
pre-vote outcomes, term changes.

---

#### POST /raft/pre_vote

Pre-vote RPC endpoint. Called by candidate nodes before initiating a full
election to prevent election storms.

---

#### POST /raft/install-snapshot

InstallSnapshot RPC endpoint. Called by the leader to send a complete
snapshot to a follower that has fallen behind.

---

#### POST /raft/install-snapshot-chunk

Chunked InstallSnapshot endpoint. Sends snapshot data in 1MB chunks for
large snapshots.

---

### Cluster Management

#### POST /cluster/add

Add a new node to the cluster. Must be sent to the leader.

**Request:**

```json
{
  "id": "node-6",
  "addr": "node-6:9505"
}
```

**Response (200):**

```json
{
  "ok": true,
  "message": "node-6 added"
}
```

---

#### POST /cluster/remove

Remove a node from the cluster. Must be sent to the leader.

**Request:**

```json
{
  "id": "node-6"
}
```

**Response (200):**

```json
{
  "ok": true,
  "message": "node-6 removed"
}
```

---

#### GET /cluster/members

List all current cluster members.

**Response:**

```json
{
  "current_members": ["node-1", "node-2", "node-3"],
  "all_nodes": ["node-1", "node-2", "node-3"],
  "quorum_size": 2,
  "joint_consensus": false,
  "peer_count": 3
}
```

---

### Health & Observability

#### GET /health/live

Liveness probe. Returns 200 with plain text `OK` if the process is alive.

```
OK
```

---

#### GET /health/ready

Readiness probe. Returns 200 with plain text `READY` if the node is ready to
serve requests (Raft initialized, WAL loaded).

```
READY
```

---

#### GET /metrics

Prometheus-format metrics. Includes Raft state, WAL statistics, pipeline
batching, latency, and custom application metrics.

---

#### GET /raft/stats

Detailed Raft statistics: term, commit index, log length, message counts,
snapshot count, election count.

---

#### GET /pipeline/stats

Pipeline batching statistics: batch count, average batch size, pipeline depth,
quorum wait overlap stats.

---

#### GET /wal/stats

WAL statistics: total entries, file size, SM4 encryption status, write count,
error count.

---

#### POST /wal/reset

Reset WAL statistics counters. Does not modify WAL data.

---

#### GET /replay/stats

Log replay statistics: replay count, replay duration, entries replayed.

---

#### GET /latency/stats

Latency statistics: count, mean, P50, P95, P99, max.

---

#### GET /latency/metrics

Latency metrics in Prometheus format.

---

#### GET /latency/decomp

Latency decomposition: breakdown of request latency into components
(consensus, WAL write, network, snapshot).

---

#### GET /idem/stats

Idempotency token statistics: total tokens, hit count, miss count,
eviction count.

---

#### GET /sm3/status

SM3 integrity check status: last check time, verified entries, corrupted
entries, integrity status.

---

#### GET /license/status

License status: fail mode (`closed` or `open`), degraded flag, degraded
reason.

---

## gRPC Service

The gRPC service provides the standard Raft RPCs for inter-node communication.

### Service Path

```
raftkv.RaftService
```

> **Note**: The gRPC service path retains the original proto package name
> `raftkv` because `protoc` is not available in the build environment to
> regenerate the `.pb.go` files. This is a proto-level identifier and not a
> security concern. See [Known Limitations](../README.md#known-limitations)
> in the README.

### RPCs

| RPC | Description |
|-----|-------------|
| `AppendEntries` | Leader → follower: append log entries + heartbeat |
| `RequestVote` | Candidate → peers: request vote for election |
| `InstallSnapshot` | Leader → follower: send snapshot when follower is far behind |

### Proto Definition

The proto definition is at [`proto/raftkv.proto`](../proto/raftkv.proto).

---

## Error Codes

| HTTP Status | Meaning |
|-------------|---------|
| 200 | Success |
| 307 | Redirect to leader (write to follower) |
| 400 | Bad request (malformed JSON, missing parameter) |
| 404 | Key not found |
| 409 | Conflict (concurrent modification) |
| 500 | Internal server error |
| 503 | Service unavailable (node not ready, degraded mode) |