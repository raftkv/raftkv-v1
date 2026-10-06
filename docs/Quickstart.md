# Quickstart Guide

This guide gets you from zero to a running RaftKV cluster in under 5 minutes.

---

## Prerequisites

- Go 1.24.0 or later ([install](https://go.dev/dl/))
- Docker and Docker Compose (for multi-node deployment)
- `curl` (for API testing)

---

## Option A: Single Node (Binary)

### 1. Build

```bash
git clone https://github.com/raftkv/raftkv.git
cd raftkv
go build -o raftkv .
```

### 2. Set the SM4 Key and License Mode

RaftKV requires a 16-byte SM4 encryption key as a 32-character hex-encoded
string. If missing or invalid, the process exits immediately (fail-closed).

For demo/evaluation without a license key, set `LICENSE_FAIL_MODE=open` to
run in degraded read-only mode (Raft elections and reads work; writes are
rejected). For production use, generate a license via `cmd/license-tool`
and keep `LICENSE_FAIL_MODE=closed` (the default).

```bash
# SM4 key: 16 bytes as 32-char hex (TEST KEY ONLY - do not use in production)
export SM4_KEY="726166746b765f736d34746573743031"

# Demo mode: allows startup without a license (read-only)
export LICENSE_FAIL_MODE=open
```

### 3. Start

```bash
./raftkv -id node-1 -port 9500 -http 9000
```

You should see:

```
═════════════════════════════════════════════════
  RaftKV 键值存储 (Go gRPC 微服务版)
  ...
═════════════════════════════════════════════════
```

### 4. Test

```bash
# Health check
curl http://localhost:9000/health/live
# {"status":"alive"}

# Put a value (requires valid license; rejected in degraded mode)
curl -X PUT http://localhost:9000/raft/entry \
  -H "Content-Type: application/json" \
  -d '{"key":"hello","value":"world"}'

# Get the value
curl http://localhost:9000/raft/get?key=hello
# {"value":"world"}

# Raft status
curl http://localhost:9000/raft/status
```

> **Note**: In degraded mode (`LICENSE_FAIL_MODE=open`), write operations
> return HTTP 503. To enable writes, generate a license key using
> `cmd/license-tool` and set `LICENSE_FAIL_MODE=closed`.

### 3. Start

```bash
./raftkv -id node-1 -port 9500 -http 9000
```

You should see:

```
══════════════════════════════════════════════════
  RaftKV 键值存储 (Go gRPC 微服务版)
  ...
══════════════════════════════════════════════════
```

### 4. Test

```bash
# Health check
curl http://localhost:9000/health/live
# {"status":"alive"}

# Put a value
curl -X PUT http://localhost:9000/raft/entry \
  -H "Content-Type: application/json" \
  -d '{"key":"hello","value":"world"}'

# Get the value
curl http://localhost:9000/raft/get?key=hello
# {"value":"world"}

# Raft status
curl http://localhost:9000/raft/status
```

---

## Option B: 3-Node Cluster (Local Binary)

### 1. Build

```bash
go build -o raftkv .
export SM4_KEY="726166746b765f736d34746573743031"
export LICENSE_FAIL_MODE=open
```

### 2. Start 3 Nodes

Open 3 terminals:

```bash
# Terminal 1
./raftkv -id node-1 -port 9500 -http 9001 \
  -peers node-2=localhost:9501,node-3=localhost:9502

# Terminal 2
./raftkv -id node-2 -port 9501 -http 9002 \
  -peers node-1=localhost:9500,node-3=localhost:9502

# Terminal 3
./raftkv -id node-3 -port 9502 -http 9003 \
  -peers node-1=localhost:9500,node-2=localhost:9501
```

### 3. Verify Cluster

```bash
# Check which node is the leader
curl http://localhost:9001/raft/status
curl http://localhost:9002/raft/status
curl http://localhost:9003/raft/status

# View cluster members
curl http://localhost:9001/cluster/members

# Write to the leader (writes to followers will redirect)
curl -X PUT http://localhost:9001/raft/entry \
  -H "Content-Type: application/json" \
  -d '{"key":"counter","value":"1"}'

# Read from any node
curl http://localhost:9003/raft/get?key=counter
```

---

## Option C: 5-Node Cluster (Docker Compose)

### 1. Start the Cluster

Uses the self-contained quickstart compose (builds from source, demo mode):

```bash
docker compose -f examples/docker-compose-quickstart.yml up -d
```

> **Note**: This builds the Docker image from source (first run takes a few
> minutes). Uses `LICENSE_FAIL_MODE=open` (read-only demo mode).

### 2. Verify

```bash
# All 5 nodes should be healthy
for i in 1 2 3 4 5; do
  echo "Node $i: $(curl -s http://localhost:900$i/health/live)"
done

# Check cluster membership
curl http://localhost:9001/cluster/members | python3 -m json.tool
```

### 3. Stop

```bash
docker compose -p deploy5 down -v
```

---

## Common Operations

### Write a Key-Value Pair

```bash
curl -X PUT http://localhost:9001/raft/entry \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"myvalue"}'
```

### Read a Key

```bash
curl "http://localhost:9001/raft/get?key=mykey"
```

### Add a Node to the Cluster

```bash
curl -X POST http://localhost:9001/cluster/add \
  -H "Content-Type: application/json" \
  -d '{"id":"node-6","addr":"localhost:9505"}'
```

### Remove a Node from the Cluster

```bash
curl -X POST http://localhost:9001/cluster/remove \
  -H "Content-Type: application/json" \
  -d '{"id":"node-6"}'
```

### View Metrics (Prometheus Format)

```bash
curl http://localhost:9001/metrics
```

### View Latency Decomposition

```bash
curl http://localhost:9001/latency/decomp
```

---

## Troubleshooting

### Process exits immediately with "SM4_KEY missing"

The SM4 key is required and must be a 32-character hex-encoded string
(16 bytes). Set it before starting:

```bash
export SM4_KEY="726166746b765f736d34746573743031"
```

### Process exits with "授权校验失败（Fail-Closed 拒绝启动）"

No valid license key found. For demo/evaluation, set:

```bash
export LICENSE_FAIL_MODE=open
```

This starts in degraded read-only mode (writes rejected, reads and Raft
elections work normally).

### Writes return 307 redirect

Writes must go to the leader. Check `/raft/status` to find the leader, or
follow redirects with `curl -L`.

### Node cannot join cluster

- Verify the peer list is correct on all nodes
- Ensure all ports are reachable (firewall, network)
- For Docker Compose, use container names, not `localhost`

### gRPC connection refused

gRPC uses a separate port from HTTP (default: 9500 for gRPC, 9000 for HTTP).
Ensure both ports are exposed.