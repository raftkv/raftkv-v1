# Examples

This directory contains example applications and quickstart deployments for RaftKV.

## Contents

### kv_client

A simple Go client that demonstrates KV operations (put, get, cluster status,
latency decomposition) against a running RaftKV cluster via the HTTP API.

```bash
# Build and run against a cluster
go run examples/kv_client/main.go http://localhost:9001
```

### docker-compose-quickstart.yml

A 5-node Docker Compose deployment that builds from source for quick evaluation.

```bash
# Start the cluster
docker compose -f examples/docker-compose-quickstart.yml up -d

# Wait for nodes to become healthy
sleep 15

# Run the example client
go run examples/kv_client/main.go http://localhost:9001

# Stop the cluster
docker compose -f examples/docker-compose-quickstart.yml down -v
```

**Note**: The quickstart uses `LICENSE_FAIL_MODE=open` (degraded read-only mode)
for demo purposes. In this mode:
- Raft elections, heartbeats, and read-only queries work normally
- Write requests are rejected (HTTP 503)
- To enable writes, generate a license key using `cmd/license-tool` and set
  `LICENSE_FAIL_MODE=closed`

## Port Mapping

| Node | HTTP API | gRPC |
|------|----------|------|
| node-1 | localhost:9001 | localhost:9501 |
| node-2 | localhost:9002 | localhost:9502 |
| node-3 | localhost:9003 | localhost:9503 |
| node-4 | localhost:9004 | localhost:9504 |
| node-5 | localhost:9005 | localhost:9505 |