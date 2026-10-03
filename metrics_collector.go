package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type MetricsCollector struct {
	node      *RaftNode
	scheduler *SnapshotScheduler
	limiter   *TokenBucketLimiter

	proposeCount atomic.Int64
	windowStart  atomic.Int64
}

func NewMetricsCollector(node *RaftNode, scheduler *SnapshotScheduler, limiter *TokenBucketLimiter) *MetricsCollector {
	mc := &MetricsCollector{
		node:      node,
		scheduler: scheduler,
		limiter:   limiter,
	}
	mc.windowStart.Store(time.Now().UnixNano())
	return mc
}

func (mc *MetricsCollector) RecordPropose() {
	mc.proposeCount.Add(1)
}

func (mc *MetricsCollector) RenderPrometheus() string {
	var out strings.Builder

	now := time.Now().UnixNano()
	windowStart := mc.windowStart.Load()
	elapsed := now - windowStart
	var tps float64
	if elapsed > 0 {
		count := mc.proposeCount.Load()
		tps = float64(count) / (float64(elapsed) / 1e9)
		if elapsed > int64(10*time.Second) {
			mc.proposeCount.Store(0)
			mc.windowStart.Store(now)
		}
	}

	out.WriteString("# HELP raft_tps Requests per second (sliding window)\n")
	out.WriteString("# TYPE raft_tps gauge\n")
	out.WriteString(fmt.Sprintf("raft_tps %.1f\n", tps))

	out.WriteString("# HELP raft_request_duration_seconds gRPC request duration distribution\n")
	out.WriteString("# TYPE raft_request_duration_seconds histogram\n")
	out.WriteString(globalLatency.prometheusText())

	hbInterval := time.Duration(mc.node.heartbeatIntervalAtomic.Load())
	out.WriteString("# HELP raft_heartbeat_interval_seconds Current heartbeat interval\n")
	out.WriteString("# TYPE raft_heartbeat_interval_seconds gauge\n")
	out.WriteString(fmt.Sprintf("raft_heartbeat_interval_seconds %.6f\n", hbInterval.Seconds()))

	out.WriteString("# HELP raft_election_events_total Total election events\n")
	out.WriteString("# TYPE raft_election_events_total counter\n")
	out.WriteString(fmt.Sprintf("raft_election_events_total %d\n", mc.node.electionEventCount.Load()))

	var compactionCount, snapshotProgress int64
	if mc.scheduler != nil {
		compactionCount = mc.scheduler.compactionCount.Load()
		snapshotProgress = int64(mc.scheduler.snapshotProgress.Load())
	}
	out.WriteString("# HELP raft_compaction_count_total Total compaction events\n")
	out.WriteString("# TYPE raft_compaction_count_total counter\n")
	out.WriteString(fmt.Sprintf("raft_compaction_count_total %d\n", compactionCount))

	out.WriteString("# HELP raft_log_length Current log length\n")
	out.WriteString("# TYPE raft_log_length gauge\n")
	mc.node.mu.RLock()
	out.WriteString(fmt.Sprintf("raft_log_length %d\n", len(mc.node.logs)))
	out.WriteString("# HELP raft_log_start_index Log start index after compaction\n")
	out.WriteString("# TYPE raft_log_start_index gauge\n")
	out.WriteString(fmt.Sprintf("raft_log_start_index %d\n", mc.node.logStartIndex))
	mc.node.mu.RUnlock()

	out.WriteString("# HELP raft_snapshot_progress Snapshot progress (0.0-1.0)\n")
	out.WriteString("# TYPE raft_snapshot_progress gauge\n")
	out.WriteString(fmt.Sprintf("raft_snapshot_progress %.3f\n", float64(snapshotProgress)/1000.0))

	var rateLimitTriggers int64
	if mc.limiter != nil {
		rateLimitTriggers = mc.limiter.rejectedCount.Load()
	}
	out.WriteString("# HELP raft_rate_limit_rejected_total Total requests rejected by rate limiter\n")
	out.WriteString("# TYPE raft_rate_limit_rejected_total counter\n")
	out.WriteString(fmt.Sprintf("raft_rate_limit_rejected_total %d\n", rateLimitTriggers))

	return out.String()
}
