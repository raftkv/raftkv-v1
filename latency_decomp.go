package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

type LatencyDecompCollector struct {
	enabled atomic.Bool

	batchWaitSamples    []int64
	batchFlushSamples   []int64
	quorumWaitSamples   []int64
	commitNotifySamples []int64
	totalSamples        []int64
	mu                  sync.Mutex

	snapshotIOCount  atomic.Int64
	snapshotIOWallMs atomic.Int64
}

var globalLatencyDecomp *LatencyDecompCollector

func InitLatencyDecomp() *LatencyDecompCollector {
	c := &LatencyDecompCollector{}
	globalLatencyDecomp = c
	return c
}

func (c *LatencyDecompCollector) SetEnabled(b bool) {
	c.enabled.Store(b)
}

func (c *LatencyDecompCollector) Record(batchWait, batchFlush, quorumWait, commitNotify int64) {
	if !c.enabled.Load() {
		return
	}
	total := batchWait + batchFlush + quorumWait + commitNotify
	c.mu.Lock()
	c.batchWaitSamples = append(c.batchWaitSamples, batchWait)
	c.batchFlushSamples = append(c.batchFlushSamples, batchFlush)
	c.quorumWaitSamples = append(c.quorumWaitSamples, quorumWait)
	c.commitNotifySamples = append(c.commitNotifySamples, commitNotify)
	c.totalSamples = append(c.totalSamples, total)
	if len(c.totalSamples) > 100000 {
		c.batchWaitSamples = c.batchWaitSamples[len(c.batchWaitSamples)-50000:]
		c.batchFlushSamples = c.batchFlushSamples[len(c.batchFlushSamples)-50000:]
		c.quorumWaitSamples = c.quorumWaitSamples[len(c.quorumWaitSamples)-50000:]
		c.commitNotifySamples = c.commitNotifySamples[len(c.commitNotifySamples)-50000:]
		c.totalSamples = c.totalSamples[len(c.totalSamples)-50000:]
	}
	c.mu.Unlock()
}

func (c *LatencyDecompCollector) RecordSnapshotIO(wallMs int64) {
	c.snapshotIOCount.Add(1)
	c.snapshotIOWallMs.Add(wallMs)
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (c *LatencyDecompCollector) Snapshot() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	copyAndSort := func(s []int64) []int64 {
		cp := make([]int64, len(s))
		copy(cp, s)
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		return cp
	}

	bw := copyAndSort(c.batchWaitSamples)
	bf := copyAndSort(c.batchFlushSamples)
	qw := copyAndSort(c.quorumWaitSamples)
	cn := copyAndSort(c.commitNotifySamples)
	tot := copyAndSort(c.totalSamples)

	makeStats := func(sorted []int64) map[string]interface{} {
		if len(sorted) == 0 {
			return map[string]interface{}{"count": 0, "p50_us": 0, "p99_us": 0, "p999_us": 0, "max_us": 0}
		}
		var sum int64
		for _, v := range sorted {
			sum += v
		}
		return map[string]interface{}{
			"count":   len(sorted),
			"p50_us":  percentile(sorted, 0.50),
			"p99_us":  percentile(sorted, 0.99),
			"p999_us": percentile(sorted, 0.999),
			"max_us":  sorted[len(sorted)-1],
			"avg_us":  sum / int64(len(sorted)),
		}
	}

	result := map[string]interface{}{
		"enabled":       c.enabled.Load(),
		"sample_count":  len(tot),
		"batch_wait":    makeStats(bw),
		"batch_flush":   makeStats(bf),
		"quorum_wait":   makeStats(qw),
		"commit_notify": makeStats(cn),
		"total":         makeStats(tot),
		"snapshot_io": map[string]interface{}{
			"count":   c.snapshotIOCount.Load(),
			"wall_ms": c.snapshotIOWallMs.Load(),
		},
	}

	if len(tot) > 0 {
		p99Total := float64(percentile(tot, 0.99))
		if p99Total > 0 {
			result["p99_breakdown"] = map[string]interface{}{
				"batch_wait_pct":    fmt.Sprintf("%.1f%%", float64(percentile(bw, 0.99))/p99Total*100),
				"batch_flush_pct":   fmt.Sprintf("%.1f%%", float64(percentile(bf, 0.99))/p99Total*100),
				"quorum_wait_pct":   fmt.Sprintf("%.1f%%", float64(percentile(qw, 0.99))/p99Total*100),
				"commit_notify_pct": fmt.Sprintf("%.1f%%", float64(percentile(cn, 0.99))/p99Total*100),
			}
		}
		if p50Total := float64(percentile(tot, 0.50)); p50Total > 0 {
			result["p50_breakdown"] = map[string]interface{}{
				"batch_wait_pct":    fmt.Sprintf("%.1f%%", float64(percentile(bw, 0.50))/p50Total*100),
				"batch_flush_pct":   fmt.Sprintf("%.1f%%", float64(percentile(bf, 0.50))/p50Total*100),
				"quorum_wait_pct":   fmt.Sprintf("%.1f%%", float64(percentile(qw, 0.50))/p50Total*100),
				"commit_notify_pct": fmt.Sprintf("%.1f%%", float64(percentile(cn, 0.50))/p50Total*100),
			}
		}
	}

	return result
}
