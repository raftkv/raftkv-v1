package main

import (
	"net/http"
	"testing"
	"time"
)

func TestBatch15_MetricsCollector(t *testing.T) {
	node := &RaftNode{
		logs:          make([]RaftLog, 0),
		logStartIndex: 1,
		state:         StateLeader,
	}
	node.heartbeatIntervalAtomic.Store(int64(20 * time.Millisecond))
	node.electionEventCount.Store(3)

	mc := NewMetricsCollector(node, nil, nil)
	mc.RecordPropose()
	mc.RecordPropose()
	mc.RecordPropose()

	time.Sleep(10 * time.Millisecond)
	output := mc.RenderPrometheus()

	if output == "" {
		t.Fatal("RenderPrometheus 返回空字符串")
	}

	checks := []string{
		"raft_tps",
		"raft_request_duration_seconds",
		"raft_heartbeat_interval_seconds",
		"raft_election_events_total",
		"raft_compaction_count_total",
		"raft_log_length",
		"raft_log_start_index",
		"raft_snapshot_progress",
		"raft_rate_limit_rejected_total",
	}
	for _, m := range checks {
		if !stringContains(output, m) {
			t.Errorf("指标 %s 未在输出中找到", m)
		}
	}
	t.Log("metrics 埋点 8 项指标全部输出")
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestBatch15_TokenBucketLimiter(t *testing.T) {
	limiter := NewTokenBucketLimiter(10, 100)
	limiter.Enable()

	allowed := 0
	rejected := 0
	for i := 0; i < 20; i++ {
		if limiter.Allow() {
			allowed++
		} else {
			rejected++
		}
	}
	if allowed == 0 {
		t.Error("令牌桶应允许部分请求通过")
	}
	if rejected == 0 {
		t.Log("令牌桶未拒绝请求（令牌充足）")
	} else {
		t.Logf("令牌桶状态: allowed=%d, rejected=%d", allowed, rejected)
	}

	limiter.Disable()
	for i := 0; i < 10; i++ {
		if !limiter.Allow() {
			t.Error("禁用后应允许所有请求")
		}
	}

	rej, total, rate, enabled := limiter.Stats()
	t.Logf("限流器统计: rejected=%d, total=%d, rate=%d, enabled=%v", rej, total, rate, enabled)
}

func TestBatch15_AuthMiddleware(t *testing.T) {
	m := NewAuthMiddleware()
	if m.enabled {
		t.Error("未设置 AUTH_BEARER_TOKEN 时应禁用")
	}

	called := false
	handler := m.Middleware(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	if !called {
		handler(nil, nil)
	}
	if !called {
		t.Error("禁用状态下应直接调用 next handler")
	}
	t.Log("鉴权中间件禁用状态正确")
}
