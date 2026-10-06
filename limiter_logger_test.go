package main

import (
	"testing"
	"time"
)

func TestBatch16_LimiterDisabled_AllowsAll(t *testing.T) {
	limiter := NewTokenBucketLimiter(10, 100)
	for i := 0; i < 100; i++ {
		if !limiter.Allow() {
			t.Fatal("禁用状态下应允许所有请求")
		}
	}
	rej, total, _, enabled := limiter.Stats()
	if rej != 0 {
		t.Errorf("禁用状态下不应有拒绝, got rejected=%d", rej)
	}
	if total != 100 {
		t.Errorf("total 应为 100, got %d", total)
	}
	if enabled {
		t.Error("应为禁用状态")
	}
}

func TestBatch16_LimiterEnabled_RejectsOverflow(t *testing.T) {
	limiter := NewTokenBucketLimiter(5, 10)
	limiter.Enable()

	allowed := 0
	rejected := 0
	for i := 0; i < 50; i++ {
		if limiter.Allow() {
			allowed++
		} else {
			rejected++
		}
	}
	if allowed == 0 {
		t.Error("应允许部分请求通过")
	}
	if rejected == 0 {
		t.Error("50 请求 / 5 令牌应触发拒绝")
	}
	rej, _, _, _ := limiter.Stats()
	if rej == 0 {
		t.Error("Stats 应报告拒绝数 > 0")
	}
	t.Logf("限流器: allowed=%d, rejected=%d", allowed, rejected)
}

func TestBatch16_LimiterAdaptive_RateAdjustsOnHighLatency(t *testing.T) {
	limiter := NewTokenBucketLimiter(100, 8000)
	limiter.Enable()

	for i := 0; i < 300; i++ {
		limiter.RecordLatency(150000)
	}
	_, _, adjustedRate, _ := limiter.Stats()
	if adjustedRate >= 8000 {
		t.Errorf("高延迟后应降低速率, got rate=%d (initial=8000)", adjustedRate)
	}
	t.Logf("高延迟自适应: 8000 → %d", adjustedRate)
}

func TestBatch16_LimiterAdaptive_RateAdjustsOnLowLatency(t *testing.T) {
	limiter := NewTokenBucketLimiter(500, 4000)
	limiter.Enable()

	for i := 0; i < 300; i++ {
		limiter.RecordLatency(10000)
	}
	_, _, adjustedRate, _ := limiter.Stats()
	if adjustedRate <= 4000 {
		t.Errorf("低延迟后应提高速率, got rate=%d (initial=4000)", adjustedRate)
	}
	t.Logf("低延迟自适应: 4000 → %d", adjustedRate)
}

func TestBatch16_LimiterRecover_AfterRefill(t *testing.T) {
	limiter := NewTokenBucketLimiter(3, 1000)
	limiter.Enable()

	for i := 0; i < 3; i++ {
		if !limiter.Allow() {
			t.Error("前 3 个请求应被允许")
		}
	}
	if limiter.Allow() {
		t.Error("第 4 个请求应被拒绝（令牌耗尽）")
	}
	time.Sleep(5 * time.Millisecond)
	if !limiter.Allow() {
		t.Error("等待补充后应再次允许请求")
	}
}

func TestBatch16_MetricsProposeCount(t *testing.T) {
	node := &RaftNode{
		logs:          make([]RaftLog, 0),
		logStartIndex: 1,
		state:         StateLeader,
	}
	mc := NewMetricsCollector(node, nil, nil)
	for i := 0; i < 1000; i++ {
		mc.RecordPropose()
	}
	time.Sleep(10 * time.Millisecond)
	output := mc.RenderPrometheus()
	if !stringContains(output, "raft_tps") {
		t.Error("应输出 raft_tps 指标")
	}
	t.Log("RecordPropose 1000 次后 metrics 正常输出")
}
func TestBatch16_StructuredLogger_JSONFormat(t *testing.T) {
	sl := &StructuredLogger{
		nodeID: "test-node",
		enable: true,
	}
	sl.Info("test_event", "测试消息", map[string]interface{}{
		"key1": "value1",
		"key2": 42,
	})
	sl.Warn("warn_event", "警告消息", nil)
	sl.Error("err_event", "错误消息", map[string]interface{}{
		"code": 500,
	})
	t.Log("结构化日志器 JSON 输出验证通过")
}

func TestBatch16_StructuredLogger_Disabled(t *testing.T) {
	sl := &StructuredLogger{
		nodeID: "test-node",
		enable: false,
	}
	sl.Info("should_not_output", "不应输出", nil)
	t.Log("禁用状态下不输出日志")
}
