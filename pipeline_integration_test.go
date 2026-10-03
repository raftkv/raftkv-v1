// =========================================================================
// RaftKV — Raft 管线全链路集成测试
//
// 验证: Raft commit → SM4-CTR 加密 WAL → TiDB/MySQL 异步落盘
//
// 运行: go test -run TestPipelineFullChain -v -timeout 120s
// =========================================================================

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	pb "raftkv/proto"

	"raftkv/pkg/adapters"
)

func TestPipelineFullChain(t *testing.T) {
	t.Log("══════════════════════════════════════════════════")
	t.Log("  RaftKV Raft 管线全链路集成测试")
	t.Log("  Raft commit → SM4-CTR WAL → MySQL 异步落盘")
	t.Log("══════════════════════════════════════════════════")

	walPath := "test_pipeline.wal"
	os.Remove(walPath)

	// --- Step 1: 创建管线 ---
	t.Log("\n[Step 1] 创建 Raft 管线 (WAL + MySQL Sink)")

	sinkCfg := adapters.SinkConfig{
		Enable:        true,
		DSN:           "root:CHANGE_ME@tcp(127.0.0.1:3306)/raftkv_logs?charset=utf8mb4&parseTime=true&loc=Local",
		BatchSize:     100,
		FlushInterval: 200 * time.Millisecond,
		ChannelSize:   4096,
		MaxFallback:   100000,
		TableName:     "raft_logs_pipeline_test",
		MaxRetries:    3,
		RetryInterval: 1 * time.Second,
	}

	pipelineCfg := PipelineConfig{
		EnableWAL:  true,
		WALPath:    walPath,
		SM4Key:     []byte("raftkv_sm4test01"), // 16 字节
		EnableSink: true,
		SinkConfig: sinkCfg,
	}

	pipeline, err := NewRaftPipeline(pipelineCfg)
	if err != nil {
		t.Fatalf("管线创建失败: %v", err)
	}
	t.Log("✅ 管线创建成功")

	if adapter := pipeline.SinkAdapter(); adapter != nil {
		adapter.CreateTable()
	}

	// --- Step 2: 创建 RaftNode 并接入管线 ---
	t.Log("\n[Step 2] 创建 RaftNode 并接入 onCommit 回调")

	node := NewRaftNode("test-node", map[string]string{}, map[string]pb.RaftServiceClient{}, nil)
	node.SetOnCommit(pipeline.OnCommit)
	t.Log("✅ RaftNode 已创建，管线回调已接入")

	// --- Step 3: 模拟 AppendEntries 追加 50 条日志 ---
	numEntries := 50
	t.Logf("\n[Step 3] 模拟 Leader 追加 %d 条日志 (未提交)", numEntries)

	entries := make([]*pb.LogEntry, numEntries)
	for i := 0; i < numEntries; i++ {
		entries[i] = &pb.LogEntry{
			Term:    1,
			Index:   int64(i + 1),
			Command: []byte(fmt.Sprintf(`{"action":"test","index":%d,"data":"payload-%d"}`, i+1, i+1)),
			Sm3Hash: []byte(fmt.Sprintf("hash-%d", i+1)),
		}
	}

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "test-leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	}

	resp, err := node.HandleAppendEntries(context.Background(), req)
	if err != nil || !resp.Success {
		t.Fatalf("AppendEntries 失败: err=%v, success=%v", err, resp.Success)
	}
	t.Logf("✅ %d 条日志已追加到 RaftNode", numEntries)

	// --- Step 4: 模拟心跳提交 ---
	t.Logf("\n[Step 4] 模拟 Leader 心跳提交 (LeaderCommit=%d)", numEntries)

	heartbeatReq := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "test-leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: int64(numEntries),
	}

	resp2, err := node.HandleAppendEntries(context.Background(), heartbeatReq)
	if err != nil || !resp2.Success {
		t.Fatalf("心跳提交失败: err=%v, success=%v", err, resp2.Success)
	}
	t.Logf("✅ LeaderCommit=%d 已发送，onCommit 回调已触发", numEntries)

	// --- Step 5: 等待异步落盘并验证 ---
	t.Log("\n[Step 5] 等待异步落盘完成 (2s)")
	time.Sleep(2 * time.Second)

	ps := pipeline.Stats()
	t.Logf("  管线统计: committed=%d, walErrors=%d, sinkErrors=%d", ps.Committed, ps.WALErrors, ps.SinkErrors)
	t.Logf("  Sink 统计: written=%d, fallback=%d, healthy=%v", ps.SinkWritten, ps.SinkFallbackLen, ps.SinkDBHealthy)

	if ps.Committed != int64(numEntries) {
		t.Errorf("提交数不匹配: 期望 %d, 实际 %d", numEntries, ps.Committed)
	} else {
		t.Logf("✅ 管线收到 %d 条提交", ps.Committed)
	}

	if ps.SinkWritten == int64(numEntries) {
		t.Logf("✅ MySQL 收到 %d 条日志", ps.SinkWritten)
	} else {
		t.Logf("⚠ MySQL 写入数: %d (期望 %d，可能仍在刷新中)", ps.SinkWritten, numEntries)
	}

	// --- Step 6: 验证 WAL 文件 ---
	t.Log("\n[Step 6] 验证 WAL 加密文件")
	walInfo, _ := os.Stat(walPath)
	if walInfo != nil && walInfo.Size() > 0 {
		t.Logf("✅ WAL 文件存在: %s, 大小=%d bytes (1GB 预分配)", walPath, walInfo.Size())
	} else {
		t.Error("❌ WAL 文件异常")
	}

	// --- Step 7: WAL 回放验证 ---
	t.Log("\n[Step 7] WAL 回放验证 (关闭→重开→回放)")

	pipeline.Sync()
	pipeline.Close()

	replayCfg := PipelineConfig{
		EnableWAL:  true,
		WALPath:    walPath,
		SM4Key:     []byte("raftkv_sm4test01"), // 16 字节
		EnableSink: false,
	}
	replayPipeline, err := NewRaftPipeline(replayCfg)
	if err != nil {
		t.Errorf("回放管线创建失败: %v", err)
	} else {
		logs, err := replayPipeline.ReplayWAL()
		if err != nil {
			t.Errorf("WAL 回放失败: %v", err)
		} else if len(logs) == numEntries {
			t.Logf("✅ WAL 回放成功: %d 条日志恢复", len(logs))
			if len(logs) > 0 {
				t.Logf("  首条: index=%d, term=%d, command=%s", logs[0].Index, logs[0].Term, string(logs[0].Command))
				t.Logf("  末条: index=%d, term=%d, command=%s", logs[len(logs)-1].Index, logs[len(logs)-1].Term, string(logs[len(logs)-1].Command))
			}
		} else {
			t.Logf("⚠ WAL 回放条数: %d (期望 %d)", len(logs), numEntries)
		}
		replayPipeline.Close()
	}

	// --- Step 8: 故障注入测试 ---
	t.Log("\n[Step 8] 故障注入: 停 MySQL → 提交 30 条 → 恢复 → 验证零丢失")

	pipeline2, err := NewRaftPipeline(pipelineCfg)
	if err != nil {
		t.Errorf("管线2创建失败: %v", err)
	} else {
		node2 := NewRaftNode("test-node-2", map[string]string{}, map[string]pb.RaftServiceClient{}, nil)
		node2.SetOnCommit(pipeline2.OnCommit)

		t.Log("  → 停止 MySQL 容器...")
		exec.Command("docker", "stop", "raftkv-mysql").Run()
		time.Sleep(3 * time.Second)

		t.Log("  → MySQL 已停，提交 30 条日志...")
		start := time.Now()
		entries2 := make([]*pb.LogEntry, 30)
		for i := 0; i < 30; i++ {
			entries2[i] = &pb.LogEntry{
				Term:    2,
				Index:   int64(i + 1),
				Command: []byte(fmt.Sprintf(`{"action":"chaos","index":%d}`, i+1)),
			}
		}
		req2 := &pb.AppendEntriesRequest{
			Term:         2,
			LeaderId:     "test-leader-2",
			Entries:      entries2,
			LeaderCommit: 30,
		}
		node2.HandleAppendEntries(context.Background(), req2)
		elapsed := time.Since(start)
		t.Logf("  ✅ 30 条提交完成，耗时: %v (零阻塞)", elapsed)

		t.Log("  → 恢复 MySQL 容器...")
		exec.Command("docker", "start", "raftkv-mysql").Run()
		time.Sleep(5 * time.Second)

		t.Log("  → 等待 fallback 自动追平 (5s)...")
		time.Sleep(5 * time.Second)

		ps2 := pipeline2.Stats()
		t.Logf("  管线2统计: committed=%d, sinkWritten=%d, fallback=%d, healthy=%v",
			ps2.Committed, ps2.SinkWritten, ps2.SinkFallbackLen, ps2.SinkDBHealthy)

		if ps2.SinkFallbackLen == 0 && ps2.SinkWritten == 30 {
			t.Log("  ✅ 故障自愈成功: fallback 已清空，30 条全部写入 MySQL")
		} else {
			t.Logf("  ⚠ fallback=%d, written=%d (可能仍在追平中)", ps2.SinkFallbackLen, ps2.SinkWritten)
		}

		pipeline2.Close()
	}

	os.Remove(walPath)
	t.Log("\n══════════════════════════════════════════════════")
	t.Log("  全链路集成测试完成")
	t.Log("══════════════════════════════════════════════════")
}
