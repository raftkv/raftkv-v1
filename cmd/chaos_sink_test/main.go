// =========================================================================
// RaftKV TiDB 适配器 — 背压与故障注入极限测试
//
// 测试链路:
//   Phase 1: DB 健康 → 写入 200 条 → Sync 验证
//   Phase 2: docker stop MySQL → 写入 300 条 → 验证 Write() 零阻塞 + fallback 积压
//   Phase 3: docker start MySQL → 等待就绪 → 验证 fallback 自动追平
//   Phase 4: 查询 MySQL → 验证总条数 = 500 (零丢失)
//
// 验证目标: 0 阻塞 · 0 数据丢失 · 0 服务崩溃
// =========================================================================

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"raftkv/pkg/adapters"
)

const (
	phase1Count = 200
	phase2Count = 300
	totalExpect = phase1Count + phase2Count
)

var dsn = os.Getenv("SINK_DSN")

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  RaftKV TiDB 适配器 — 背压与故障注入极限测试                  ║")
	fmt.Println("║  验证目标: 0 阻塞 · 0 丢失 · 0 崩溃                          ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// 创建适配器
	config := adapters.SinkConfig{
		Enable:        true,
		DSN:           dsn,
		BatchSize:     50,
		FlushInterval: 200 * time.Millisecond,
		ChannelSize:   256,
		MaxFallback:   100000,
		TableName:     "raft_logs",
		MaxRetries:    2,
		RetryInterval: 300 * time.Millisecond,
	}

	adapter, err := adapters.NewTiDBAdapter(config)
	if err != nil {
		log.Fatalf("创建适配器失败: %v", err)
	}
	defer adapter.Close()

	// 清空旧数据
	db, _ := sql.Open("mysql", dsn)
	defer db.Close()
	db.Exec("TRUNCATE TABLE raft_logs")
	fmt.Println("[预备] 已清空 raft_logs 表")
	fmt.Println()

	// ===================================================================
	// Phase 1: DB 健康 — 写入 200 条基线
	// ===================================================================
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Phase 1: DB 健康 — 写入 200 条基线日志")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	p1Start := time.Now()
	for i := 0; i < phase1Count; i++ {
		entry := map[string]interface{}{
			"phase": 1,
			"index": i + 1,
			"cmd":   fmt.Sprintf("ALLOCATE task-p1-%d", i),
			"ts":    time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(entry)
		adapter.Write(data)
	}
	adapter.Sync()
	time.Sleep(500 * time.Millisecond)
	p1Elapsed := time.Since(p1Start)

	s1 := adapter.Stats()
	var p1Count int
	db.QueryRow("SELECT COUNT(*) FROM raft_logs").Scan(&p1Count)
	fmt.Printf("  写入耗时: %v\n", p1Elapsed)
	fmt.Printf("  适配器: Written=%d Failed=%d Fallback=%d DBHealthy=%v\n", s1.Written, s1.Failed, s1.Fallback, s1.DBHealthy)
	fmt.Printf("  MySQL:  raft_logs=%d 条\n", p1Count)
	if p1Count == phase1Count {
		fmt.Println("  ✅ Phase 1 通过")
	} else {
		fmt.Printf("  ❌ Phase 1 失败: 预期 %d, 实际 %d\n", phase1Count, p1Count)
	}
	fmt.Println()

	// ===================================================================
	// Phase 2: docker stop MySQL — 写入 300 条验证非阻塞
	// ===================================================================
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Phase 2: docker stop MySQL — 故障注入 + 写入 300 条")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	fmt.Print("  执行 docker stop raftkv-mysql...")
	stopCmd := exec.Command("docker", "stop", "raftkv-mysql")
	if err := stopCmd.Run(); err != nil {
		fmt.Printf(" 失败: %v\n", err)
	} else {
		fmt.Println(" 成功")
	}
	time.Sleep(2 * time.Second)

	fmt.Printf("  开始写入 %d 条 (DB 宕机中)...\n", phase2Count)
	p2Start := time.Now()
	for i := 0; i < phase2Count; i++ {
		entry := map[string]interface{}{
			"phase": 2,
			"index": i + 1,
			"cmd":   fmt.Sprintf("ALLOCATE task-p2-%d", i),
			"ts":    time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(entry)
		err := adapter.Write(data)
		if err != nil {
			fmt.Printf("  ❌ Write() 阻塞或失败 at i=%d: %v\n", i, err)
			break
		}
	}
	p2Elapsed := time.Since(p2Start)

	s2 := adapter.Stats()
	fmt.Printf("  写入耗时: %v (300条非阻塞写入)\n", p2Elapsed)
	fmt.Printf("  适配器: Written=%d Failed=%d Retried=%d Fallback=%d\n", s2.Written, s2.Failed, s2.Retried, s2.Fallback)
	fmt.Printf("          FallbackLen=%d ChannelLen=%d DBHealthy=%v\n", s2.FallbackLen, s2.ChannelLen, s2.DBHealthy)

	if p2Elapsed < 1*time.Second {
		fmt.Println("  ✅ Write() 零阻塞: 300条写入 < 1s")
	} else {
		fmt.Printf("  ⚠️  写入耗时 %v, 检查是否阻塞\n", p2Elapsed)
	}
	if s2.FallbackLen > 0 || s2.Fallback > 0 {
		fmt.Printf("  ✅ Fallback 队列生效: %d 条数据缓存中\n", s2.FallbackLen)
	} else {
		fmt.Println("  ⚠️  Fallback 队列为空, 数据可能在通道中等待")
	}
	fmt.Println()

	// ===================================================================
	// Phase 3: docker start MySQL — 验证自动追平
	// ===================================================================
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Phase 3: docker start MySQL — 故障恢复 + 自动追平")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	fmt.Print("  执行 docker start raftkv-mysql...")
	startCmd := exec.Command("docker", "start", "raftkv-mysql")
	if err := startCmd.Run(); err != nil {
		fmt.Printf(" 失败: %v\n", err)
	} else {
		fmt.Println(" 成功")
	}

	fmt.Print("  等待 MySQL 就绪...")
	mysqlReady := false
	for i := 0; i < 30; i++ {
		if db.Ping() == nil {
			mysqlReady = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if mysqlReady {
		fmt.Println(" 就绪")
	} else {
		fmt.Println(" 超时")
	}

	fmt.Println("  等待适配器自动追平 fallback 队列...")
	drainStart := time.Now()
	drained := false
	for i := 0; i < 60; i++ {
		s := adapter.Stats()
		if s.FallbackLen == 0 && s.ChannelLen == 0 {
			drained = true
			break
		}
		if i%5 == 0 {
			fmt.Printf("    追平中... FallbackLen=%d ChannelLen=%d DBHealthy=%v\n", s.FallbackLen, s.ChannelLen, s.DBHealthy)
		}
		time.Sleep(500 * time.Millisecond)
	}
	drainElapsed := time.Since(drainStart)

	s3 := adapter.Stats()
	fmt.Printf("  追平耗时: %v\n", drainElapsed)
	fmt.Printf("  适配器: Written=%d Failed=%d Retried=%d Fallback=%d\n", s3.Written, s3.Failed, s3.Retried, s3.Fallback)
	fmt.Printf("          FallbackLen=%d ChannelLen=%d DBHealthy=%v\n", s3.FallbackLen, s3.ChannelLen, s3.DBHealthy)

	if drained {
		fmt.Println("  ✅ Fallback 队列已排空")
	} else {
		fmt.Printf("  ⚠️  Fallback 仍有 %d 条未排空\n", s3.FallbackLen)
	}
	fmt.Println()

	// ===================================================================
	// Phase 4: 最终验证 — 查询 MySQL 数据完整性
	// ===================================================================
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("Phase 4: 最终验证 — MySQL 数据完整性检查")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	adapter.Sync()
	time.Sleep(2 * time.Second)

	s4 := adapter.Stats()
	var finalCount int
	db.QueryRow("SELECT COUNT(*) FROM raft_logs").Scan(&finalCount)

	var p1Count2, p2Count2 int
	db.QueryRow("SELECT COUNT(*) FROM raft_logs WHERE JSON_EXTRACT(log_data, '$.phase') = 1").Scan(&p1Count2)
	db.QueryRow("SELECT COUNT(*) FROM raft_logs WHERE JSON_EXTRACT(log_data, '$.phase') = 2").Scan(&p2Count2)

	fmt.Printf("  MySQL 总条数:     %d (预期 %d)\n", finalCount, totalExpect)
	fmt.Printf("  Phase 1 数据:     %d 条 (预期 %d)\n", p1Count2, phase1Count)
	fmt.Printf("  Phase 2 数据:     %d 条 (预期 %d)\n", p2Count2, phase2Count)
	fmt.Printf("  适配器最终统计:   Written=%d Failed=%d Retried=%d\n", s4.Written, s4.Failed, s4.Retried)
	fmt.Printf("  Fallback 残余:    %d\n", s4.FallbackLen)
	fmt.Printf("  DB 健康状态:      %v\n", s4.DBHealthy)
	fmt.Println()

	// ===================================================================
	// 结论
	// ===================================================================
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        极限测试结论                           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	allPass := true
	if p2Elapsed < 1*time.Second {
		fmt.Println("  ✅ 0 阻塞: DB 宕机时 Write() 300条 < 1s")
	} else {
		fmt.Printf("  ❌ 阻塞: Write() 耗时 %v\n", p2Elapsed)
		allPass = false
	}

	if finalCount == totalExpect {
		fmt.Printf("  ✅ 0 丢失: MySQL 总数 %d = 预期 %d\n", finalCount, totalExpect)
	} else {
		fmt.Printf("  ❌ 丢失: MySQL 总数 %d ≠ 预期 %d (差 %d)\n", finalCount, totalExpect, totalExpect-finalCount)
		allPass = false
	}

	if p1Count2 == phase1Count && p2Count2 == phase2Count {
		fmt.Println("  ✅ 数据完整: Phase1 + Phase2 均无缺失")
	} else {
		fmt.Printf("  ❌ 数据不完整: P1=%d/%d P2=%d/%d\n", p1Count2, phase1Count, p2Count2, phase2Count)
		allPass = false
	}

	if s4.FallbackLen == 0 {
		fmt.Println("  ✅ 0 积压: Fallback 队列已排空")
	} else {
		fmt.Printf("  ❌ 积压: Fallback 仍有 %d 条\n", s4.FallbackLen)
		allPass = false
	}

	if s4.DBHealthy {
		fmt.Println("  ✅ 0 崩溃: DB 恢复后适配器健康")
	} else {
		fmt.Println("  ❌ 适配器未恢复健康")
		allPass = false
	}

	fmt.Println()
	if allPass {
		fmt.Println("  ═══════════════════════════════════════════════════")
		fmt.Println("  ║  工业级韧性验证通过: 0 阻塞 0 丢失 0 崩溃  ║")
		fmt.Println("  ═══════════════════════════════════════════════════")
	} else {
		fmt.Println("  ⚠️  部分指标未达标, 请检查上方详细输出")
	}
}
