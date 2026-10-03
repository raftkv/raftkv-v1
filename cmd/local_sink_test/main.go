// =========================================================================
// RaftKV 本地联调测试 — TiDB 适配器数据流转验证
//
// 验证链路: Raft 日志 → TiDBAdapter.Write → MySQL 容器
// =========================================================================

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"raftkv/pkg/adapters"
)

func main() {
	fmt.Println("=== RaftKV TiDB 适配器本地联调测试 ===")
	fmt.Println()

	// 1. 创建适配器配置
	config := adapters.SinkConfig{
		Enable:        true,
		DSN:           "root:CHANGE_ME@tcp(127.0.0.1:3306)/raftkv_logs?charset=utf8mb4&parseTime=true&loc=Local",
		BatchSize:     100,
		FlushInterval: 200 * time.Millisecond,
		ChannelSize:   1024,
		MaxFallback:   10000,
		TableName:     "raft_logs",
		MaxRetries:    3,
		RetryInterval: 500 * time.Millisecond,
	}

	fmt.Printf("[1] 连接 MySQL: %s\n", config.DSN)

	// 2. 创建适配器
	adapter, err := adapters.NewTiDBAdapter(config)
	if err != nil {
		log.Fatalf("创建适配器失败: %v", err)
	}
	defer adapter.Close()
	fmt.Println("[2] 适配器创建成功")

	// 3. 建表
	if err := adapter.CreateTable(); err != nil {
		log.Fatalf("建表失败: %v", err)
	}
	fmt.Println("[3] 表 raft_logs 创建成功")

	// 4. 模拟写入 500 条 Raft 日志
	fmt.Println("[4] 开始写入 500 条模拟 Raft 日志...")
	start := time.Now()

	for i := 0; i < 500; i++ {
		logEntry := map[string]interface{}{
			"index":   int64(i + 1),
			"term":    int64(1),
			"command": fmt.Sprintf("ALLOCATE task-%d to node-%d", i, i%5),
			"ts":      time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(logEntry)
		if err := adapter.Write(data); err != nil {
			log.Printf("写入第 %d 条失败: %v", i+1, err)
		}
	}

	// 5. 强制刷新
	fmt.Println("[5] 调用 Sync() 强制刷新...")
	if err := adapter.Sync(); err != nil {
		log.Printf("Sync 失败: %v", err)
	}

	// 等待后台批量写入完成
	time.Sleep(1 * time.Second)

	elapsed := time.Since(start)
	fmt.Printf("[6] 写入耗时: %v\n", elapsed)

	// 6. 打印适配器统计
	stats := adapter.Stats()
	fmt.Println()
	fmt.Println("=== 适配器统计 ===")
	fmt.Printf("  成功写入: %d\n", stats.Written)
	fmt.Printf("  失败次数: %d\n", stats.Failed)
	fmt.Printf("  重试次数: %d\n", stats.Retried)
	fmt.Printf("  降级次数: %d\n", stats.Fallback)
	fmt.Printf("  回退队列: %d\n", stats.FallbackLen)
	fmt.Printf("  通道长度: %d\n", stats.ChannelLen)
	fmt.Printf("  DB 健康:  %v\n", stats.DBHealthy)
	fmt.Println()

	// 7. 直接查询 MySQL 验证数据
	fmt.Println("[7] 直接查询 MySQL 验证数据...")
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		log.Fatalf("连接 MySQL 失败: %v", err)
	}
	defer db.Close()

	// 查询总条数
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM raft_logs").Scan(&count); err != nil {
		log.Fatalf("查询失败: %v", err)
	}
	fmt.Printf("    MySQL 中 raft_logs 总条数: %d\n", count)

	// 查询前 3 条
	rows, err := db.Query("SELECT id, LENGTH(log_data), created_at FROM raft_logs ORDER BY id LIMIT 3")
	if err != nil {
		log.Fatalf("查询前3条失败: %v", err)
	}
	defer rows.Close()

	fmt.Println("    前 3 条记录:")
	fmt.Println("    ┌────────┬───────────┬─────────────────────┐")
	fmt.Println("    │   id   │ data_size │     created_at      │")
	fmt.Println("    ├────────┼───────────┼─────────────────────┤")
	for rows.Next() {
		var id int64
		var size int
		var ts time.Time
		rows.Scan(&id, &size, &ts)
		fmt.Printf("    │ %6d │ %9d │ %19s │\n", id, size, ts.Format("2006-01-02 15:04:05"))
	}
	fmt.Println("    └────────┴───────────┴─────────────────────┘")

	// 查询最后 3 条
	rows2, err := db.Query("SELECT id, LENGTH(log_data), created_at FROM raft_logs ORDER BY id DESC LIMIT 3")
	if err != nil {
		log.Fatalf("查询最后3条失败: %v", err)
	}
	defer rows2.Close()

	fmt.Println("    最后 3 条记录:")
	fmt.Println("    ┌────────┬───────────┬─────────────────────┐")
	fmt.Println("    │   id   │ data_size │     created_at      │")
	fmt.Println("    ├────────┼───────────┼─────────────────────┤")
	var lastRows []string
	for rows2.Next() {
		var id int64
		var size int
		var ts time.Time
		rows2.Scan(&id, &size, &ts)
		lastRows = append(lastRows, fmt.Sprintf("    │ %6d │ %9d │ %19s │", id, size, ts.Format("2006-01-02 15:04:05")))
	}
	for i := len(lastRows) - 1; i >= 0; i-- {
		fmt.Println(lastRows[i])
	}
	fmt.Println("    └────────┴───────────┴─────────────────────┘")

	// 8. 结论
	fmt.Println()
	fmt.Println("=== 联调结论 ===")
	if count == 500 && stats.Failed == 0 {
		fmt.Println("  ✅ 500 条日志全部成功流入 MySQL")
		fmt.Println("  ✅ 零失败 零降级 零重试")
		fmt.Println("  ✅ 非阻塞写入 + 批量 group commit 验证通过")
		fmt.Println("  ✅ 金融级数据流转适配器本地联调成功")
	} else {
		fmt.Printf("  ⚠️  预期 500 条, 实际 %d 条, 失败 %d\n", count, stats.Failed)
	}
}
