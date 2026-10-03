// =========================================================================
// RaftKV Module01 — Raft 共识引擎独立沙箱验证测试程序
//
// 验证目标：
//   1. 3 节点 Raft 集群成功启动
//   2. Leader 选举成功（有且仅有 1 个 Leader）
//   3. 通过 Leader 写入 10 条日志
//   4. 10 条日志在所有节点上正确同步提交
//   5. 日志内容在所有节点上一致
//
// 运行方式：
//   go run cmd/raft-test/main.go
//
// 输出规范：
//   ANSI 颜色（cyan 标题 / green 通过 / yellow 警告 / red 失败）
//   emoji 图标（✅🚀🔑🔒📋）
//   === 分节分隔符
// =========================================================================

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	raft "raftkv/raft-module"
)

// =========================================================================
// ANSI 颜色常量
// =========================================================================

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

// =========================================================================
// 测试配置
// =========================================================================

const (
	nodeCount    = 3               // 节点数
	basePort     = 21001           // 起始端口
	logCount     = 10              // 写入日志条数
	electionWait = 5 * time.Second // 选举等待超时
	proposeWait  = 3 * time.Second // 单条提案等待超时
	syncWait     = 5 * time.Second // 日志同步等待超时
)

// =========================================================================
// 节点包装器
// =========================================================================

type testNode struct {
	id        string
	addr      string
	server    *raft.HTTPServer
	node      *raft.RaftNode
	logger    *log.Logger
	walPath   string
	commitCnt int64 // 已提交日志计数（通过 onCommit 回调）
}

// =========================================================================
// 主函数
// =========================================================================

func main() {
	printHeader()

	// 捕获中断信号，确保优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n" + colorYellow + "⚠ 收到中断信号，正在退出..." + colorReset)
		os.Exit(1)
	}()

	// --- 阶段 1: 集群启动 ---
	fmt.Println(colorCyan + "=== 阶段 1/4: 集群启动 ===" + colorReset)
	nodes, err := startCluster()
	if err != nil {
		fmt.Printf(colorRed+"❌ 集群启动失败: %v\n"+colorReset, err)
		os.Exit(1)
	}
	defer shutdownCluster(nodes)
	fmt.Printf(colorGreen+"✅ %d 节点集群已启动，HTTP RPC 服务端就绪\n"+colorReset, nodeCount)

	// --- 阶段 2: Leader 选举 ---
	fmt.Println(colorCyan + "=== 阶段 2/4: Leader 选举 ===" + colorReset)
	leaderID, err := waitForLeader(nodes)
	if err != nil {
		fmt.Printf(colorRed+"❌ Leader 选举失败: %v\n"+colorReset, err)
		os.Exit(1)
	}
	fmt.Printf(colorGreen+"✅ Leader 选举成功！Leader = %s\n"+colorReset, leaderID)
	printClusterStatus(nodes)

	// --- 阶段 3: 日志写入与同步 ---
	fmt.Println(colorCyan + "=== 阶段 3/4: 日志写入与同步 ===" + colorReset)
	if err := writeAndSyncLogs(nodes, leaderID); err != nil {
		fmt.Printf(colorRed+"❌ 日志写入同步失败: %v\n"+colorReset, err)
		os.Exit(1)
	}

	// --- 阶段 4: 最终验证 ---
	fmt.Println(colorCyan + "=== 阶段 4/4: 最终验证 ===" + colorReset)
	passed := finalVerify(nodes, leaderID)

	printFooter(passed)
	if !passed {
		os.Exit(1)
	}
}

// =========================================================================
// 集群启动
// =========================================================================

func startCluster() ([]*testNode, error) {
	// 准备节点配置
	ids := make([]string, nodeCount)
	addrs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		ids[i] = fmt.Sprintf("node%d", i+1)
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	// WAL 临时目录
	walDir := filepath.Join(os.TempDir(), "raftkv_test_wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("创建 WAL 目录失败: %w", err)
	}

	nodes := make([]*testNode, nodeCount)

	// 第一阶段：创建所有 RaftNode + HTTPServer
	for i := 0; i < nodeCount; i++ {
		// 构造 peerAddrs（不含自身）
		peerAddrs := make(map[string]string)
		for j := 0; j < nodeCount; j++ {
			if j != i {
				peerAddrs[ids[j]] = addrs[j]
			}
		}

		// 创建 logger（输出到 stderr，带节点前缀）
		logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", ids[i]), log.LstdFlags)

		// 先创建节点（transports 延后注入）
		node := raft.NewRaftNode(ids[i], peerAddrs, nil, logger)

		// 启动 HTTP 服务端
		server, err := raft.NewHTTPServer(addrs[i], node)
		if err != nil {
			return nil, fmt.Errorf("节点 %s HTTP 服务端启动失败: %w", ids[i], err)
		}
		server.Start()

		walPath := filepath.Join(walDir, fmt.Sprintf("%s.wal", ids[i]))

		tn := &testNode{
			id:      ids[i],
			addr:    addrs[i],
			server:  server,
			node:    node,
			logger:  logger,
			walPath: walPath,
		}
		nodes[i] = tn

		// 设置 onCommit 回调（统计提交数）
		node.SetOnCommit(func(l raft.RaftLog) {
			atomic.AddInt64(&tn.commitCnt, 1)
		})
	}

	// 第二阶段：注入 transports（所有服务端已就绪）
	for i := 0; i < nodeCount; i++ {
		transports := make(map[string]raft.Transport)
		for j := 0; j < nodeCount; j++ {
			if j != i {
				transports[ids[j]] = raft.NewHTTPTransport(addrs[j])
			}
		}
		// 通过反射或公开方法注入 transports
		// 这里用 SetTransports 方法（需在 raft 包暴露）
		nodes[i].node.SetTransports(transports)
	}

	// 第三阶段：启动所有节点的状态机主循环
	for i := 0; i < nodeCount; i++ {
		go nodes[i].node.Run()
	}

	// 等待所有节点就绪
	time.Sleep(100 * time.Millisecond)

	return nodes, nil
}

// =========================================================================
// 等待 Leader 选举
// =========================================================================

func waitForLeader(nodes []*testNode) (string, error) {
	deadline := time.Now().Add(electionWait)
	var leaderID string

	for time.Now().Before(deadline) {
		leaderCount := 0
		leaderID = ""
		for _, tn := range nodes {
			if tn.node.IsLeader() {
				leaderCount++
				leaderID = tn.id
			}
		}
		if leaderCount == 1 {
			return leaderID, nil
		}
		if leaderCount > 1 {
			return "", fmt.Errorf("脑裂：检测到 %d 个 Leader", leaderCount)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", fmt.Errorf("选举超时 (%v)，未选出 Leader", electionWait)
}

// =========================================================================
// 写入并同步日志
// =========================================================================

func writeAndSyncLogs(nodes []*testNode, leaderID string) error {
	// 找到 Leader 节点
	var leader *testNode
	for _, tn := range nodes {
		if tn.id == leaderID {
			leader = tn
			break
		}
	}
	if leader == nil {
		return fmt.Errorf("未找到 Leader 节点 %s", leaderID)
	}

	// 写入 10 条日志
	fmt.Printf(colorBold+"🔑 通过 Leader (%s) 写入 %d 条日志...\n"+colorReset, leaderID, logCount)
	for i := 1; i <= logCount; i++ {
		cmd := fmt.Sprintf("test-log-entry-%02d", i)
		idx, err := leader.node.ProposeSync([]byte(cmd), proposeWait)
		if err != nil {
			return fmt.Errorf("第 %d 条日志提案失败: %w", i, err)
		}
		fmt.Printf("  📝 日志 #%02d 已提交 (index=%d, cmd=%s)\n", i, idx, cmd)
	}

	fmt.Printf(colorGreen+"✅ %d 条日志已全部通过 Leader 提交\n"+colorReset, logCount)

	// 等待所有节点同步
	fmt.Println(colorBold + "📋 等待所有节点日志同步..." + colorReset)
	deadline := time.Now().Add(syncWait)
	for time.Now().Before(deadline) {
		allSynced := true
		for _, tn := range nodes {
			if tn.node.CommitIndex() < int64(logCount) {
				allSynced = false
				break
			}
		}
		if allSynced {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 检查同步结果
	for _, tn := range nodes {
		ci := tn.node.CommitIndex()
		lc := tn.node.LogCount()
		if ci >= int64(logCount) {
			fmt.Printf("  ✅ 节点 %s: commitIdx=%d, logCount=%d\n", tn.id, ci, lc)
		} else {
			fmt.Printf(colorYellow+"  ⚠ 节点 %s: commitIdx=%d (未达 %d)\n"+colorReset, tn.id, ci, logCount)
		}
	}

	return nil
}

// =========================================================================
// 最终验证
// =========================================================================

func finalVerify(nodes []*testNode, leaderID string) bool {
	allPass := true

	// 验证 1: 所有节点 commitIdx >= logCount
	fmt.Println(colorBold + "🔒 验证 1: 所有节点提交索引 ≥ " + fmt.Sprint(logCount) + colorReset)
	for _, tn := range nodes {
		ci := tn.node.CommitIndex()
		if ci >= int64(logCount) {
			fmt.Printf("  ✅ 节点 %s commitIdx=%d\n", tn.id, ci)
		} else {
			fmt.Printf(colorRed+"  ❌ 节点 %s commitIdx=%d (不足)\n"+colorReset, tn.id, ci)
			allPass = false
		}
	}

	// 验证 2: 日志内容跨节点一致性
	fmt.Println(colorBold + "🔒 验证 2: 日志内容跨节点一致性" + colorReset)
	refLogs := nodes[0].node.GetCommittedLogs()
	for i := 1; i < len(nodes); i++ {
		otherLogs := nodes[i].node.GetCommittedLogs()
		if !logsEqual(refLogs, otherLogs) {
			fmt.Printf(colorRed+"  ❌ 节点 %s 日志与参考节点 %s 不一致\n"+colorReset,
				nodes[i].id, nodes[0].id)
			allPass = false
		} else {
			fmt.Printf("  ✅ 节点 %s 日志与节点 %s 一致 (%d 条)\n",
				nodes[i].id, nodes[0].id, len(otherLogs))
		}
	}

	// 验证 3: 日志条目内容正确性
	fmt.Println(colorBold + "🔒 验证 3: 日志条目内容正确性" + colorReset)
	if len(refLogs) >= logCount {
		contentOk := true
		for i := 0; i < logCount; i++ {
			expected := fmt.Sprintf("test-log-entry-%02d", i+1)
			actual := string(refLogs[i].Command)
			if actual != expected {
				fmt.Printf(colorRed+"  ❌ 日志 #%d 内容不符: 期望 %q, 实际 %q\n"+colorReset,
					i+1, expected, actual)
				contentOk = false
				allPass = false
			}
		}
		if contentOk {
			fmt.Printf("  ✅ %d 条日志内容全部正确\n", logCount)
		}
	} else {
		fmt.Printf(colorRed+"  ❌ 已提交日志数 %d < %d\n"+colorReset, len(refLogs), logCount)
		allPass = false
	}

	// 验证 4: onCommit 回调计数
	fmt.Println(colorBold + "🔒 验证 4: onCommit 回调触发计数" + colorReset)
	for _, tn := range nodes {
		cnt := atomic.LoadInt64(&tn.commitCnt)
		if cnt >= int64(logCount) {
			fmt.Printf("  ✅ 节点 %s onCommit 回调 %d 次\n", tn.id, cnt)
		} else {
			fmt.Printf(colorYellow+"  ⚠ 节点 %s onCommit 回调 %d 次 (期望 ≥ %d)\n"+colorReset,
				tn.id, cnt, logCount)
		}
	}

	return allPass
}

// logsEqual 比较两份日志是否一致
func logsEqual(a, b []raft.RaftLog) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index {
			return false
		}
		if a[i].Term != b[i].Term {
			return false
		}
		if string(a[i].Command) != string(b[i].Command) {
			return false
		}
	}
	return true
}

// =========================================================================
// 集群关闭
// =========================================================================

func shutdownCluster(nodes []*testNode) {
	var wg sync.WaitGroup
	for _, tn := range nodes {
		wg.Add(1)
		go func(t *testNode) {
			defer wg.Done()
			t.node.Shutdown()
			t.server.Stop()
		}(tn)
	}
	wg.Wait()
}

// =========================================================================
// 输出辅助
// =========================================================================

func printHeader() {
	fmt.Println(colorCyan + colorBold)
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║   RaftKV Module01 — Raft 强一致性共识引擎 独立沙箱验证      ║")
	fmt.Println("║   纯标准库零外部依赖 | 3 节点集群 | 10 条日志同步             ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println(colorReset)
	fmt.Printf(colorCyan+"🚀 测试配置: 节点数=%d, 端口基址=%d, 日志条数=%d\n"+colorReset,
		nodeCount, basePort, logCount)
	fmt.Println()
}

func printClusterStatus(nodes []*testNode) {
	fmt.Println(colorBold + "📋 集群状态:" + colorReset)
	for _, tn := range nodes {
		state := tn.node.State().String()
		term := tn.node.Term()
		leader := tn.node.LeaderID()
		marker := "  "
		if state == "Leader" {
			marker = "👑"
		}
		fmt.Printf("  %s 节点 %s: state=%s, term=%d, leader=%s\n",
			marker, tn.id, state, term, leader)
	}
}

func printFooter(passed bool) {
	fmt.Println()
	fmt.Println(colorCyan + "=== 验证结论 ===" + colorReset)
	if passed {
		fmt.Println(colorGreen + colorBold)
		fmt.Println("╔══════════════════════════════════════════════════════════════╗")
		fmt.Println("║   ✅ 全部验证通过 — Raft 共识引擎独立沙箱跑测 PASS            ║")
		fmt.Println("║   Leader 选举成功 + 10 条日志同步完成 + 内容一致              ║")
		fmt.Println("╚══════════════════════════════════════════════════════════════╝")
		fmt.Println(colorReset)
	} else {
		fmt.Println(colorRed + colorBold)
		fmt.Println("╔══════════════════════════════════════════════════════════════╗")
		fmt.Println("║   ❌ 验证失败 — Raft 共识引擎独立沙箱跑测 FAIL                ║")
		fmt.Println("╚══════════════════════════════════════════════════════════════╝")
		fmt.Println(colorReset)
	}
}
