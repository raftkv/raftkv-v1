package main

// =========================================================================
// RaftKV 跨模块集成验证 — Module03 Pipeline → Module02 SM4+WAL
//
// 极简集成测试：不依赖完整 5 节点系统，仅验证两个模块的接口匹配性。
//
// 数据流：
//   Pipeline.Submit(1000 条)
//     → Ingest Stage → Process Stage → Batcher → Output Stage
//     → outCh (<-chan Item)
//     → [适配层 Adapter] interface{} → []byte
//     → SM4Storage.AppendData (SM4-CTR + HMAC-SHA256 加密 → WAL 落盘)
//
// 验证项：
//   (a) Pipeline 注入 1000 条, 全部通过 Output Stage 写入加密 WAL
//   (b) 关闭 Pipeline 后, WAL 落盘记录数 = 1000 (零丢失)
//   (c) 重启后 Replay WAL, 解密恢复 1000 条记录, 内容与原始一致
//   (d) 磁盘文件确认为 SM4 密文 (非明文)
//   (e) 端到端 TPS
//
// 零外部依赖：纯 Go 标准库 + 两个本地模块
// =========================================================================

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	pipeline "raftkv/pipeline-module"
	walsm4 "raftkv/wal-sm4-module"
)

// =========================================================================
// ANSI 颜色 + 极客风格输出
// =========================================================================

const (
	cReset  = "\x1b[0m"
	cCyan   = "\x1b[36m"
	cGreen  = "\x1b[32m"
	cYellow = "\x1b[33m"
	cRed    = "\x1b[31m"
	cBold   = "\x1b[1m"
)

func section(title string) {
	fmt.Printf("\n%s%s===================================================================%s\n", cCyan, cBold, cReset)
	fmt.Printf("%s%s  🚀 %s%s\n", cCyan, cBold, title, cReset)
	fmt.Printf("%s%s===================================================================%s\n", cCyan, cBold, cReset)
}

func okf(format string, args ...interface{}) {
	fmt.Printf("%s✅ %s%s\n", cGreen, fmt.Sprintf(format, args...), cReset)
}
func warnf(format string, args ...interface{}) {
	fmt.Printf("%s⚠️  %s%s\n", cYellow, fmt.Sprintf(format, args...), cReset)
}
func failf(format string, args ...interface{}) {
	fmt.Printf("%s❌ %s%s\n", cRed, fmt.Sprintf(format, args...), cReset)
}
func infof(format string, args ...interface{}) {
	fmt.Printf("%s📋 %s%s\n", cCyan, fmt.Sprintf(format, args...), cReset)
}

// =========================================================================
// 测试参数
// =========================================================================

const (
	N       = 1000
	walPath = "integration_test.wal"
)

// =========================================================================
// 主流程
// =========================================================================

func main() {
	section("RaftKV 跨模块集成验证：Module03 Pipeline → Module02 SM4+WAL")
	infof("测试规模: %d 条数据, 零外部依赖, 纯 Go 标准库", N)

	// 清理旧 WAL
	os.Remove(walPath)

	// ---------------------------------------------------------------------
	// Step 1: 密钥准备
	// ---------------------------------------------------------------------
	section("Step 1: 密钥准备 (SM4 16B + HMAC-SHA256 32B)")
	sm4Key := []byte(os.Getenv("SM4_KEY"))
	if len(sm4Key) != 16 {
		sm4Key = []byte("${SM4_TEST_KEY}") // 占位符，16字节，实际部署必须通过 SM4_KEY 环境变量注入
	}
	hmacKeyArr := sha256.Sum256([]byte("raftkv-hmac-secret-2026"))
	hmacKey := hmacKeyArr[:] // 32 字节
	okf("SM4 主密钥: %x (%d 字节, 符合 GB/T 32907-2016)", sm4Key, len(sm4Key))
	okf("HMAC 密钥: %x... (%d 字节, SHA-256)", hmacKey[:8], len(hmacKey))

	// ---------------------------------------------------------------------
	// Step 2: 创建 Module02 SM4 加密存储
	// ---------------------------------------------------------------------
	section("Step 2: 创建 Module02 SM4Storage (SM4-CTR + HMAC-SHA256 认证加密)")
	storage, err := walsm4.NewSM4Storage(walPath, sm4Key, hmacKey)
	if err != nil {
		failf("SM4Storage 创建失败: %v", err)
		os.Exit(1)
	}
	okf("SM4Storage 创建成功, WAL 路径: %s (1GB 预分配)", walPath)

	// ---------------------------------------------------------------------
	// Step 3: 创建 Module03 Pipeline
	// ---------------------------------------------------------------------
	section("Step 3: 创建 Module03 Pipeline (Ingest→Process→Batcher→Output)")
	cfg := pipeline.DefaultPipelineConfig()
	// 调小缓冲以触发背压, 测试背压流控
	cfg.IngestBuf = 256
	cfg.ProcessBuf = 256
	cfg.OutputBuf = 256
	cfg.IngestWorkers = 1
	cfg.ProcessWorkers = 4 // 多 worker 并发处理
	cfg.OutputWorkers = 1
	cfg.EnableBatch = true
	cfg.BatchConfig = pipeline.BatchConfig{
		Size:          64,
		FlushInterval: 2 * time.Millisecond,
	}

	// Ingest: identity (打 ID)
	ingestProc := pipeline.IdentityFunc()
	// Process: identity (演示处理点, 实际可做转换)
	processProc := pipeline.IdentityFunc()
	// Output: identity (适配层在消费端实现)
	outputProc := pipeline.IdentityFunc()

	p := pipeline.NewPipeline(cfg, ingestProc, processProc, outputProc)
	outCh, err := p.Start()
	if err != nil {
		failf("Pipeline 启动失败: %v", err)
		os.Exit(1)
	}
	okf("Pipeline 启动成功 (IngestBuf=%d, ProcessWorkers=%d, Batch={size:%d,flush:%v})",
		cfg.IngestBuf, cfg.ProcessWorkers, cfg.BatchConfig.Size, cfg.BatchConfig.FlushInterval)

	// ---------------------------------------------------------------------
	// Step 4: 适配层 Adapter (Pipeline outCh → SM4Storage.AppendData)
	// ---------------------------------------------------------------------
	section("Step 4: 适配层 Adapter (Pipeline.Item → WALEntry)")
	infof("适配点 1: Pipeline.Item.Data (interface{}) → []byte (类型断言)")
	infof("适配点 2: Pipeline 输出为 <-chan Item, 消费 goroutine 调用 AppendData")
	infof("适配点 3: Pipeline 为 channel 输出模型, WAL 为同步 Append 接口, 适配层桥接")

	var walCount int64
	var walCountMu sync.Mutex
	adapterWg := sync.WaitGroup{}
	adapterWg.Add(1)
	start := time.Now()
	go func() {
		defer adapterWg.Done()
		for item := range outCh {
			// 适配：interface{} → []byte
			data, ok := item.Data.([]byte)
			if !ok {
				failf("适配层类型断言失败, item.ID=%d, 实际类型=%T", item.ID, item.Data)
				continue
			}
			// 对接 Module02 加密 WAL (SM4-CTR + HMAC-SHA256)
			if err := storage.AppendData(data); err != nil {
				failf("AppendData 失败: %v", err)
				continue
			}
			walCountMu.Lock()
			walCount++
			walCountMu.Unlock()
			p.MarkOutput()
		}
	}()
	okf("适配层消费 goroutine 已启动, 等待 Pipeline 输出...")

	// ---------------------------------------------------------------------
	// Step 5: 注入 1000 条数据
	// ---------------------------------------------------------------------
	section("Step 5: Pipeline 注入 1000 条数据 (计时开始)")
	originalData := make([][]byte, N)
	originalSet := make(map[string]int, N)
	for i := 0; i < N; i++ {
		s := fmt.Sprintf("PAYLOAD-%04d-CONTENT-raftkv-sm4-wal-test", i)
		originalData[i] = []byte(s)
		originalSet[s]++
	}
	infof("明文样例: %q (长度 %d 字节)", string(originalData[0]), len(originalData[0]))

	for i := 0; i < N; i++ {
		item := pipeline.Item{
			ID:   uint64(i + 1),
			Data: originalData[i],
		}
		if !p.Submit(item) {
			failf("Submit 失败 at i=%d", i)
		}
	}
	okf("已 Submit %d 条数据到 Pipeline", N)

	// ---------------------------------------------------------------------
	// Step 6: 关闭 Pipeline, 等待排空
	// ---------------------------------------------------------------------
	p.Close()
	adapterWg.Wait()
	elapsed := time.Since(start)
	walCountMu.Lock()
	finalWalCount := walCount
	walCountMu.Unlock()

	section("Step 6: Pipeline 关闭 + 适配层排空完成")
	okf("Pipeline TotalIn  = %d", p.TotalIn())
	okf("Pipeline TotalOut = %d", p.TotalOut())
	okf("适配层写入 WAL 条数 = %d", finalWalCount)
	okf("端到端耗时 = %v", elapsed)

	// ---------------------------------------------------------------------
	// Step 7: 验证 (a)(b) — 1000 条零丢失
	// ---------------------------------------------------------------------
	section("Step 7: 验证 (a)(b) — 1000 条全部流入加密 WAL, 零丢失")
	passCount := (finalWalCount == N)
	if passCount {
		okf("(a) Pipeline → WAL 流转成功: %d 条全部写入", finalWalCount)
		okf("(b) WAL 落盘记录数 = %d == %d, 零丢失 ✨", finalWalCount, N)
	} else {
		failf("WAL 落盘记录数 = %d != %d (丢失 %d 条)", finalWalCount, N, N-finalWalCount)
	}

	// ---------------------------------------------------------------------
	// 关闭 storage, 重启 Replay
	// ---------------------------------------------------------------------
	if err := storage.Close(); err != nil {
		failf("Storage Close 失败: %v", err)
	}
	okf("SM4Storage 已关闭 (WAL 已 fsync 落盘)")

	section("Step 8: 重启 SM4Storage, ReplayAll 解密恢复")
	storage2, err := walsm4.NewSM4Storage(walPath, sm4Key, hmacKey)
	if err != nil {
		failf("SM4Storage 重启失败: %v", err)
		os.Exit(1)
	}
	defer storage2.Close()
	okf("SM4Storage 重启成功 (同一 WAL 文件, 模拟崩溃恢复)")

	replayed, err := storage2.ReplayAll()
	if err != nil {
		failf("ReplayAll 失败: %v", err)
		os.Exit(1)
	}
	okf("ReplayAll 解密恢复 %d 条记录", len(replayed))

	// ---------------------------------------------------------------------
	// Step 9: 验证 (c) — 内容一致 (multiset 比对)
	// ---------------------------------------------------------------------
	section("Step 9: 验证 (c) — 解密内容与原始一致 (multiset 比对)")
	passContent := false
	if len(replayed) == N {
		replayedSet := make(map[string]int, N)
		for _, e := range replayed {
			replayedSet[string(e.Data)]++
		}
		passContent = true
		if len(replayedSet) != len(originalSet) {
			passContent = false
		} else {
			for k, v := range originalSet {
				if replayedSet[k] != v {
					passContent = false
					break
				}
			}
		}
	}
	if passContent {
		okf("(c) 解密内容 multiset 与原始完全一致 (%d 条, 零差异)", N)
		// 展示前 3 条解密样例
		for i := 0; i < 3 && i < len(replayed); i++ {
			infof("  解密样例[%d]: %q", i, string(replayed[i].Data))
		}
	} else {
		failf("解密内容与原始不一致 (replayed=%d, expected=%d)", len(replayed), N)
	}

	// ---------------------------------------------------------------------
	// Step 10: 验证 (d) — 磁盘密文确认
	// ---------------------------------------------------------------------
	section("Step 10: 验证 (d) — 磁盘文件确认为 SM4 密文 (非明文)")

	// 检查 1: 磁盘原始字节中不应出现明文 PAYLOAD 子串
	// (WAL 为 JSON 格式, []byte 字段经 base64 编码, 明文不会直接出现)
	f, err := os.Open(walPath)
	if err != nil {
		failf("打开 WAL 文件失败: %v", err)
		os.Exit(1)
	}
	diskBuf := make([]byte, 65536) // 读前 64KB 足以覆盖前若干条记录
	nRead, _ := f.Read(diskBuf)
	f.Close()
	diskContent := string(diskBuf[:nRead])
	plaintextLeak := 0
	for i := 0; i < N; i++ {
		needle := fmt.Sprintf("PAYLOAD-%04d", i)
		if strings.Contains(diskContent, needle) {
			plaintextLeak++
		}
	}
	if plaintextLeak == 0 {
		okf("(d-1) 磁盘前 %d 字节中未发现任何明文 PAYLOAD 子串 (0/%d)", nRead, N)
	} else {
		failf("磁盘文件中发现 %d 条明文泄露", plaintextLeak)
	}

	// 检查 2: 通过 WAL.Replay() 取原始密文 entry, 验证 SM4-CTR+HMAC 密文格式
	// 密文格式: [IV(16B 非零)][ciphertext(=明文长度)][HMAC-Tag(32B)]
	rawEntries, err := storage2.WAL().Replay()
	if err != nil {
		failf("WAL.Replay (密文) 失败: %v", err)
	}
	plainLen := len(originalData[0])
	expectedCipherLen := 16 + plainLen + 32 // IV + ciphertext + HMAC tag
	ciphertextFormatOk := 0
	ciphertextNotPlaintext := 0
	for _, e := range rawEntries {
		// 检查密文长度
		if len(e.Data) != expectedCipherLen {
			continue
		}
		// 检查 IV (前 16 字节) 非零
		ivNonZero := false
		for _, b := range e.Data[:16] {
			if b != 0 {
				ivNonZero = true
				break
			}
		}
		if !ivNonZero {
			continue
		}
		ciphertextFormatOk++

		// 检查密文部分不等于任何原始明文
		ciphertextPart := e.Data[16 : 16+plainLen]
		isPlaintext := false
		for i := 0; i < N; i++ {
			if string(ciphertextPart) == string(originalData[i]) {
				isPlaintext = true
				break
			}
		}
		if !isPlaintext {
			ciphertextNotPlaintext++
		}
	}
	if ciphertextFormatOk == N {
		okf("(d-2) 密文格式全部正确: IV(16B 非零) + ciphertext(%dB) + HMAC(32B) = %d 字节/条",
			plainLen, expectedCipherLen)
	} else {
		failf("密文格式正确条数 %d / %d", ciphertextFormatOk, N)
	}
	if ciphertextNotPlaintext == N {
		okf("(d-3) 密文部分与所有原始明文均不同 (确认已加密, 非明文落盘)")
	} else {
		failf("密文等于明文的条数 %d / %d (未加密?)", N-ciphertextNotPlaintext, N)
	}

	// ---------------------------------------------------------------------
	// Step 11: 验证 (e) — 端到端 TPS
	// ---------------------------------------------------------------------
	section("Step 11: 验证 (e) — 端到端 TPS")
	tps := float64(N) / elapsed.Seconds()
	okf("(e) 端到端 TPS = %.2f 条/秒 (%d 条 / %v)", tps, N, elapsed)
	infof("Pipeline 累计: TotalIn=%d, TotalOut=%d", p.TotalIn(), p.TotalOut())

	// ---------------------------------------------------------------------
	// Step 12: 接口匹配性结论
	// ---------------------------------------------------------------------
	section("Step 12: 接口匹配性结论")

	// 适配点总结
	infof("适配点总结:")
	infof("  1. Pipeline.Item.Data (interface{}) → []byte: 类型断言适配")
	infof("  2. Pipeline channel 输出 → WAL 同步 Append: 消费 goroutine 桥接")
	infof("  3. Pipeline 顺序不保证 (多 worker) → WAL 顺序写入: multiset 比对验证")

	pass := passCount && passContent && (plaintextLeak == 0) &&
		(ciphertextFormatOk == N) && (ciphertextNotPlaintext == N) && (len(replayed) == N)

	if pass {
		okf("Pipeline → SM4Storage 流转成功")
		okf("1000 条零丢失 (WAL 落盘 %d)", finalWalCount)
		okf("重启 Replay 解密恢复一致 (multiset 相等)")
		okf("磁盘密文确认 (无明文泄露, SM4-CTR+HMAC 格式正确)")
		okf("端到端 TPS = %.2f 条/秒", tps)
		fmt.Printf("\n%s%s===================================================================%s\n", cGreen, cBold, cReset)
		fmt.Printf("%s%s  🎯 接口匹配性验证: PASS ✅✅✅%s\n", cGreen, cBold, cReset)
		fmt.Printf("%s%s  模块间接口完全匹配, 适配层桥接正确%s\n", cGreen, cBold, cReset)
		fmt.Printf("%s%s===================================================================%s\n", cGreen, cBold, cReset)
	} else {
		failf("验证失败")
		fmt.Printf("\n%s%s===================================================================%s\n", cRed, cBold, cReset)
		fmt.Printf("%s%s  🎯 接口匹配性验证: FAIL ❌❌❌%s\n", cRed, cBold, cReset)
		fmt.Printf("%s%s===================================================================%s\n", cRed, cBold, cReset)
		os.Exit(1)
	}

	// 清理测试 WAL 文件
	os.Remove(walPath)
	fmt.Printf("\n%s🧹 测试 WAL 文件已清理%s\n", cYellow, cReset)
}
