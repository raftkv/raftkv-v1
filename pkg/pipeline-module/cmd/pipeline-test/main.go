// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline 沙箱验证测试
//
// 验证项：
//   a) 多阶段 Pipeline（接入→处理→输出）串联正确
//   b) 高吞吐压测：100,000 条数据端到端零丢失
//   c) 背压/流控：下游慢处理时不丢数据、不 panic
//   d) 批处理：按批次聚合输出（batch size 可配置）
//   e) 并发安全：多生产者并发写入 + 多消费者并发读取，无竞态、无丢失
//   f) 吞吐量统计：TPS（每秒处理条数）
//
// 运行：
//   go run cmd/pipeline-test/main.go
//   go run -race cmd/pipeline-test/main.go   (race 检测)
//
// 终端输出：ANSI 颜色 + emoji + === 分节分隔符
// =========================================================================

package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/pipeline-module"
)

// -------------------------------------------------------------------------
// ANSI 颜色与图标
// -------------------------------------------------------------------------

const (
	cReset  = "\x1b[0m"
	cCyan   = "\x1b[36m"
	cGreen  = "\x1b[32m"
	cYellow = "\x1b[33m"
	cRed    = "\x1b[31m"
	cBold   = "\x1b[1m"
)

func title(s string) string {
	return fmt.Sprintf("%s%s=== %s ===%s", cBold, cCyan, s, cReset)
}
func pass(s string) string { return fmt.Sprintf("%s✅ %s%s", cGreen, s, cReset) }
func warn(s string) string { return fmt.Sprintf("%s⚠ %s%s", cYellow, s, cReset) }
func fail(s string) string { return fmt.Sprintf("%s✗ %s%s", cRed, s, cReset) }
func info(s string) string { return fmt.Sprintf("%s📋 %s%s", cCyan, s, cReset) }
func section(s string)     { fmt.Printf("\n%s\n", title(s)) }

// -------------------------------------------------------------------------
// 主入口
// -------------------------------------------------------------------------

func main() {
	fmt.Printf("%s%s🚀 RaftKV Module03 — 高吞吐数据流转 Pipeline 沙箱验证%s\n", cBold, cCyan, cReset)
	fmt.Printf("%s🔑 零外部依赖 | 纯标准库 | GOOS=%s GOARCH=%s | Go %s%s\n",
		cCyan, runtime.GOOS, runtime.GOARCH, runtime.Version(), cReset)
	fmt.Println()

	allPass := true

	// 测试 1：多阶段串联 + 高吞吐 + 顺序性
	allPass = testMultiStageHighThroughput() && allPass

	// 测试 2：背压流控
	allPass = testBackpressure() && allPass

	// 测试 3：批处理聚合
	allPass = testBatcher() && allPass

	// 测试 4：并发安全（多生产者 + 多消费者）
	allPass = testConcurrentSafety() && allPass

	// 测试 5：RingBuffer 背压
	allPass = testRingBuffer() && allPass

	// 最终结论
	section("最终结论")
	if allPass {
		fmt.Println(pass("全部验证通过 — 高吞吐数据流转 Pipeline 独立闭环模块交付合格"))
	} else {
		fmt.Println(fail("存在失败项 — 需修正"))
	}
	fmt.Println()
}

// -------------------------------------------------------------------------
// 测试 1：多阶段 Pipeline 串联 + 高吞吐 + 顺序性
// -------------------------------------------------------------------------

func testMultiStageHighThroughput() bool {
	section("测试 1：多阶段 Pipeline 串联 + 高吞吐 100,000 条 + 顺序性")

	const N = 100000

	// 配置：单 worker 保序 + 大缓冲高吞吐
	cfg := pipeline.DefaultPipelineConfig()
	cfg.IngestWorkers = 1
	cfg.ProcessWorkers = 1 // 单 worker 保序
	cfg.OutputWorkers = 1
	cfg.EnableBatch = false // 测试 1 不启用批处理，纯串联
	cfg.IngestBuf = 16384
	cfg.ProcessBuf = 16384
	cfg.OutputBuf = 16384
	cfg.TPSWindow = 20 * time.Millisecond

	// Stage 处理函数：
	//   Ingest:  identity（直通）
	//   Process: map（Data 转换：int → int*2，附加标记）
	//   Output:  identity（直通）
	ingestProc := pipeline.IdentityFunc()
	processProc := pipeline.MapFunc(func(item pipeline.Item) pipeline.Item {
		// 模拟轻量处理：Data 是 int，转换为 int*2
		if v, ok := item.Data.(int); ok {
			item.Data = v * 2
		}
		if item.Meta == nil {
			item.Meta = make(map[string]string)
		}
		item.Meta["processed"] = "true"
		return item
	})
	outputProc := pipeline.IdentityFunc()

	p := pipeline.NewPipeline(cfg, ingestProc, processProc, outputProc)
	outCh, err := p.Start()
	if err != nil {
		fmt.Println(fail("Pipeline 启动失败: " + err.Error()))
		return false
	}
	fmt.Println(pass("多阶段 Pipeline 串联启动成功（Ingest → Process → Output）"))

	// 统计
	var stats pipeline.E2EStats
	stats.Sent = N

	// 单消费者：收集结果 + 顺序校验
	var received []pipeline.Item
	received = make([]pipeline.Item, 0, N)
	var recvCount int64
	consumerDone := make(chan struct{})
	go func() {
		for item := range outCh {
			p.MarkOutput()
			stats.RecordRecv(item)
			received = append(received, item)
			atomic.AddInt64(&recvCount, 1)
		}
		close(consumerDone)
	}()

	// 生产者：注入 N 条
	start := time.Now()
	for i := 0; i < N; i++ {
		p.Submit(pipeline.Item{
			ID:   uint64(i + 1),
			Data: i,
		})
	}

	// 关闭并等待消费完成
	p.Close()
	<-consumerDone
	elapsed := time.Since(start)
	stats.Finalize()

	// 校验
	fmt.Println(info(fmt.Sprintf("注入: %d 条, 接收: %d 条, 耗时: %v",
		N, len(received), elapsed)))

	ok := true
	if stats.Lost != 0 {
		fmt.Println(fail(fmt.Sprintf("数据丢失: Sent=%d Received=%d Lost=%d",
			stats.Sent, stats.Received, stats.Lost)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("100,000 条数据端到端流转完成，零丢失（Lost=0）")))
	}

	if stats.OutOfOrder != 0 || stats.Duplicated != 0 {
		fmt.Println(fail(fmt.Sprintf("顺序性失败: OutOfOrder=%d Duplicated=%d",
			stats.OutOfOrder, stats.Duplicated)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("顺序性符合 Pipeline 语义（单 worker 保序，OutOfOrder=0 Duplicated=0）")))
	}

	// 校验处理正确性（Data 应为 i*2）
	wrong := 0
	for idx, item := range received {
		expect := idx * 2
		if v, ok := item.Data.(int); !ok || v != expect {
			wrong++
			if wrong <= 3 {
				fmt.Println(warn(fmt.Sprintf("处理错误: idx=%d expect=%d got=%v", idx, expect, item.Data)))
			}
		}
	}
	if wrong == 0 {
		fmt.Println(pass(fmt.Sprintf("处理正确性: 全部 %d 条 Data=int*2 转换正确", N)))
	} else {
		fmt.Println(fail(fmt.Sprintf("处理错误: %d 条不正确", wrong)))
		ok = false
	}

	// TPS
	tps := float64(N) / elapsed.Seconds()
	fmt.Println(info(fmt.Sprintf("⚡ 端到端 TPS: %.0f 条/秒 （耗时 %v）", tps, elapsed)))
	fmt.Println(info(fmt.Sprintf("⚡ 瞬时 TPS（最后窗口）: %d 条/秒", p.InstantTPS())))

	if ok {
		fmt.Println(pass("测试 1 通过"))
	} else {
		fmt.Println(fail("测试 1 失败"))
	}
	return ok
}

// -------------------------------------------------------------------------
// 测试 2：背压流控
// -------------------------------------------------------------------------

func testBackpressure() bool {
	section("测试 2：背压流控（下游慢处理，不丢数据、不崩溃）")

	const N = 5000

	cfg := pipeline.DefaultPipelineConfig()
	cfg.IngestWorkers = 1
	cfg.ProcessWorkers = 1
	cfg.OutputWorkers = 1
	cfg.EnableBatch = false
	// 故意小缓冲，强制背压
	cfg.IngestBuf = 64
	cfg.ProcessBuf = 64
	cfg.OutputBuf = 64
	cfg.TPSWindow = 100 * time.Millisecond

	// Process 慢处理：每条 sleep 100μs，制造下游慢场景
	ingestProc := pipeline.IdentityFunc()
	processProc := pipeline.MapFunc(func(item pipeline.Item) pipeline.Item {
		time.Sleep(100 * time.Microsecond)
		return item
	})
	outputProc := pipeline.IdentityFunc()

	p := pipeline.NewPipeline(cfg, ingestProc, processProc, outputProc)
	outCh, err := p.Start()
	if err != nil {
		fmt.Println(fail("Pipeline 启动失败: " + err.Error()))
		return false
	}
	fmt.Println(pass("背压测试 Pipeline 启动成功（小缓冲 64 + 慢处理 100μs/条）"))

	var recvCount int64
	done := make(chan struct{})
	go func() {
		for range outCh {
			p.MarkOutput()
			atomic.AddInt64(&recvCount, 1)
		}
		close(done)
	}()

	// 多生产者并发注入（背压下应阻塞而非崩溃）
	start := time.Now()
	var prodWg sync.WaitGroup
	producers := 4
	per := N / producers
	for pp := 0; pp < producers; pp++ {
		prodWg.Add(1)
		go func(base int) {
			defer prodWg.Done()
			for i := 0; i < per; i++ {
				p.Submit(pipeline.Item{
					ID:   uint64(base*per + i + 1),
					Data: base*per + i,
				})
			}
		}(pp)
	}
	prodWg.Wait()
	p.Close()
	<-done
	elapsed := time.Since(start)

	got := atomic.LoadInt64(&recvCount)
	fmt.Println(info(fmt.Sprintf("注入: %d 条（%d 生产者并发）, 接收: %d 条, 耗时: %v",
		N, producers, got, elapsed)))

	ok := true
	if got != int64(N) {
		fmt.Println(fail(fmt.Sprintf("背压下数据丢失: 期望 %d, 实际 %d", N, got)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("背压流控正常，无数据丢失（%d 条全部到达）", N)))
	}

	tps := float64(N) / elapsed.Seconds()
	fmt.Println(info(fmt.Sprintf("⚡ 背压下 TPS: %.0f 条/秒", tps)))

	if ok {
		fmt.Println(pass("测试 2 通过"))
	} else {
		fmt.Println(fail("测试 2 失败"))
	}
	return ok
}

// -------------------------------------------------------------------------
// 测试 3：批处理聚合
// -------------------------------------------------------------------------

func testBatcher() bool {
	section("测试 3：批处理聚合（batch size 可配置）")

	const N = 50000
	const batchSize = 256

	cfg := pipeline.DefaultPipelineConfig()
	cfg.IngestWorkers = 1
	cfg.ProcessWorkers = 2
	cfg.OutputWorkers = 1
	cfg.EnableBatch = true
	cfg.BatchConfig = pipeline.BatchConfig{
		Size:          batchSize,
		FlushInterval: 5 * time.Millisecond,
	}
	cfg.IngestBuf = 8192
	cfg.ProcessBuf = 8192
	cfg.OutputBuf = 8192

	ingestProc := pipeline.IdentityFunc()
	processProc := pipeline.IdentityFunc()
	outputProc := pipeline.IdentityFunc()

	p := pipeline.NewPipeline(cfg, ingestProc, processProc, outputProc)
	outCh, err := p.Start()
	if err != nil {
		fmt.Println(fail("Pipeline 启动失败: " + err.Error()))
		return false
	}
	fmt.Println(pass(fmt.Sprintf("批处理 Pipeline 启动成功（batch size=%d, flush=5ms）", batchSize)))

	var recvCount int64
	done := make(chan struct{})
	go func() {
		for range outCh {
			p.MarkOutput()
			atomic.AddInt64(&recvCount, 1)
		}
		close(done)
	}()

	start := time.Now()
	for i := 0; i < N; i++ {
		p.Submit(pipeline.Item{
			ID:   uint64(i + 1),
			Data: i,
		})
	}
	p.Close()
	<-done
	elapsed := time.Since(start)

	got := atomic.LoadInt64(&recvCount)
	fmt.Println(info(fmt.Sprintf("注入: %d 条, 接收: %d 条, 耗时: %v", N, got, elapsed)))

	ok := true
	if got != int64(N) {
		fmt.Println(fail(fmt.Sprintf("批处理数据丢失: 期望 %d, 实际 %d", N, got)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("批处理聚合正确，%d 条全部到达（batch=%d）", N, batchSize)))
	}

	tps := float64(N) / elapsed.Seconds()
	fmt.Println(info(fmt.Sprintf("⚡ 批处理 TPS: %.0f 条/秒", tps)))

	if ok {
		fmt.Println(pass("测试 3 通过"))
	} else {
		fmt.Println(fail("测试 3 失败"))
	}
	return ok
}

// -------------------------------------------------------------------------
// 测试 4：并发安全（多生产者 + 多消费者）
// -------------------------------------------------------------------------

func testConcurrentSafety() bool {
	section("测试 4：并发安全（多生产者并发写入 + 多消费者并发读取）")

	const N = 100000
	producers := 8
	consumers := 4

	cfg := pipeline.DefaultPipelineConfig()
	cfg.IngestWorkers = 1
	cfg.ProcessWorkers = 4 // 多 worker 并行处理
	cfg.OutputWorkers = 1
	cfg.EnableBatch = false
	cfg.IngestBuf = 16384
	cfg.ProcessBuf = 16384
	cfg.OutputBuf = 16384

	ingestProc := pipeline.IdentityFunc()
	processProc := pipeline.MapFunc(func(item pipeline.Item) pipeline.Item {
		// 轻量处理
		if v, ok := item.Data.(int); ok {
			item.Data = v + 1
		}
		return item
	})
	outputProc := pipeline.IdentityFunc()

	p := pipeline.NewPipeline(cfg, ingestProc, processProc, outputProc)
	outCh, err := p.Start()
	if err != nil {
		fmt.Println(fail("Pipeline 启动失败: " + err.Error()))
		return false
	}
	fmt.Println(pass(fmt.Sprintf("并发安全 Pipeline 启动成功（%d 生产者 + %d 消费者 + %d process worker）",
		producers, consumers, cfg.ProcessWorkers)))

	// 多消费者并发读取，用原子计数 + map 去重（用 ID set）
	var recvCount int64
	recvIDs := make(map[uint64]struct{})
	var mu sync.Mutex
	var consumerWg sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			for item := range outCh {
				p.MarkOutput()
				atomic.AddInt64(&recvCount, 1)
				mu.Lock()
				recvIDs[item.ID] = struct{}{}
				mu.Unlock()
			}
		}()
	}

	// 多生产者并发写入
	start := time.Now()
	var prodWg sync.WaitGroup
	per := N / producers
	for pp := 0; pp < producers; pp++ {
		prodWg.Add(1)
		go func(base int) {
			defer prodWg.Done()
			for i := 0; i < per; i++ {
				p.Submit(pipeline.Item{
					ID:   uint64(base*per + i + 1),
					Data: base*per + i,
				})
			}
		}(pp)
	}
	prodWg.Wait()
	p.Close()
	consumerWg.Wait()
	elapsed := time.Since(start)

	got := atomic.LoadInt64(&recvCount)
	unique := len(recvIDs)
	fmt.Println(info(fmt.Sprintf("注入: %d 条（%d 生产者）, 接收: %d 条（%d 消费者）, 去重后唯一 ID: %d, 耗时: %v",
		N, producers, got, consumers, unique, elapsed)))

	ok := true
	if got != int64(N) {
		fmt.Println(fail(fmt.Sprintf("并发安全: 接收数不匹配 期望 %d 实际 %d", N, got)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("并发安全: %d 条全部到达，无丢失", N)))
	}

	if unique != N {
		fmt.Println(fail(fmt.Sprintf("并发安全: 唯一 ID 数不匹配 期望 %d 实际 %d（存在重复或丢失）", N, unique)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("并发安全: %d 个唯一 ID，无重复、无丢失", unique)))
	}

	tps := float64(N) / elapsed.Seconds()
	fmt.Println(info(fmt.Sprintf("⚡ 并发 TPS: %.0f 条/秒", tps)))

	if ok {
		fmt.Println(pass("测试 4 通过（go race 检测请用 go run -race 验证）"))
	} else {
		fmt.Println(fail("测试 4 失败"))
	}
	return ok
}

// -------------------------------------------------------------------------
// 测试 5：RingBuffer 背压
// -------------------------------------------------------------------------

func testRingBuffer() bool {
	section("测试 5：RingBuffer 环形缓冲区背压")

	const N = 20000
	const cap = 1024

	rb := pipeline.NewRingBuffer(cap)
	fmt.Println(pass(fmt.Sprintf("RingBuffer 创建成功（cap=%d）", cap)))

	// 多生产者 + 单消费者
	var recvCount int64
	done := make(chan struct{})
	go func() {
		for {
			_, ok := rb.Pop()
			if !ok {
				break
			}
			atomic.AddInt64(&recvCount, 1)
		}
		close(done)
	}()

	start := time.Now()
	producers := 4
	var prodWg sync.WaitGroup
	per := N / producers
	for pp := 0; pp < producers; pp++ {
		prodWg.Add(1)
		go func(base int) {
			defer prodWg.Done()
			for i := 0; i < per; i++ {
				rb.Push(pipeline.Item{
					ID:   uint64(base*per + i + 1),
					Data: base*per + i,
				})
			}
		}(pp)
	}
	prodWg.Wait()
	rb.Close()
	<-done
	elapsed := time.Since(start)

	got := atomic.LoadInt64(&recvCount)
	st := rb.Stats()
	fmt.Println(info(fmt.Sprintf("注入: %d 条, 接收: %d 条, Pushed=%d Popped=%d Dropped=%d, 耗时: %v",
		N, got, st.Pushed, st.Popped, st.Dropped, elapsed)))

	ok := true
	if got != int64(N) {
		fmt.Println(fail(fmt.Sprintf("RingBuffer 数据丢失: 期望 %d 实际 %d", N, got)))
		ok = false
	} else {
		fmt.Println(pass(fmt.Sprintf("RingBuffer 背压正常，%d 条全部到达，零丢失", N)))
	}
	if st.Dropped != 0 {
		fmt.Println(fail(fmt.Sprintf("RingBuffer 不应有 Dropped, 实际 %d", st.Dropped)))
		ok = false
	}

	tps := float64(N) / elapsed.Seconds()
	fmt.Println(info(fmt.Sprintf("⚡ RingBuffer TPS: %.0f 条/秒", tps)))

	if ok {
		fmt.Println(pass("测试 5 通过"))
	} else {
		fmt.Println(fail("测试 5 失败"))
	}
	return ok
}
