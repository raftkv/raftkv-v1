// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline
//
// Pipeline 核心：多阶段串联编排引擎。
//
// 数据流：
//   Ingest(接入) → Process(处理) → [Batcher(聚批)] → Output(输出) → Sink
//
// 设计要点：
//   1. 多阶段串联：Stage 之间通过带缓冲 channel 连接，形成背压链
//   2. 高吞吐：每 Stage 多 worker 并行 + channel 大缓冲
//   3. 背压流控：channel 满时阻塞上游，不丢数据、不崩溃
//   4. 批处理聚合：可选 Batcher 在 Output 前聚批
//   5. 并发安全：多生产者 Submit + 多消费者 Output，全程原子计数
//   6. TPS 统计：内置 TPSMeter，实时输出吞吐量
//   7. 优雅关闭：Close 后排空所有 Stage，保证零丢失
//
// 与前序模块协调：
//   - Pipeline.Output 可对接 Module02（WAL + SM4 持久化）的 Append 接口
//   - Pipeline 的协调/提交可依赖 Module01（Raft 共识）的 ProposeSync
//   - 本模块零依赖，不直接 import 前序模块，仅通过回调/接口衔接
//
// 零外部依赖：仅使用 Go 标准库
// =========================================================================

package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// 配置
// -------------------------------------------------------------------------

// PipelineConfig Pipeline 配置。
type PipelineConfig struct {
	// 缓冲大小
	IngestBuf  int // 接入→处理 channel 缓冲
	ProcessBuf int // 处理→批/输出 channel 缓冲
	OutputBuf  int // 输出 channel 缓冲

	// 并行度
	IngestWorkers  int
	ProcessWorkers int
	OutputWorkers  int

	// 批处理（可选）
	EnableBatch bool
	BatchConfig BatchConfig

	// TPS 采样窗口
	TPSWindow time.Duration
}

// DefaultPipelineConfig 默认配置：高吞吐偏优。
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		IngestBuf:      4096,
		ProcessBuf:     4096,
		OutputBuf:      4096,
		IngestWorkers:  1,
		ProcessWorkers: 4,
		OutputWorkers:  1,
		EnableBatch:    true,
		BatchConfig:    DefaultBatchConfig(),
		TPSWindow:      1 * time.Second,
	}
}

// -------------------------------------------------------------------------
// Pipeline 核心
// -------------------------------------------------------------------------

// Pipeline 多阶段数据流转引擎。
//
// 线程安全：可被多生产者并发调用 Submit。
type Pipeline struct {
	cfg PipelineConfig

	// Stages
	ingestStage  *Stage
	processStage *Stage
	outputStage  *Stage
	batcher      *Batcher

	// Channels
	ingestCh chan Item // 外部 → Ingest
	procCh   chan Item // Ingest → Process
	afterCh  chan Item // Process → (Batcher or Output)
	outputCh chan Item // (Batcher or Output) → 外部消费

	// 统计
	tps      *TPSMeter
	totalIn  atomic.Int64
	totalOut atomic.Int64

	// 生命周期
	ctx     context.Context
	cancel  context.CancelFunc
	started atomic.Bool
	closed  atomic.Bool

	wg sync.WaitGroup
}

// NewPipeline 创建一个 Pipeline。
//
//   - ingestProc:  接入 Stage 处理函数（通常为 identity，或打时间戳）
//   - processProc: 处理 Stage 处理函数（核心业务转换）
//   - outputProc:  输出 Stage 处理函数（通常为 identity，或格式化）
func NewPipeline(cfg PipelineConfig, ingestProc, processProc, outputProc ProcessFunc) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{
		cfg:      cfg,
		ingestCh: make(chan Item, cfg.IngestBuf),
		procCh:   make(chan Item, cfg.ProcessBuf),
		outputCh: make(chan Item, cfg.OutputBuf),
		tps:      NewTPSMeter(cfg.TPSWindow),
		ctx:      ctx,
		cancel:   cancel,
	}
	p.ingestStage = NewStage("ingest", cfg.IngestWorkers, ingestProc)
	p.processStage = NewStage("process", cfg.ProcessWorkers, processProc)
	p.outputStage = NewStage("output", cfg.OutputWorkers, outputProc)
	return p
}

// Start 启动 Pipeline 所有 Stage。
//
// 返回 output channel 供外部消费（若启用 Batcher，则 output 来自 Batcher.Out 经 Output Stage）。
func (p *Pipeline) Start() (<-chan Item, error) {
	if !p.started.CompareAndSwap(false, true) {
		return nil, errors.New("pipeline: already started")
	}

	p.tps.Start()

	// 接入 Stage: ingestCh → procCh
	p.ingestStage.Link(p.ingestCh, p.procCh)
	p.ingestStage.Start(p.ctx)

	if p.cfg.EnableBatch {
		// 处理 Stage: procCh → batcher.in
		p.batcher = NewBatcher(p.cfg.BatchConfig, p.cfg.OutputBuf, p.cfg.OutputBuf)
		// 用一个中间 channel 接 batcher.Out 的 []Item，拆回单条送 outputCh
		// 这里我们让 Process 直接输出到 batcher.in
		// 但 Stage.Link 需要 chan<- Item，batcher.in 是 chan Item，可转换
		p.processStage.Link(p.procCh, chan<- Item(p.batcher.in))
		p.processStage.Start(p.ctx)
		p.batcher.Start(p.ctx)

		// batcher.Out 是 []Item，需要拆批 → outputCh
		p.wg.Add(1)
		go p.unbatchToOutput()
	} else {
		// 处理 Stage: procCh → outputCh（经 output Stage）
		p.processStage.Link(p.procCh, p.outputCh)
		p.processStage.Start(p.ctx)
	}

	// Output Stage: outputCh → 外部
	// 这里 output Stage 输出到一个新 channel 供外部消费
	finalOut := make(chan Item, p.cfg.OutputBuf)
	p.outputStage.Link(p.outputCh, finalOut)
	p.outputStage.Start(p.ctx)

	return finalOut, nil
}

// unbatchToOutput 将 Batcher 输出的批拆回单条，送入 outputCh。
//
// 关键：退出时关闭 outputCh，触发 output Stage 级联关闭。
func (p *Pipeline) unbatchToOutput() {
	defer p.wg.Done()
	defer close(p.outputCh) // 级联关闭：batcher.Out 关闭 → 此处 → outputCh 关闭 → output Stage 排空
	for batch := range p.batcher.Out {
		for _, item := range batch {
			p.outputCh <- item
		}
	}
}

// Submit 提交一个 Item 到 Pipeline（多生产者并发安全）。
//
// 若接入 channel 满，将阻塞（背压）。
// 返回 false 表示 Pipeline 已关闭。
func (p *Pipeline) Submit(item Item) bool {
	if p.closed.Load() {
		return false
	}
	p.totalIn.Add(1)
	p.tps.Inc()
	p.ingestCh <- item
	return true
}

// MarkOutput 消费端每消费一条调用，用于统计输出数。
func (p *Pipeline) MarkOutput() {
	p.totalOut.Add(1)
}

// TotalIn 累计输入数。
func (p *Pipeline) TotalIn() int64 { return p.totalIn.Load() }

// TotalOut 累计输出数。
func (p *Pipeline) TotalOut() int64 { return p.totalOut.Load() }

// InstantTPS 瞬时 TPS。
func (p *Pipeline) InstantTPS() int64 { return p.tps.InstantTPS() }

// Close 优雅关闭 Pipeline。
//
// 行为（自然级联关闭，零丢失）：
//  1. 标记关闭，停止接收新 Submit
//  2. 关闭 ingestCh，触发级联排空：
//     ingestCh 关闭 → ingest Stage 排空 → close(procCh)
//     → process Stage 排空 → close(batcher.in) 或 close(outputCh)
//     → batcher flush+退出 → close(batcher.Out) → unbatchToOutput 退出 → close(outputCh)
//     → output Stage 排空 → close(finalOut) → 消费者 range 退出
//  3. 停止 TPS 采样
//
// 注意：不调用 cancel(ctx)，避免 worker 提前退出丢数据。
//
//	ctx 仅用于异常强制终止（ForceClose）。
//
// 调用后应继续排空 Start 返回的 output channel 直到关闭（range 自然结束）。
func (p *Pipeline) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.ingestCh)
	// 等待 unbatchToOutput 退出（它会关闭 outputCh，触发 output Stage 级联关闭）
	if p.cfg.EnableBatch {
		p.wg.Wait()
	}
	p.tps.Stop()
}

// ForceClose 强制关闭 Pipeline（异常场景）。
//
// 调用 cancel(ctx) 使所有 Stage worker 立即退出。可能丢失 in-flight 数据。
// 正常场景应使用 Close。
func (p *Pipeline) ForceClose() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.ingestCh)
	p.cancel()
	p.tps.Stop()
}

// -------------------------------------------------------------------------
// 高级 API：Run（阻塞运行直到所有输入处理完）
// -------------------------------------------------------------------------

// RunBlocking 阻塞运行 Pipeline，将 inputs 全部 Submit 后关闭并排空。
//
//   - inputs: 输入数据切片
//   - 返回：输出切片（按消费顺序）+ 端到端统计
//
// 适用于批量压测场景。
func RunBlocking(cfg PipelineConfig, ingestProc, processProc, outputProc ProcessFunc, inputs []Item) ([]Item, E2EStats) {
	p := NewPipeline(cfg, ingestProc, processProc, outputProc)
	outCh, err := p.Start()
	if err != nil {
		panic(err)
	}

	var stats E2EStats
	stats.Sent = int64(len(inputs))

	// 多生产者
	producerWg := sync.WaitGroup{}
	producerWg.Add(1)
	go func() {
		defer producerWg.Done()
		for _, item := range inputs {
			p.Submit(item)
		}
	}()

	// 消费者
	outputs := make([]Item, 0, len(inputs))
	consumerWg := sync.WaitGroup{}
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for item := range outCh {
			p.MarkOutput()
			stats.RecordRecv(item)
			outputs = append(outputs, item)
		}
	}()

	producerWg.Wait()
	p.Close()
	consumerWg.Wait()

	stats.Finalize()
	return outputs, stats
}
