// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline
//
// Batcher 批处理聚合器：将上游单条数据按批次大小或时间窗口聚合后输出。
//
// 设计要点：
//   1. 两种触发条件：批次满（size）或时间窗口到期（flush interval）
//   2. 输出单位为 []Item（一批），下游处理批的效率远高于单条
//   3. 优雅关闭：Close 后排空缓冲并 flush 最后一帧
//   4. 并发安全：单个 Batcher 由独立 goroutine 驱动，外部仅 Push/Close
//
// 在 Pipeline 中的角色：
//   - 通常作为 Output Stage 前的聚合层
//   - 例如：接入→处理→[Batcher 聚批]→批量输出（可对接 WAL 批量 fsync）
//
// 零外部依赖：仅使用 Go 标准库
// =========================================================================

package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// Batcher 配置与实现
// -------------------------------------------------------------------------

// BatchConfig Batcher 配置。
type BatchConfig struct {
	Size          int           // 每批最大条数（≥1）
	FlushInterval time.Duration // 强制 flush 时间窗口（>0 时启用定时 flush）
}

// DefaultBatchConfig 默认批配置：256 条/批，5ms 窗口。
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		Size:          256,
		FlushInterval: 5 * time.Millisecond,
	}
}

// Batcher 批处理聚合器。
//
// 行为：
//   - Push(item): 将 item 加入当前缓冲；缓冲满则触发 flush
//   - 定时器到期则触发 flush（若 FlushInterval > 0）
//   - flush 将当前缓冲作为一批输出到 Out channel
//   - Close: 排空并 flush 最后一帧，关闭 Out
type Batcher struct {
	cfg BatchConfig

	in   chan Item     // 外部 Push 入口
	Out  chan []Item   // 批量输出口
	done chan struct{} // 关闭信号
	wg   sync.WaitGroup

	// 统计
	totalIn      atomic.Int64
	totalBatches atomic.Int64
	totalOut     atomic.Int64
}

// NewBatcher 创建一个 Batcher。
//
//   - cfg.Size < 1 时修正为 1
//   - inBuf:   in channel 缓冲大小
//   - outBuf:  Out channel 缓冲大小
func NewBatcher(cfg BatchConfig, inBuf, outBuf int) *Batcher {
	if cfg.Size < 1 {
		cfg.Size = 1
	}
	if inBuf < 0 {
		inBuf = 0
	}
	if outBuf < 0 {
		outBuf = 0
	}
	b := &Batcher{
		cfg:  cfg,
		in:   make(chan Item, inBuf),
		Out:  make(chan []Item, outBuf),
		done: make(chan struct{}),
	}
	return b
}

// Push 非阻塞地将 item 投入 Batcher（in channel 带缓冲）。
//
// 若 in channel 满（背压），将阻塞直到被消费。
func (b *Batcher) Push(item Item) {
	b.totalIn.Add(1)
	b.in <- item
}

// Start 启动 Batcher 的内部 goroutine。
//
// 必须在 Push 之前调用。
func (b *Batcher) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.run(ctx)
}

// Close 关闭 Batcher：停止接收新数据，排空并 flush 最后一帧，关闭 Out。
//
// 调用后应等待 Out 排空（range 自然结束）。
func (b *Batcher) Close() {
	close(b.in)
	b.wg.Wait()
}

// run Batcher 主循环。
func (b *Batcher) run(ctx context.Context) {
	defer b.wg.Done()
	defer close(b.Out)

	buf := make([]Item, 0, b.cfg.Size)
	var timerC <-chan time.Time
	var timer *time.Timer

	if b.cfg.FlushInterval > 0 {
		timer = time.NewTimer(b.cfg.FlushInterval)
		timerC = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		batch := buf
		buf = make([]Item, 0, b.cfg.Size)
		select {
		case b.Out <- batch:
			b.totalBatches.Add(1)
			b.totalOut.Add(int64(len(batch)))
		case <-ctx.Done():
			return
		}
	}

	resetTimer := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(b.cfg.FlushInterval)
		}
	}

	for {
		select {
		case <-ctx.Done():
			// 排空 in 后 flush 最后一帧
			for item := range b.in {
				buf = append(buf, item)
				if len(buf) >= b.cfg.Size {
					flush()
				}
			}
			flush()
			return

		case item, ok := <-b.in:
			if !ok {
				// in 已关闭，排空并 flush
				flush()
				return
			}
			buf = append(buf, item)
			if len(buf) >= b.cfg.Size {
				flush()
				resetTimer()
			}

		case <-timerC:
			flush()
			timer.Reset(b.cfg.FlushInterval)
		}
	}
}

// -------------------------------------------------------------------------
// 统计
// -------------------------------------------------------------------------

// BatchStats Batcher 累计统计。
type BatchStats struct {
	In      int64 // 累计 Push 条数
	Batches int64 // 累计输出批数
	Out     int64 // 累计输出条数（应等于 In）
}

// Stats 返回累计统计快照。
func (b *Batcher) Stats() BatchStats {
	return BatchStats{
		In:      b.totalIn.Load(),
		Batches: b.totalBatches.Load(),
		Out:     b.totalOut.Load(),
	}
}
