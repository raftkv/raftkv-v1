// =========================================================================
// RaftKV Module03 — 高吞吐数据流转 Pipeline
//
// Stage 抽象：Pipeline 的最小执行单元。
//   - IngestStage  接入：接收外部数据，注入 Pipeline
//   - ProcessStage 处理：对数据做转换/过滤/聚合
//   - OutputStage  输出：将处理结果交付下游（可对接 WAL 持久化）
//
// 设计原则：
//   1. 每个 Stage 拥有独立 goroutine，从 in channel 读取，处理后写入 out channel
//   2. Stage 之间通过带缓冲 channel 连接，形成背压链
//   3. Stage 处理函数由用户注入（ProcessFunc），保证灵活可组合
//   4. 优雅关闭：in channel 关闭后，Stage 排空剩余数据再关闭 out channel
//
// 零外部依赖：仅使用 Go 标准库
// =========================================================================

package pipeline

import (
	"context"
	"sync"
)

// -------------------------------------------------------------------------
// 核心数据载体
// -------------------------------------------------------------------------

// Item Pipeline 中流转的数据项。
//
// 字段语义：
//   - ID:    全局唯一递增序号（由 Ingest 分配），用于端到端顺序性校验
//   - Data:  任意业务载荷（保持 interface{} 以最大化通用性）
//   - Meta:  附加元数据（如来源、时间戳），可选
type Item struct {
	ID   uint64
	Data interface{}
	Meta map[string]string
}

// ProcessFunc Stage 处理函数签名。
//
//   - 输入：上游 Item
//   - 输出：处理后的 Item（可能被修改/聚合），以及一个 keep 标志
//     keep=false 表示该 Item 被过滤掉，不再向下游传递
type ProcessFunc func(item Item) (Item, bool)

// -------------------------------------------------------------------------
// Stage 抽象
// -------------------------------------------------------------------------

// Stage Pipeline 的一个执行阶段。
//
// 不变量：
//   - 每个 Stage 拥有独立 goroutine（由 Start 启动）
//   - Stage 从 inCh 读取，处理后写入 outCh
//   - inCh 关闭后，Stage 排空并关闭 outCh
type Stage struct {
	Name    string // 阶段名（业务语义，如 "ingest"/"process"/"output"）
	Workers int    // 并行 worker 数（≥1）
	proc    ProcessFunc

	inCh  <-chan Item
	outCh chan<- Item

	wg sync.WaitGroup
}

// NewStage 创建一个 Stage。
//
//   - name:   阶段名
//   - workers: 并行度（<1 时自动修正为 1）
//   - proc:   处理函数（nil 表示直通，等价于 identity）
func NewStage(name string, workers int, proc ProcessFunc) *Stage {
	if workers < 1 {
		workers = 1
	}
	if proc == nil {
		proc = func(item Item) (Item, bool) { return item, true }
	}
	return &Stage{
		Name:    name,
		Workers: workers,
		proc:    proc,
	}
}

// Link 将 Stage 接入 channel 链。
//
//   - in:  上游输入 channel（接入 Stage 的 inCh）
//   - out: 下游输出 channel（接入 Stage 的 outCh）
//
// 注意：调用 Link 后 Stage 尚未启动，需调用 Start 启动 worker。
func (s *Stage) Link(in <-chan Item, out chan<- Item) {
	s.inCh = in
	s.outCh = out
}

// Start 启动 Stage 的所有 worker goroutine。
//
// 行为：
//   - 启动 s.Workers 个 goroutine 并发从 inCh 读取并处理
//   - 任一 worker 在 inCh 关闭且排空后退出
//   - 所有 worker 退出后关闭 outCh
//
// ctx 用于异常终止（cancel 时 worker 优先退出）。
func (s *Stage) Start(ctx context.Context) {
	if s.inCh == nil || s.outCh == nil {
		panic("pipeline: Stage.Link must be called before Start")
	}
	for i := 0; i < s.Workers; i++ {
		s.wg.Add(1)
		go s.runWorker(ctx, i)
	}
	// 排空后关闭 outCh
	go func() {
		s.wg.Wait()
		close(s.outCh)
	}()
}

// runWorker 单个 worker 循环。
func (s *Stage) runWorker(ctx context.Context, idx int) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-s.inCh:
			if !ok {
				return
			}
			out, keep := s.proc(item)
			if !keep {
				continue
			}
			select {
			case s.outCh <- out:
			case <-ctx.Done():
				return
			}
		}
	}
}

// -------------------------------------------------------------------------
// 内置 Stage 处理函数
// -------------------------------------------------------------------------

// IdentityFunc 直通处理函数（keep=true，原样传递）。
func IdentityFunc() ProcessFunc {
	return func(item Item) (Item, bool) { return item, true }
}

// FilterFunc 构造一个过滤函数：仅保留 pred(item)==true 的 Item。
func FilterFunc(pred func(item Item) bool) ProcessFunc {
	return func(item Item) (Item, bool) {
		if !pred(item) {
			return Item{}, false
		}
		return item, true
	}
}

// MapFunc 构造一个映射函数：对 Item.Data 做转换。
func MapFunc(transform func(item Item) Item) ProcessFunc {
	return func(item Item) (Item, bool) {
		return transform(item), true
	}
}
