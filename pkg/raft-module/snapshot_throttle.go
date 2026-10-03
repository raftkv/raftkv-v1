// =========================================================================
// RaftKV Module01 — 快照传输限流器接口骨架（SnapshotThrottle）
//
// 令牌桶限流器骨架，与 design.md 2.2.2 一致（chain2-campaign4-design-v1）。
// 限流快照传输出站带宽，避免快照大流量挤占正常日志复制带宽。
//
// batch35-S T033 实现完整令牌桶 refill 逻辑；本文件仅定义接口骨架。
// =========================================================================

package raft

import (
	"context"
	"sync/atomic"
)

// SnapshotThrottle 快照传输出站带宽限流器（令牌桶骨架）。
type SnapshotThrottle struct {
	tokens chan struct{} // 令牌通道（信号量式令牌）
	rate   int           // 每秒令牌数（出站带宽上限）
	burst  int           // 突发容量（瞬时允许的最大并发令牌数）
	closed atomic.Bool   // 关闭标志
}

// NewSnapshotThrottle 创建快照传输限流器。
// rate: 每秒令牌数；burst: 突发容量。
func NewSnapshotThrottle(rate int, burst int) *SnapshotThrottle {
	if burst < 1 {
		burst = 1
	}
	return &SnapshotThrottle{
		tokens: make(chan struct{}, burst),
		rate:   rate,
		burst:  burst,
	}
}

// Acquire 阻塞获取一个令牌许可（ctx 可取消）。
// 返回 nil 表示获取成功；返回 ctx.Err() 表示等待被取消。
func (t *SnapshotThrottle) Acquire(ctx context.Context) error {
	select {
	case t.tokens <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release 释放一个令牌许可。
func (t *SnapshotThrottle) Release() {
	select {
	case <-t.tokens:
	default:
	}
}
