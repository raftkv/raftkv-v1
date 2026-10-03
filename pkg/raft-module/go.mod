// =========================================================================
// RaftKV Module01 — Raft 强一致性共识引擎（独立闭环模块）
//
// 纯标准库零外部依赖：
//   - 通信层: net/http + encoding/json（替代 gRPC）
//   - 加密层: crypto/aes + crypto/cipher GCM（替代 SM4）
//   - 持久化: os + encoding/binary（WAL 预写式日志）
//   - 并发:   sync + sync/atomic
// =========================================================================

module raftkv/raft-module

go 1.21