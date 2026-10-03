// =========================================================================
// RaftKV Module02 — WAL + 国密 SM4 持久化存储引擎（独立闭环模块）
//
// 纯标准库零外部依赖：
//   - 加密层: 自研 SM4 分组密码（符合 GB/T 32907-2016）+ crypto/cipher CTR 模式
//   - 完整性: crypto/hmac + crypto/sha256（SM4-CTR + HMAC-SHA256 认证加密）
//   - 持久化: os + encoding/binary（WAL 预写式日志，1GB 预分配 + 批量 fsync）
//   - 并发:   sync
//   - 随机:   crypto/rand（IV 与 HMAC 密钥派生）
//
// 禁止任何第三方加密库，SM4 必须自研。
// =========================================================================

module raftkv/wal-sm4-module

go 1.21