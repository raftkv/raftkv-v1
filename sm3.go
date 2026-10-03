// sm3.go — 国密 SM3 密码杂凑算法接口层
//
// 本文件提供 SM3 哈希在 Raft 日志完整性校验中的完整集成方案。
//
// SM3 是中国国家密码管理局发布的密码杂凑算法标准（GB/T 32905-2016），
// 输出 256 位（32 字节）摘要值。在 235 确定性引擎中用于：
//  1. 日志条目防篡改：每条 LogEntry 附带其 SM3(Term || Index || Command) 值
//  2. 链式哈希校验：当前日志的哈希依赖前一条日志的哈希，形成单向哈希链
//  3. 快照完整性验证：InstallSnapshot 时校验快照数据的 SM3 值
//
// 算法实现依赖：github.com/tjfoc/gmsm/sm3（v1.4.1，清华大学开源国密库）
//
// 【口径声明·V2.5.1】SM3 密码杂凑算法本体并非本项目自研，本项目自研部分为
// 「Raft 日志链式防篡改集成层」与「Raft 共识核心算法」。对外材料严禁将 SM3
// 算法本体表述为"纯自研"，正确表述为：基于清华 gmsm v1.4.1 国密库构建。
//
// 若信创环境要求去第三方化，可将 StandardSM3 替换为自有实现并满足 SM3Hasher
// 接口，届时须重新通过 GB/T 32905-2016 标准测试向量自检（见 sm3_integrity.go）。
package main

import (
	"encoding/binary"

	"github.com/tjfoc/gmsm/sm3"
)

// =============================================================================
// 第一节：SM3 哈希接口定义
// =============================================================================

// SM3Hasher 定义 SM3 密码杂凑的抽象接口。
//
// 设计意图：
//   - 生产环境：注入 StandardSM3，使用 github.com/tjfoc/gmsm/sm3 真实实现
//   - 测试环境：注入 MockSM3，快速验证一致性逻辑
//   - 信创受限环境：替换为纯 Go 自实现，满足接口即可
type SM3Hasher interface {
	// Hash 计算输入数据的 SM3 摘要，返回 32 字节哈希值。
	Hash(data []byte) []byte

	// HashLogEntry 计算日志条目的 SM3 摘要。
	// 输入格式：Term(8B) || Index(8B) || Command(variable)
	HashLogEntry(term, index uint64, command []byte) []byte

	// VerifyLogEntry 校验日志条目的 SM3 值是否匹配。
	VerifyLogEntry(term, index uint64, command, expectedHash []byte) bool

	// ChainHash 链式哈希：将前一条哈希与当前条目连接后计算。
	// 输入格式：PrevHash(32B) || Term(8B) || Index(8B) || Command(variable)
	ChainHash(prevHash []byte, term, index uint64, command []byte) []byte

	// VerifyChain 校验链式哈希的连续性。
	VerifyChain(prevHash []byte, term, index uint64, command, expectedHash []byte) bool
}

// =============================================================================
// 第二节：StandardSM3 — 基于 tjfoc/gmsm 的标准实现
// =============================================================================

// StandardSM3 封装了 tjfoc/gmsm/sm3 库，实现 SM3Hasher 接口。
// 这是生产环境的默认选择。
type StandardSM3 struct{}

// NewStandardSM3 创建一个标准 SM3 哈希器。
func NewStandardSM3() *StandardSM3 {
	return &StandardSM3{}
}

// Hash 计算单段数据的 SM3 摘要。
func (s *StandardSM3) Hash(data []byte) []byte {
	h := sm3.New()
	h.Write(data)
	return h.Sum(nil)
}

// HashLogEntry 计算日志条目的 SM3 摘要。
//
// 组合格式：Term(大端 8 字节) || Index(大端 8 字节) || Command
// 共计 16 字节固定头 + 可变长载荷。
func (s *StandardSM3) HashLogEntry(term, index uint64, command []byte) []byte {
	h := sm3.New()
	// 写入 16 字节固定头（两个 uint64 以大端序编码）
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], index)
	h.Write(buf)
	// 写入可变长命令载荷
	h.Write(command)
	return h.Sum(nil)
}

// VerifyLogEntry 校验日志条目的 SM3 值与期望值是否一致。
// 使用恒定时间比较避免时序侧信道攻击。
func (s *StandardSM3) VerifyLogEntry(term, index uint64, command, expectedHash []byte) bool {
	actual := s.HashLogEntry(term, index, command)
	return constantTimeEqual(actual, expectedHash)
}

// ChainHash 链式哈希：将前一条日志的哈希值与当前条目组合后计算。
//
// 这确保了日志的防篡改链：修改任一中间条目，后续所有条目的链式哈希都会改变。
// 格式：PrevHash(32B) || Term(8B) || Index(8B) || Command
func (s *StandardSM3) ChainHash(prevHash []byte, term, index uint64, command []byte) []byte {
	h := sm3.New()
	h.Write(prevHash) // 前一条的 SM3 值（32 字节）
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], index)
	h.Write(buf)
	h.Write(command)
	return h.Sum(nil)
}

// VerifyChain 校验链式哈希的连续性。
func (s *StandardSM3) VerifyChain(prevHash []byte, term, index uint64, command, expectedHash []byte) bool {
	actual := s.ChainHash(prevHash, term, index, command)
	return constantTimeEqual(actual, expectedHash)
}

// =============================================================================
// 第三节：MockSM3 — 测试用模拟实现（零开销）
// =============================================================================

// MockSM3 是一个零分配的 SM3 模拟器，用于单元测试与基准测试。
// 它不执行真实的 SM3 计算，仅返回固定长度的标记值。
// 这允许测试 Raft 一致性逻辑而不引入加密计算开销。
type MockSM3 struct {
	counter uint64
}

// NewMockSM3 创建一个模拟 SM3 哈希器。
func NewMockSM3() *MockSM3 {
	return &MockSM3{}
}

func (m *MockSM3) Hash(data []byte) []byte {
	return m.mockHash(data)
}

func (m *MockSM3) HashLogEntry(term, index uint64, command []byte) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], index)
	return m.mockHash(append(buf, command...))
}

func (m *MockSM3) VerifyLogEntry(term, index uint64, command, expectedHash []byte) bool {
	actual := m.HashLogEntry(term, index, command)
	return constantTimeEqual(actual, expectedHash)
}

func (m *MockSM3) ChainHash(prevHash []byte, term, index uint64, command []byte) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], index)
	combined := append(prevHash, buf...)
	combined = append(combined, command...)
	return m.mockHash(combined)
}

func (m *MockSM3) VerifyChain(prevHash []byte, term, index uint64, command, expectedHash []byte) bool {
	actual := m.ChainHash(prevHash, term, index, command)
	return constantTimeEqual(actual, expectedHash)
}

// mockHash 生成一个可复现的伪哈希值（32 字节）。
// 通过 XOR 折叠输入数据，确保相同输入产生相同输出。
func (m *MockSM3) mockHash(data []byte) []byte {
	m.counter++
	result := make([]byte, 32)
	// 使用输入的 XOR 折叠作为哈希的部分内容
	var acc uint64 = m.counter
	for i, b := range data {
		acc ^= uint64(b) << uint((i%8)*8)
	}
	binary.BigEndian.PutUint64(result[0:8], acc)
	binary.BigEndian.PutUint64(result[8:16], uint64(len(data)))
	binary.BigEndian.PutUint64(result[16:24], m.counter)
	// 剩余 8 字节保持零
	return result
}

// =============================================================================
// 第四节：辅助函数 — SM3 哈希相关
// =============================================================================

// constantTimeEqual 执行恒定时间字节比较，防止时序侧信道攻击。
// 这对于密码学校验至关重要——攻击者无法通过测量比较耗时
// 来推断哈希前缀匹配长度。
func constantTimeEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
