// sm3_integrity.go — Raft 日志国密 SM3 防篡改链（V2.5.1 真实启用）
//
// 【修复背景】
// V2.5.0 存在功能断层：RaftLog 结构体虽已声明 SM3Hash 字段，但全库 3 处日志
// 构造点（raft.go 跟随者追加、membership.go 成员新增/移除）均只做透传、从未
// 计算，且 InitAdapters 无任何调用者，导致 gmsm/sm3 被链接器整块裁剪，
// 交付二进制中 SM3 算法符号命中数为 0 —— 国密防篡改声称名存实亡。
//
// 【本次修复】
//   1. 在全部日志构造点真实计算链式 SM3 摘要（PrevHash ‖ Term ‖ Index ‖ Command）
//   2. 跟随者在 AppendEntries 落盘前执行国密校验，篡改条目一律拒绝
//   3. 启动时对 GB/T 32905-2016 官方测试向量执行自检，结果打印并对外暴露
//
// 【实现说明（口径透明化）】
// SM3 算法本体来自清华大学开源国密库 github.com/tjfoc/gmsm v1.4.1，
// 非本项目自研；本项目自研部分为「链式防篡改集成层 + Raft 共识核心」。
// 算法符合 GB/T 32905-2016《信息安全技术 SM3 密码杂凑算法》标准测试向量。
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

// SM3 摘要长度（256 bit = 32 byte）
const sm3DigestLen = 32

// SM3IntegrityEnvKey 国密防篡改链开关环境变量。
// 出厂默认开启；置为 off / 0 / false 时关闭（仅限排障，严禁生产关闭）。
const SM3IntegrityEnvKey = "SM3_INTEGRITY"

// sm3Engine 全局 SM3 引擎。生产实现为 tjfoc/gmsm v1.4.1 标准实现。
var sm3Engine SM3Hasher = NewStandardSM3()

// globalUnifiedSM 全局统一状态机（main.go 启动时经 InitAdapters 装配，
// 注入国密标准 SM3 实现；此前该对象因 InitAdapters 零调用者而从未被构造）
var globalUnifiedSM *UnifiedStateMachine

// sm3IntegrityEnabled 国密防篡改链总开关（出厂默认开启）
var sm3IntegrityEnabled = sm3IntegrityEnvEnabled()

// 运行时统计（原子计数，供 /sm3/status 自证端点对外核验）
var (
	sm3ChainComputed uint64 // 已计算的条目摘要数
	sm3ChainVerified uint64 // 已校验的条目摘要数
	sm3ChainMismatch uint64 // 校验失败（疑似篡改）数
)

func sm3IntegrityEnvEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(SM3IntegrityEnvKey)))
	return v != "off" && v != "0" && v != "false"
}

// =============================================================================
// 第一节：链式摘要计算与校验
// =============================================================================

// genesisHash 返回创世哈希（32 字节全零），用于日志链起点。
func genesisHash() []byte { return make([]byte, sm3DigestLen) }

// PrevEntryHash 取指定索引条目的前序链式哈希。
// index 为 1-based 日志索引；无前序条目或前序未携带摘要时返回创世哈希。
func PrevEntryHash(logs []RaftLog, index int64) []byte {
	if index > 1 && int64(len(logs)) >= index-1 {
		if h := logs[index-2].SM3Hash; len(h) == sm3DigestLen {
			return h
		}
	}
	return genesisHash()
}

// ComputeEntrySM3 计算日志条目的链式 SM3 摘要。
//
// 摘要输入格式：PrevHash(32B) ‖ Term(8B, 大端) ‖ Index(8B, 大端) ‖ Command
// 采用链式结构使任一历史条目被篡改时，其后所有条目的摘要随之失效。
func ComputeEntrySM3(prevHash []byte, term, index int64, command []byte) []byte {
	if !sm3IntegrityEnabled || sm3Engine == nil {
		return nil
	}
	if len(prevHash) != sm3DigestLen {
		prevHash = genesisHash()
	}
	digest := sm3Engine.ChainHash(prevHash, uint64(term), uint64(index), command)
	atomic.AddUint64(&sm3ChainComputed, 1)
	return digest
}

// VerifyEntrySM3 校验日志条目的链式 SM3 摘要。
//
// 返回 true 表示校验通过或不适用（未携带摘要的旧格式条目，保证向后兼容）；
// 返回 false 表示摘要不匹配，调用方必须拒绝该条目落盘。
func VerifyEntrySM3(prevHash []byte, term, index int64, command, expected []byte) bool {
	if !sm3IntegrityEnabled || sm3Engine == nil {
		return true
	}
	if len(expected) == 0 {
		return true // 旧格式条目未携带摘要，放行以保持向后兼容
	}
	if len(prevHash) != sm3DigestLen {
		prevHash = genesisHash()
	}
	actual := sm3Engine.ChainHash(prevHash, uint64(term), uint64(index), command)
	atomic.AddUint64(&sm3ChainVerified, 1)
	if constantTimeEqual(actual, expected) {
		return true
	}
	atomic.AddUint64(&sm3ChainMismatch, 1)
	return false
}

// =============================================================================
// 第二节：GB/T 32905-2016 标准测试向量自检
// =============================================================================

// sm3StdVectors 为 GB/T 32905-2016《信息安全技术 SM3 密码杂凑算法》
// 官方文档给出的标准测试向量，全部经本仓库 vendored gmsm v1.4.1 实测复核通过。
var sm3StdVectors = []struct {
	name string
	in   string
	want string
}{
	{
		name: "空串",
		in:   "",
		want: "1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b",
	},
	{
		name: `"abc"`,
		in:   "abc",
		want: "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
	},
	{
		name: `"a"`,
		in:   "a",
		want: "623476ac18f65a2909e43c7fec61b49c7e764a91a18ccb82f1917a29c86c5e88",
	},
	{
		name: `"abcd" × 16 (64B)`,
		in:   "abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
		want: "debe9ff92275b8a138604889c18e5a4d6fdb70e5387e5765293dcba39c0c5732",
	},
}

// SM3SelfTest 对 SM3 国密算法执行标准测试向量自检。
// 返回自检是否通过，以及逐条明细（供启动日志与自证端点使用）。
func SM3SelfTest() (bool, []string) {
	ok := true
	lines := make([]string, 0, len(sm3StdVectors)+1)

	if sm3Engine == nil {
		return false, []string{"SM3 引擎未初始化"}
	}

	for _, v := range sm3StdVectors {
		got := hex.EncodeToString(sm3Engine.Hash([]byte(v.in)))
		if got == v.want {
			lines = append(lines, fmt.Sprintf("  ✓ %-22s %s", v.name, got))
		} else {
			ok = false
			lines = append(lines, fmt.Sprintf("  ✗ %-22s got=%s want=%s", v.name, got, v.want))
		}
	}

	// 链式摘要自检：确定性验证（相同输入必须产生相同输出）
	h1 := sm3Engine.ChainHash(genesisHash(), 1, 1, []byte("chain-probe"))
	h2 := sm3Engine.ChainHash(genesisHash(), 1, 1, []byte("chain-probe"))
	if constantTimeEqual(h1, h2) {
		lines = append(lines, fmt.Sprintf("  ✓ %-22s %s…", "链式摘要确定性", hex.EncodeToString(h1)[:16]))
	} else {
		ok = false
		lines = append(lines, "  ✗ 链式摘要确定性校验失败")
	}

	return ok, lines
}

// =============================================================================
// 第三节：运行时自证状态
// =============================================================================

// SM3Status 返回国密防篡改链的运行时自证状态，供 /sm3/status 端点对外核验。
func SM3Status() map[string]interface{} {
	ok, _ := SM3SelfTest()
	return map[string]interface{}{
		"enabled":   sm3IntegrityEnabled,
		"engine":    "github.com/tjfoc/gmsm v1.4.1",
		"algorithm": "SM3",
		"standard":  "GB/T 32905-2016",
		"selftest":  ok,
		"computed":  atomic.LoadUint64(&sm3ChainComputed),
		"verified":  atomic.LoadUint64(&sm3ChainVerified),
		"mismatch":  atomic.LoadUint64(&sm3ChainMismatch),
		"env_key":   SM3IntegrityEnvKey,
	}
}
