package main

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

// TestSM3StdVectors 验证 SM3 算法本体符合 GB/T 32905-2016 官方测试向量。
func TestSM3StdVectors(t *testing.T) {
	ok, lines := SM3SelfTest()
	for _, l := range lines {
		t.Log(l)
	}
	if !ok {
		t.Fatal("SM3 标准测试向量自检失败")
	}
}

// TestNewStandardSM3Digest 直接校验算法输出与国标值逐位一致。
func TestNewStandardSM3Digest(t *testing.T) {
	h := NewStandardSM3()
	got := hex.EncodeToString(h.Hash([]byte("abc")))
	want := "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0"
	if got != want {
		t.Fatalf("SM3(\"abc\") = %s, want %s", got, want)
	}
	if len(h.Hash([]byte("x"))) != sm3DigestLen {
		t.Fatalf("摘要长度应为 %d 字节", sm3DigestLen)
	}
}

// TestEntrySM3RoundTrip 计算摘要后校验应通过。
func TestEntrySM3RoundTrip(t *testing.T) {
	prev := genesisHash()
	digest := ComputeEntrySM3(prev, 7, 42, []byte("payload-A"))
	if len(digest) != sm3DigestLen {
		t.Fatalf("摘要长度异常: %d", len(digest))
	}
	if !VerifyEntrySM3(prev, 7, 42, []byte("payload-A"), digest) {
		t.Fatal("合法条目校验应通过")
	}
}

// TestEntrySM3TamperDetection 篡改命令后校验必须失败——防篡改能力的核心断言。
func TestEntrySM3TamperDetection(t *testing.T) {
	prev := genesisHash()
	digest := ComputeEntrySM3(prev, 7, 42, []byte("payload-A"))

	// 构造非零前序哈希，避免与创世哈希（全零）撞车
	fakePrev := make([]byte, sm3DigestLen)
	for i := range fakePrev {
		fakePrev[i] = 0xAB
	}

	cases := []struct {
		name        string
		prev        []byte
		term, index int64
		cmd         []byte
	}{
		{"篡改命令载荷", prev, 7, 42, []byte("payload-B")},
		{"篡改任期号", prev, 8, 42, []byte("payload-A")},
		{"篡改索引号", prev, 7, 43, []byte("payload-A")},
		{"篡改前序哈希", fakePrev, 7, 42, []byte("payload-A")},
	}
	for _, c := range cases {
		if VerifyEntrySM3(c.prev, c.term, c.index, c.cmd, digest) {
			t.Fatalf("%s：校验本应失败却通过", c.name)
		}
	}
}

// TestEntrySM3BackwardCompat 未携带摘要的旧格式条目应放行（向后兼容策略）。
func TestEntrySM3BackwardCompat(t *testing.T) {
	if !VerifyEntrySM3(genesisHash(), 1, 1, []byte("legacy"), nil) {
		t.Fatal("未携带摘要的旧格式条目应放行")
	}
}

// TestPrevEntryHashChain 链式结构：前序摘要缺失时回落创世哈希。
func TestPrevEntryHashChain(t *testing.T) {
	if h := PrevEntryHash(nil, 1); len(h) != sm3DigestLen {
		t.Fatalf("首个条目前序应为创世哈希，长度 %d", len(h))
	}
	logs := []RaftLog{
		{Index: 1, Term: 1, Command: []byte("a"), SM3Hash: ComputeEntrySM3(genesisHash(), 1, 1, []byte("a"))},
	}
	got := PrevEntryHash(logs, 2)
	want := logs[0].SM3Hash
	if hex.EncodeToString(got) != hex.EncodeToString(want) {
		t.Fatal("索引 2 的前序哈希应等于索引 1 的摘要")
	}
	// 前序未携带摘要时回落创世哈希
	logs2 := []RaftLog{{Index: 1, Term: 1, Command: []byte("a")}}
	if h := PrevEntryHash(logs2, 2); !isAllZero(h) {
		t.Fatal("前序无摘要时应回落创世哈希")
	}
}

func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// TestUnifiedStateMachineDefaultHasher 修复验证：
// V2.5.0 中 hasher 为 nil 会导致 Apply 静默跳过校验；V2.5.1 必须注入国密实现。
func TestUnifiedStateMachineDefaultHasher(t *testing.T) {
	sm := NewUnifiedStateMachine(nil)
	if sm.SM3Hasher == nil {
		t.Fatal("hasher 为 nil 时应默认注入国密标准实现")
	}
	if _, ok := sm.SM3Hasher.(*StandardSM3); !ok {
		t.Fatalf("默认实现应为 *StandardSM3，实际为 %T", sm.SM3Hasher)
	}
}

// TestStateMachineApplyTamperRejected 状态机必须拒绝被篡改的命令。
func TestStateMachineApplyTamperRejected(t *testing.T) {
	sm := NewUnifiedStateMachine(NewStandardSM3())

	ru := &ResourceUnit{
		ID:          "ru-test-node-001",
		Name:        "test-node",
		ClusterID:   "cluster-test",
		ClusterType: "k8s",
	}
	cmd, err := NewCommand(CmdResourceRegister, "unit-test", 1, ru, genesisHash(), sm.SM3Hasher)
	if err != nil {
		t.Fatalf("构造命令失败: %v", err)
	}
	raw, err := cmd.ToBytes()
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	// 合法命令：apply 应成功
	if err := sm.Apply(raw); err != nil {
		t.Fatalf("合法命令 apply 失败: %v", err)
	}

	// 篡改载荷：apply 必须被国密校验拦截
	var tampered StateMachineCommand
	if err := json.Unmarshal(raw, &tampered); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	tamperedRU := &ResourceUnit{
		ID:          ru.ID,
		Name:        "TAMPERED-BY-REDTEAM",
		ClusterID:   ru.ClusterID,
		ClusterType: ru.ClusterType,
	}
	badPayload, _ := json.Marshal(tamperedRU)
	tampered.Payload = badPayload
	badRaw, _ := tampered.ToBytes()

	err = sm.Apply(badRaw)
	if err == nil {
		t.Fatal("被篡改的命令竟通过国密校验，防篡改能力未真正生效")
	}
	if !strings.Contains(err.Error(), "国密") {
		t.Fatalf("拒绝原因应为国密校验失败，实际: %v", err)
	}
	t.Logf("篡改拦截成功: %v", err)
}

// TestInitAdaptersInjectsSM3 修复验证：
// InitAdapters 此前全库零调用者且返回 nil 适配器；V2.5.1 必须真实装配。
func TestInitAdaptersInjectsSM3(t *testing.T) {
	sm, k8s, agent := InitAdapters(nil, nil)
	if sm == nil {
		t.Fatal("状态机不应为 nil")
	}
	if sm.SM3Hasher == nil {
		t.Fatal("未注入 hasher 时应默认使用国密标准实现")
	}
	if k8s == nil {
		t.Fatal("K8s 适配器不应为 nil（V2.5.0 恒返回 nil）")
	}
	if agent == nil {
		t.Fatal("Agent 适配器不应为 nil（V2.5.0 恒返回 nil）")
	}
}

// TestRaftNodeProposerAdapter 适配器应正确透出 Leader 与统计信息。
func TestRaftNodeProposerAdapter(t *testing.T) {
	p := NewRaftNodeProposer(nil)
	if p.IsLeader() {
		t.Fatal("空节点不应为 Leader")
	}
	if p.LeaderID() != "" {
		t.Fatal("空节点 LeaderID 应为空")
	}
	if p.Stats() != nil {
		t.Fatal("空节点统计应为 nil")
	}
	// 写入路径缺失应明确报错，而非静默返回 false
	if ok, err := p.Propose([]byte("x")); ok || err == nil {
		t.Fatal("写入路径缺失时应返回明确错误")
	}
}
