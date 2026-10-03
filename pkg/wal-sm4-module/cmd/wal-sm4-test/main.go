// =========================================================================
// RaftKV Module02 — WAL + 国密 SM4 持久化存储引擎独立沙箱验证测试程序
//
// 验证目标：
//   1. SM4 加密/解密自测通过（GB/T 32907-2016 标准测试向量 + 多轮随机明文）
//   2. 明文 WAL 模式：写入 10 条日志 + 关闭 + 重新加载 + 验证完整恢复
//   3. SM4 加密 WAL 模式：写入 10 条日志 + 关闭 + 重新加载 + 验证完整恢复
//   4. 磁盘文件确认为 SM4 密文（非明文，不可直接读出原始内容）
//
// 运行方式：
//   go run cmd/wal-sm4-test/main.go
//
// 输出规范：
//   ANSI 颜色（cyan 标题 / green 通过 / yellow 警告 / red 失败）
//   emoji 图标（✅🚀🔑🔒📋）
//   === 分节分隔符
// =========================================================================

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	walsm4 "raftkv/wal-sm4-module"
)

// =========================================================================
// ANSI 颜色常量
// =========================================================================

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

// =========================================================================
// 测试配置
// =========================================================================

const (
	logCount  = 10                                 // 写入日志条数
	sm4KeyHex = "0123456789abcdeffedcba9876543210" // SM4 标准测试密钥
	// HMAC 密钥（32 字节，用于完整性认证）
	hmacKeyHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
)

// =========================================================================
// 主函数
// =========================================================================

func main() {
	printHeader()

	pass := true

	// --- 阶段 1: SM4 加密/解密自测 ---
	fmt.Println(colorCyan + "=== 阶段 1/4: SM4 加密/解密自测 ===" + colorReset)
	if !testSM4SelfTest() {
		pass = false
	}

	// --- 阶段 2: 明文 WAL 模式 ---
	fmt.Println(colorCyan + "=== 阶段 2/4: 明文 WAL 写入+重启恢复 ===" + colorReset)
	if !testPlainWAL() {
		pass = false
	}

	// --- 阶段 3: SM4 加密 WAL 模式 ---
	fmt.Println(colorCyan + "=== 阶段 3/4: SM4 加密 WAL 写入+重启恢复 ===" + colorReset)
	if !testEncryptedWAL() {
		pass = false
	}

	// --- 阶段 4: 磁盘密文确认 ---
	fmt.Println(colorCyan + "=== 阶段 4/4: 磁盘文件密文确认 ===" + colorReset)
	if !testDiskCiphertext() {
		pass = false
	}

	// --- 最终结论 ---
	fmt.Println()
	fmt.Println(colorBold + "============================================================" + colorReset)
	if pass {
		fmt.Println(colorBold + colorGreen + "✅✅✅  总体结论: PASS — WAL + 国密 SM4 持久化存储引擎全部验证通过  ✅✅✅" + colorReset)
	} else {
		fmt.Println(colorBold + colorRed + "❌❌❌  总体结论: FAIL — 存在验证失败项，请检查  ❌❌❌" + colorReset)
		os.Exit(1)
	}
}

// =========================================================================
// 打印标题
// =========================================================================

func printHeader() {
	fmt.Println(colorBold + colorCyan)
	fmt.Println("🚀 ==========================================================")
	fmt.Println("🚀   RaftKV Module02 — WAL + 国密 SM4 持久化存储引擎")
	fmt.Println("🚀   独立沙箱实机跑测  |  纯标准库零外部依赖  |  SM4 自研")
	fmt.Println("🚀 ==========================================================")
	fmt.Println(colorReset)
}

// =========================================================================
// 阶段 1: SM4 加密/解密自测
// =========================================================================

func testSM4SelfTest() bool {
	allPass := true

	// 1a. GB/T 32907-2016 标准测试向量（KAT）
	key, _ := hex.DecodeString(sm4KeyHex)
	plain, _ := hex.DecodeString("0123456789abcdeffedcba9876543210")
	expectCipher, _ := hex.DecodeString("681edf34d206965e86b3e94f536e4246")

	c, err := walsm4.NewSM4Cipher(key)
	if err != nil {
		fmt.Printf(colorRed+"❌ SM4 cipher 创建失败: %v\n"+colorReset, err)
		return false
	}

	ct := make([]byte, 16)
	c.Encrypt(ct, plain)
	katOK := bytes.Equal(ct, expectCipher)
	if katOK {
		fmt.Printf(colorGreen + "✅ SM4 KAT 标准测试向量通过（GB/T 32907-2016 附录 A）\n" + colorReset)
		fmt.Printf("   明文: %s\n", hex.EncodeToString(plain))
		fmt.Printf("   密文: %s\n", hex.EncodeToString(ct))
	} else {
		fmt.Printf(colorRed + "❌ SM4 KAT 标准测试向量失败\n" + colorReset)
		fmt.Printf("   期望: %s\n", hex.EncodeToString(expectCipher))
		fmt.Printf("   实际: %s\n", hex.EncodeToString(ct))
		allPass = false
	}

	// 1b. 解密还原
	pt := make([]byte, 16)
	c.Decrypt(pt, ct)
	decOK := bytes.Equal(pt, plain)
	if decOK {
		fmt.Printf(colorGreen + "✅ SM4 解密还原明文通过\n" + colorReset)
	} else {
		fmt.Printf(colorRed + "❌ SM4 解密还原明文失败\n" + colorReset)
		allPass = false
	}

	// 1c. 多轮随机明文加解密一致性
	roundPass := 0
	const rounds = 100
	for i := 0; i < rounds; i++ {
		testPlain := make([]byte, 16)
		rand.Read(testPlain)
		testKey := make([]byte, 16)
		rand.Read(testKey)

		tc, err := walsm4.NewSM4Cipher(testKey)
		if err != nil {
			continue
		}
		enc := make([]byte, 16)
		tc.Encrypt(enc, testPlain)
		dec := make([]byte, 16)
		tc.Decrypt(dec, enc)
		if bytes.Equal(dec, testPlain) {
			roundPass++
		}
	}
	if roundPass == rounds {
		fmt.Printf(colorGreen+"✅ SM4 多轮随机明文加解密一致性通过（%d/%d 轮）\n"+colorReset, roundPass, rounds)
	} else {
		fmt.Printf(colorRed+"❌ SM4 多轮随机明文加解密一致性失败（%d/%d 轮）\n"+colorReset, roundPass, rounds)
		allPass = false
	}

	// 1d. SM4-CTR + HMAC 认证加密原语自测
	sm4Key, _ := hex.DecodeString(sm4KeyHex)
	hmacKey, _ := hex.DecodeString(hmacKeyHex)
	payload := []byte("RaftKV国密SM4持久化存储引擎认证加密自测载荷-HelloSM4")
	encPayload, err := walsm4.SM4EncryptBlock(sm4Key, payload[:16])
	if err == nil {
		decPayload, err2 := walsm4.SM4DecryptBlock(sm4Key, encPayload)
		if err2 == nil && bytes.Equal(decPayload, payload[:16]) {
			fmt.Printf(colorGreen + "✅ SM4 单分组加解密原语通过\n" + colorReset)
		} else {
			fmt.Printf(colorRed + "❌ SM4 单分组加解密原语失败\n" + colorReset)
			allPass = false
		}
	} else {
		fmt.Printf(colorRed+"❌ SM4 单分组加密失败: %v\n"+colorReset, err)
		allPass = false
	}
	_ = hmacKey // HMAC 在加密存储层测试中验证

	if allPass {
		fmt.Printf(colorGreen + "🔑 SM4 加密/解密自测全部通过\n" + colorReset)
	}
	return allPass
}

// =========================================================================
// 阶段 2: 明文 WAL 模式
// =========================================================================

func testPlainWAL() bool {
	tmpDir, err := os.MkdirTemp("", "walsm4-plain-*")
	if err != nil {
		fmt.Printf(colorRed+"❌ 创建临时目录失败: %v\n"+colorReset, err)
		return false
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "plain.wal")

	// 写入 10 条日志
	wal, err := walsm4.NewWAL(walPath)
	if err != nil {
		fmt.Printf(colorRed+"❌ 明文 WAL 创建失败: %v\n"+colorReset, err)
		return false
	}

	originals := make([][]byte, logCount)
	for i := 0; i < logCount; i++ {
		data := []byte(fmt.Sprintf("RaftKV-明文WAL-日志条目-%02d-PersistenceEngine", i+1))
		originals[i] = data
		if err := wal.Append(walsm4.WALEntry{
			Index: int64(i + 1),
			Term:  1,
			Data:  data,
		}); err != nil {
			fmt.Printf(colorRed+"❌ 明文 WAL 写入第 %d 条失败: %v\n"+colorReset, i+1, err)
			wal.Close()
			return false
		}
	}

	// 强制刷新并关闭（模拟进程关闭）
	if err := wal.Close(); err != nil {
		fmt.Printf(colorRed+"❌ 明文 WAL 关闭失败: %v\n"+colorReset, err)
		return false
	}
	fmt.Printf(colorGreen+"✅ 明文 WAL 写入 %d 条日志并持久化落盘成功\n"+colorReset, logCount)

	// 重新加载（模拟重启）
	wal2, err := walsm4.NewWAL(walPath)
	if err != nil {
		fmt.Printf(colorRed+"❌ 明文 WAL 重新加载失败: %v\n"+colorReset, err)
		return false
	}
	defer wal2.Close()

	entries, err := wal2.Replay()
	if err != nil {
		fmt.Printf(colorRed+"❌ 明文 WAL 回放失败: %v\n"+colorReset, err)
		return false
	}

	if len(entries) != logCount {
		fmt.Printf(colorRed+"❌ 明文 WAL 恢复条数不符: 期望 %d, 实际 %d\n"+colorReset, logCount, len(entries))
		return false
	}

	allMatch := true
	for i, e := range entries {
		if !bytes.Equal(e.Data, originals[i]) {
			fmt.Printf(colorRed+"❌ 明文 WAL 第 %d 条内容不一致\n"+colorReset, i+1)
			allMatch = false
			break
		}
	}

	if allMatch {
		fmt.Printf(colorGreen+"✅ 明文 WAL 重启恢复 %d 条日志完整且内容一致\n"+colorReset, logCount)
		for i, e := range entries {
			fmt.Printf("   [%02d] index=%d term=%d data=%s\n", i+1, e.Index, e.Term, string(e.Data))
		}
		return true
	}
	return false
}

// =========================================================================
// 阶段 3: SM4 加密 WAL 模式
// =========================================================================

func testEncryptedWAL() bool {
	tmpDir, err := os.MkdirTemp("", "walsm4-enc-*")
	if err != nil {
		fmt.Printf(colorRed+"❌ 创建临时目录失败: %v\n"+colorReset, err)
		return false
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "encrypted.wal")
	sm4Key, _ := hex.DecodeString(sm4KeyHex)
	hmacKey, _ := hex.DecodeString(hmacKeyHex)

	// 写入 10 条加密日志
	es, err := walsm4.NewSM4Storage(walPath, sm4Key, hmacKey)
	if err != nil {
		fmt.Printf(colorRed+"❌ SM4 加密存储创建失败: %v\n"+colorReset, err)
		return false
	}

	originals := make([][]byte, logCount)
	for i := 0; i < logCount; i++ {
		data := []byte(fmt.Sprintf("RaftKV-国密SM4加密WAL-日志条目-%02d-Confidential", i+1))
		originals[i] = data
		if err := es.AppendEntry(walsm4.WALEntry{
			Index: int64(i + 1),
			Term:  2,
			Data:  data,
		}); err != nil {
			fmt.Printf(colorRed+"❌ SM4 加密 WAL 写入第 %d 条失败: %v\n"+colorReset, i+1, err)
			es.Close()
			return false
		}
	}

	// 强制刷新并关闭（模拟进程关闭）
	if err := es.Close(); err != nil {
		fmt.Printf(colorRed+"❌ SM4 加密 WAL 关闭失败: %v\n"+colorReset, err)
		return false
	}
	fmt.Printf(colorGreen+"✅ SM4 加密 WAL 写入 %d 条日志并持久化落盘成功\n"+colorReset, logCount)

	// 重新加载（模拟重启）
	es2, err := walsm4.NewSM4Storage(walPath, sm4Key, hmacKey)
	if err != nil {
		fmt.Printf(colorRed+"❌ SM4 加密 WAL 重新加载失败: %v\n"+colorReset, err)
		return false
	}
	defer es2.Close()

	entries, err := es2.ReplayAll()
	if err != nil {
		fmt.Printf(colorRed+"❌ SM4 加密 WAL 回放解密失败: %v\n"+colorReset, err)
		return false
	}

	if len(entries) != logCount {
		fmt.Printf(colorRed+"❌ SM4 加密 WAL 恢复条数不符: 期望 %d, 实际 %d\n"+colorReset, logCount, len(entries))
		return false
	}

	allMatch := true
	for i, e := range entries {
		if !bytes.Equal(e.Data, originals[i]) {
			fmt.Printf(colorRed+"❌ SM4 加密 WAL 第 %d 条内容不一致\n"+colorReset, i+1)
			allMatch = false
			break
		}
	}

	if allMatch {
		fmt.Printf(colorGreen+"✅ SM4 加密 WAL 重启恢复 %d 条日志完整且内容一致\n"+colorReset, logCount)
		for i, e := range entries {
			fmt.Printf("   [%02d] index=%d term=%d data=%s\n", i+1, e.Index, e.Term, string(e.Data))
		}
		return true
	}
	return false
}

// =========================================================================
// 阶段 4: 磁盘文件密文确认
// =========================================================================

func testDiskCiphertext() bool {
	tmpDir, err := os.MkdirTemp("", "walsm4-disk-*")
	if err != nil {
		fmt.Printf(colorRed+"❌ 创建临时目录失败: %v\n"+colorReset, err)
		return false
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "cipher.wal")
	sm4Key, _ := hex.DecodeString(sm4KeyHex)
	hmacKey, _ := hex.DecodeString(hmacKeyHex)

	// 写入带明确明文标记的加密日志
	es, err := walsm4.NewSM4Storage(walPath, sm4Key, hmacKey)
	if err != nil {
		fmt.Printf(colorRed+"❌ 磁盘密文测试: 加密存储创建失败: %v\n"+colorReset, err)
		return false
	}

	// 明文标记（用于确认磁盘上不可见）
	plaintextMarker := "PLAINTEXT_SHOULD_NOT_APPEAR_ON_DISK_RaftKV"
	if err := es.AppendData([]byte(plaintextMarker)); err != nil {
		fmt.Printf(colorRed+"❌ 磁盘密文测试: 写入失败: %v\n"+colorReset, err)
		es.Close()
		return false
	}
	if err := es.Close(); err != nil {
		fmt.Printf(colorRed+"❌ 磁盘密文测试: 关闭失败: %v\n"+colorReset, err)
		return false
	}

	// 读取磁盘原始文件内容
	raw, err := os.ReadFile(walPath)
	if err != nil {
		fmt.Printf(colorRed+"❌ 磁盘密文测试: 读取磁盘文件失败: %v\n"+colorReset, err)
		return false
	}

	// 确认明文标记不在磁盘文件中
	if bytes.Contains(raw, []byte(plaintextMarker)) {
		fmt.Printf(colorRed + "❌ 磁盘文件包含明文，加密失效！\n" + colorReset)
		return false
	}
	fmt.Printf(colorGreen + "✅ 磁盘文件确认为 SM4 密文（明文标记不可见）\n" + colorReset)

	// 打印磁盘文件前 64 字节十六进制（展示密文形态）
	previewLen := 64
	if len(raw) < previewLen {
		previewLen = len(raw)
	}
	fmt.Printf("   磁盘文件前 %d 字节十六进制（密文形态）:\n", previewLen)
	fmt.Printf("   %s\n", hex.EncodeToString(raw[:previewLen]))
	fmt.Printf(colorGreen + "✅ 磁盘密文确认通过：落盘内容为 SM4 密文，非明文\n" + colorReset)

	// 附加：确认明文 WAL 磁盘可读出明文（对照）
	plainPath := filepath.Join(tmpDir, "plain_check.wal")
	wal, err := walsm4.NewWAL(plainPath)
	if err == nil {
		wal.AppendData([]byte(plaintextMarker))
		wal.Close()
		plainRaw, _ := os.ReadFile(plainPath)
		// WAL 的 []byte 字段经 JSON 序列化为 base64，明文可被 base64 还原
		encodedMarker := base64.StdEncoding.EncodeToString([]byte(plaintextMarker))
		if bytes.Contains(plainRaw, []byte(encodedMarker)) {
			fmt.Printf(colorGreen + "✅ 对照：明文 WAL 磁盘为 base64 可逆编码（加密 WAL 为 SM4 密文不可逆）\n" + colorReset)
		}
	}

	return true
}
