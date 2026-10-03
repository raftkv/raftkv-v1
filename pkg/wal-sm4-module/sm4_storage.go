// =========================================================================
// RaftKV Module02 — SM4-CTR + HMAC-SHA256 认证加密存储层
//
// 设计要点（国密 SM4 落盘加密，替代 Module01 的 AES-128-GCM）：
//   1. 自研 SM4 分组密码（符合 GB/T 32907-2016）+ crypto/cipher CTR 流模式
//   2. 每条记录随机 IV（16 字节），SM4-CTR 加密提供机密性
//   3. HMAC-SHA256 提供完整性认证（密文 + IV 均参与 MAC 计算）
//   4. 透明叠加在 WAL 之上：写入前加密，读取时解密
//
// 密文记录格式（每条）：
//   [IV(16B)][ciphertext][HMAC-Tag(32B)]
//   - IV: 随机初始向量，每条记录独立
//   - ciphertext: SM4-CTR(plaintext) 长度与明文相同
//   - HMAC-Tag: HMAC-SHA256(hmacKey, IV||ciphertext) 32 字节
//
// 本文件为纯 Go 标准库自研，零外部依赖，零 CGO。
// =========================================================================

package walsm4

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	sm4IVSize   = 16 // SM4-CTR IV 长度 16 字节（等于分组长度）
	hmacTagSize = 32 // HMAC-SHA256 标签长度 32 字节
	hmacKeySize = 32 // HMAC-SHA256 密钥长度 32 字节
)

// =========================================================================
// SM4 加密存储（SM4-CTR + HMAC-SHA256 认证加密）
// =========================================================================

// SM4Storage SM4-CTR + HMAC-SHA256 认证加密存储层
// 透明叠加在 WAL 之上：写入前加密，读取时解密
type SM4Storage struct {
	wal       *WAL
	sm4Cipher *SM4Cipher
	hmacKey   []byte
}

// NewSM4Storage 创建 SM4 加密存储
//
//	walPath: WAL 文件路径
//	sm4Key:  16 字节 SM4 主密钥
//	hmacKey: 32 字节 HMAC-SHA256 密钥（完整性认证）
func NewSM4Storage(walPath string, sm4Key, hmacKey []byte) (*SM4Storage, error) {
	if len(sm4Key) != sm4KeySize {
		return nil, fmt.Errorf("SM4 密钥必须为 %d 字节，当前 %d 字节", sm4KeySize, len(sm4Key))
	}
	if len(hmacKey) != hmacKeySize {
		return nil, fmt.Errorf("HMAC 密钥必须为 %d 字节，当前 %d 字节", hmacKeySize, len(hmacKey))
	}

	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	sm4c, err := NewSM4Cipher(sm4Key)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("SM4 cipher 创建失败: %w", err)
	}

	return &SM4Storage{
		wal:       wal,
		sm4Cipher: sm4c,
		hmacKey:   append([]byte(nil), hmacKey...), // 防御性拷贝
	}, nil
}

// =========================================================================
// SM4-CTR + HMAC-SHA256 加解密原语
// =========================================================================

// sm4CTREncrypt SM4-CTR 加密
// 生成随机 IV，输出格式 = IV(16B) + ciphertext
func sm4CTREncrypt(block cipher.Block, plaintext []byte) ([]byte, error) {
	iv := make([]byte, sm4IVSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("IV 生成失败: %w", err)
	}

	stream := cipher.NewCTR(block, iv)
	ciphertext := make([]byte, len(plaintext))
	stream.XORKeyStream(ciphertext, plaintext)

	// 输出 = IV + ciphertext
	result := make([]byte, sm4IVSize+len(ciphertext))
	copy(result[:sm4IVSize], iv)
	copy(result[sm4IVSize:], ciphertext)
	return result, nil
}

// sm4CTRDecrypt SM4-CTR 解密
// 输入格式 = IV(16B) + ciphertext
func sm4CTRDecrypt(block cipher.Block, data []byte) ([]byte, error) {
	if len(data) < sm4IVSize {
		return nil, fmt.Errorf("密文长度不足，至少需要 %d 字节 IV", sm4IVSize)
	}

	iv := data[:sm4IVSize]
	ciphertext := data[sm4IVSize:]

	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(ciphertext))
	stream.XORKeyStream(plaintext, ciphertext)
	return plaintext, nil
}

// hmacSHA256 计算 HMAC-SHA256(key, data)
func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// =========================================================================
// 认证加密原语（SM4-CTR + HMAC-SHA256）
// =========================================================================

// authEncrypt 认证加密：SM4-CTR 加密 + HMAC-SHA256 认证
// 输出格式 = IV(16B) + ciphertext + HMAC-Tag(32B)
func authEncrypt(block cipher.Block, hmacKey, plaintext []byte) ([]byte, error) {
	// SM4-CTR 加密 → IV + ciphertext
	enc, err := sm4CTREncrypt(block, plaintext)
	if err != nil {
		return nil, err
	}

	// HMAC-SHA256(hmacKey, IV||ciphertext)
	tag := hmacSHA256(hmacKey, enc)

	// 输出 = IV + ciphertext + tag
	result := make([]byte, len(enc)+hmacTagSize)
	copy(result[:len(enc)], enc)
	copy(result[len(enc):], tag)
	return result, nil
}

// authDecrypt 认证解密：验证 HMAC + SM4-CTR 解密
// 输入格式 = IV(16B) + ciphertext + HMAC-Tag(32B)
func authDecrypt(block cipher.Block, hmacKey, data []byte) ([]byte, error) {
	if len(data) < sm4IVSize+hmacTagSize {
		return nil, fmt.Errorf("密文长度不足，至少需要 %d 字节", sm4IVSize+hmacTagSize)
	}

	enc := data[:len(data)-hmacTagSize]
	tag := data[len(data)-hmacTagSize:]

	// 验证 HMAC
	expectedTag := hmacSHA256(hmacKey, enc)
	if !hmac.Equal(tag, expectedTag) {
		return nil, fmt.Errorf("HMAC 认证失败，数据可能被篡改")
	}

	// SM4-CTR 解密
	return sm4CTRDecrypt(block, enc)
}

// =========================================================================
// 加密存储读写接口
// =========================================================================

// AppendEntry 加密后追加记录到 WAL
func (es *SM4Storage) AppendEntry(entry WALEntry) error {
	encrypted, err := authEncrypt(es.sm4Cipher, es.hmacKey, entry.Data)
	if err != nil {
		return fmt.Errorf("SM4 认证加密失败: %w", err)
	}

	return es.wal.Append(WALEntry{
		Index: entry.Index,
		Term:  entry.Term,
		Data:  encrypted,
	})
}

// AppendData 便捷方法：加密后追加原始字节载荷
func (es *SM4Storage) AppendData(data []byte) error {
	return es.AppendEntry(WALEntry{Data: data})
}

// ReplayAll 读取 WAL 并解密所有记录
func (es *SM4Storage) ReplayAll() ([]WALEntry, error) {
	entries, err := es.wal.Replay()
	if err != nil {
		return nil, err
	}

	result := make([]WALEntry, 0, len(entries))
	for _, entry := range entries {
		decrypted, err := authDecrypt(es.sm4Cipher, es.hmacKey, entry.Data)
		if err != nil {
			return nil, fmt.Errorf("SM4 认证解密失败 index=%d: %w", entry.Index, err)
		}
		result = append(result, WALEntry{
			Index: entry.Index,
			Term:  entry.Term,
			Data:  decrypted,
		})
	}
	return result, nil
}

// Close 关闭加密存储（含 WAL）
func (es *SM4Storage) Close() error {
	return es.wal.Close()
}

// WAL 返回底层 WAL 实例
func (es *SM4Storage) WAL() *WAL {
	return es.wal
}
