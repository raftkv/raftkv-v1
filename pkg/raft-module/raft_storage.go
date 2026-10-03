// =========================================================================
// RaftKV Module01 — AES-128-GCM 落盘加密存储层
//
// 改造三（白皮书 Phase 2 ✅ [可立刻试点]）：
//   1. AES-128-GCM 认证加密（标准库 crypto/aes + crypto/cipher）
//   2. 每条记录随机 Nonce，输出 = Nonce(12B) + ciphertext + GCM-Tag(16B)
//   3. 透明叠加在 WAL 之上：写入前加密，读取时解密
//
// 本文件从原 raftkv/raft_storage.go 改造：
//   - github.com/tjfoc/gmsm/sm4 (SM4-CTR 国密) → crypto/aes (AES-128-GCM 标准库)
//   - 保持相同的透明叠加架构和接口语义
//
// 设计原则：
//   - 纯 Go 标准库零 CGO，零外部依赖
//   - 每条记录独立 Nonce，GCM 提供机密性 + 完整性认证
// =========================================================================

package raft

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	aesKeySize   = 16 // AES-128 密钥长度 16 字节
	gcmNonceSize = 12 // GCM 标准 Nonce 长度 12 字节
)

// =========================================================================
// AES-128-GCM 加密存储
// =========================================================================

// EncryptedStorage AES-128-GCM 加密存储层
// 透明叠加在 WAL 之上：写入前加密，读取时解密
type EncryptedStorage struct {
	wal *WAL
	gcm cipher.AEAD
	key []byte
}

// NewEncryptedStorage 创建加密存储
//
//	walPath: WAL 文件路径
//	key: 16 字节 AES-128 密钥
func NewEncryptedStorage(walPath string, key []byte) (*EncryptedStorage, error) {
	if len(key) != aesKeySize {
		return nil, fmt.Errorf("AES 密钥必须为 %d 字节，当前 %d 字节", aesKeySize, len(key))
	}

	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("AES cipher 创建失败: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("GCM 创建失败: %w", err)
	}

	return &EncryptedStorage{
		wal: wal,
		gcm: gcm,
		key: key,
	}, nil
}

// =========================================================================
// AES-128-GCM 加解密原语
// =========================================================================

// aesGCMEncrypt AES-128-GCM 加密
// 生成随机 Nonce，输出格式 = Nonce(12B) + ciphertext + GCM-Tag(16B)
func aesGCMEncrypt(gcm cipher.AEAD, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, gcmNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("Nonce 生成失败: %w", err)
	}

	// GCM.Seal 会把 tag 附加在密文末尾
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)

	// 输出 = nonce + ciphertext(含 tag)
	result := make([]byte, gcmNonceSize+len(ciphertext))
	copy(result[:gcmNonceSize], nonce)
	copy(result[gcmNonceSize:], ciphertext)
	return result, nil
}

// aesGCMDecrypt AES-128-GCM 解密
// 输入格式 = Nonce(12B) + ciphertext + GCM-Tag(16B)
func aesGCMDecrypt(gcm cipher.AEAD, data []byte) ([]byte, error) {
	if len(data) < gcmNonceSize+gcm.Overhead() {
		return nil, fmt.Errorf("密文长度不足，至少需要 %d 字节", gcmNonceSize+gcm.Overhead())
	}

	nonce := data[:gcmNonceSize]
	ciphertext := data[gcmNonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("GCM 解密认证失败: %w", err)
	}
	return plaintext, nil
}

// =========================================================================
// 加密存储读写接口
// =========================================================================

// AppendRaftLog 加密后追加日志到 WAL
func (es *EncryptedStorage) AppendRaftLog(log RaftLog) error {
	data, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("序列化失败: %w", err)
	}

	encrypted, err := aesGCMEncrypt(es.gcm, data)
	if err != nil {
		return fmt.Errorf("AES-GCM 加密失败: %w", err)
	}

	return es.wal.Append(walEntry{
		Index: log.Index,
		Term:  log.Term,
		Data:  encrypted,
	})
}

// ReplayAll 读取 WAL 并解密所有日志
func (es *EncryptedStorage) ReplayAll() ([]RaftLog, error) {
	entries, err := es.wal.Replay()
	if err != nil {
		return nil, err
	}

	logs := make([]RaftLog, 0, len(entries))
	for _, entry := range entries {
		decrypted, err := aesGCMDecrypt(es.gcm, entry.Data)
		if err != nil {
			return nil, fmt.Errorf("AES-GCM 解密失败 index=%d: %w", entry.Index, err)
		}

		var log RaftLog
		if err := json.Unmarshal(decrypted, &log); err != nil {
			return nil, fmt.Errorf("反序列化失败 index=%d: %w", entry.Index, err)
		}
		logs = append(logs, log)
	}
	return logs, nil
}

// Close 关闭加密存储（含 WAL）
func (es *EncryptedStorage) Close() error {
	return es.wal.Close()
}

// WAL 返回底层 WAL 实例
func (es *EncryptedStorage) WAL() *WAL {
	return es.wal
}
