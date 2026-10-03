// =========================================================================
// RaftKV Module02 — SM4 分组密码 cipher.Block 接口适配
//
// 实现 Go 标准 crypto/cipher.Block 接口，使自研 SM4 可直接配合
// crypto/cipher 的 CTR / CBC / OFB 等分组模式使用。
//
// cipher.Block 接口：
//   - BlockSize() int
//   - Encrypt(dst, src []byte)
//   - Decrypt(dst, src []byte)
//
// 本实现为纯 Go 标准库自研，零外部依赖。
// =========================================================================

package walsm4

import (
	"fmt"
)

// =========================================================================
// SM4Cipher SM4 分组密码（实现 cipher.Block 接口）
// =========================================================================

// SM4Cipher 实现 crypto/cipher.Block 接口
// 持有预计算的 32 个轮密钥，避免每次加解密重复密钥扩展
type SM4Cipher struct {
	rk [32]uint32 // 预计算的 32 个轮密钥
}

// NewSM4Cipher 由 16 字节主密钥创建 SM4 分组密码实例
// 密钥预计算一次轮密钥，后续加解密直接复用
func NewSM4Cipher(key []byte) (*SM4Cipher, error) {
	rk, err := expandKey(key)
	if err != nil {
		return nil, err
	}
	return &SM4Cipher{rk: rk}, nil
}

// BlockSize 返回 SM4 分组长度（16 字节），实现 cipher.Block 接口
func (c *SM4Cipher) BlockSize() int {
	return sm4BlockSize
}

// Encrypt 加密单个分组，实现 cipher.Block 接口
// dst 与 src 可以为同一切片（原地加解密）
func (c *SM4Cipher) Encrypt(dst, src []byte) {
	if len(src) < sm4BlockSize {
		panic(fmt.Sprintf("SM4 Encrypt: src 长度不足 %d 字节，当前 %d", sm4BlockSize, len(src)))
	}
	if len(dst) < sm4BlockSize {
		panic(fmt.Sprintf("SM4 Encrypt: dst 长度不足 %d 字节，当前 %d", sm4BlockSize, len(dst)))
	}
	out := cryptBlock(c.rk, src, false)
	copy(dst, out[:])
}

// Decrypt 解密单个分组，实现 cipher.Block 接口
// dst 与 src 可以为同一切片（原地加解密）
func (c *SM4Cipher) Decrypt(dst, src []byte) {
	if len(src) < sm4BlockSize {
		panic(fmt.Sprintf("SM4 Decrypt: src 长度不足 %d 字节，当前 %d", sm4BlockSize, len(src)))
	}
	if len(dst) < sm4BlockSize {
		panic(fmt.Sprintf("SM4 Decrypt: dst 长度不足 %d 字节，当前 %d", sm4BlockSize, len(dst)))
	}
	out := cryptBlock(c.rk, src, true)
	copy(dst, out[:])
}

// KeySize 返回 SM4 密钥长度（16 字节）
func (c *SM4Cipher) KeySize() int {
	return sm4KeySize
}
