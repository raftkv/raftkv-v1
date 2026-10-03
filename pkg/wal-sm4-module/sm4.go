// =========================================================================
// RaftKV Module02 — 国密 SM4 分组密码自研实现（符合 GB/T 32907-2016）
//
// 算法参数：
//   - 分组长度: 128 位（16 字节）
//   - 密钥长度: 128 位（16 字节）
//   - 轮数:     32 轮
//   - S 盒:     8 进 8 出非线性置换（标准固定常量）
//
// 算法结构：
//   - 非线性变换 τ：4 个 S 盒并行
//   - 线性变换 L：B = X ⊕ (X<<<2) ⊕ (X<<<10) ⊕ (X<<<18) ⊕ (X<<<24)
//   - 轮函数 F：F(X0,X1,X2,X3,rk) = X0 ⊕ T(X1 ⊕ X2 ⊕ X3 ⊕ rk)
//   - 合成置换 T = L ∘ τ
//   - 密钥扩展：FK 固定参数 + CK 固定参数，轮密钥 rk_i 由 K_i 递推
//
// 加解密：
//   - 加密：Y = (X0,X1,X2,X3,X4,...,X35) 经 32 轮 Feistel 型迭代
//   - 解密：与加密结构相同，轮密钥逆序使用 (rk31, rk30, ..., rk0)
//
// 本实现为纯 Go 标准库自研，零外部依赖，零 CGO。
// =========================================================================

package walsm4

import (
	"encoding/binary"
	"fmt"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	sm4BlockSize = 16 // 分组长度 16 字节（128 位）
	sm4KeySize   = 16 // 密钥长度 16 字节（128 位）
	sm4Rounds    = 32 // 迭代轮数
)

// =========================================================================
// SM4 S 盒（GB/T 32907-2016 规定的固定非线性置换 SBOX）
// =========================================================================

var sm4SBox = [256]byte{
	0xd6, 0x90, 0xe9, 0xfe, 0xcc, 0xe1, 0x3d, 0xb7, 0x16, 0xb6, 0x14, 0xc2, 0x28, 0xfb, 0x2c, 0x05,
	0x2b, 0x67, 0x9a, 0x76, 0x2a, 0xbe, 0x04, 0xc3, 0xaa, 0x44, 0x13, 0x26, 0x49, 0x86, 0x06, 0x99,
	0x9c, 0x42, 0x50, 0xf4, 0x91, 0xef, 0x98, 0x7a, 0x33, 0x54, 0x0b, 0x43, 0xed, 0xcf, 0xac, 0x62,
	0xe4, 0xb3, 0x1c, 0xa9, 0xc9, 0x08, 0xe8, 0x95, 0x80, 0xdf, 0x94, 0xfa, 0x75, 0x8f, 0x3f, 0xa6,
	0x47, 0x07, 0xa7, 0xfc, 0xf3, 0x73, 0x17, 0xba, 0x83, 0x59, 0x3c, 0x19, 0xe6, 0x85, 0x4f, 0xa8,
	0x68, 0x6b, 0x81, 0xb2, 0x71, 0x64, 0xda, 0x8b, 0xf8, 0xeb, 0x0f, 0x4b, 0x70, 0x56, 0x9d, 0x35,
	0x1e, 0x24, 0x0e, 0x5e, 0x63, 0x58, 0xd1, 0xa2, 0x25, 0x22, 0x7c, 0x3b, 0x01, 0x21, 0x78, 0x87,
	0xd4, 0x00, 0x46, 0x57, 0x9f, 0xd3, 0x27, 0x52, 0x4c, 0x36, 0x02, 0xe7, 0xa0, 0xc4, 0xc8, 0x9e,
	0xea, 0xbf, 0x8a, 0xd2, 0x40, 0xc7, 0x38, 0xb5, 0xa3, 0xf7, 0xf2, 0xce, 0xf9, 0x61, 0x15, 0xa1,
	0xe0, 0xae, 0x5d, 0xa4, 0x9b, 0x34, 0x1a, 0x55, 0xad, 0x93, 0x32, 0x30, 0xf5, 0x8c, 0xb1, 0xe3,
	0x1d, 0xf6, 0xe2, 0x2e, 0x82, 0x66, 0xca, 0x60, 0xc0, 0x29, 0x23, 0xab, 0x0d, 0x53, 0x4e, 0x6f,
	0xd5, 0xdb, 0x37, 0x45, 0xde, 0xfd, 0x8e, 0x2f, 0x03, 0xff, 0x6a, 0x72, 0x6d, 0x6c, 0x5b, 0x51,
	0x8d, 0x1b, 0xaf, 0x92, 0xbb, 0xdd, 0xbc, 0x7f, 0x11, 0xd9, 0x5c, 0x41, 0x1f, 0x10, 0x5a, 0xd8,
	0x0a, 0xc1, 0x31, 0x88, 0xa5, 0xcd, 0x7b, 0xbd, 0x2d, 0x74, 0xd0, 0x12, 0xb8, 0xe5, 0xb4, 0xb0,
	0x89, 0x69, 0x97, 0x4a, 0x0c, 0x96, 0x77, 0x7e, 0x65, 0xb9, 0xf1, 0x09, 0xc5, 0x6e, 0xc6, 0x84,
	0x18, 0xf0, 0x7d, 0xec, 0x3a, 0xdc, 0x4d, 0x20, 0x79, 0xee, 0x5f, 0x3e, 0xd7, 0xcb, 0x39, 0x48,
}

// 系统参数 FK（密钥扩展固定参数）
var sm4FK = [4]uint32{
	0xa3b1bac6, 0x56aa3350, 0x677d9197, 0xb27022dc,
}

// 固定参数 CK（密钥扩展固定参数）
var sm4CK = [32]uint32{
	0x00070e15, 0x1c232a31, 0x383f464d, 0x545b6269,
	0x70777e85, 0x8c939aa1, 0xa8afb6bd, 0xc4cbd2d9,
	0xe0e7eef5, 0xfc030a11, 0x181f262d, 0x343b4249,
	0x50575e65, 0x6c737a81, 0x888f969d, 0xa4abb2b9,
	0xc0c7ced5, 0xdce3eaf1, 0xf8ff060d, 0x141b2229,
	0x30373e45, 0x4c535a61, 0x686f767d, 0x848b9299,
	0xa0a7aeb5, 0xbcc3cad1, 0xd8dfe6ed, 0xf4fb0209,
	0x10171e25, 0x2c333a41, 0x484f565d, 0x646b7279,
}

// =========================================================================
// 基本运算：循环左移
// =========================================================================

// rotl32 将 32 位整数 x 循环左移 n 位
func rotl32(x uint32, n uint) uint32 {
	return (x << n) | (x >> (32 - n))
}

// =========================================================================
// 非线性变换 τ 与线性变换 L，合成置换 T = L ∘ τ
// =========================================================================

// tauNonlinear 非线性变换 τ：4 个 S 盒并行
// 输入 A = (a0, a1, a2, a3)，输出 B = (S[a0], S[a1], S[a2], S[a3])
func tauNonlinear(a uint32) uint32 {
	b0 := sm4SBox[byte(a>>24)]
	b1 := sm4SBox[byte(a>>16)]
	b2 := sm4SBox[byte(a>>8)]
	b3 := sm4SBox[byte(a)]
	return uint32(b0)<<24 | uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3)
}

// linearL 线性变换 L（用于加密轮函数）
// L(B) = B ⊕ (B<<<2) ⊕ (B<<<10) ⊕ (B<<<18) ⊕ (B<<<24)
func linearL(b uint32) uint32 {
	return b ^ rotl32(b, 2) ^ rotl32(b, 10) ^ rotl32(b, 18) ^ rotl32(b, 24)
}

// linearLPrime 线性变换 L'（用于密钥扩展）
// L'(B) = B ⊕ (B<<<13) ⊕ (B<<<23)
func linearLPrime(b uint32) uint32 {
	return b ^ rotl32(b, 13) ^ rotl32(b, 23)
}

// transformT 合成置换 T = L ∘ τ（用于加密轮函数）
func transformT(x uint32) uint32 {
	return linearL(tauNonlinear(x))
}

// transformTPrime 合成置换 T' = L' ∘ τ（用于密钥扩展）
func transformTPrime(x uint32) uint32 {
	return linearLPrime(tauNonlinear(x))
}

// =========================================================================
// 密钥扩展：由 16 字节主密钥生成 32 个轮密钥
// =========================================================================

// expandKey 由 16 字节主密钥生成 32 个轮密钥
func expandKey(key []byte) ([32]uint32, error) {
	if len(key) != sm4KeySize {
		return [32]uint32{}, fmt.Errorf("SM4 密钥必须为 %d 字节，当前 %d 字节", sm4KeySize, len(key))
	}

	// 将 16 字节密钥转为 4 个 32 位大端字 (K0, K1, K2, K3)
	k := [4]uint32{
		binary.BigEndian.Uint32(key[0:4]),
		binary.BigEndian.Uint32(key[4:8]),
		binary.BigEndian.Uint32(key[8:12]),
		binary.BigEndian.Uint32(key[12:16]),
	}

	// 与系统参数 FK 异或：K_i = K_i ⊕ FK_i
	k[0] ^= sm4FK[0]
	k[1] ^= sm4FK[1]
	k[2] ^= sm4FK[2]
	k[3] ^= sm4FK[3]

	// 递推生成轮密钥：rk_i = K_{i+4} = K_i ⊕ T'(K_{i+1} ⊕ K_{i+2} ⊕ K_{i+3} ⊕ CK_i)
	var rk [32]uint32
	for i := 0; i < sm4Rounds; i++ {
		tmp := k[1] ^ k[2] ^ k[3] ^ sm4CK[i]
		rk[i] = k[0] ^ transformTPrime(tmp)
		// 滑动窗口：丢弃 k[0]，追加 rk[i]
		k[0] = k[1]
		k[1] = k[2]
		k[2] = k[3]
		k[3] = rk[i]
	}
	return rk, nil
}

// =========================================================================
// 单分组加解密（16 字节 → 16 字节）
// =========================================================================

// cryptBlock 对单个 16 字节分组执行 32 轮迭代
// decrypt 为 true 时轮密钥逆序使用
func cryptBlock(rk [32]uint32, in []byte, decrypt bool) [16]byte {
	// 将 16 字节输入转为 4 个 32 位大端字 (X0, X1, X2, X3)
	x := [4]uint32{
		binary.BigEndian.Uint32(in[0:4]),
		binary.BigEndian.Uint32(in[4:8]),
		binary.BigEndian.Uint32(in[8:12]),
		binary.BigEndian.Uint32(in[12:16]),
	}

	// 32 轮迭代：X_{i+4} = X_i ⊕ T(X_{i+1} ⊕ X_{i+2} ⊕ X_{i+3} ⊕ rk_i)
	// 解密时轮密钥逆序：rk_31, rk_30, ..., rk_0
	for i := 0; i < sm4Rounds; i++ {
		var rki uint32
		if decrypt {
			rki = rk[sm4Rounds-1-i]
		} else {
			rki = rk[i]
		}
		tmp := x[1] ^ x[2] ^ x[3] ^ rki
		xnew := x[0] ^ transformT(tmp)
		// 滑动窗口
		x[0] = x[1]
		x[1] = x[2]
		x[2] = x[3]
		x[3] = xnew
	}

	// 反序输出：(X35, X34, X33, X32)
	var out [16]byte
	binary.BigEndian.PutUint32(out[0:4], x[3])
	binary.BigEndian.PutUint32(out[4:8], x[2])
	binary.BigEndian.PutUint32(out[8:12], x[1])
	binary.BigEndian.PutUint32(out[12:16], x[0])
	return out
}

// =========================================================================
// 对外公开的分组加解密原语
// =========================================================================

// SM4EncryptBlock 加密单个 16 字节分组
// key 必须为 16 字节，in 必须为 16 字节，返回 16 字节密文
func SM4EncryptBlock(key, in []byte) ([]byte, error) {
	if len(key) != sm4KeySize {
		return nil, fmt.Errorf("SM4 密钥必须为 %d 字节，当前 %d 字节", sm4KeySize, len(key))
	}
	if len(in) != sm4BlockSize {
		return nil, fmt.Errorf("SM4 明文分组必须为 %d 字节，当前 %d 字节", sm4BlockSize, len(in))
	}
	rk, err := expandKey(key)
	if err != nil {
		return nil, err
	}
	out := cryptBlock(rk, in, false)
	return out[:], nil
}

// SM4DecryptBlock 解密单个 16 字节分组
// key 必须为 16 字节，in 必须为 16 字节，返回 16 字节明文
func SM4DecryptBlock(key, in []byte) ([]byte, error) {
	if len(key) != sm4KeySize {
		return nil, fmt.Errorf("SM4 密钥必须为 %d 字节，当前 %d 字节", sm4KeySize, len(key))
	}
	if len(in) != sm4BlockSize {
		return nil, fmt.Errorf("SM4 密文分组必须为 %d 字节，当前 %d 字节", sm4BlockSize, len(in))
	}
	rk, err := expandKey(key)
	if err != nil {
		return nil, err
	}
	out := cryptBlock(rk, in, true)
	return out[:], nil
}

// SM4BlockSize 返回 SM4 分组长度（16 字节）
func SM4BlockSize() int { return sm4BlockSize }

// SM4KeySize 返回 SM4 密钥长度（16 字节）
func SM4KeySize() int { return sm4KeySize }
