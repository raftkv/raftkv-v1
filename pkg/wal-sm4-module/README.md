# RaftKV Module02 — WAL + 国密 SM4 持久化存储引擎（独立闭环模块）

> 纯标准库零外部依赖 | SM4 自研（符合 GB/T 32907-2016）| WAL 1GB 预分配 + 批量 fsync | SM4-CTR + HMAC-SHA256 认证加密

---

## 1. 模块概述

本模块从 `raftkv_go_engine` 主工程中剥离 **WAL 预写式日志 + 国密 SM4 持久化存储引擎**，形成**独立闭环、零外部依赖**的纯 Go 标准库模块。

### 核心能力

| 能力 | 说明 |
|------|------|
| WAL 预写式日志 | 1GB 预分配 + 批量 fsync group commit（256 条/5ms 窗口），崩溃恢复 |
| 国密 SM4 自研 | 纯 Go 实现 SM4 分组密码（符合 GB/T 32907-2016），32 轮 Feistel 型迭代 |
| SM4-CTR 加密 | 每条记录随机 IV，SM4-CTR 流模式加密，机密性 |
| HMAC-SHA256 认证 | 完整性认证，防篡改，密文+IV 均参与 MAC 计算 |
| 透明叠加 | 加密层透明叠加在 WAL 之上：写入前加密，读取时解密 |
| 崩溃恢复 | 重启后扫描 WAL 有效记录，解密回放，完整恢复 |

### 与 Module01（Raft 共识引擎）的协调关系

| 方面 | Module01（Raft 引擎） | Module02（本模块） |
|------|----------------------|---------------------|
| 加密算法 | AES-128-GCM（标准库） | **国密 SM4（自研，符合 GB/T 32907-2016）** |
| WAL 架构 | 1GB 预分配 + 批量 fsync | 复用同一 WAL 架构，独立化去除 Raft 耦合 |
| 存储类型 | EncryptedStorage（Raft 专用） | SM4Storage（通用字节记录） |
| 完整性 | GCM 内置认证 | HMAC-SHA256 独立认证 |
| 依赖 | 零外部依赖 | 零外部依赖 |

### 改造要点（零外部依赖 + 国密合规）

| 原依赖 | 改造为 | 说明 |
|--------|--------|------|
| `github.com/tjfoc/gmsm/sm4` | **自研 SM4**（sm4.go + sm4_cipher.go） | 符合 GB/T 32907-2016，KAT 标准测试向量验证通过 |
| 第三方加密库 | **禁止** | SM4 必须自研，零第三方加密库 |
| Raft 类型耦合 | 去除 | WAL 存储通用 `WALEntry` 字节记录，不依赖 Raft 类型 |

---

## 2. 文件清单

```
pkg/wal-sm4-module/
├── go.mod                        # 模块定义（零 require，纯标准库）
├── sm4.go                        # 国密 SM4 分组密码自研核心（S 盒/L/T/密钥扩展/加解密）
├── sm4_cipher.go                 # SM4 cipher.Block 接口适配（配合 crypto/cipher CTR 模式）
├── sm4_storage.go                # SM4-CTR + HMAC-SHA256 认证加密存储层
├── wal.go                        # WAL 预写式日志（1GB 预分配 + 批量 fsync group commit）
├── wal-sm4-test-linux-arm64      # 交叉编译产物（GOOS=linux GOARCH=arm64）
├── cmd/
│   └── wal-sm4-test/
│       └── main.go               # 独立沙箱验证测试程序（SM4 自测 + 明文/加密 WAL）
└── README.md                     # 本说明文档
```

---

## 3. 架构设计

### 3.1 分层架构

```
┌─────────────────────────────────────────────┐
│         cmd/wal-sm4-test/main.go            │  沙箱测试驱动
├─────────────────────────────────────────────┤
│         sm4_storage.go (加密存储层)          │  SM4-CTR + HMAC-SHA256 认证加密
│  ┌─────────────────────────────────────┐    │
│  │  sm4.go + sm4_cipher.go (SM4 自研)  │    │  国密 SM4 分组密码（GB/T 32907-2016）
│  └─────────────────────────────────────┘    │
├─────────────────────────────────────────────┤
│            wal.go (WAL 持久化层)             │  1GB 预分配 + 批量 fsync + 崩溃恢复
└─────────────────────────────────────────────┘
```

### 3.2 国密 SM4 算法结构

- **分组/密钥长度**：128 位（16 字节）
- **轮数**：32 轮 Feistel 型迭代
- **非线性变换 τ**：4 个 S 盒并行（GB/T 32907-2016 固定 S 盒）
- **线性变换 L**：B = X ⊕ (X<<<2) ⊕ (X<<<10) ⊕ (X<<<18) ⊕ (X<<<24)
- **合成置换 T** = L ∘ τ
- **轮函数 F**：F(X0,X1,X2,X3,rk) = X0 ⊕ T(X1 ⊕ X2 ⊕ X3 ⊕ rk)
- **密钥扩展**：FK 固定参数 + CK 固定参数，轮密钥 rk_i 由 K_i 递推，使用 L' = B ⊕ (B<<<13) ⊕ (B<<<23)
- **解密**：与加密结构相同，轮密钥逆序使用 (rk31, rk30, ..., rk0)

### 3.3 SM4-CTR + HMAC-SHA256 认证加密流程

```
写入：
  plaintext → SM4-CTR(随机 IV) 加密 → IV(16B) + ciphertext
            → HMAC-SHA256(hmacKey, IV||ciphertext) → tag(32B)
            → 输出 = IV(16B) + ciphertext + tag(32B) → 写入 WAL

读取：
  WAL 记录 → 解析 IV(16B) + ciphertext + tag(32B)
           → 验证 HMAC-SHA256(hmacKey, IV||ciphertext) == tag
           → SM4-CTR(IV) 解密 → plaintext
```

### 3.4 WAL 持久化流程

```
Append(entry) → 批量缓冲（攒批 256 条或 5ms 窗口）
             → doFlush: [4 字节大端长度][JSON payload] 写入文件
             → file.Sync()（单次 fsync group commit）

Replay() → 扫描文件 [长度前缀][payload]
        → 遇到 length=0（预分配零区域）停止
        → 返回所有有效记录
```

---

## 4. 纯标准库零依赖声明

`go.mod` 内容：

```
module raftkv/wal-sm4-module

go 1.21
```

**零 `require`**，仅使用 Go 标准库：

| 标准库包 | 用途 |
|----------|------|
| `crypto/cipher` | CTR 流模式（NewCTR） |
| `crypto/hmac` | HMAC-SHA256 完整性认证 |
| `crypto/sha256` | SHA-256 哈希 |
| `crypto/rand` | 随机 IV 生成 |
| `encoding/binary` | 大端序整数编码 |
| `encoding/json` | WAL 记录序列化 |
| `os` + `io` | WAL 文件 IO |
| `sync` | 并发控制 |
| `time` | 批量 fsync 定时窗口 |
| `fmt` | 错误格式化 |

**SM4 完全自研**，未引入任何第三方加密库（`github.com/tjfoc/gmsm` 等均未使用）。

---

## 5. 运行方式

### 5.1 本地沙箱跑测

```bash
cd pkg/wal-sm4-module
go run cmd/wal-sm4-test/main.go
```

### 5.2 交叉编译（linux/arm64）

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o wal-sm4-test-linux-arm64 ./cmd/wal-sm4-test/
```

### 5.3 静态检查

```bash
go vet ./...
go build ./...
```

---

## 6. 沙箱验证结果

### 结论：✅ PASS

| 验证项 | 结果 |
|--------|------|
| SM4 KAT 标准测试向量（GB/T 32907-2016 附录 A） | ✅ 通过（密文 681edf34d206965e86b3e94f536e4246） |
| SM4 解密还原明文 | ✅ 通过 |
| SM4 多轮随机明文加解密一致性（100 轮） | ✅ 通过（100/100） |
| SM4 单分组加解密原语 | ✅ 通过 |
| 明文 WAL 写入 10 条日志 + 持久化落盘 | ✅ 通过 |
| 明文 WAL 重启恢复 10 条日志完整且内容一致 | ✅ 通过 |
| SM4 加密 WAL 写入 10 条日志 + 持久化落盘 | ✅ 通过 |
| SM4 加密 WAL 重启恢复 10 条日志完整且内容一致 | ✅ 通过 |
| 磁盘文件确认为 SM4 密文（明文标记不可见） | ✅ 通过 |
| 对照：明文 WAL 为 base64 可逆编码 vs 加密 WAL 为 SM4 密文 | ✅ 通过 |

### 关键指标

- SM4 KAT 密文：**681edf34d206965e86b3e94f536e4246**（与 GB/T 32907-2016 标准一致）
- SM4 随机明文加解密一致性：**100/100 轮通过**
- 明文 WAL 10 条日志恢复：**全部成功，内容一致**
- 加密 WAL 10 条日志恢复：**全部成功，内容一致**
- 磁盘密文确认：**明文标记不可见，落盘为 SM4 密文**

---

## 7. MD5 校验清单

| 文件 | 大小（字节） | MD5 |
|------|-------------|-----|
| go.mod | 753 | `3F547B13A632699665E403EDDABEEB0E` |
| sm4.go | 9,643 | `3E283D9E61DC44C621CE586A64C38FFB` |
| sm4_cipher.go | 2,585 | `01B9A3F75833BC7D570287CCFF1C96AB` |
| sm4_storage.go | 7,222 | `AD37E49A800545318AE443609891477A` |
| wal.go | 8,068 | `512EDA06C7F0659552199A3A56CC8A13` |
| cmd/wal-sm4-test/main.go | 15,055 | `783DFD6EA0C5C121B7109EB66BA37E63` |
| wal-sm4-test-linux-arm64 | 3,341,929 | `B1983AD23CEC719DE55540401DDF41BA` |

---

## 8. 交叉编译产物

| 属性 | 值 |
|------|-----|
| 文件名 | wal-sm4-test-linux-arm64 |
| 目标平台 | linux/arm64 |
| 编译选项 | CGO_ENABLED=0 |
| 文件大小 | 3,341,929 字节（约 3.2 MB） |
| MD5 | B1983AD23CEC719DE55540401DDF41BA |

---

## 9. 最终结论

| 项目 | 结论 |
|------|------|
| 模块独立性 | ✅ PASS — 零外部依赖，纯标准库 |
| SM4 自研合规性 | ✅ PASS — 符合 GB/T 32907-2016，KAT 标准测试向量通过 |
| SM4 加解密正确性 | ✅ PASS — 100 轮随机明文加解密一致性通过 |
| 明文 WAL 持久化 | ✅ PASS — 10 条日志写入+重启恢复成功 |
| SM4 加密 WAL 持久化 | ✅ PASS — 10 条日志写入+重启恢复成功 |
| 磁盘密文确认 | ✅ PASS — 落盘为 SM4 密文，非明文 |
| 沙箱跑测 | ✅ PASS — 全部验证通过 |
| 交叉编译 | ✅ PASS — linux/arm64 产物生成 |
| **总体结论** | **✅ PASS — WAL + 国密 SM4 持久化存储引擎独立闭环模块交付合格** |