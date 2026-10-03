# D3 F10/F11 根因分析与修复方案

## 1. 缺陷描述

| 用例 | 场景 | 预期 | 实际 |
|------|------|------|------|
| F10 | SM4_KEY 未设置 | 容器 exit≠0 (fail-closed) | 容器 running (3秒内未退出) |
| F11 | SM4_KEY 非法值 | 容器 exit≠0 (fail-closed) | 容器 running (3秒内未退出) |

## 2. 根因定位

### 2.1 SM4_KEY 校验代码存在且使用 log.Fatalf

`raft_pipeline.go:52-62` `loadSM4KeyFromEnv()` 函数：

```go
func loadSM4KeyFromEnv() []byte {
    v := os.Getenv("SM4_KEY")
    if v == "" {
        log.Fatalf("[pipeline] SM4_KEY 未设置，拒绝启动 (fail-closed): ...")
    }
    key, err := hex.DecodeString(v)
    if err != nil || len(key) != 16 {
        log.Fatalf("[pipeline] SM4_KEY 非法，拒绝启动 (fail-closed): ...")
    }
    return key
}
```

该函数在 SM4_KEY 缺失或非法时调用 `log.Fatalf`，正确地实现了 fail-closed 语义。

### 2.2 调用链

```
main.go:142  PipelineConfigFromEnv()
  → raft_pipeline.go:426  DefaultPipelineConfig()
    → raft_pipeline.go:69  loadSM4KeyFromEnv()
      → log.Fatalf  (SM4_KEY 缺失/非法时)
```

### 2.3 根因：校验位置靠后，3秒测试窗口内未到达

`main.go` 启动流程中，`PipelineConfigFromEnv()` 在 L142 调用，**之前**有大量耗时初始化：

| 行号 | 操作 | 耗时因素 |
|------|------|----------|
| L48-54 | GRPC_PORT 校验 | 极快 |
| L66-99 | License 校验 | 中等（文件读取+签名验证） |
| L108-112 | Peer 连接 | **高**（网络连接，可能数秒） |
| L114 | Raft 节点初始化 | 中等 |
| L122-133 | SM3 自检 | 中等 |
| L136 | 适配器初始化 | 中等 |
| **L142** | **PipelineConfigFromEnv → SM4_KEY 校验** | **此处才校验 SM4_KEY** |

F10/F11 测试脚本使用 `docker run -d` 启动容器后等待 **3秒** 即检查状态。此时容器可能仍在执行 L142 之前的耗时初始化（尤其是 Peer 连接），尚未到达 SM4_KEY 校验点，因此容器状态为 `running` 而非 `exited`。

### 2.4 对比：其他 fail-closed 校验为何 PASS

| 校验项 | 位置 | 耗时初始化之前 | F用例 | 结果 |
|--------|------|----------------|-------|------|
| GRPC_PORT | main.go L49-53 | 是 | F12 | PASS |
| LICENSE | main.go L79-95 | 是（peer连接之前） | F16 | PASS |
| **SM4_KEY** | **main.go L142** | **否（peer连接之后）** | **F10/F11** | **FAIL** |

## 3. 修复方案

### 3.1 方案：将 SM4_KEY 校验提前到 main.go 启动初期

在 `main.go` GRPC_PORT 校验之后（L54之后）立即添加 SM4_KEY 早期校验调用：

```go
// SM4_KEY 早期校验（fail-closed）：在 peer 连接等耗时初始化之前校验
_ = loadSM4KeyFromEnv()
```

此调用在 SM4_KEY 缺失/非法时立即 `log.Fatalf` 退出，不执行后续耗时初始化。

`PipelineConfigFromEnv()` 在 L142 仍会再次调用 `loadSM4KeyFromEnv()` 加载密钥，此处仅为早期前置校验。两次调用均为纯环境变量读取+hex解码，无副作用，开销可忽略。

### 3.2 影响范围

- 仅修改 `main.go`，新增 2 行代码
- 不修改 `raft_pipeline.go`（`loadSM4KeyFromEnv` 函数不变）
- 不修改任何测试脚本或断言
- 不影响正常启动流程（SM4_KEY 合法时早期校验通过，后续照常）

## 4. 验证计划

1. 修复后重测 F10/F11（r2-fix 轮次），确认容器在 3 秒内 exit≠0
2. 回归 F07/F08/F09 幂等三件套，确认无副作用
3. 修复 diff 单独 commit，不夹带其他变更

## 5. 时间线

- 2026-09-07 诊断完成，根因定位
- 2026-09-07 修复实施 + 重测