# D2 收官总报告（交付文档）

生成时间: 2026-09-07
用途: 半年后完全失忆也能照着重建RaftKV环境
状态: D2批次正式关账

---

## 1. 资产清单

### 1.1 Git 仓库
- 仓库路径: `.`
- 当前分支: `v1.0-dev`
- commit 总数: **59**
- tag 总数: **17**
- 远程: 无真远端（已放弃，见 §3）

### 1.2 Tag 清单（17个）

| 阶段 | tag | 说明 |
|---|---|---|
| v0.9 系 | v0.9.0-frozen | 冻结基线 |
| | v0.9.1-fixa ~ v0.9.6-fixg | bug-a/b/c/d/f/g 修复 |
| v1.0 系 | v1.0.0-dev1 ~ dev7 | 开发迭代 |
| | v1.0.0-d1 / d2 / d3 | 批次收官门禁 |

### 1.3 evidence 目录结构一览

`tests/evidence/` 按运行期产物分组（运行期不入 git，收官人工筛选后部分入档）：

```
tests/evidence/
├── base-*          baseline套件 t02-t05 运行证据（每次run生成）
├── health-*        health套件 t30-t33 运行证据
├── idem-*          idem套件 t10-t14 运行证据
├── snap-*          wal_snap套件 t20-t23 运行证据
├── run-*           knife_run 完整运行证据（含DECISION/PASS/FAIL）
├── pipeline-*      run_pipeline 完整运行证据（含DECISION.md）
├── soak-*          soak测试运行证据
├── deploy-verify/  D2-batch1部署验证证据（已筛选入档4份）
│   ├── step2-up-20260906_230231/deploy_up.log
│   ├── step3-verify-20260906_230624/verify.log  (9/9 PASS)
│   ├── step4-repeat-20260906_230722/verify.log  (9/9 PASS)
│   └── compose-config-output.txt
└── baseline-verify/ 基线验证
```

关键归档文件: `tests/evidence/pipeline-20260905_174307/DECISION.md`（D1-batch2 + D2-batch1决策记录）

---

## 2. 两条主线成果

### 2.1 batch0：资产保全

| 项 | 成果 |
|---|---|
| Docker迁移 | 82镜像从 C盘 `AppData\Local\Docker` 迁到 `D:\DockerData`（VHDX+目录联接），验证6/6通过 |
| evidence入git | 441文件全量入git（含progress.log/裁决材料），换机clone后audit 6/6通过 |
| C盘清理 | 释放 **42.24GB** |
| push白名单 | run_pipeline.sh加ALLOWED_REMOTE + safe_push函数 |
| 双机保护 | 本地裸仓库 V2.4_remote (D盘) + 换机验证 |

**C盘清理明细**:

| 类别 | 释放量 | 数据去向 |
|---|---|---|
| Docker迁移 | 24.51GB | D:\DockerData (VHDX+联接) |
| B类 (Temp+npm) | 3.88GB | <BACKUP_DIR>/quarantine |
| C类 (Desktop+取证) | 13.17GB | <BACKUP_DIR>/archive-20260906 |
| D类 (.jdks+go) | 0.92GB | 归档+隔离区 |

C盘: 205.60GB → 163.36GB

### 2.2 batch1：部署工程化

| 项 | 成果 |
|---|---|
| docker-compose.yml | 2节点集群拓扑声明 |
| deploy.env.example | 配置模板（占位符） |
| deploy_up.sh | 一键拉起（SM4_KEY双模式） |
| deploy_verify.sh | 一键验收（9断言全流程） |
| 验证 | 2轮9/9全绿，可重复性确认 |
| commit | `bbe96e3` |

**验证证据**:
- 步骤3: verify.log md5=ea6ab38d963e517b8aaaec0394ba216a (9/9 PASS)
- 步骤4: verify.log md5=7266d5a590940b3ede777206dd0dd68d (9/9 PASS)

---

## 3. 备份方案（已定稿）

### 决策
- **真远端push**: 评估后放弃（宿主机无GitHub网络 + gh认证操作成本高 + Docker容器内GitHub可达但宿主机不可达）
- **替代**: git bundle 单文件离线备份

### bundle 文件
- 路径: `<BACKUP_DIR>/backup\v24-full-backup-20260907.bundle`
- 大小: 26298277 字节 (25.08MB)
- refs: 36 (17分支 + 1远程 + 17tag + HEAD)
- verify: 通过（"is okay" + "complete history"）
- hash: sha1

### 恢复命令
```bash
git clone <bundle完整路径>
# 例: git clone "<BACKUP_DIR>/backup\v24-full-backup-20260907.bundle"
```

### 异地备份
将bundle文件复制到 U盘/移动硬盘/网盘 任意一处，即等效异地备份。

---

## 4. 已知限制与债务

### 4.1 bash入口限制
- `deploy_up.sh` / `deploy_verify.sh` 为bash脚本
- 本机WSL2无docker CLI，bash入口未直跑
- 功能逻辑由PowerShell (ps1) 等价包装执行，两轮9/9验证通过
- 后续在Linux/WSL有docker环境可直接运行bash脚本，无需ps1包装

### 4.2 首跑FAIL的bug及修复
- **现象**: deploy_verify step3首跑，断言 `entry[index=2] found=true` 返回 FAIL (actual为空)
- **根因**: PowerShell的JsonField函数正则表达式 `"\"$field\"`":([^,}]+)"` 转义错误，无法提取JSON字段
- **修复**: 改用 `[regex]::Match($json, '"'+$field+'":([^,}]+)')` 构造模式字符串
- **结果**: 修复后重跑 9/9 PASS

### 4.3 环境注意事项
- Windows Docker Desktop挂载文件有缓存延迟，编辑.gitignore后需 `git add -f` 补救
- 宿主机PowerShell变量`$_`/`$env:`易被bash工具转义，需写ps1脚本文件用`powershell -File`执行
- 宿主机无法直连GitHub，但Docker容器(WSL2网络)可达

---

## 5. 换机重建步骤（完整流程）

### 前置条件
- 目标机器有 Docker Desktop (Linux容器)
- 有 deploy.env 所需环境（license文件）

### 5.1 从bundle恢复仓库
```bash
git clone "<BACKUP_DIR>/backup\v24-full-backup-20260907.bundle"
# 或拷贝整个 V2.4_Performance_Sandbox 目录
```

### 5.2 恢复Docker镜像
镜像清单: `raftkv:latest-knife` (41.2MB) 为核心运行镜像
- 方式1: 从源机器 `docker save raftkv:latest-knife > image.tar` 导出，新机器 `docker load < image.tar`
- 方式2: 在新机器 `docker build -t raftkv:latest-knife -f Dockerfile .`

### 5.3 配置 deploy.env
```bash
cd tests/deploy
cp deploy.env.example deploy.env
# 编辑 deploy.env 填入:
#   IMAGE_NAME=raftkv:latest-knife
#   LICENSE_DIR=<license目录绝对路径>
#   SM4_KEY=  (留空自动生成复用)
```

### 5.4 拉起集群
```bash
cd tests/deploy
bash deploy_up.sh
# 预期输出: Leader=raft-node-1 after 5s
```

### 5.5 一键验收
```bash
bash deploy_verify.sh
# 预期: 9断言全绿 PASS=9 FAIL=0
# 验收项: 拉起集群→gRPC探针两节点SERVING→写10条→读回applied==commit→entry一致→follower同步→down -v→无残留
```

### 5.6 关账确认
- deploy_up.sh 集群就绪
- deploy_verify.sh 9/9 PASS
- docker ps -a 无残留 `raftkv-node` 容器

---

## 附: 关键commit记录

```
bbe96e3 D2-batch1: deploy engineering (compose+env+one-key up/verify), 2x9/9 repeatable, closes batch1
0432660 fix(ci): D2-batch0补漏 evidence缺失文件(progress.log等188文件)
64b5209 feat(ci): D2-batch0 资产保全 evidence入git+push白名单化
073f782 fix(ci): 收尾补丁 修正BRANCH=v1.0-dev + 重算封印值
714bf44 feat(ci): D1-batch3裁决材料正式存档
```

---

**D2批次正式关账。**