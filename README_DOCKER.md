# RaftKV 键值存储 — Docker 部署与验证手册

---

## 前置条件

- Docker Desktop 已安装并运行
- 终端 (PowerShell / CMD) 已打开
- **SM4_KEY**：必填。16 字节 SM4 加密密钥（32 字符 hex 编码）。缺失或无效时进程立即退出（fail-closed）
- **LICENSE_FAIL_MODE**：默认 `closed`（生产模式，需 license key）。评估模式设为 `open`（降级只读，写入被拒绝）

---

## 第一步：进入项目目录

```powershell
cd <path-to-cloned-repo>
```

确认文件齐全：

```powershell
# 应显示 9+ 个 .go 文件、Dockerfile、docker-compose.yml、go.mod、go.sum
dir *.go Dockerfile docker-compose.yml
```

---

## 第二步：构建并启动集群

```text
┌──────────────────────────────────────────────────────┐
│ ⚠ 生产集群正在运行                                    │
│ 本命令会重建镜像并可能                                │
│ 触发 tag 重定向 + 旧镜像 GC                           │
│ (2026-10-08 事故实录,                                 │
│  详见 verify/v-IMG1/)                                 │
│ 执行前须用户显式授权                                  │
└──────────────────────────────────────────────────────┘
```
> 注：`verify/v-IMG1/` 为审计取证目录（本地留存，不在本仓库内）；
> 其结论为：该次 `--build` 使 `raftkit-gateway:v1` 的 tag 重定向到新镜像 ID，
> 旧镜像清单对象被回收，容器仍以旧层运行。

### 2.1 环境变量说明（2026-10-10 更正）

> **⚠ 更正**：根目录 `docker-compose.yml` 已为每个节点**内置**所需环境变量
> （`NODE_ID` / `GRPC_PORT` / `HTTP_PORT` / `LICENSE_FAIL_MODE: "closed"` /
> `WAL_PATH: "/tmp/raft.wal"` / `PEERS`），**其中并没有 `SM4_KEY`**。
> Docker Compose **不会**把宿主机环境变量自动透传进容器，因此在**本（根
> compose）轨道**下执行 `export SM4_KEY=...` **对容器不生效**——
> `SM4_KEY` 属于『当前该 compose 轨道不适用』项。
> 需要自定义密钥时，请直接编辑 `docker-compose.yml` 对应服务的 `environment:`
> 段加入 `SM4_KEY: "<32位hex>"`，再重新 `up -d`。
>
> **同时须知（实况）**：该 compose 把 `LICENSE_FAIL_MODE` 硬编码为 `"closed"`
> 且**未挂载任何 license 目录** ⇒ 无有效 license 时进程按 fail-closed 拒绝启动，
> 而服务 `restart: always` 会使其**反复重启**。若只想做评估/只读演示，请改用
> [`examples/docker-compose-quickstart.yml`](examples/docker-compose-quickstart.yml)
> （`LICENSE_FAIL_MODE=open`，无需 license），或按
> [README 生产部署](README.md#5-node-docker-compose-production)
> 用 `tests/deploy/deploy.env` 提供 license。

本轨道**无需在宿主机设置任何环境变量**：

```powershell
# 无需 export：所有变量已在 docker-compose.yml 的 environment: 内定义
# 如需切换模式，请编辑该文件（而不是在此 export）
```

### 2.2 构建并启动

> ⚠ **高危命令**：`--build` 会重建镜像。若 compose 中某服务**同时**写了 `image:` 与
> `build:`，`--build` 会生成新镜像并把该 **tag 重定向到新镜像 ID**，旧镜像随即失去引用、
> 可能被内容存储回收（GC），**不可按 ID 恢复**。2026-10-08 事故实录详见
> `verify/v-IMG1/`（本地审计取证目录，不在本仓库内）。**执行前须用户显式授权。**

```powershell
docker-compose up -d --build
```

首次构建约 2-3 分钟（下载 Go 依赖 + 编译）。后续启动仅需 5 秒。

预期输出：
```
[+] Building 120.0s (15/15) FINISHED
[+] Running 6/6
 ✔ Container raft-node-1     Started
 ✔ Container raft-node-2     Started
 ✔ Container raft-node-3     Started
 ✔ Container raft-node-4     Started
 ✔ Container raft-node-5     Started
 ✔ Container raftkv-frontend   Started
```

---

## 第三步：验证五节点集群

### 3.1 健康检查

```powershell
curl http://localhost:9001/health/live
curl http://localhost:9002/health/live
curl http://localhost:9003/health/live
curl http://localhost:9004/health/live
curl http://localhost:9005/health/live
```

预期：五个节点均返回 `OK`

### 3.2 Raft 集群状态

```powershell
curl http://localhost:9001/raft/stats
```

预期输出：

```
id=node-1 state=Leader term=1 leader=node-1 commit=X applied=X logs=X peers=4 voted=node-1
```

关键字段：
- `state=Leader` → node-1 已当选 Leader
- `peers=4` → 四个 Follower 已连接
- `term≥1` → 已完成至少一轮选举

### 3.3 查看 Follower 状态

```powershell
curl http://localhost:9002/raft/stats
curl http://localhost:9003/raft/stats
curl http://localhost:9004/raft/stats
curl http://localhost:9005/raft/stats
```

预期：`state=Follower`，`leader=node-1`

---

## 第四步：验证前端大屏

浏览器打开：

```
http://localhost:8096
```

你应该看到RaftKV 键值存储大屏，包含：
- 左上：系统总览 KPI 指标
- 左中：确定性引擎参数
- 中上：7 步 Agent Pipeline
- 右上：实时事件审计
- 右下：系统性能与安全

DeepSeek 统一管控大屏：

```
http://localhost:8096/deepseek.html
```

---

## 第五步：验证降级联动（关键）

### 5.1 模拟节点故障

> ⚠ **高危命令**：`docker stop` 会**中断正在运行的同名节点**。若本机此前已运行过本
> compose（端口 9001-9005 / 9500-9505 被占用即为标志），该命令会直接停掉那批容器。
> **执行前须确认目标非在运行的生产集群，并取得用户显式授权。**

五节点集群的法定票数为 3，需停止 3 个节点才会失去仲裁。停止 node-2 / node-3 / node-4：

```powershell
docker stop raft-node-2 raft-node-3 raft-node-4
```

### 5.2 验证仲裁丢失

```powershell
curl http://localhost:9001/raft/stats
```

预期：`peers=1`（三个 Follower 离线，仅剩 node-5）

### 5.3 前端降级横幅

刷新前端大屏 `http://localhost:8096`，应看到：

```
⚠ 系统异常 — 降级只读模式
存活节点: 2/5 (法定票数需 ≥ 3) · 写操作已全部阻断 · 等待节点恢复
```

红色闪烁横幅，所有写操作按钮灰化不可点击。

### 5.4 恢复验证

```powershell
docker start raft-node-2 raft-node-3 raft-node-4
```

等待约 10 秒，刷新前端大屏——降级横幅自动消失，按钮恢复可用。

---

## 第六步：测试 API 写操作

### 6.1 正常模式（5 节点全部存活）

```powershell
# 查询 node-1 的 Raft 状态（返回 JSON）
# 说明：/raft/status 是只读状态端点，源码未校验 HTTP 方法，POST 与 GET 等价
curl -X POST http://localhost:9001/raft/status

# 推荐写法：GET
curl http://localhost:9001/raft/status
```

> **更正（2026-10-10）**：原文将本条描述为「注册一个模拟 GPU 节点 / 查询资源池」，
> 与本仓库源码不符——仓库中**不存在** GPU 节点或资源池 API，该命令实际只是一次
> 只读状态查询。描述已按实况更正。

### 6.2 降级模式（3 节点离线，5 节点集群失去仲裁）

```powershell
# ⚠ 高危：会中断同名在运行节点，执行前须用户显式授权
docker stop raft-node-2 raft-node-3 raft-node-4
curl http://localhost:9001/raft/stats
# 预期: peers=1，写操作被拒绝
```

---

## 常用命令速查

> ⚠ **以下标 `[高危]` 的命令会改变本机 Docker 状态**（停止/删除容器、重建镜像、删除数据卷）；
> 若本机已存在在运行集群，执行前须用户显式授权。

```powershell
# 查看所有容器状态（只读）
docker-compose ps

# 查看节点 1 日志
docker logs -f raft-node-1

# 查看所有节点日志
docker-compose logs -f

# [高危] 停止并移除容器与网络
docker-compose down

# [高危·最高] 停止、移除容器，并**删除数据卷**（WAL 数据一并丢失，不可恢复）
docker-compose down -v

# [高危] 重新构建（代码修改后）；见第二步顶部警示——可能触发 tag 重定向 + 旧镜像 GC
docker-compose up -d --build

# [高危] 启动全部 5 个节点（已内置 node-1 ~ node-5，无需再扩容）；同名容器已存在时会被替换
docker-compose up -d
```

---

## 端口映射速查

| 服务 | 容器内端口 | 宿主机端口 | 用途 |
|------|-----------|-----------|------|
| node-1 gRPC | 9500 | 9500 | Raft 共识通信 |
| node-1 HTTP | 9000 | 9001 | API / 健康检查 |
| node-2 gRPC | 9501 | 9501 | Raft 共识通信 |
| node-2 HTTP | 9000 | 9002 | API / 健康检查 |
| node-3 gRPC | 9502 | 9502 | Raft 共识通信 |
| node-3 HTTP | 9000 | 9003 | API / 健康检查 |
| node-4 gRPC | 9604 | 9604 | Raft 共识通信 |
| node-4 HTTP | 9000 | 9104 | API / 健康检查 |
| node-5 gRPC | 9605 | 9605 | Raft 共识通信 |
| node-5 HTTP | 9000 | 9105 | API / 健康检查 |
| frontend | 80 | 8096 | 前端大屏 |

---

## 故障排查

**容器启动失败：**
> ⚠ **高危**：下列 `down` 与 `up -d --build` 会移除容器并重建镜像，
> 见第二步顶部警示。执行前须用户显式授权。

```powershell
docker-compose logs node-1    # 查看具体错误（只读）
docker-compose down            # [高危] 移除容器与网络
docker-compose up -d --build   # [高危] 重建镜像（可能触发 tag 重定向 + 旧镜像 GC）
```

**端口被占用：**
```powershell
netstat -ano | findstr :9500   # 查找占用进程
taskkill /PID <PID> /F         # [高危] 强制终止进程；误杀会导致其他服务中断
```

**前端大屏不显示：**
```powershell
docker logs raftkv-frontend  # 检查 nginx 日志
```

---

## 附：3 节点 → 5 节点扩容（历史）

> **注（2026-10-10）**：`docker-compose.yml` 现**已内置 5 个节点**（node-1 ~ node-5，
> node-4/node-5 的 gRPC 端口为 9604/9605、HTTP 宿主机端口为 9104/9105），
> 因此本节无需再执行，仅作历史记录保留。

若确需自行把 3 节点配置扩展到 5 节点，需在 `docker-compose.yml` 中新增 `node-4` 和 `node-5`：

```yaml
  node-4:
    build: .
    image: raftkit-gateway:v1
    container_name: raft-node-4
    restart: always
    environment:
      - NODE_ID=node-4
      - GRPC_PORT=9503
      - HTTP_PORT=9000
      - PEERS=node-1=raft-node-1:9500,node-2=raft-node-2:9501,node-3=raft-node-3:9502,node-5=raft-node-5:9504
    ports:
      - "9503:9503"
      - "9004:9000"
    networks:
      - raftkv-net

  node-5:
    build: .
    image: raftkit-gateway:v1
    container_name: raft-node-5
    restart: always
    environment:
      - NODE_ID=node-5
      - GRPC_PORT=9504
      - HTTP_PORT=9000
      - PEERS=node-1=raft-node-1:9500,node-2=raft-node-2:9501,node-3=raft-node-3:9502,node-4=raft-node-4:9503
    ports:
      - "9504:9504"
      - "9005:9000"
    networks:
      - raftkv-net
```

> ⚠ **注意**：上面示例中每个服务**同时**写了 `build: .` 与 `image: raftkit-gateway:v1`。
> 一旦对该 compose 执行 `up -d --build`，该 tag 会被**重定向到新构建的镜像 ID**，
> 原先该 tag 指向的镜像将失去引用并可能被 GC 回收。若要保留原镜像，请先
> `docker tag` 另存一个独立标签，或为 `build:` 指定不同 tag。

同时更新 node-1/node-2/node-3 的 PEERS 环境变量，加入 node-4 和 node-5。

5 节点法定票数 = 3，可容忍 2 节点故障。
