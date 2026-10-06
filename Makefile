=============================================================================
Makefile — 分布式键值存储·微服务网关
=============================================================================
RaftKV Deterministic Engine
=============================================================================
APP_NAME := deterministic-gateway VERSION := 0.2.0 BUILD_TIME := 
(
s
h
e
l
l
d
a
t
e
−
u
+
"
G
I
T
C
O
M
M
I
T
:
=
(shelldate−u+"GIT 
C
​
 OMMIT:=(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown") LDFLAGS := -s -w
-X main.Version=
(
V
E
R
S
I
O
N
)
 
−
X
m
a
i
n
.
B
u
i
l
d
T
i
m
e
=
(VERSION) −Xmain.BuildTime=(BUILD_TIME)
-X main.GitCommit=$(GIT_COMMIT)

GO := go GOFLAGS := -ldflags="$(LDFLAGS)"

=============================================================================
默认目标
=============================================================================
.DEFAULT_GOAL := help

.PHONY: help help: ## 显示帮助信息 @echo "分布式键值存储·微服务网关 v$(VERSION)" @echo "" @echo "构建命令:" @echo " make build 编译二进制（本机架构）" @echo " make build-arm64 编译 ARM64 二进制（鲲鹏）" @echo " make build-linux 编译 Linux amd64 二进制" @echo "" @echo "运行命令:" @echo " make run 单节点运行（开发模式）" @echo " make test-cluster 单机 3 节点测试集群" @echo "" @echo "Docker 命令:" @echo " make docker-build 构建 Docker 镜像" @echo " make docker-up 启动 3 节点 Docker 集群" @echo " make docker-down 关闭 Docker 集群" @echo " make docker-logs 查看 Docker 集群日志" @echo "" @echo "其他:" @echo " make proto 生成 protobuf Go 代码" @echo " make clean 清理构建产物" @echo " make tidy 整理 Go 依赖"

=============================================================================
构建
=============================================================================
.PHONY: build build: ## 编译本机架构二进制 @echo "编译 
(
A
P
P
N
A
M
E
)
(
本机架构
)
.
.
.
"
C
G
O
E
N
A
B
L
E
D
=
0
(APP 
N
​
 AME)(本机架构)..."CGO 
E
​
 NABLED=0(GO) build 
(
G
O
F
L
A
G
S
)
−
o
b
i
n
/
(GOFLAGS)−obin/(APP_NAME) .

.PHONY: build-arm64 build-arm64: ## 编译 Linux ARM64 二进制（华为云鲲鹏） @echo "编译 
(
A
P
P
N
A
M
E
)
(
l
i
n
u
x
/
a
r
m
64
)
.
.
.
"
C
G
O
E
N
A
B
L
E
D
=
0
G
O
O
S
=
l
i
n
u
x
G
O
A
R
C
H
=
a
r
m
64
(APP 
N
​
 AME)(linux/arm64)..."CGO 
E
​
 NABLED=0GOOS=linuxGOARCH=arm64(GO) build 
(
G
O
F
L
A
G
S
)
−
o
b
i
n
/
(GOFLAGS)−obin/(APP_NAME)-arm64 .

.PHONY: build-linux build-linux: ## 编译 Linux amd64 二进制 @echo "编译 
(
A
P
P
N
A
M
E
)
(
l
i
n
u
x
/
a
m
d
64
)
.
.
.
"
C
G
O
E
N
A
B
L
E
D
=
0
G
O
O
S
=
l
i
n
u
x
G
O
A
R
C
H
=
a
m
d
64
(APP 
N
​
 AME)(linux/amd64)..."CGO 
E
​
 NABLED=0GOOS=linuxGOARCH=amd64(GO) build 
(
G
O
F
L
A
G
S
)
−
o
b
i
n
/
(GOFLAGS)−obin/(APP_NAME)-linux .

.PHONY: build-all build-all: build build-arm64 build-linux ## 编译所有平台

=============================================================================
运行
=============================================================================
.PHONY: run run: build ## 单节点运行 @echo "启动单节点..." ./bin/$(APP_NAME) -config config.example.yaml -id 1

.PHONY: test-cluster test-cluster: build ## 单机 3 节点测试集群 @echo "启动 3 节点测试集群..." ./bin/$(APP_NAME) -test-cluster

=============================================================================
Docker
=============================================================================
.PHONY: docker-build docker-build: ## 构建 Docker 镜像 @echo "构建 Docker 镜像..." docker build
--build-arg VERSION=
(
V
E
R
S
I
O
N
)
 
−
−
b
u
i
l
d
−
a
r
g
B
U
I
L
D
T
I
M
E
=
(VERSION) −−build−argBUILD 
T
​
 IME=(BUILD_TIME)
--build-arg GIT_COMMIT=
(
G
I
T
C
O
M
M
I
T
)
 
−
t
(GIT 
C
​
 OMMIT) −t(APP_NAME):
(
V
E
R
S
I
O
N
)
 
−
t
(VERSION) −t(APP_NAME):latest
.

.PHONY: docker-build-arm64 docker-build-arm64: ## 构建 ARM64 Docker 镜像 @echo "构建 ARM64 Docker 镜像（鲲鹏）..." docker buildx build --platform linux/arm64
--build-arg VERSION=
(
V
E
R
S
I
O
N
)
 
−
t
(VERSION) −t(APP_NAME):$(VERSION)-arm64
--load .

.PHONY: docker-up docker-up: ## 启动 3 节点 Docker 集群 @echo "启动 Docker Compose 集群..." docker compose up -d @echo "" @echo "等待服务就绪..." @sleep 8 @echo "" @echo "═══ 集群状态 ═══" @curl -s http://localhost:9001/cluster 2>/dev/null || echo "等待服务启动中..." @echo "" @echo "HTTP API: http://localhost:9001" @echo "gRPC: localhost:9501" @echo "集群状态: curl http://localhost:9001/cluster" @echo "日志: make docker-logs"

.PHONY: docker-down docker-down: ## 关闭 Docker 集群 @echo "关闭 Docker Compose 集群..." docker compose down -v

.PHONY: docker-logs docker-logs: ## 查看 Docker 集群日志 docker compose logs -f

.PHONY: docker-status docker-status: ## 查看集群状态 @echo "═══ Node 1 ═══" @curl -s http://localhost:9001/health | python3 -m json.tool 2>/dev/null || echo "未就绪" @echo "" @echo "═══ Node 2 ═══" @curl -s http://localhost:9002/health 2>/dev/null || echo "未就绪" @echo "" @echo "═══ Node 3 ═══" @curl -s http://localhost:9003/health 2>/dev/null || echo "未就绪"

=============================================================================
代码生成
=============================================================================
.PHONY: proto proto: ## 生成 protobuf Go 代码 @echo "生成 protobuf Go 代码..." @if command -v protoc >/dev/null 2>&1; then
protoc --go_out=. --go_opt=paths=source_relative
--go-grpc_out=. --go-grpc_opt=paths=source_relative
proto/control_center.proto;
echo "protobuf 代码已生成";
else
echo "错误: protoc 未安装，请先安装 Protocol Buffers 编译器";
echo " macOS: brew install protobuf";
echo " Linux: apt install protobuf-compiler";
echo " 然后: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest";
echo " go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest";
exit 1;
fi

=============================================================================
工具
=============================================================================
.PHONY: tidy tidy: ## 整理 Go 依赖 $(GO) mod tidy

.PHONY: fmt fmt: ## 格式化代码 $(GO) fmt ./...

.PHONY: vet vet: ## 静态分析 $(GO) vet ./...

.PHONY: clean clean: ## 清理构建产物 @echo "清理构建产物..." rm -rf bin/ data/ @echo "清理完成"

.PHONY: dev-setup dev-setup: ## 安装开发工具 
(
G
O
)
i
n
s
t
a
l
l
g
o
o
g
l
e
.
g
o
l
a
n
g
.
o
r
g
/
p
r
o
t
o
b
u
f
/
c
m
d
/
p
r
o
t
o
c
−
g
e
n
−
g
o
@
l
a
t
e
s
t
(GO)installgoogle.golang.org/protobuf/cmd/protoc−gen−go@latest(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest $(GO) install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest @echo "开发工具安装完成"

.PHONY: build-release
build-release: ## 编译加固版（garble 混淆 + 符号剥离）
	@echo "编译加固版 $(APP_NAME)..."
	@if ! command -v garble >/dev/null 2>&1; then echo "安装 garble..."; go install mvdan.cc/garble@latest; fi
	CGO_ENABLED=0 garble -literals -tiny -seed=random go build -ldflags="$(LDFLAGS)" -o bin/$(APP_NAME) .
	@echo "加固编译完成: bin/$(APP_NAME)"