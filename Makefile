# =============================================================================
# Makefile — 分布式键值存储·微服务网关
# =============================================================================
# RaftKV Deterministic Engine
#
# ── v-REPAIR2 还原说明 ────────────────────────────────────────────────────────
# 本文件在公开仓中曾遭结构性损坏：每个字符被拆到独立一行、连字符 '-' 被替换为
# U+2212、部分 '$' 从变量引用中丢失，导致所有 target 均不可用。本次按 git 历史
# 中的字符流重组结果忠实还原，并修复上述损坏。
#
# 目标名与历史集合**逐一对应，未增未删**（共 20 个）：
#   help build build-arm64 build-linux build-all run test-cluster
#   docker-build docker-build-arm64 docker-up docker-down docker-logs docker-status
#   proto tidy fmt vet clean dev-setup build-release
#
# ── 已知语义缺陷（未修正，明确标注为「语义未验证」）────────────────────────────
# 以下 3 个 target 引用了本仓库中**不存在**的 flag 或文件。原样保留（不擅自
# 变更语义），运行时会失败。详见 verify/v-REPAIR2/ 报告。
#   run          引用 '-config' flag 与 'config.example.yaml'（二者均不存在）
#   test-cluster 引用 '-test-cluster' flag（不存在；main.go 仅有 -id/-port/-http/-peers/-production）
#   proto        引用 'proto/control_center.proto'（不存在；仓库仅有 proto/raftkv.proto，
#                且 CONTRIBUTING.md RL-07 规定 proto 文件已冻结、不得重新生成）
# ── 换行符提示 ────────────────────────────────────────────────────────────────
# 本仓库 .gitattributes 未声明 Makefile 的行尾策略，且 core.autocrlf 常为 true，
# 因此在 Windows 上检出时本文件可能被转成 CRLF。GNU make 在部分环境下会因 CRLF
# 报错（典型：'/bin/sh: \r: not found'）。若遇到该现象，请将本文件转为 LF：
#   git config core.autocrlf false && rm Makefile && git checkout -- Makefile
# 建议后续在 .gitattributes 增补一行（本批未改）：   Makefile eol=lf
# =============================================================================

APP_NAME   := deterministic-gateway
VERSION    := 0.2.0
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS    := -s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)
GO         := go
GOFLAGS    := -ldflags="$(LDFLAGS)"

# =============================================================================
# 默认目标
# =============================================================================
.DEFAULT_GOAL := help

.PHONY: help
help: ## 显示帮助信息
	@echo "分布式键值存储·微服务网关 v$(VERSION)"
	@echo ""
	@echo "构建命令:"
	@echo "  make build          编译二进制（本机架构）"
	@echo "  make build-arm64    编译 ARM64 二进制（鲲鹏）"
	@echo "  make build-linux    编译 Linux amd64 二进制"
	@echo ""
	@echo "运行命令:"
	@echo "  make run            单节点运行（开发模式）"
	@echo "  make test-cluster   单机 3 节点测试集群"
	@echo ""
	@echo "Docker 命令:"
	@echo "  make docker-build   构建 Docker 镜像"
	@echo "  make docker-up      启动 3 节点 Docker 集群"
	@echo "  make docker-down    关闭 Docker 集群"
	@echo "  make docker-logs    查看 Docker 集群日志"
	@echo ""
	@echo "其他:"
	@echo "  make proto          生成 protobuf Go 代码"
	@echo "  make clean          清理构建产物"
	@echo "  make tidy           整理 Go 依赖"

# =============================================================================
# 构建
# =============================================================================
.PHONY: build
build: ## 编译本机架构二进制
	@echo "编译 $(APP_NAME)（本机架构）..."
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -o bin/$(APP_NAME) .

.PHONY: build-arm64
build-arm64: ## 编译 Linux ARM64 二进制（华为云鲲鹏）
	@echo "编译 $(APP_NAME)（linux/arm64）..."
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build $(GOFLAGS) -o bin/$(APP_NAME)-arm64 .

.PHONY: build-linux
build-linux: ## 编译 Linux amd64 二进制
	@echo "编译 $(APP_NAME)（linux/amd64）..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -o bin/$(APP_NAME)-linux .

.PHONY: build-all
build-all: build build-arm64 build-linux ## 编译所有平台

# =============================================================================
# 运行
# =============================================================================
.PHONY: run
run: build ## 单节点运行（开发模式）
# 语义未验证: '-config' flag 与 'config.example.yaml' 在本仓库中均不存在
	@echo "启动单节点..."
	./bin/$(APP_NAME) -config config.example.yaml -id 1

.PHONY: test-cluster
test-cluster: build ## 单机 3 节点测试集群
# 语义未验证: '-test-cluster' flag 在本仓库中不存在
	@echo "启动 3 节点测试集群..."
	./bin/$(APP_NAME) -test-cluster

# =============================================================================
# Docker
# =============================================================================
.PHONY: docker-build
docker-build: ## 构建 Docker 镜像
	@echo "构建 Docker 镜像..."
	docker build --build-arg VERSION=$(VERSION) --build-arg BUILD_TIME=$(BUILD_TIME) --build-arg GIT_COMMIT=$(GIT_COMMIT) -t $(APP_NAME):$(VERSION) -t $(APP_NAME):latest .

.PHONY: docker-build-arm64
docker-build-arm64: ## 构建 ARM64 Docker 镜像（鲲鹏）
	@echo "构建 ARM64 Docker 镜像（鲲鹏）..."
	docker buildx build --platform linux/arm64 --build-arg VERSION=$(VERSION) -t $(APP_NAME):$(VERSION)-arm64 --load .

.PHONY: docker-up
docker-up: ## 启动 3 节点 Docker 集群
	@echo "启动 Docker Compose 集群..."
	docker compose up -d
	@echo ""
	@echo "等待服务就绪..."
	@sleep 8
	@echo ""
	@echo "═══ 集群状态 ═══"
	@curl -s http://localhost:9001/cluster 2>/dev/null || echo "等待服务启动中..."
	@echo ""
	@echo "HTTP API: http://localhost:9001"
	@echo "gRPC: localhost:9501"
	@echo "集群状态: curl http://localhost:9001/cluster"
	@echo "日志: make docker-logs"

.PHONY: docker-down
docker-down: ## 关闭 Docker 集群
	@echo "关闭 Docker Compose 集群..."
	docker compose down -v

.PHONY: docker-logs
docker-logs: ## 查看 Docker 集群日志
	docker compose logs -f

.PHONY: docker-status
docker-status: ## 查看集群状态
	@echo "═══ Node 1 ═══"
	@curl -s http://localhost:9001/health 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "未就绪"
	@echo ""
	@echo "═══ Node 2 ═══"
	@curl -s http://localhost:9002/health 2>/dev/null || echo "未就绪"
	@echo ""
	@echo "═══ Node 3 ═══"
	@curl -s http://localhost:9003/health 2>/dev/null || echo "未就绪"

# =============================================================================
# 代码生成
# =============================================================================
.PHONY: proto
proto: ## 生成 protobuf Go 代码
# 语义未验证: 'proto/control_center.proto' 在本仓库中不存在（仅有 proto/raftkv.proto）
	@echo "生成 protobuf Go 代码..."
	@if command -v protoc >/dev/null 2>&1; then \
		protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/control_center.proto; \
		echo "protobuf 代码已生成"; \
	else \
		echo "错误: protoc 未安装，请先安装 Protocol Buffers 编译器"; \
		echo "  macOS: brew install protobuf"; \
		echo "  Linux: apt install protobuf-compiler"; \
		echo "  然后: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; \
		echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; \
		exit 1; \
	fi

# =============================================================================
# 工具
# =============================================================================
.PHONY: tidy
tidy: ## 整理 Go 依赖
	$(GO) mod tidy

.PHONY: fmt
fmt: ## 格式化代码
	$(GO) fmt ./...

.PHONY: vet
vet: ## 静态分析
	$(GO) vet ./...

.PHONY: clean
clean: ## 清理构建产物
	@echo "清理构建产物..."
	rm -rf bin/ data/
	@echo "清理完成"

.PHONY: dev-setup
dev-setup: ## 安装开发工具
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(GO) install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
	@echo "开发工具安装完成"

.PHONY: build-release
build-release: ## 编译加固版（garble 混淆 + 符号剥离）
	@echo "编译加固版 $(APP_NAME)..."
	@if ! command -v garble >/dev/null 2>&1; then echo "安装 garble..."; $(GO) install mvdan.cc/garble@latest; fi
	CGO_ENABLED=0 garble -literals -tiny -seed=random $(GO) build -ldflags="$(LDFLAGS)" -o bin/$(APP_NAME) .
	@echo "加固编译完成: bin/$(APP_NAME)"
