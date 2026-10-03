# ---- Stage 1: Builder ----
FROM golang:1.24-alpine AS builder

ENV GOPROXY=https://goproxy.cn,direct
ENV GO111MODULE=on

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /gateway .

# ---- Stage 2: Runtime ----
FROM alpine:3.21

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk add --no-cache ca-certificates tzdata curl
ENV TZ=Asia/Shanghai
# 双模式授权防线（2026-09-01 姜总裁决二）
# closed = 授权校验失败即拒绝启动（Fail-Closed，出厂默认，唯一安全值）
# open   = 授权校验失败降级只读运行，严禁在未签补充条款前启用
ENV LICENSE_FAIL_MODE=closed

WORKDIR /app
COPY --from=builder /gateway .

EXPOSE 9500 9000
ENTRYPOINT ["./gateway"]
