// =========================================================================
// RaftKV Module01 — 纯标准库 HTTP RPC 传输层（替代 gRPC）
//
// 设计原则：
//   1. 零外部依赖：仅用 net/http + encoding/json
//   2. 实现 Transport 接口，可被 RaftNode 直接使用
//   3. HTTPServer 暴露 RequestVote / AppendEntries 两个端点
//   4. 短超时 + 连接复用，适合高频心跳
//
// RPC 端点：
//   POST /raft/request_vote   → HandleRequestVote
//   POST /raft/append_entries → HandleAppendEntries
//
// 本文件为新增，替代原 gRPC proto + google.golang.org/grpc 通信层。
// =========================================================================

package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// =========================================================================
// 编译时常量
// =========================================================================

const (
	// HTTP 客户端超时
	httpClientTimeout = 600 * time.Millisecond

	// HTTP 服务端读写超时
	httpServerReadTimeout  = 1 * time.Second
	httpServerWriteTimeout = 1 * time.Second

	// RPC 路径
	pathRequestVote   = "/raft/request_vote"
	pathAppendEntries = "/raft/append_entries"
	pathPreVote       = "/raft/pre_vote"
)

// =========================================================================
// HTTPTransport — Transport 接口的 HTTP 实现（客户端）
// =========================================================================

// HTTPTransport 基于 net/http 的 Raft RPC 客户端
type HTTPTransport struct {
	serverAddr string       // 目标节点地址 (host:port)
	client     *http.Client // 复用 HTTP 连接
}

// NewHTTPTransport 创建 HTTP 传输客户端
//
//	serverAddr: 目标节点地址 (host:port)
func NewHTTPTransport(serverAddr string) *HTTPTransport {
	return &HTTPTransport{
		serverAddr: serverAddr,
		client: &http.Client{
			Timeout: httpClientTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        4,
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     30 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   httpClientTimeout,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		},
	}
}

// RequestVote 发送 RequestVote RPC
func (t *HTTPTransport) RequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp, err := t.doRPC(pathRequestVote, req)
	if err != nil {
		return nil, err
	}
	var result RequestVoteResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("RequestVote 响应解码失败: %w", err)
	}
	return &result, nil
}

// PreVote 发送 PreVote RPC（batch34 T016: pre-vote 探测）
func (t *HTTPTransport) PreVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp, err := t.doRPC(pathPreVote, req)
	if err != nil {
		return nil, err
	}
	var result RequestVoteResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("PreVote 响应解码失败: %w", err)
	}
	return &result, nil
}

// AppendEntries 发送 AppendEntries RPC
func (t *HTTPTransport) AppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp, err := t.doRPC(pathAppendEntries, req)
	if err != nil {
		return nil, err
	}
	var result AppendEntriesResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("AppendEntries 响应解码失败: %w", err)
	}
	return &result, nil
}

// doRPC 发送 JSON RPC 请求的底层方法
func (t *HTTPTransport) doRPC(path string, req interface{}) ([]byte, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("RPC 请求编码失败: %w", err)
	}

	url := "http://" + t.serverAddr + path
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("构造 HTTP 请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP 调用失败: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP 状态码 %d", httpResp.StatusCode)
	}

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应体失败: %w", err)
	}
	return respBody, nil
}

// Close 关闭传输连接（HTTP 客户端无需显式关闭）
func (t *HTTPTransport) Close() error {
	t.client.CloseIdleConnections()
	return nil
}

// =========================================================================
// HTTPServer — Raft RPC 服务端（接收并分发 RPC 请求）
// =========================================================================

// HTTPServer Raft RPC HTTP 服务端
type HTTPServer struct {
	addr     string       // 监听地址
	server   *http.Server // HTTP 服务
	listener net.Listener // 监听器
	node     *RaftNode    // 关联的 Raft 节点
}

// NewHTTPServer 创建 Raft RPC 服务端
//
//	addr: 监听地址 (host:port)
//	node: 关联的 Raft 节点（RPC 请求将委托给该节点处理）
func NewHTTPServer(addr string, node *RaftNode) (*HTTPServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("监听 %s 失败: %w", addr, err)
	}

	mux := http.NewServeMux()
	srv := &HTTPServer{
		addr:     ln.Addr().String(),
		listener: ln,
		node:     node,
	}

	mux.HandleFunc(pathRequestVote, srv.handleRequestVote)
	mux.HandleFunc(pathAppendEntries, srv.handleAppendEntries)
	mux.HandleFunc(pathPreVote, srv.handlePreVote)

	srv.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  httpServerReadTimeout,
		WriteTimeout: httpServerWriteTimeout,
	}

	return srv, nil
}

// Start 启动 HTTP 服务端（非阻塞，在后台 goroutine 中运行）
func (s *HTTPServer) Start() {
	go func() {
		_ = s.server.Serve(s.listener)
	}()
}

// Stop 停止 HTTP 服务端
func (s *HTTPServer) Stop() error {
	return s.server.Close()
}

// Addr 返回实际监听地址（可能端口为 0 时由系统分配）
func (s *HTTPServer) Addr() string {
	return s.addr
}

// =========================================================================
// RPC 处理函数
// =========================================================================

func (s *HTTPServer) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req RequestVoteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := s.node.HandleRequestVote(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handlePreVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req RequestVoteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	respTerm, granted := s.node.HandlePreVote(req.Term, req.CandidateId, req.LastLogIndex, req.LastLogTerm)
	resp := &RequestVoteResponse{Term: respTerm, VoteGranted: granted}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req AppendEntriesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := s.node.HandleAppendEntries(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
