// =========================================================================
// RaftKV 确定性引擎 — gRPC 服务端 + Raft 服务实现 + Peer 客户端管理
// =========================================================================

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "raftkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// =========================================================================
// RaftServiceImpl — 实现 proto 生成的 RaftServiceServer 接口
// =========================================================================

type RaftServiceImpl struct {
	pb.UnimplementedRaftServiceServer
	node *RaftNode
}

func newRaftServiceImpl(node *RaftNode) *RaftServiceImpl {
	return &RaftServiceImpl{node: node}
}

func (svc *RaftServiceImpl) RequestVote(
	ctx context.Context, req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	return svc.node.HandleRequestVote(ctx, req)
}

func (svc *RaftServiceImpl) AppendEntries(
	ctx context.Context, req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	return svc.node.HandleAppendEntries(ctx, req)
}

// =========================================================================
// GRPCServer — gRPC 服务器封装
// =========================================================================

type GRPCServer struct {
	node      *RaftNode
	listener  net.Listener
	server    *grpc.Server
	healthSrv *healthServer
	ready     atomic.Bool
	port      string
}

// =========================================================================
// healthServer — grpc.health.v1 健康检查实现
// 判定标准: !walGateClosed → SERVING; walGateClosed → NOT_SERVING
// Watch流式暂不实现(进backlog)
// =========================================================================

type healthServer struct {
	healthpb.UnimplementedHealthServer
	node *RaftNode
}

func (h *healthServer) Check(
	ctx context.Context,
	req *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	if h.node.IsWALGateClosed() {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_NOT_SERVING,
		}, nil
	}
	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}, nil
}

func (h *healthServer) Watch(
	req *healthpb.HealthCheckRequest,
	stream healthpb.Health_WatchServer,
) error {
	return status.Error(codes.Unimplemented, "Watch not implemented (backlog)")
}

func NewGRPCServer(node *RaftNode, port string) *GRPCServer {
	return &GRPCServer{
		node: node,
		port: port,
	}
}

// Start 启动 gRPC 服务器（非阻塞，在 goroutine 中 serve）
func (s *GRPCServer) Start() error {
	addr := fmt.Sprintf("0.0.0.0:%s", s.port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("gRPC 端口 %s 监听失败: %w", s.port, err)
	}
	s.listener = lis

	// 创建 gRPC 服务器并注册 Raft 服务
	// mTLS：优先加载 gen_certs.sh 生成的证书（环境变量 TLS_CERT_FILE/TLS_KEY_FILE 或默认 ./certs/<node>-cert.pem）
	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(256),
		grpc.ChainUnaryInterceptor(walGateInterceptor, licenseGuardInterceptor, latencyInterceptor),
	}
	certFile := envOr("TLS_CERT_FILE", "", fmt.Sprintf("./certs/%s-cert.pem", s.node.id))
	keyFile := envOr("TLS_KEY_FILE", "", fmt.Sprintf("./certs/%s-key.pem", s.node.id))
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			fmt.Printf("[mTLS] 证书加载失败，降级为明文传输: %v\n", err)
		} else {
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			}
			caFile := os.Getenv("TLS_CA_FILE")
			if caFile != "" {
				caData, err := os.ReadFile(caFile)
				if err != nil {
					fmt.Printf("[mTLS] CA证书读取失败，降级为明文传输: %v\n", err)
				} else {
					caPool := x509.NewCertPool()
					if !caPool.AppendCertsFromPEM(caData) {
						fmt.Printf("[mTLS] CA证书解析失败，降级为明文传输\n")
					} else {
						tlsConfig.ClientCAs = caPool
						tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
						fmt.Printf("[mTLS] 客户端证书验证已启用 (CA: %s)\n", caFile)
					}
				}
			}
			opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
			fmt.Printf("[mTLS] TLS 已启用 (证书: %s)\n", certFile)
		}
	}
	s.server = grpc.NewServer(opts...)

	// 注册 Raft 服务实现
	raftSvc := newRaftServiceImpl(s.node)
	pb.RegisterRaftServiceServer(s.server, raftSvc)

	// 注册 gRPC 健康检查服务 (grpc.health.v1)
	s.healthSrv = &healthServer{node: s.node}
	healthpb.RegisterHealthServer(s.server, s.healthSrv)

	// 注册 gRPC reflection（方便 grpcurl 调试）
	reflection.Register(s.server)

	// 后台启动 serve
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[recover] gRPC goroutine panic: %v\n", r)
			}
		}()
		s.ready.Store(true)
		fmt.Printf("[RaftKV 网关] gRPC 服务启动成功，端口: %s\n", s.port)
		if err := s.server.Serve(lis); err != nil {
			fmt.Printf("[RaftKV 网关] gRPC 异常停止: %v\n", err)
		}
	}()

	return nil
}

// Stop 优雅关闭 gRPC 服务器
func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
	fmt.Printf("[RaftKV 网关] gRPC 服务已关闭 (端口: %s)\n", s.port)
}

// IsReady 检查 gRPC 服务是否已就绪
func (s *GRPCServer) IsReady() bool {
	return s.ready.Load()
}

// Address 返回监听地址
func (s *GRPCServer) Address() string {
	return fmt.Sprintf("localhost:%s", s.port)
}

// =========================================================================
// PeerClientManager — 对等节点 gRPC 客户端管理器
// =========================================================================

type PeerClientManager struct {
	mu            sync.RWMutex
	clients       map[string]peerConn                              // peerID → 连接 + 客户端
	address       map[string]string                                // peerID → 地址
	onReconnect   func(peerID string, client pb.RaftServiceClient) // R-04修复A: 重连成功回调
	stopReconnect chan struct{}                                    // R-04修复A: 停止重连循环信号
	reconnectOnce sync.Once                                        // R-04修复A: 确保停止通道只关闭一次
}

type peerConn struct {
	conn   *grpc.ClientConn
	client pb.RaftServiceClient
}

// NewPeerClientManager 创建客户端管理器
func NewPeerClientManager(peerAddrs map[string]string) *PeerClientManager {
	return &PeerClientManager{
		clients: make(map[string]peerConn),
		address: peerAddrs,
	}
}

// ConnectAll 连接所有 peer 节点（带重试，最多重试 30 次，每次间隔 2 秒）
func (m *PeerClientManager) ConnectAll() map[string]pb.RaftServiceClient {
	result := make(map[string]pb.RaftServiceClient)
	var mu sync.Mutex

	for peerID, addr := range m.address {
		go func(id, a string) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("[recover] peer-client goroutine panic: %v\n", r)
				}
			}()
			for retry := 0; retry < 30; retry++ {
				client, err := m.connect(id, a)
				if err == nil {
					mu.Lock()
					result[id] = client
					mu.Unlock()
					fmt.Printf("[peer-client] 已连接 peer: %s @ %s (重试=%d)\n", id, a, retry)
					slogInfo("peer_connected", "peer连接成功", map[string]interface{}{
						"peer":  id,
						"addr":  a,
						"retry": retry,
					})
					return
				}
				if retry == 0 {
					fmt.Printf("[peer-client] 连接 %s (%s) 失败: %v, 将在后台持续重试...\n", id, a, err)
				}
				time.Sleep(2 * time.Second)
			}
			fmt.Printf("[peer-client] 连接 %s (%s) 最终失败 (已重试30次)\n", peerID, addr)
			slogError("peer_connect_failed", "peer连接最终失败", map[string]interface{}{
				"peer": peerID,
				"addr": addr,
			})
		}(peerID, addr)
	}

	// 等待至少 4 秒让初始连接建立，但不阻塞启动
	time.Sleep(4 * time.Second)
	return result
}

// connect 连接到单个 peer（无限重试直到成功或 context 取消）
func (m *PeerClientManager) connect(peerID, addr string) (pb.RaftServiceClient, error) {
	m.mu.Lock()
	if existing, ok := m.clients[peerID]; ok {
		m.mu.Unlock()
		return existing.client, nil
	}
	m.mu.Unlock()

	// gRPC 连接 — 优先使用 mTLS 双向认证，证书不存在时降级为明文
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16*1024*1024),
			grpc.MaxCallSendMsgSize(16*1024*1024),
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   5 * time.Second,
			},
		}),
	}

	caFile := os.Getenv("TLS_CA_FILE")
	certFile := os.Getenv("TLS_CLIENT_CERT_FILE")
	keyFile := os.Getenv("TLS_CLIENT_KEY_FILE")
	if caFile != "" && certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("mTLS 客户端证书加载失败: %w", err)
		}
		caData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("mTLS CA证书读取失败: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("mTLS CA证书解析失败")
		}
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool,
			MinVersion:   tls.VersionTLS12,
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		fmt.Printf("[mTLS] 客户端 TLS 已启用 (CA: %s, Cert: %s)\n", caFile, certFile)
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	client := pb.NewRaftServiceClient(conn)

	m.mu.Lock()
	m.clients[peerID] = peerConn{conn: conn, client: client}
	m.mu.Unlock()

	return client, nil
}

// GetClient 获取指定 peer 的客户端
func (m *PeerClientManager) GetClient(peerID string) (pb.RaftServiceClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pc, ok := m.clients[peerID]
	if !ok {
		return nil, false
	}
	// 检查连接状态
	state := pc.conn.GetState()
	if state == connectivity.Shutdown {
		return nil, false
	}
	// TransientFailure 或 Idle 时主动触发重连，返回 client 让调用方尝试 RPC
	if state == connectivity.TransientFailure || state == connectivity.Idle {
		pc.conn.Connect()
	}
	return pc.client, true
}

// CloseAll 关闭所有连接
func (m *PeerClientManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, pc := range m.clients {
		if err := pc.conn.Close(); err != nil {
			log.Printf("[peer-client] 关闭 %s 连接时出错: %v", id, err)
		}
	}
}

// Peers 返回所有 peer ID
func (m *PeerClientManager) Peers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.address))
	for id := range m.address {
		ids = append(ids, id)
	}
	return ids
}

// : AddPeer 动态添加 peer 连接
func (m *PeerClientManager) AddPeer(peerID, addr string) error {
	m.mu.Lock()
	m.address[peerID] = addr
	m.mu.Unlock()
	_, err := m.connect(peerID, addr)
	return err
}

// : RemovePeer 动态移除 peer 连接
func (m *PeerClientManager) RemovePeer(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pc, ok := m.clients[peerID]; ok {
		pc.conn.Close()
		delete(m.clients, peerID)
	}
	delete(m.address, peerID)
}

// R-04修复A: SetOnReconnect 设置重连成功回调（更新RaftNode.peerClients map）
func (m *PeerClientManager) SetOnReconnect(fn func(peerID string, client pb.RaftServiceClient)) {
	m.onReconnect = fn
}

// R-04修复A: StartReconnectLoop 启动后台重连循环
// 每2s检查所有peer连接状态，若某peer持续TransientFailure/Idle超过5s，
// 关闭旧连接并创建新连接（强制DNS重解析），通过onReconnect回调更新RaftNode.peerClients
// 判据: DNS恢复后≤10s内重连成功（2s检测+5s阈值+新连接建立~1s）
func (m *PeerClientManager) StartReconnectLoop() {
	m.mu.Lock()
	m.stopReconnect = make(chan struct{})
	m.mu.Unlock()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		failureSince := make(map[string]time.Time)

		for {
			select {
			case <-m.stopReconnect:
				return
			case <-ticker.C:
				m.mu.RLock()
				peerIDs := make([]string, 0, len(m.clients))
				for id := range m.clients {
					peerIDs = append(peerIDs, id)
				}
				m.mu.RUnlock()

				for _, id := range peerIDs {
					m.mu.RLock()
					pc, ok := m.clients[id]
					m.mu.RUnlock()
					if !ok {
						continue
					}

					state := pc.conn.GetState()
					// 修复F: 移除 connectivity.Idle 误判——grpc.NewClient 是惰性连接，
					// 新建连接默认 Idle 状态，仅在真正 TransientFailure(连接失败) 时才应重连，
					// 否则重连循环会把健康的 Idle 连接每5s误判为失败并无条件重建
					if state == connectivity.TransientFailure || state == connectivity.Shutdown {
						if _, exists := failureSince[id]; !exists {
							failureSince[id] = time.Now()
							log.Printf("[peer-client] R-04修复A: peer %s 连接状态=%s，开始计时", id, state)
						}
						if time.Since(failureSince[id]) > 5*time.Second {
							log.Printf("[peer-client] R-04修复A: peer %s 持续失败>5s，强制重连（DNS重解析）", id)
							m.reconnectPeer(id)
							delete(failureSince, id)
						}
					} else if state == connectivity.Ready {
						if _, wasFailing := failureSince[id]; wasFailing {
							log.Printf("[peer-client] R-04修复A: peer %s 已恢复连接(Ready)", id)
						}
						delete(failureSince, id)
					}
				}
			}
		}
	}()
}

// R-04修复A: StopReconnectLoop 停止后台重连循环
func (m *PeerClientManager) StopReconnectLoop() {
	m.reconnectOnce.Do(func() {
		m.mu.Lock()
		if m.stopReconnect != nil {
			close(m.stopReconnect)
			m.stopReconnect = nil
		}
		m.mu.Unlock()
	})
}

// R-04修复A: reconnectPeer 关闭旧连接并创建新连接（强制DNS重解析）
func (m *PeerClientManager) reconnectPeer(peerID string) {
	m.mu.Lock()
	addr, ok := m.address[peerID]
	if !ok {
		m.mu.Unlock()
		return
	}
	old, exists := m.clients[peerID]
	if exists {
		delete(m.clients, peerID)
	}
	m.mu.Unlock()

	if exists {
		old.conn.Close()
	}

	client, err := m.connect(peerID, addr)
	if err != nil {
		log.Printf("[peer-client] R-04修复A: 重连 peer %s 失败: %v", peerID, err)
		return
	}

	log.Printf("[peer-client] R-04修复A: peer %s 重连成功（新DNS resolver）", peerID)

	if m.onReconnect != nil {
		m.onReconnect(peerID, client)
	}
}

func licenseGuardInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	degraded, reason := IsDegradedMode()
	if !degraded {
		return handler(ctx, req)
	}

	method := info.FullMethod
	readOnlyMethods := map[string]bool{
		"/raftkv.RaftService/RequestVote":   true,
		"/raftkv.RaftService/AppendEntries": true,
		"/grpc.health.v1.Health/Check":         true,
		"/grpc.health.v1.Health/Watch":         true,
	}

	if readOnlyMethods[method] {
		return handler(ctx, req)
	}

	return nil, status.Errorf(codes.PermissionDenied,
		"授权无效或检测到时间异常，已降级为只读模式。原因: %s", reason)
}

func walGateInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if globalWALGate != nil && globalWALGate.IsClosed() {
		method := info.FullMethod
		if method == "/raftkv.RaftService/AppendEntries" {
			return nil, status.Errorf(codes.Unavailable, "WAL不可用: %s", globalWALGate.Reason())
		}
	}
	return handler(ctx, req)
}

var globalWALGate *WALGate
