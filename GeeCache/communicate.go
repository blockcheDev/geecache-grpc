package geecache

import (
	"context"
	"fmt"
	"geecache/consistenthash"
	pb "geecache/groupcachepb"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultReplicas = 50
)

// 实现了PeerPicker接口
// 分布式节点池
// 节点服务端，可处理远程节点的请求
type GRPCPool struct {
	// 节点的addr，例如： "example.net:8000"
	self string
	Port string
	pb.UnimplementedGroupCacheServer

	mu          sync.Mutex // 保护一致性哈希并发安全
	peers       *consistenthash.Map
	grpcGetters map[string]*grpcGetter // key为节点addr，例如： "10.0.0.2:8008"
}

func NewGRPCPool(self string) *GRPCPool {
	port := strings.Split(self, ":")[1]
	return &GRPCPool{
		self: self,
		Port: port,
	}
}

// 启动grpc服务端
func (p *GRPCPool) Listen() (err error) {
	lis, err := net.Listen("tcp", ":"+p.Port)
	if err != nil {
		logrus.Errorf("[net] Listen fail, err: %s", err)
		return
	}
	defer lis.Close()

	server := grpc.NewServer()
	pb.RegisterGroupCacheServer(server, p)

	logrus.Println("peer server is running at", lis.Addr())
	if err := server.Serve(lis); err != nil {
		logrus.Errorf("[grpc] Server 结束, err: %v", err)
		return err
	}
	return
}

// 关闭pool内所有grpc连接
func (p *GRPCPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, getter := range p.grpcGetters {
		if err := getter.conn.Close(); err != nil {
			logrus.Errorf("[grpc] 关闭连接失败, err: %v", err)
		}
	}
}

// 带有节点URL的日志信息
func (p *GRPCPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *GRPCPool) Get(ctx context.Context, req *pb.GroupRequest) (*pb.GroupReply, error) {
	logrus.Infof("[grpc][%s] 收到其他peer的请求。", p.self)

	groupName, key := req.GetGroup(), req.GetKey()
	if key == "" {
		return nil, fmt.Errorf("key is empty")
	}
	group := GetGroup(groupName)
	if group == nil {
		return nil, fmt.Errorf("no such group: %s", groupName)
	}

	view, err := group.Get(key)
	if err != nil {
		return nil, err
	}
	return &pb.GroupReply{View: view.ByteSlice()}, nil
}

// 更新分布式节点列表，初始化grpc连接
func (p *GRPCPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)

	// 初始化grpc客户端连接
	p.grpcGetters = make(map[string]*grpcGetter, len(peers))
	for _, peer := range peers {
		conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logrus.Errorf("[grpc] 连接服务端失败, err: %v", err)
			continue
		}
		p.grpcGetters[peer] = &grpcGetter{addr: peer, conn: conn}
	}
}

// 根据key使用一致性哈希选择节点
func (p *GRPCPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 如果选择了本机，返回nil, false
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		p.Log("Get Getter: %s", p.grpcGetters[peer].addr)
		return p.grpcGetters[peer], true
	}
	return nil, false
}

var _ PeerPicker = (*GRPCPool)(nil)

// 实现了PeerGetter接口
// 节点客户端，用来向远程节点请求数据
type grpcGetter struct {
	addr string
	conn *grpc.ClientConn
}

func (h *grpcGetter) Get(group string, key string) ([]byte, error) {
	client := pb.NewGroupCacheClient(h.conn)
	req := &pb.GroupRequest{Group: group, Key: key}
	rpy, err := client.Get(context.Background(), req)
	if err != nil {
		logrus.Errorf("[grpc] 从peer: %s 获取数据失败, err: %v", h.addr, err)
		return nil, err
	}
	logrus.Infof("[grpc] 成功从peer: %s 获取数据", h.addr)
	return rpy.GetView(), nil
}

var _ PeerGetter = (*grpcGetter)(nil)
