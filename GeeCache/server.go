package geecache

import (
	pb "geecache/groupcachepb"

	"context"
	"fmt"
	"geecache/consistenthash"
	"geecache/etcd"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

	// 注册到etcd
	go etcd.RegisterEndPointToEtcd(context.Background(), p.self)

	// 检查etcd里的数据
	// time.Sleep(2 * time.Second)
	// resp, err := etcd.Client.Get(context.Background(), etcd.ServiceName, clientv3.WithPrefix())
	// if err != nil {
	// 	logrus.Errorf("[etcd] Get 失败, err: %+v", err)
	// }
	// logrus.Infof("[etcd] Get 成功, %+v", resp)

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

// 监控etcd变化，实时更新分布式节点列表，初始化grpc连接
func (p *GRPCPool) Watch() {
	// 初始化一致性hash
	p.mu.Lock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.grpcGetters = make(map[string]*grpcGetter)
	p.mu.Unlock()

	rch := etcd.Client.Watch(context.Background(), etcd.ServiceName, clientv3.WithPrefix())
	for resp := range rch {
		for _, ev := range resp.Events {
			addr := strings.TrimLeft(string(ev.Kv.Key), etcd.ServiceName+"/")
			switch ev.Type {
			case mvccpb.PUT:
				p.mu.Lock()
				// 一致性hash添加节点
				p.peers.Add(addr)
				// 初始化grpc客户端连接
				conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					logrus.Errorf("[grpc] 连接服务端失败, err: %v", err)
					continue
				}
				p.grpcGetters[addr] = &grpcGetter{addr: addr, conn: conn}
				p.mu.Unlock()
				logrus.Infof("[server][%s] 节点池添加节点成功, addr: %s", p.self, addr)
			case mvccpb.DELETE:
				p.mu.Lock()
				p.peers.Remove(addr)
				p.grpcGetters[addr].conn.Close()
				delete(p.grpcGetters, addr)
				p.mu.Unlock()
				logrus.Infof("[server][%s] 节点池删除节点成功, addr: %s", p.self, addr)
			}
		}
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
