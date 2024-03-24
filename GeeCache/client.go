package geecache

import (
	"context"
	pb "geecache/groupcachepb"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

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
