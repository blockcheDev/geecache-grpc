package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

var (
	EtcdURL     = "127.0.0.1:2379"
	ServiceName = "geecache"
	Client      *clientv3.Client
)

func init() {
	var err error
	Client, err = clientv3.NewFromURL(EtcdURL)
	if err != nil {
		logrus.Errorf("[etcd] 连接etcd失败, err: %+v", err)
		return
	}
	logrus.Infof("[etcd] 连接etcd成功。")
}

func RegisterEndPointToEtcd(ctx context.Context, addr string) {
	// 创建 etcd 管理者
	etcdManager, _ := endpoints.NewManager(Client, ServiceName)

	// 创建一个租约，每隔 10s 需要向 etcd 汇报一次心跳，证明当前节点仍然存活
	var ttl int64 = 10
	lease, _ := Client.Grant(ctx, ttl)

	// 添加注册节点到 etcd 中，并且携带上租约 id
	key := fmt.Sprintf("%s/%s", ServiceName, addr)
	_ = etcdManager.AddEndpoint(ctx, key, endpoints.Endpoint{Addr: addr}, clientv3.WithLease(lease.ID))
	logrus.Infof("[etcd] 注册节点成功, key: %s", key)

	// 每隔 5 s进行一次延续租约的动作
	for {
		select {
		case <-time.After(5 * time.Second):
			// 续约操作
			Client.KeepAliveOnce(ctx, lease.ID)
			// logrus.Infof("[etcd] keep alive resp: %+v", resp)
		case <-ctx.Done():
			return
		}
	}
}
