package geecache

// 节点服务端要实现的接口
type PeerPicker interface {
	// 根据key找出对应的节点的Getter
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// 节点客户端要实现的接口
type PeerGetter interface {
	// 向远程节点请求数据
	Get(group string, key string) ([]byte, error)
	// Get(ctx context.Context, in *pb.GroupRequest) (*pb.GroupReply, error)
}
