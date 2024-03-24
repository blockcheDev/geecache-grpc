package geecache

// 只能得到b的副本，保证缓存数据安全
type ByteView struct {
	b []byte
}

// 返回字节切片的长度，也就是占用的字节数
func (v ByteView) Len() int {
	return len(v.b)
}

// 获取切片副本
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}

// 获取副本
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
