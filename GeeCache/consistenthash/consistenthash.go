package consistenthash

import (
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash
	replicas int
	hashs    []int // 有序的，用来二分查找
	hashMap  map[int]string
	keys     map[string]struct{}
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
		keys:     make(map[string]struct{}),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) Build() {
	m.hashs = make([]int, 0)
	m.hashMap = make(map[int]string)
	for key := range m.keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.hashs = append(m.hashs, hash)
			m.hashMap[hash] = key
		}
	}
	slices.Sort(m.hashs)
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		if _, ok := m.keys[key]; !ok {
			m.keys[key] = struct{}{}
		}
	}
	m.Build()
}

func (m *Map) Remove(key string) {
	if _, ok := m.keys[key]; ok {
		delete(m.keys, key)
		m.Build()
	}
}

func (m *Map) Get(key string) string {
	if len(m.hashs) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	// 获得第一个满足比较函数条件的下标
	idx := sort.Search(len(m.hashs), func(i int) bool {
		return m.hashs[i] >= hash
	})

	return m.hashMap[m.hashs[idx%len(m.hashs)]]
}
