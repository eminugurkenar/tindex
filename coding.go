package tsindex

import (
	"encoding/binary"
	"sync"
)

// bufPool is a pool for staging buffers. Using a pool allows concurrency-safe
// reuse of buffers
var bufPool sync.Pool

// getBuf returns a buffer from the pool. The length of the returned slice is l.
func getBuf(l int) []byte {
	x := bufPool.Get()
	if x == nil {
		return make([]byte, l)
	}
	buf := x.([]byte)
	if cap(buf) < l {
		return make([]byte, l)
	}
	return buf[:l]
}

// putBuf returns a buffer to the pool.
func putBuf(buf []byte) {
	bufPool.Put(buf)
}

func encodeUint64(x uint64) []byte {
	buf := getBuf(8)
	binary.BigEndian.PutUint64(buf, x)
	return buf
}

func decodeUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
