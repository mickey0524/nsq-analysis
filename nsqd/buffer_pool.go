package nsqd

import (
	"bytes"
	"sync"
)

// 利用 sync.Pool 避免 nsqd 重复创建，释放 bytes.Buffer 对象
// 用于将 topic 和 channel 中的消息进行磁盘存储

var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func bufferPoolPut(b *bytes.Buffer) {
	bp.Put(b)
}
