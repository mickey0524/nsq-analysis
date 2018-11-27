package nsqd

// BackendQueue represents the behavior for the secondary message
// storage system
// BackendQueue 接口指代了 Nsqd 中的二级消息存储系统，分为 newDummyBackendQueue 和 diskqueue
// 当一个 topic 只是暂时存在的(后缀为 #ephemeral)，创建 newDummyBackendQueue，否则创建
// diskqueue

type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}
