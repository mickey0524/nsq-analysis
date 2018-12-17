## client_v2.go

`client_v2.go` 与 `client_v1.go` 有所不同，`client_v1.go` 中定义的 ClientV1 指代的是一个 nsqd 节点，用于存储 nsqd 和 nsqlookupd 的 tcp 连接以及 nsqd 的元数据，`client_v2.go` 中 定义的 ClientV2 指代的是一个 producer 或者 consumer，结构比 ClientV1 复杂很多，下面我们具体来看一下

### Clientv2 的结构

```go
// clientV2 指代连上 nsqd server 的客户端
type clientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount    int64  // client 能接收的消息数目
	InFlightCount int64  // 发送中的消息数目
	MessageCount  uint64 // 发送的消息总数
	FinishCount   uint64 // 发送成功的消息总数
	RequeueCount  uint64 // 重新发送的消息总数

	pubCounts map[string]uint64 // 作用于 producer，统计生产者向每个 topic 发送的消息数目

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64    // client ID
	ctx       *context // 上下文作用域，存放当前 nsqd
	UserAgent string   // client ua

	// original connection
	net.Conn // client 和 nsqd server 的连接

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int           // bufio buffer size
	OutputBufferTimeout time.Duration // 每隔多少时间，bufio writer flush 一次

	HeartbeatInterval time.Duration // 心跳间隔

	MsgTimeout time.Duration // 消息超时时间

	State          int32     // client 当前状态
	ConnectTime    time.Time // client 连上 nsqd server 的时间点
	Channel        *Channel  // client 消费的 channel
	ReadyStateChan chan int  // client param 更新的 chan
	ExitChan       chan int  // client 退出的 chan

	ClientID string // 标识 client 的 key，默认等于 hostname
	Hostname string

	SampleRate int32 // 客户端的消费速度，例如 SampleRate 为 80，如果 rand 一个 0-100 的随机数大于 80，本次 client 则不消费

	IdentifyEventChan chan identifyEvent // IDENTITY 请求的回调 chan
	SubEventChan      chan *Channel      // client subscribe channel 的回调 chan

	TLS     int32 // 是否 tls 加密
	Snappy  int32 // 是否 snappy 压缩
	Deflate int32 // 是否 deflate 压缩

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	lenSlice []byte

	AuthSecret string      // 鉴权秘钥
	AuthState  *auth.State // client 的权限状态
}
```

### ClientV2 实例的创建

实例创建的时候，参数多为 nsqd.opts 中的默认值，等待 IDENTITY 事件传来，会调用 Identify 函数对参数进行更新

```go
// 新建一个 client
func newClientV2(id int64, conn net.Conn, ctx *context) *clientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String()) // 获取 client 的 hostname
	}

	c := &clientV2{
		ID:  id,
		ctx: ctx,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: 250 * time.Millisecond,

		MsgTimeout: ctx.nsqd.getOpts().MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),
		IdentifyEventChan: make(chan identifyEvent, 1),

		// heartbeats are client configurable but default to 30s
		HeartbeatInterval: ctx.nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}
```

### ClientV2 的 Identify

当 client 向 nsqd tcp server 发送 IDENTITY 的 request 时，body 可以被解析成一个 identifyDataV2 实例，然后调用 Identify 函数进行参数更新，最后依据更新后的参数创建一个 identifyEvent 实例并 send 到 c.IdentifyEventChan 中，在 protocol_v2.go 的 messagePump 中会捕获此事件，并作出相应的更新

```go
// client 向 nsqd server 发送 IDENTITY 请求，给 client 的一些 param 赋值
func (c *clientV2) Identify(data identifyDataV2) error {
	c.ctx.nsqd.logf(LOG_INFO, "[%s] IDENTIFY: %+v", c, data)

	c.metaLock.Lock()
	c.ClientID = data.ClientID   // 更新 ClientID
	c.Hostname = data.Hostname   // 更新 Hostname
	c.UserAgent = data.UserAgent // 更新 ua
	c.metaLock.Unlock()

	err := c.SetHeartbeatInterval(data.HeartbeatInterval) // 更新心跳间隔
	if err != nil {
		return err
	}

	err = c.SetOutputBufferSize(data.OutputBufferSize) // 更新 client reader writer 的 buffer size
	if err != nil {
		return err
	}

	err = c.SetOutputBufferTimeout(data.OutputBufferTimeout) // 更新 client writer flush 的间隔
	if err != nil {
		return err
	}

	err = c.SetSampleRate(data.SampleRate) // 更新 consumer 的消费速率
	if err != nil {
		return err
	}

	err = c.SetMsgTimeout(data.MsgTimeout) // 更新消息的 timeout
	if err != nil {
		return err
	}

	// 更新完毕之后，实例化一个 identifyEvent，用于通知 nsqd server 的 messagePump 进行对应的更新
	ie := identifyEvent{
		OutputBufferTimeout: c.OutputBufferTimeout,
		HeartbeatInterval:   c.HeartbeatInterval,
		SampleRate:          c.SampleRate,
		MsgTimeout:          c.MsgTimeout,
	}

	// update the client's message pump
	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	return nil
}
```

### ClientV2 的鉴权

当 producer 想要发布消息或者 consumer 想要订阅某个 channel 的时候，nsqd tcp server 相应的 handler 都会进行权限鉴定，实际调用的就是 ClientV2 鉴权的代码

下面的注释写的比较清楚了，这里就不详细介绍了，需要注意的是权限有 expireTime，过期后重新向对应地址发送 http 请求鉴权即可

```go
// QueryAuthd 请求鉴权
func (c *clientV2) QueryAuthd() error {
	remoteIP, _, err := net.SplitHostPort(c.String())
	if err != nil {
		return err
	}

	tls := atomic.LoadInt32(&c.TLS) == 1
	tlsEnabled := "false"
	if tls {
		tlsEnabled = "true"
	}

	authState, err := auth.QueryAnyAuthd(c.ctx.nsqd.getOpts().AuthHTTPAddresses,
		remoteIP, tlsEnabled, c.AuthSecret, c.ctx.nsqd.getOpts().HTTPClientConnectTimeout,
		c.ctx.nsqd.getOpts().HTTPClientRequestTimeout)
	if err != nil {
		return err
	}
	c.AuthState = authState
	return nil
}

// Auth 鉴权
func (c *clientV2) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

// IsAuthorized 指代 client 针对 (topic, channel) 是否拥有权限
func (c *clientV2) IsAuthorized(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

// HasAuthorizations 指代 client 是否拥有权限
func (c *clientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
```