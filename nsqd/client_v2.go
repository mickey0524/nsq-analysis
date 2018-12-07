package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/nsq/internal/auth"
)

const defaultBufferSize = 16 * 1024 // client 默认的 buffer size，reader 和 writer 用的是 bufio

// 当前 client 的状态
const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

// client 向 nsqd tcp server IDENTIFY
type identifyDataV2 struct {
	ClientID            string `json:"client_id"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

// IDENTIFY 完成之后，使用 identifyEvent 记录 client 的一些最新参数，用于更新 nsqd server 的 messagePump
type identifyEvent struct {
	OutputBufferTimeout time.Duration
	HeartbeatInterval   time.Duration
	SampleRate          int32
	MsgTimeout          time.Duration
}

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

// String 返回 client 地址
func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

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

// Stats 返回 client 的状态，服务于 /stats 这个 http 接口
func (c *clientV2) Stats() ClientStats {
	c.metaLock.RLock()
	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	pubCounts := make([]PubCount, 0, len(c.pubCounts))
	for topic, count := range c.pubCounts {
		pubCounts = append(pubCounts, PubCount{
			Topic: topic,
			Count: count,
		})
	}
	c.metaLock.RUnlock()
	stats := ClientStats{
		Version:         "V2",
		RemoteAddress:   c.RemoteAddr().String(),
		ClientID:        clientID,
		Hostname:        hostname,
		UserAgent:       userAgent,
		State:           atomic.LoadInt32(&c.State),
		ReadyCount:      atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:   atomic.LoadInt64(&c.InFlightCount),
		MessageCount:    atomic.LoadUint64(&c.MessageCount),
		FinishCount:     atomic.LoadUint64(&c.FinishCount),
		RequeueCount:    atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:     c.ConnectTime.Unix(),
		SampleRate:      atomic.LoadInt32(&c.SampleRate),
		TLS:             atomic.LoadInt32(&c.TLS) == 1,
		Deflate:         atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:          atomic.LoadInt32(&c.Snappy) == 1,
		Authed:          c.HasAuthorizations(),
		AuthIdentity:    identity,
		AuthIdentityURL: identityURL,
		PubCounts:       pubCounts,
	}
	if stats.TLS {
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}
	return stats
}

// client 是否为生产者
func (c *clientV2) IsProducer() bool {
	c.metaLock.RLock()
	retval := len(c.pubCounts) > 0 // 如果发布了消息，即为生产者
	c.metaLock.RUnlock()
	return retval
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}

func (p *prettyConnectionState) GetCipherSuite() string {
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionSSL30:
		return "SSL30"
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

// client 消费者是否准备好了接收 message
func (c *clientV2) IsReadyForMessages() bool {
	// 当消费者订阅的 channel 处于 Pause 状态，返回 false
	if c.Channel.IsPaused() {
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	c.ctx.nsqd.logf(LOG_DEBUG, "[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	// 当消费者能够接收的消息数目小于发送中的消息总数时，返回 false
	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

// SetReadyCount 更新消费者能够接收的消息数目
func (c *clientV2) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *clientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

// FinishedMessage 表明一条消息到达消费者，InFlightCount 数目减一，FinishCount 数目加一
func (c *clientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// Empty 将 InFlightCount 置为 0
func (c *clientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

// SendingMessage 发送一条消息给消费者
func (c *clientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

// PublishedMessage 生产者生产消息，记录在 pubCounts 中，方便 stats 查看
func (c *clientV2) PublishedMessage(topic string, count uint64) {
	c.metaLock.Lock()
	c.pubCounts[topic] += count
	c.metaLock.Unlock()
}

// TimedOutMessage 表明一条消息 timeout，InFlightCount 数目减一
func (c *clientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// RequeuedMessage 将一条消息从 inFlightMessages 中删除，然后重新发送
func (c *clientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// StartClose 关闭 client
func (c *clientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, stateClosing)
}

func (c *clientV2) Pause() {
	c.tryUpdateReadyState()
}

func (c *clientV2) UnPause() {
	c.tryUpdateReadyState()
}

// SetHeartbeatInterval 更新 client 的心跳间隔
func (c *clientV2) SetHeartbeatInterval(desiredInterval int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredInterval == -1:
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.ctx.nsqd.getOpts().MaxHeartbeatInterval/time.Millisecond):
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	return nil
}

// SetOutputBufferSize 更新 client writer buffer size
func (c *clientV2) SetOutputBufferSize(desiredSize int) error {
	var size int

	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		size = 1
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(c.ctx.nsqd.getOpts().MaxOutputBufferSize):
		size = desiredSize
	default:
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}

	if size > 0 {
		c.writeLock.Lock()
		defer c.writeLock.Unlock()
		c.OutputBufferSize = size
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, size)
	}

	return nil
}

// SetOutputBufferTimeout 更新 client writer flush 的时间间隔
func (c *clientV2) SetOutputBufferTimeout(desiredTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredTimeout == -1:
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// do nothing (use default)
	case desiredTimeout >= 1 &&
		desiredTimeout <= int(c.ctx.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):
		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}

	return nil
}

// SetSampleRate 设置 client 的消费比率
func (c *clientV2) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

// SetMsgTimeout 设置消息的 timeout
func (c *clientV2) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
		// do nothing (use default)
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.ctx.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

func (c *clientV2) UpgradeTLS() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	tlsConn := tls.Server(c.Conn, c.ctx.nsqd.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, defaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

func (c *clientV2) UpgradeDeflate(level int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), defaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

func (c *clientV2) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappy.NewReader(conn), defaultBufferSize)
	c.Writer = bufio.NewWriterSize(snappy.NewWriter(conn), c.OutputBufferSize)

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

// Flush 将 writer buffer 中的缓存的字节全部写入底层 conn
func (c *clientV2) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

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
