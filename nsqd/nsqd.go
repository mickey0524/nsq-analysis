package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}

// NSQD 结构体
type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64 // 连接 nsqd tcp server 的 client 数目

	sync.RWMutex

	opts atomic.Value // nsqd 的配置文件

	dl        *dirlock.DirLock // 文件锁
	isLoading int32            // nsqd 是否在读取存储在 datapath 中的 metadata 文件
	errValue  atomic.Value     // 错误信息
	startTime time.Time        // nsqd 开始运行的时间

	topicMap map[string]*Topic // nsqd 中的 topic map

	clientLock sync.RWMutex
	clients    map[int64]Client

	lookupPeers atomic.Value

	tcpListener   net.Listener // tcp 监听器
	httpListener  net.Listener // http 监听器
	httpsListener net.Listener // https 监听器
	tlsConfig     *tls.Config  // tls 的配置文件

	poolSize int // nsqd 执行 queueScan 的 worker 数目

	notifyChan           chan interface{}      // 通知 chan，当 增加/删除 topic/channel 的时候，通知 lookupLoop 进行 Register/Unregister
	optsNotificationChan chan struct{}         // 配置文件 chan
	exitChan             chan int              // 退出 chan
	waitGroup            util.WaitGroupWrapper // 包裹一层 waitGroup，当执行 Exit 的时候，需要等待部分函数执行完毕

	ci *clusterinfo.ClusterInfo
}

// New 新建一个 nsqd 实例
func New(opts *Options) *NSQD {
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		clients:              make(map[int64]Client),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})

	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	var err error
	opts.logLevel, err = lg.ParseLogLevel(opts.LogLevel, opts.Verbose)
	if err != nil {
		n.logf(LOG_FATAL, "%s", err)
		os.Exit(1)
	}

	err = n.dl.Lock() // 给 datapath 上文件锁，为了必要时候存储 metadata
	if err != nil {
		n.logf(LOG_FATAL, "--data-path=%s in use (possibly by another instance of nsqd)", dataPath)
		os.Exit(1)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		n.logf(LOG_FATAL, "--max-deflate-level must be [1,9]")
		os.Exit(1)
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		n.logf(LOG_FATAL, "--node-id must be [0,1024)")
		os.Exit(1)
	}

	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			n.logf(LOG_FATAL, "failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
			os.Exit(1)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		n.logf(LOG_FATAL, "failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		n.logf(LOG_FATAL, "cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			n.logf(LOG_FATAL, "Invalid percentile: %v", v)
			os.Exit(1)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	return n
}

// 获取 nsqd 当前的 options
func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// 更新 nsqd 的 options
func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

// RealTCPAddr 返回 tcp 监听器的地址
func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	return n.tcpListener.Addr().(*net.TCPAddr)
}

// RealHTTPAddr 返回 http 监听器的地址
func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

// RealHTTPSAddr 返回 https 监听器的地址
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	return n.httpsListener.Addr().(*net.TCPAddr)
}

// SetHealth 存储错误状态
func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

// IsHealthy 判断当前 nsqd 是否健康
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// GetError 获取错误信息
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

// GetHealth 得到当前 nsqd 的健康状态
func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

// GetStartTime 获取 nsqd 的启动时间
func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// AddClient 新增消费者
func (n *NSQD) AddClient(clientID int64, client Client) {
	n.clientLock.Lock()
	n.clients[clientID] = client
	n.clientLock.Unlock()
}

// RemoveClient 删除消费者
func (n *NSQD) RemoveClient(clientID int64) {
	n.clientLock.Lock()
	_, ok := n.clients[clientID]
	if !ok {
		n.clientLock.Unlock()
		return
	}
	delete(n.clients, clientID)
	n.clientLock.Unlock()
}

// Main 函数启动 nsqd
func (n *NSQD) Main() {
	var err error
	ctx := &context{n} // 创建 context 实例，存储 nsqd 上下文

	n.tcpListener, err = net.Listen("tcp", n.getOpts().TCPAddress) // 创建 nsqd 的 tcp 监听器
	if err != nil {
		n.logf(LOG_FATAL, "listen (%s) failed - %s", n.getOpts().TCPAddress, err)
		os.Exit(1)
	}
	n.httpListener, err = net.Listen("tcp", n.getOpts().HTTPAddress) // 创建 nsqd 的 http 监听器
	if err != nil {
		n.logf(LOG_FATAL, "listen (%s) failed - %s", n.getOpts().HTTPAddress, err)
		os.Exit(1)
	}
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" { // 如果有需要，创建 nsqd 的 https 监听器
		n.httpsListener, err = tls.Listen("tcp", n.getOpts().HTTPSAddress, n.tlsConfig)
		if err != nil {
			n.logf(LOG_FATAL, "listen (%s) failed - %s", n.getOpts().HTTPSAddress, err)
			os.Exit(1)
		}
	}

	tcpServer := &tcpServer{ctx: ctx} // 创建 tcp Server
	n.waitGroup.Wrap(func() {
		protocol.TCPServer(n.tcpListener, tcpServer, n.logf)
	})
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired) // 创建 http Server
	n.waitGroup.Wrap(func() {
		http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf)
	})
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" { // 如果有必要的话，创建 https Server
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf)
		})
	}

	n.waitGroup.Wrap(n.queueScanLoop)
	n.waitGroup.Wrap(n.lookupLoop)
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}
}

// metadata(元数据)结构体，用于存储 nsqd 中的 topic 和 channel
type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

// 获取存储元数据的路径
func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

// 读取指定路径的文件
func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

// 同步写入，确保写入元数据后，close nsqd
func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// LoadMetadata 读取存储的 nsqd 元数据，恢复意外断开时候的状态
func (n *NSQD) LoadMetadata() error {
	atomic.StoreInt32(&n.isLoading, 1) // 将正在 loading 的标识符设置为1
	defer atomic.StoreInt32(&n.isLoading, 0)

	fn := newMetadataFile(n.getOpts())

	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m) // 将元数据解析到 meta 实例中
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	// 根据解析所得的元数据，恢复 nsqd 的 topics 和各 topic 的 channels
	for _, t := range m.Topics {
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		topic := n.GetTopic(t.Name)
		if t.Paused {
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		topic.Start()
	}
	return nil
}

// PersistMetadata 存储的 nsqd 元数据
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

// Exit 执行 nsqd 关闭时的操作
func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close() // 关闭 tcp 监听器
	}

	if n.httpListener != nil {
		n.httpListener.Close() // 关闭 http 监听器
	}

	if n.httpsListener != nil {
		n.httpsListener.Close() // 关闭 https 监听器
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	n.waitGroup.Wait()
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()

	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if loading metadata at startup, no lookupd connections yet, topic started after
	// 如果当前 nsqd 处于 loading metadata 的状态，在 load 完毕，会进行 topic.Start
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	// 如果不处于 loading metadata 的状态，同时使用 nsqlookupd，那么请求 nsqlookupd，获取 topic 对应的全部 channel
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	t.Start()
	return t
}

// GetExistingTopic 获取一个存在的 topic
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic 删除一个存在的 topic
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// Notify 通知 lookupLoop topic 和 channel 的更改
func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// 获取全部 topic 的 channels
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool 调整 queueScanWorker goroutine 的数目
// 1 <= pool <= min(channel 数目 * 0.25, nsqd 配置文件中设置允许的最大 worker 数目)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
// nsqd 启动之后，执行 queueScanLoop 函数，queueScanLoop 执行在一个 goroutine 之上
// 用于处理飞行队列和延迟队列，它管理一个 queueScanWorker 池并行进行处理
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount) // worker chan
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount) // worker chan
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)           // worker 定时器，用于定时选取一定数量的 channel
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval) // refersh 定时器，用于定时更新 pool 数量

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
