## nsqd

`nsqd` 是 Nsq 中用来接收、管理消息并将消息 push 给消费者的常驻进程

首先我们来捋一捋 Nsq 的工作流程，Nsq 是一个消息队列，那么自然就存在生产者和消费者，生产者通过 http 或者 tcp 连接，将消息发送到 nsqd 实例的相应的 topic 内，然后 topic 将消息复制到每一个 channel 中，channel 再将消息 push 给随意一个订阅的消费者

接下来，我们逐个文件来看一下具体的源码

### nsqd.go

`nsqd.do` 定义了 NSQD 结构，实现了 nsqd 实例的创建，nsqd 服务的启动，nsqd 实例异常中断时的元数据持久化以及 nsqd 实例重新启动时候的元数据恢复

nsqd 结构如下所示

```go
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
```

#### nsqd 实例的创建

首先，当 options 中没有给出 dataPath 的时候，nsqd 会将当前目录作为持久化存储 nsqd 实例元数据、topic 和 channel 的 backendMsgQueue 的地方

然后，初始化了 nsqd 实例的部分参数，这里使用了 atomic.Value 来存储 opts、lookupPeers 以及 errValue，轻量级的实现了数据的原子性

```go
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
```

#### nsqd 服务的启动

和 nsqlookupd 服务的启动类似，nsqd 服务也需要新建 http 和 tcp server，然后用 waitGroupWrap 包裹执行，nsqd 服务有两个常驻 goroutine，queueScanLoop 用来实时控制 queueScanWorker 的数量，同时处理延迟发送的 message 以及 timeout 的 message，lookupLoop 用于对 topic 和 channel 执行 REGISTER 操作和 UNREGISTER 操作，以及当 nsqlookupd 实例变动的时候，进行响应的处理

```go
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
```

#### 元数据的持久化

LoadMetadata 用于在 nsqd 实例意外 exit 的时候存储 nsqd 实例的元数据

首先，获取存储路径 - dataPath + nsqd.dat，然后遍历获取每个 topic 的 name 和 pause 状态，最后**同步**写入，完成持久化

```go
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
```

#### 元数据的获取

LoadMetadata 完成的就是 PersistMetadata 的镜像操作，从 dataPath + nsqd.dat 中读出存储的元数据，然后逐一恢复即可

```go
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
```

#### queueScanLoop

👇的源码是 queueScanLoop 最重要的部分，当 refreshTicker.C 赢得 select 的竞争的时候，queueScanLoop 会根据当前的情况，动态的增删 queueScanWorker，queueScanWorker 用于处理每个 channel 中延迟发送的 message 和 timeout 的 message；当 workTicker.C 赢得 select 的竞争的时候，queueScanLoop 会计算当前的 dirtyPercent，也就是存在需要延迟发送/timeout 的channel 比率，若该比率大于 opts 中的额定值，queueScanLoop 会暂停 loop 等待 dirtyPercent 降低

```go
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
```
