## channel

`channel` 和 golang 的 chan 类型不一样， Nsq 中的 channel 隶属于一个 topic，用于接收 topic 分发的消息，并将消息 push 给 consumer，在 topic 实例中，channelMap 用来保存所有的 channel 实例

### channel.go

`channel.go` 定义了 Channel 结构，实现了 channel 实例的创建，消息的写入，已发送队列和延迟发送队列的维护

Channel 结构如下所示

```go
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64 // 重新发送的消息总数
	messageCount uint64 // 写入 channel 的消息总数
	timeoutCount uint64 // 发送超时的消息总数

	sync.RWMutex

	topicName          // channel 所属的 topic 的名字
	name      string   // channel 的名字
	ctx       *context // 上下文作用域

	backend BackendQueue // 二级存储队列

	memoryMsgChan chan *Message // 消息 chan
	exitFlag      int32         // 当前 channel 的状态，是否处于 close/delete 状态
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer // 消费者 map
	paused         int32              // 当前 channel 是否处于暂停状态
	ephemeral      bool               // channel 是否为临时的
	deleteCallback func(*Channel)     // channel 被删除的时候执行的回调函数
	deleter        sync.Once          // 用于 clients map 为空时候，临时 channel 的删除

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item // 延迟发送类型消息 map
	deferredPQ       pqueue.PriorityQueue       // 延迟发送消息 优先级队列，时间越小的排在越前面，小根堆
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message // 处于飞行中的消息 map
	inFlightPQ       inFlightPqueue         // 处于飞行中的消息 优先级队列，时间越小的排在越前面，用于处理超时的消息
	inFlightMutex    sync.Mutex
}
```

#### channel 实例的创建

首先，和 topic 实例的创建一样，给部分参数赋初值，生成一个 Channel 的实例，然后调用 initPQ 函数，初始化已发送队列和延迟发送队列，然后根据 channelName 是否带 #ephemeral 的后缀判断 channel 是否为临时的，并据此创建不同的 backendQueue，最后，同样调用 Notity 函数通知 nsqd 有新的 channel 生成，在 lookupLoop 中会向 nsqlookupd 发送 REGISTER 请求注册本 channel

```go
// NewChannel 创建一个新的 Channel
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ() // 初始化 deferredMessages，deferredPQ，inFlightMessages，inFlightPQ

	if strings.HasSuffix(channelName, "#ephemeral") { // 临时 channel，创建假的二级存储队列
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.logLevel, lg.LogLevel(level), f, args...)
		}
		// 创建真正的二级存储队列
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	c.ctx.nsqd.Notify(c) // 通知 lookupdLoop 创建了新的 Channel，执行 REGISTER 告知 nsqlookupd

	return c
}
```

#### 消息的写入

channel 消息的写入和 topic 类似，这里不详细介绍了，channel 提供了 PutMessageDeferred 方法，当 message 的 deferred > 0 的时候，调用实现延迟发送

```go
// IsPaused 判断当前 channel 是否是暂停状态
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage 将一条 message 写入队列
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 将一条 message 写入队列，如果 message chan 没满，写入内存，反之，写入二级存储队列
func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m:
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// PutMessageDeferred 将一条 message 写入延迟发送队列
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}
```

#### 已发送队列的维护

已发送队列是一个小根堆，依据 message.pri 排序，pri 字段代表 message 的 timeout 时间，StartInFlightTimeout 方法用来将一条消息写入 inFlight 队列，消息的 pri 等于当前时间和 timeout 之和，processInFlightQueue 方法用来检查当前 channel 是否有 timeout 的消息，存在的话，重新 push，processInFlightQueue 在 nsqd.go 的 queueScanWorker 中被调用

```go
// StartInFlightTimeout 将一条消息写入 inFlight 队列
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// 用于 nsqd.go 中的 queueScanLoop，用于重发超时的消息
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		c.put(msg)
	}

exit:
	return dirty
}
```

#### 延迟发送队列的维护

延迟发送队列同样是一个小根堆，依据 pqueue.Item.Priority 排序，Priority 字段代表 message 应该被发送的时间，StartDeferredTimeout 方法用来将一条消息写入 Deferred 队列，processDeferredQueue 方法用来检查当前 channel 是否有需要发送的消息，processInFlightQueue 在 nsqd.go 的 queueScanWorker 中被调用

```go
// StartDeferredTimeout 将一条消息写入 Deferred 队列
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano() // 生成延迟发送的时间
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// 用于 nsqd.go 中的 queueScanLoop，用于 put 延迟发送的消息
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}

exit:
	return dirty
}
```