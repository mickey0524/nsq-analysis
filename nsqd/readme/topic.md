## topic

`topic`，顾名思义，代表话题，在 nsq 中，topic 用来区分不同种类的消息，无论生产者通过 http 还是 tcp 发布消息，都需要带上 topic 字段，用来告诉 nsqd 消息的类别，在 nsqd 实例中，topicMap 用来保存所有的 topic 实例

### topic.go

`topic.go` 定义了 Topic 结构，实现了 topic 实例的创建，消息的写入以及最重要的 messagePump

Topic 结构如下所示

```go
type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64 // topic 中的消息数目

	sync.RWMutex

	name              string                // topic 的名字
	channelMap        map[string]*Channel   // topic 中的 channel map
	backend           BackendQueue          // 二级磁盘存储队列
	memoryMsgChan     chan *Message         // 消息 chan，当 nsqd 接收到一条消息，写入该 channel
	startChan         chan int              // 启动 chan
	exitChan          chan int              // 终止 chan
	channelUpdateChan chan int              // topic 内的 channel map 增加/删除
	waitGroup         util.WaitGroupWrapper // 包裹一层 waitGroup，当执行 Exit 的时候，需要等待部分函数执行完毕
	exitFlag          int32                 // topic 是否处于退出状态
	idFactory         *guidFactory

	ephemeral      bool         // topic 是否为临时的，临时的 topic 不会存入磁盘
	deleteCallback func(*Topic) // 删除的回调函数
	deleter        sync.Once

	paused    int32    // topic 是否处于暂停状态
	pauseChan chan int // pause 事件的回调 chan

	ctx *context // nsqd 上下文
}
```

#### topic 实例的创建

首先，给部分参数赋初值，生成一个 Topic 的实例，然后根据 topicName 是否带有 #ephemeral 的后缀判断 topic 是否为临时的，临时的 topic 是不会将超出内存限制的 message 写入磁盘的，然后用 waitGroupWrap 包裹执行 messagePump，最后调用 Notity 函数通知 nsqd 有新的 topic 生成，在 lookupLoop 中会向 nsqlookupd 发送 REGISTER 请求注册本 topic

```go
// NewTopic 生成一个新的 topic 实例
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback,
		idFactory:         NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}

	if strings.HasSuffix(topicName, "#ephemeral") { // 临时 topic，消息超出内存限制不落磁盘
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.logLevel, lg.LogLevel(level), f, args...)
		}
		t.backend = diskqueue.New(
			topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	t.waitGroup.Wrap(t.messagePump)

	t.ctx.nsqd.Notify(t) // 通知 lookupLoop，新增了一个 Topic，进行 Register 操作

	return t
}
```

#### 消息的写入

PutMessage 方法实现了消息的写入，http 的 /pub handler 和 tcp 的 PUB handler 调用本方法发布消息，PutMessage 调用 put 方法，put 方法首先尝试写入 memoryMsgQueue，当内存队列已满的时候，将消息写入 backendQueue 存储磁盘

```go
// PutMessage 向 topic 写一条消息，如果 memoryMsgQueue 满了，则写入 backendQueue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

// 收到一条 message，将其写入 memoryMsgChan 或者 BackendQueue
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m: // 当 chan 中还能写入时，写入 memoryMsgChan
	default: // 否则，写入 BackendQueue，落入磁盘
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, t.backend)
		bufferPoolPut(b)
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}
```

#### messagePump

messagePump 是 topic 中相当重要的部分，这是 topic 实例中的常驻 goroutine，主要用于消息的分发，将 topic 中的消息分发给 topic 下的每一个 channel，然后 channel 会将消息随机 push 给订阅的 consumer

👇源码是 messagePump 的主循环，可以看到，如果 memoryMsgChan 或 backendChan 赢得了 select 的竞争的话，
会得到一个 msg，然后循环遍历 channels，调用 channel 的 PutMessage/putMessageDeferred 方法将消息分发下去

```go
// main message loop
for {
	select {
	// 从这里可以看到，如果消息已经被写入磁盘的话，nsq 消费消息就是无序的
	case msg = <-memoryMsgChan:
	case buf = <-backendChan:
		msg, err = decodeMessage(buf)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
			continue
		}
	case <-t.channelUpdateChan:
		chans = chans[:0]
		t.RLock()
		for _, c := range t.channelMap {
			chans = append(chans, c)
		}
		t.RUnlock()
		if len(chans) == 0 || t.IsPaused() {
			memoryMsgChan = nil
			backendChan = nil
		} else {
			memoryMsgChan = t.memoryMsgChan
			backendChan = t.backend.ReadChan()
		}
		continue
	case <-t.pauseChan:
		if len(chans) == 0 || t.IsPaused() {
			memoryMsgChan = nil
			backendChan = nil
		} else {
			memoryMsgChan = t.memoryMsgChan
			backendChan = t.backend.ReadChan()
		}
		continue
	case <-t.exitChan:
		goto exit
	}

	for i, channel := range chans {
		chanMsg := msg
		// copy the message because each channel
		// needs a unique instance but...
		// fastpath to avoid copy if its the first channel
		// (the topic already created the first copy)
		// 这里 msg 实例要进行深拷贝，因为每个 channel 需要自己的实例
		// 为了重发/延迟发送等
		if i > 0 {
			chanMsg = NewMessage(msg.ID, msg.Body)
			chanMsg.Timestamp = msg.Timestamp
			chanMsg.deferred = msg.deferred
		}
		if chanMsg.deferred != 0 {
			channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
			continue
		}
		err := channel.PutMessage(chanMsg)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
				t.name, msg.ID, channel.name, err)
		}
	}
}
```

#### topic 的 Delete/Close

topic 的 delete 和 close 是两种完全不同的操作，当 nsqd 实例意外退出的时候，会遍历 nsqd.topicMap，执行每一个 topic 的 Close 方法，而 Delete 是在 topic 删除的时候执行的，👇源码显示了 delete 和 close 的不同，delete 会清空内存和磁盘上的消息并且 delete backendQueue，同时告知 nsqlookupd topic 已经不存在了，而 close 会讲内存中的消息全部存入磁盘，等待 nsqd 实例下次启动的时候恢复

```go
func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t) // 告知 lookupLoop，topic 被删除了，执行 UNREGISTER 告知 nsqlookupd
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	if deleted {
		// topic 删除，topic 下的 channel 同样执行 Delete 操作
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// 清空 topic 的消息 chan，删除落在磁盘上的消息
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// 将当前内存消息 chan 中的 msg 写入磁盘
	t.flush()
	return t.backend.Close()
}
```