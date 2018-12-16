## protocol_v2.go

`protocol_v2.go` 在 nsqd 中相当重要，定义了 nsqd tcp server 的 IOLoop，提供了 IDENTIFY、SUB 等请求的 handler 函数，同时实现了 messagePump 用于将 channel 中的 message push 给 consumer，接下来，我们结合源码来看一下

### IOLoop

IOLoop 可以类似理解为 http 服务中的 router。当 client 通过 tcp 连接上 nsqd 之后，启动一个 IOLoop，接收 client 的 tcp 请求，解析后然后分配给不同的 handler 执行

```go
for {
	// ReadSlice does not allocate new space for the data each request
	// ie. the returned slice is only valid until the next call to it
	// 这里使用 ReadSlice 而不是 ReadString/ReadBytes 的原因是 ReadSlice 不会
	// 重新分配内存，而在 IOLoop 中，不会出现两次 ReadSlice 的调用，变量不会被覆盖
	line, err = client.Reader.ReadSlice('\n')
	if err != nil {
		if err == io.EOF {
			err = nil
		} else {
			err = fmt.Errorf("failed to read command - %s", err)
		}
		break
	}

	// trim the '\n'
	line = line[:len(line)-1]
	// optionally trim the '\r'
	// windows 的换行是 \r\n
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	params := bytes.Split(line, separatorBytes)

	p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params)

	var response []byte
	response, err = p.Exec(client, params) // 调用相应的 handler 处理 tcp request

	if response != nil {
		err = p.Send(client, frameTypeResponse, response)
		if err != nil {
			err = fmt.Errorf("failed to send response - %s", err)
			break
		}
	}
}

// Exec 可以理解为 tcp 的 router
func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) { // client 注册
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(params[0], []byte("FIN")): // 一条消息消费完毕
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")): // consumer 当前能消费多少消息
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")): // 重新发送一条消息
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")): // producer 发布一条消息
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")): // producer 发布多条消息
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")): // producer 延迟发布消息
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")): // 无操作
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")): // 修改消息的 timeout 时间，但是不重新发送
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")): // consumer 订阅 channel
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")): // client close
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")): // client 鉴权
		return p.AUTH(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}
```

### IDENTITY 和 SUB

上方的 `Exec` 函数展示了全部的 handler，这里我们结合源码看我个人认为比较重要的两个 IDENTITY 和 SUB

先来看一下 IDENTITY 吧，当 client 连接上 tcp server 之后，正常来说的话，client 应该最先会发送 IDENTITY 请求，这个主要是用于更新 client\_v2（见 client\_v2.readme） 的 param 以及告知 client server 端的参数配置。如下所示，首先会进行一系列的正确性校验，校验通过之后，将 body 解析成一个 identifyDataV2 实例，调用 client.Identify(identifyData) 更新 client\_v2 的 param，然后将 resp 返回给 client

```go
// 当前 client 的状态不为 stateInit，报错
if atomic.LoadInt32(&client.State) != stateInit {
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
}

// 读取 tcp 包的长度
bodyLen, err := readLen(client.Reader, client.lenSlice)
if err != nil {
	return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
}

// 如果包长大于 nsqd 允许的最大长度，报错
if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
	return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
		fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
}

// 如果包长小于 0，报错
if bodyLen <= 0 {
	return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
		fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
}

body := make([]byte, bodyLen)
_, err = io.ReadFull(client.Reader, body)
if err != nil {
	return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
}

// body is a json structure with producer information
var identifyData identifyDataV2
err = json.Unmarshal(body, &identifyData)
if err != nil {
	return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
}

p.ctx.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %+v", client, identifyData)

err = client.Identify(identifyData)

...

resp, err := json.Marshal(struct {
	MaxRdyCount         int64  `json:"max_rdy_count"`
	Version             string `json:"version"`
	MaxMsgTimeout       int64  `json:"max_msg_timeout"`
	MsgTimeout          int64  `json:"msg_timeout"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	MaxDeflateLevel     int    `json:"max_deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	AuthRequired        bool   `json:"auth_required"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int64  `json:"output_buffer_timeout"`
}{
	MaxRdyCount:         p.ctx.nsqd.getOpts().MaxRdyCount,
	Version:             version.Binary,
	MaxMsgTimeout:       int64(p.ctx.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
	MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
	TLSv1:               tlsv1,
	Deflate:             deflate,
	DeflateLevel:        deflateLevel,
	MaxDeflateLevel:     p.ctx.nsqd.getOpts().MaxDeflateLevel,
	Snappy:              snappy,
	SampleRate:          client.SampleRate,
	AuthRequired:        p.ctx.nsqd.IsAuthEnabled(),
	OutputBufferSize:    client.OutputBufferSize,
	OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
})
if err != nil {
	return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
}

err = p.Send(client, frameTypeResponse, resp)
```

再来看一下 SUB，当 consumer 想要消费消息，就需要订阅一个 channel，这样 nsqd 才能向 consumer push 消息，SUB 实现的就是订阅功能。下方代码省略了和 IDENTITY 相同的包长度校验，首先，从 tcp request 中解析出 topicName 以及 channelName，然后调用 CheckAuth 函数获取 client 是否有订阅（topicName, channelName）的权限，如果具有权限，则将 client 写入 channel 的 clientMap，同时将 channel 写入 client\_v2 的 Channel 字段，最后将 channel 写入 client\_v2.SubEventChan 这个 chan，开始消费消息

```go
topicName := string(params[1])
if !protocol.IsValidTopicName(topicName) {
	return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
		fmt.Sprintf("SUB topic name %q is not valid", topicName))
}

channelName := string(params[2])
if !protocol.IsValidChannelName(channelName) {
	return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
		fmt.Sprintf("SUB channel name %q is not valid", channelName))
}

if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
	return nil, err
}

var channel *Channel
for {
	topic := p.ctx.nsqd.GetTopic(topicName)
	channel = topic.GetChannel(channelName)
	channel.AddClient(client.ID, client)

	if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
		channel.RemoveClient(client.ID)
		time.Sleep(1 * time.Millisecond)
		continue
	}
	break
}
atomic.StoreInt32(&client.State, stateSubscribed)
client.Channel = channel
// update message pump
client.SubEventChan <- channel

return okBytes, nil
```

### messagePump

messagePump 实现了消息的 push。for 循环最上面的 if-else 循环是用于控制 client\_v2.writer 的 flush 的，这里，Nsq 采用了 group commit，并不是一条消息 send 给 consumer 一次，而是多条消息一起 send。 在 select 中，当 flusherChan 赢得竞争的时候，flush 一次，将 flushed 设为 true，下次 for 循环的时候就不会 flush 了；当 subEventChan 赢得竞争时，subChannel 获得赋值，相应的，memoryMsgChan 和 backendMsgChan 就不为 nil 了，可以参与竞争，消息消费也就开始了；当 identifyEventChan 赢得竞争后，根据 client\_v2 新的 param 作出相应的更改；当 memoryMsgChan 和 backendMsgChan 赢得竞争的时候，消费一条消息

```
for {
	if subChannel == nil || !client.IsReadyForMessages() {
		// client 还没有准备好获取消息
		memoryMsgChan = nil
		backendMsgChan = nil
		flusherChan = nil
		// force flush
		client.writeLock.Lock()
		err = client.Flush()
		client.writeLock.Unlock()
		if err != nil {
			goto exit
		}
		flushed = true
	} else if flushed {
		// 上一次循环执行了 flush 操作，将 flusherChan 置为 nil，防止再次 flush
		memoryMsgChan = subChannel.memoryMsgChan
		backendMsgChan = subChannel.backend.ReadChan()
		flusherChan = nil
	} else {
		// writer buffer 了消息，设置 flusherChan，进行 select race
		memoryMsgChan = subChannel.memoryMsgChan
		backendMsgChan = subChannel.backend.ReadChan()
		flusherChan = outputBufferTicker.C
	}

	select {
	case <-flusherChan:
		// 强制 flush
		client.writeLock.Lock()
		err = client.Flush()
		client.writeLock.Unlock()
		if err != nil {
			goto exit
		}
		flushed = true
	case <-client.ReadyStateChan:
	case subChannel = <-subEventChan:
		// 一个 client 只能订阅一个 channel
		subEventChan = nil
	case identifyData := <-identifyEventChan:
		// 一个 client 只能 IDENTITY 一次
		// IDENTITY 之后，会更新 client 的一些 parameters
		// 通过回调 chan，在这里同步更新
		identifyEventChan = nil

		outputBufferTicker.Stop()
		if identifyData.OutputBufferTimeout > 0 {
			outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
		}

		heartbeatTicker.Stop()
		heartbeatChan = nil
		if identifyData.HeartbeatInterval > 0 {
			heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
			heartbeatChan = heartbeatTicker.C
		}

		if identifyData.SampleRate > 0 {
			sampleRate = identifyData.SampleRate
		}

		msgTimeout = identifyData.MsgTimeout
	case <-heartbeatChan:
		// 返回心跳包
		err = p.Send(client, frameTypeResponse, heartbeatBytes)
		if err != nil {
			goto exit
		}
	case b := <-backendMsgChan:
		// 从 channel 的 backendMsgChan 消费一条消息
		if sampleRate > 0 && rand.Int31n(100) > sampleRate {
			continue
		}

		msg, err := decodeMessage(b)
		if err != nil {
			p.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
			continue
		}
		msg.Attempts++

		subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout) // 写入 channel 的发送列表
		client.SendingMessage()
		err = p.SendMessage(client, msg)
		if err != nil {
			goto exit
		}
		flushed = false
	case msg := <-memoryMsgChan:
		// 从 channel 的内存 memoryMsgChan 中消费一条消息，同样写入 channel 的发送列表
		if sampleRate > 0 && rand.Int31n(100) > sampleRate {
			continue
		}
		msg.Attempts++

		subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
		client.SendingMessage()
		err = p.SendMessage(client, msg)
		if err != nil {
			goto exit
		}
		flushed = false
	case <-client.ExitChan:
		goto exit
	}
}
```
 