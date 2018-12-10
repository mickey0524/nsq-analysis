## nsqlookupd

`nsqlookupd` 是一个管理拓扑元数据的常驻守护进程，消费者能够访问 `nsqlookupd` 来发现对应 (topic, channel) 的地址

### nsqlookupd.go

`nsqlookupd.go` 实现了 nsqlookupd 实例的创建，nsqlookupd 服务的启动

nsqlookupd 结构如下所示

```
type NSQLookupd struct {
	sync.RWMutex                       // 读写锁，主要为了更新DB中的Registrations，Topics，Channels
	opts         *Options              // option，一些配置文件
	tcpListener  net.Listener          // nsqlookupd的tcp listener，用于nsq和nsqlookupd通信，默认监听端口为4160
	httpListener net.Listener          // nsqlookupd的http listener，用于和nsqadmin通信，默认监听端口为4161
	waitGroup    util.WaitGroupWrapper // 包了一层sync.WaitGroup，用于执行Exit时，等待两个listener close执行完毕
	DB           *RegistrationDB       // 存放当前的Registrations，Topics，Channels
}
```

#### NSQLookupd 实例的创建

传入 nsqlookupd 的 options，新建一个 DB 实例，创建一个 nsqlookupd 实例

```
n := &NSQLookupd{
	opts: opts,
	DB:   NewRegistrationDB(),
}
```

#### NSQLookupd 实例的启动

新建 tcp server 和 http server，然后用 waitGroupWrap 包裹启动两个 server，这样在执行 Exit 退出的时候，会先等待两个 server close

```
func (l *NSQLookupd) Main() error {
	ctx := &Context{l}

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		return fmt.Errorf("listen (%s) failed - %s", l.opts.TCPAddress, err)
	}
	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		return fmt.Errorf("listen (%s) failed - %s", l.opts.TCPAddress, err)
	}

	l.tcpListener = tcpListener
	l.httpListener = httpListener

	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(tcpListener, tcpServer, l.logf)
	})
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.logf)
	})

	return nil
}
```

### registration_db.go

`registration_db.go` 主要用于存储 nsq 的拓扑元数据，包括连上 nsqlookupd 的 nsqd 节点，topics 对应的 nsqd 节点 以及 channels 对应的 nsqd 节点

数据结构如下所示

```
// RegistrationDB 存放一个Registration的全部Producer
type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

// Registration 定义Registration的种类，例如，Category可以为client,topic,channel
type Registration struct {
	Category string "client/topic/channel"
	Key      string
	SubKey   string
}
type Registrations []Registration

// PeerInfo 定义一个nsq节点
type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// Producer 存放nsq节点的数据结构，以及当前节点是否处于tombstoned状态
type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer
type ProducerMap map[string]*Produc
```

### lookup_protocol_v1.go

`lookup_protocol_v1.go` 主要实现了 tcp server 的 router 和 handler，响应 PING，REGISTER，UNREGISTER 和 IDENTITY 四种请求

#### tcp router

tcp request 是字节流，读取一行数据，用空格将字节流切割成 string 切片，调用 EXEC 函数，匹配执行不同的 handler

```
line, err = reader.ReadString('\n')
if err != nil {
	break
}

line = strings.TrimSpace(line)
params := strings.Split(line, " ")

var response []byte
response, err = p.Exec(client, reader, params)

// Exec 分析指令类型，调用不同的处理函数
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}
```

#### REGISTER handler

REGISTER 首先获取 topic 和 channel，然后创建相应的 Registration，Registration 是 nsqlookupd.DB 的 key，将本连接的 nsqd 节点加入该 key 的 ProducerMap

```
topic, channel, err := getTopicChan("REGISTER", params)
if err != nil {
	return nil, err
}

if channel != "" {
	key := Registration{"channel", topic, channel}
	if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "channel", topic, channel)
	}
}
key := Registration{"topic", topic, ""}
if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
	p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
		client, "topic", topic, "")
}
```

#### UNREGISTER handler

UNREGISTER 首先获取 topic 和 channel，然后创建相应的 Registration，将该 nsqd 节点从 Registration 对应的 ProducerMap 中删除

```
topic, channel, err := getTopicChan("UNREGISTER", params)
if err != nil {
	return nil, err
}

if channel != "" {
	key := Registration{"channel", topic, channel}
	removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
	if removed {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
			client, "channel", topic, channel)
	}
	// for ephemeral channels, remove the channel as well if it has no producers
	if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
		p.ctx.nsqlookupd.DB.RemoveRegistration(key)
	}
} else {
	// no channel was specified so this is a topic unregistration
	// remove all of the channel registrations...
	// normally this shouldn't happen which is why we print a warning message
	// if anything is actually removed
	registrations := p.ctx.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
	for _, r := range registrations {
		if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
			p.ctx.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, r.SubKey)
		}
	}

	key := Registration{"topic", topic, ""}
	if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id); removed {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}
}
```

#### PING hanler

PING 主要是 nsqd 用来向 nsqlookupd 发送心跳包的，代码很简单，就是更新一下对应 peerInfo 的 lastUpdate 时间，这个时间在判断 tombstoned

```
if client.peerInfo != nil {
	// we could get a PING before other commands on the same client connection
	cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
	now := time.Now()
	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
		now.Sub(cur))
	atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
}
return []byte("OK"), nil
```

#### IDENTIFY handler

IDENTIFY 主要是 nsqd 连上 tcp server 时候发送的注册包，实例化一个 PeerInfo 对象存储 nsqd 的元信息，然后将该 PeerInfo 对象加入 `Registration{"client", "", ""}` 的对应的 ProducerMap

```
peerInfo := PeerInfo{id: client.RemoteAddr().String()}
err = json.Unmarshal(body, &peerInfo)
if err != nil {
	return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
}

...

if p.ctx.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
	p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
}
```

### http.go

`http.go` 主要实现了 http server 的 router 和 handler，响应各种 http 请求，例如 /ping，/info 等

#### http router

`http.go` 使用开源的轻量级路由库 httprouter 来进行路由选择

```
// http router，用于响应 http 请求，其实就是一个 web server，用来显示 nsqlookup.DB 的状态
func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqlookupd.logf)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqlookupd.logf)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqlookupd.logf)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqlookupd.logf)
	s := &httpServer{
		ctx:    ctx,
		router: router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.V1))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.V1))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.V1))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.V1))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.V1))

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}
```

#### handler 太多，用 /channel/create 举例子

首先，调用 http_api.NewReqParams 方法获取 topicName 和 channelName，然后新建一个对应的 Registration，添加到 DB 中

```
// 创建 channel
func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	s.ctx.nsqlookupd.logf(LOG_INFO, "DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}
``` 


