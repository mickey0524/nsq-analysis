package nsqlookupd

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex                       // 读写锁，主要为了更新DB中的Registrations，Topics，Channels
	opts         *Options              // option，一些配置文件
	tcpListener  net.Listener          // nsqlookupd的tcp listener，用于nsq和nsqlookupd通信，默认监听端口为4160
	httpListener net.Listener          // nsqlookupd的http listener，用于和nsqadmin通信，默认监听端口为4161
	waitGroup    util.WaitGroupWrapper // 包了一层sync.WaitGroup，用于执行Exit时，等待两个listener close执行完毕
	DB           *RegistrationDB       // 存放当前的Registrations，Topics，Channels
}

// 新建一个nsqlookupd实例
func New(opts *Options) *NSQLookupd {
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	var err error
	opts.logLevel, err = lg.ParseLogLevel(opts.LogLevel, opts.Verbose)
	if err != nil {
		n.logf(LOG_FATAL, "%s", err)
		os.Exit(1)
	}

	n.logf(LOG_INFO, version.String("nsqlookupd"))
	return n
}

// 初始化tcplistener和httplistener
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

// 返回tcp listener的地址
func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

// 返回http listener的地址
func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

// 关闭tcp和http两个listener
func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
