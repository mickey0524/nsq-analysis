package nsqlookupd

import (
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	LogLevel  string `flag:"log-level"`
	LogPrefix string `flag:"log-prefix"`
	Verbose   bool   `flag:"verbose"` // for backwards compatibility
	Logger    Logger
	logLevel  lg.LogLevel // private, not really an option

	TCPAddress       string `flag:"tcp-address"`
	HTTPAddress      string `flag:"http-address"`
	BroadcastAddress string `flag:"broadcast-address"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"` // 定义producer的不活跃时间，当producer超过InactiveProducerTimeout时间没与nsqlookupd进行PING通信，认为是不活跃的
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`        // 避免发生竞争，当一个nsqd不再产生一个特定的toipc, 需要去掉这个toipc，这个时候，试图尝试删除topic信息与新的消费者已经发现这个主题的节点，重连, 会更新nsqlookup产生竞争
	// 在TombstoneLifetime内，新的消费者将不能够通过 /lookup 查询到该topic的生产者，旧的生产者，删除内部状态，同时广播消息给nsqlookupd, 删除之前tombstoned的信息，这样就能防止上面提到的竞争
}

// 创建一个Options实例
func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		LogPrefix:        "[nsqlookupd] ",
		LogLevel:         "info",
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
