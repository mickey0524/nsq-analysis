package nsqd

import (
	"runtime"
	"sort"
	"sync/atomic"

	"github.com/nsqio/nsq/internal/quantile"
)

// TopicStats 代表 topic 的当前状态
type TopicStats struct {
	TopicName    string         `json:"topic_name"`    // topic 的名字
	Channels     []ChannelStats `json:"channels"`      // 属于本 topic 的全部 channel 的当前状态
	Depth        int64          `json:"depth"`         // topic 的 depth（等于内存中消息通道的消息数目 + backendQueue 的 depth）
	BackendDepth int64          `json:"backend_depth"` // backendQueue 的 depth
	MessageCount uint64         `json:"message_count"` // topic 当前的消息数目
	Paused       bool           `json:"paused"`        // topic 是否处于暂停状态

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

// NewTopicStats 新建一个 TopicStats 实例
func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{
		TopicName:    t.name,
		Channels:     channels,
		Depth:        t.Depth(),
		BackendDepth: t.backend.Depth(),
		MessageCount: atomic.LoadUint64(&t.messageCount),
		Paused:       t.IsPaused(),

		E2eProcessingLatency: t.AggregateChannelE2eProcessingLatency().Result(),
	}
}

// ChannelStats 代表 channel 的当前状态
type ChannelStats struct {
	ChannelName   string        `json:"channel_name"`    // channel 的名字
	Depth         int64         `json:"depth"`           // topic 的 depth（等于内存中消息通道的消息数目 + backendQueue 的 depth）
	BackendDepth  int64         `json:"backend_depth"`   // backendQueue 的 depth
	InFlightCount int           `json:"in_flight_count"` // 处于发送状态的消息总数
	DeferredCount int           `json:"deferred_count"`  // 延迟发送的消息总数
	MessageCount  uint64        `json:"message_count"`   // channel 当前的消息总数
	RequeueCount  uint64        `json:"requeue_count"`   // channel 当前重新安排的消息总数
	TimeoutCount  uint64        `json:"timeout_count"`   // channel 当前的超时消息总数
	Clients       []ClientStats `json:"clients"`         // 属于本 topic 的全部 consumer
	Paused        bool          `json:"paused"`          // channel 是否处于暂停状态

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

// NewChannelStats 新建一个 ChannelStats 实例
func NewChannelStats(c *Channel, clients []ClientStats) ChannelStats {
	c.inFlightMutex.Lock()
	inflight := len(c.inFlightMessages)
	c.inFlightMutex.Unlock()
	c.deferredMutex.Lock()
	deferred := len(c.deferredMessages)
	c.deferredMutex.Unlock()

	return ChannelStats{
		ChannelName:   c.name,
		Depth:         c.Depth(),
		BackendDepth:  c.backend.Depth(),
		InFlightCount: inflight,
		DeferredCount: deferred,
		MessageCount:  atomic.LoadUint64(&c.messageCount),
		RequeueCount:  atomic.LoadUint64(&c.requeueCount),
		TimeoutCount:  atomic.LoadUint64(&c.timeoutCount),
		Clients:       clients,
		Paused:        c.IsPaused(),

		E2eProcessingLatency: c.e2eProcessingLatencyStream.Result(),
	}
}

// PubCount 指代当前 producer 发布消息的 topic 和 count
type PubCount struct {
	Topic string `json:"topic"`
	Count uint64 `json:"count"`
}

// ClientStats 代表 client 的当前状态
type ClientStats struct {
	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`

	PubCounts []PubCount `json:"pub_counts,omitempty"`

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

// Topics 数组
type Topics []*Topic

// 用于 Topics 排序
func (t Topics) Len() int      { return len(t) }
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicsByName struct {
	Topics
}

func (t TopicsByName) Less(i, j int) bool { return t.Topics[i].name < t.Topics[j].name }

// Channels 数组
type Channels []*Channel

// 用于 Channels 排序
func (c Channels) Len() int      { return len(c) }
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelsByName struct {
	Channels
}

func (c ChannelsByName) Less(i, j int) bool { return c.Channels[i].name < c.Channels[j].name }

// GetStats 获取 topic，channel 的当前 stats，若为空，则返回 nsqd 全部的 topics，channels 的 stats
func (n *NSQD) GetStats(topic string, channel string) []TopicStats {
	n.RLock()
	var realTopics []*Topic
	if topic == "" {
		realTopics = make([]*Topic, 0, len(n.topicMap))
		for _, t := range n.topicMap {
			realTopics = append(realTopics, t)
		}
	} else if val, exists := n.topicMap[topic]; exists {
		realTopics = []*Topic{val}
	} else {
		n.RUnlock()
		return []TopicStats{}
	}
	n.RUnlock()
	sort.Sort(TopicsByName{realTopics})
	topics := make([]TopicStats, 0, len(realTopics))
	for _, t := range realTopics {
		t.RLock()
		var realChannels []*Channel
		if channel == "" {
			realChannels = make([]*Channel, 0, len(t.channelMap))
			for _, c := range t.channelMap {
				realChannels = append(realChannels, c)
			}
		} else if val, exists := t.channelMap[channel]; exists {
			realChannels = []*Channel{val}
		} else {
			t.RUnlock()
			continue
		}
		t.RUnlock()
		sort.Sort(ChannelsByName{realChannels})
		channels := make([]ChannelStats, 0, len(realChannels))
		for _, c := range realChannels {
			c.RLock()
			clients := make([]ClientStats, 0, len(c.clients))
			for _, client := range c.clients {
				clients = append(clients, client.Stats())
			}
			c.RUnlock()
			channels = append(channels, NewChannelStats(c, clients))
		}
		topics = append(topics, NewTopicStats(t, channels))
	}
	return topics
}

// GetProducerStats 获取 producer 的 stats
func (n *NSQD) GetProducerStats() []ClientStats {
	n.clientLock.RLock()
	var producers []Client
	for _, c := range n.clients {
		if c.IsProducer() {
			producers = append(producers, c)
		}
	}
	n.clientLock.RUnlock()
	producerStats := make([]ClientStats, 0, len(producers))
	for _, p := range producers {
		producerStats = append(producerStats, p.Stats())
	}
	return producerStats
}

type memStats struct {
	HeapObjects       uint64 `json:"heap_objects"`
	HeapIdleBytes     uint64 `json:"heap_idle_bytes"`
	HeapInUseBytes    uint64 `json:"heap_in_use_bytes"`
	HeapReleasedBytes uint64 `json:"heap_released_bytes"`
	GCPauseUsec100    uint64 `json:"gc_pause_usec_100"`
	GCPauseUsec99     uint64 `json:"gc_pause_usec_99"`
	GCPauseUsec95     uint64 `json:"gc_pause_usec_95"`
	NextGCBytes       uint64 `json:"next_gc_bytes"`
	GCTotalRuns       uint32 `json:"gc_total_runs"`
}

// getMemStats 获取当前 nsqd 的内存消耗情况
func getMemStats() memStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// sort the GC pause array
	length := len(ms.PauseNs)
	if int(ms.NumGC) < length {
		length = int(ms.NumGC)
	}
	gcPauses := make(Uint64Slice, length)
	copy(gcPauses, ms.PauseNs[:length])
	sort.Sort(gcPauses)

	return memStats{
		ms.HeapObjects,
		ms.HeapIdle,
		ms.HeapInuse,
		ms.HeapReleased,
		percentile(100.0, gcPauses, len(gcPauses)) / 1000,
		percentile(99.0, gcPauses, len(gcPauses)) / 1000,
		percentile(95.0, gcPauses, len(gcPauses)) / 1000,
		ms.NextGC,
		ms.NumGC,
	}

}
