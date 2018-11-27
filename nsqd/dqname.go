// +build !windows

package nsqd

// 得到非 windows 环境下的磁盘存储名

func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + ":" + channelName
	return backendName
}
