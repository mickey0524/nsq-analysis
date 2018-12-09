package protocol

import (
	"regexp"
)

// 合法的 topicName 和 channelName 应该符合的 pattern
var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName 检查一个 topicName 是否合法
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName 检查一个 channelName 是否合法
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

// 检查一个 name 是否合法，从长度和正则表达式两方面来看
func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}
