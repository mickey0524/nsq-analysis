package auth

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"time"

	"github.com/nsqio/nsq/internal/http_api"
)

// Authorization 指代权限 struct
type Authorization struct {
	Topic       string   `json:"topic"`
	Channels    []string `json:"channels"`
	Permissions []string `json:"permissions"`
}

// State 用于表示 client 当前当权限状态
type State struct {
	TTL            int             `json:"ttl"`
	Authorizations []Authorization `json:"authorizations"`
	Identity       string          `json:"identity"`
	IdentityURL    string          `json:"identity_url"`
	Expires        time.Time
}

// HasPermission 表明 Authorization 是否拥有 permission 权限
func (a *Authorization) HasPermission(permission string) bool {
	for _, p := range a.Permissions {
		if permission == p {
			return true
		}
	}
	return false
}

// IsAllowed 指代 Authorization 是否拥有 (topic, channel) 的权限
func (a *Authorization) IsAllowed(topic, channel string) bool {
	if channel != "" {
		if !a.HasPermission("subscribe") {
			return false
		}
	} else {
		if !a.HasPermission("publish") {
			return false
		}
	}

	topicRegex := regexp.MustCompile(a.Topic)

	if !topicRegex.MatchString(topic) {
		return false
	}

	for _, c := range a.Channels {
		channelRegex := regexp.MustCompile(c)
		if channelRegex.MatchString(channel) {
			return true
		}
	}
	return false
}

// IsAllowed 指代 client 是否拥有 (topic, channel) 的权限
func (a *State) IsAllowed(topic, channel string) bool {
	for _, aa := range a.Authorizations {
		if aa.IsAllowed(topic, channel) {
			return true
		}
	}
	return false
}

// IsExpired client 当前的权限状态是否过期
func (a *State) IsExpired() bool {
	if a.Expires.Before(time.Now()) {
		return true
	}
	return false
}

// QueryAnyAuthd 循环执行 QueryAuthd，一个返回结果就返回
func QueryAnyAuthd(authd []string, remoteIP, tlsEnabled, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	for _, a := range authd {
		authState, err := QueryAuthd(a, remoteIP, tlsEnabled, authSecret, connectTimeout, requestTimeout)
		if err != nil {
			log.Printf("Error: failed auth against %s %s", a, err)
			continue
		}
		return authState, nil
	}
	return nil, errors.New("Unable to access auth server")
}

// QueryAuthd 发送 GETV1 请求询问 client 的权限状态
func QueryAuthd(authd, remoteIP, tlsEnabled, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	v := url.Values{}
	v.Set("remote_ip", remoteIP)
	v.Set("tls", tlsEnabled)
	v.Set("secret", authSecret)

	endpoint := fmt.Sprintf("http://%s/auth?%s", authd, v.Encode())

	var authState State
	client := http_api.NewClient(nil, connectTimeout, requestTimeout)
	if err := client.GETV1(endpoint, &authState); err != nil {
		return nil, err
	}

	// validation on response
	for _, auth := range authState.Authorizations {
		for _, p := range auth.Permissions {
			switch p {
			case "subscribe", "publish":
			default:
				return nil, fmt.Errorf("unknown permission %s", p)
			}
		}

		if _, err := regexp.Compile(auth.Topic); err != nil {
			return nil, fmt.Errorf("unable to compile topic %q %s", auth.Topic, err)
		}

		for _, channel := range auth.Channels {
			if _, err := regexp.Compile(channel); err != nil {
				return nil, fmt.Errorf("unable to compile channel %q %s", channel, err)
			}
		}
	}

	if authState.TTL <= 0 {
		return nil, fmt.Errorf("invalid TTL %d (must be >0)", authState.TTL)
	}

	authState.Expires = time.Now().Add(time.Duration(authState.TTL) * time.Second)
	return &authState, nil
}
