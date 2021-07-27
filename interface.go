package cache

import (
	"context"
	"time"
)

// Type 类型接口，表示一个使用缓存的类型
type Type interface {
	// CachePrefix 前缀,在redis中会有前缀
	CachePrefix() string
	// Load 加载
	Load(ctx context.Context, key interface{}) (interface{}, error)
	// New 新建一个对象，必须返回指针
	New() interface{}
}

// Manager 缓存管理器
type Manager interface {
	// Register 注册一个类型,参数是类型，超时时间,kind 暂时忽略，返回一个客户端
	Register(t Type, expireTime time.Duration, kind Kind) (Client, error)
}

// Client 缓存客户端
type Client interface {
	// Get 获取
	Get(key string) (interface{}, error)
	// Invalidate  失效
	Invalidate(key string) error
}

// Kind 注册类型
type Kind int

const (
	// OnlyRedis 只使用Redis
	OnlyRedis Kind = 1
	// RedisAndMem redis和内存
	RedisAndMem Kind = 2
)
