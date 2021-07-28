package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/eko/gocache/v2/cache"
	"github.com/eko/gocache/v2/store"
	"github.com/fighterlyt/log"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

const (
	Delimiter    = `:`
	initCapacity = 1000
)

type typeInfo struct {
	t      Type
	expire time.Duration
	kind   Kind
}

func newTypeInfo(t Type, expire time.Duration, kind Kind) *typeInfo {
	return &typeInfo{t: t, expire: expire, kind: kind}
}

type service struct {
	logger      log.Logger
	redisClient *redis.Client
	lock        *sync.RWMutex
	types       map[string]*typeInfo
}

func NewService(logger log.Logger, redisAddr, password string, redisDB int) (target *service, err error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: password,
		DB:       redisDB,
	})

	if err = redisClient.Ping(bg).Err(); err != nil {
		return nil, errors.Wrap(err, `redis连接错误`)
	}

	return &service{
		logger:      logger,
		lock:        &sync.RWMutex{},
		types:       make(map[string]*typeInfo, initCapacity),
		redisClient: redisClient,
	}, nil
}

func NewServiceByRedisClient(logger log.Logger, client *redis.Client) (target *service, err error) {
	return &service{
		logger:      logger,
		lock:        &sync.RWMutex{},
		types:       make(map[string]*typeInfo, initCapacity),
		redisClient: client,
	}, nil
}

func (s *service) Register(t Type, expireTime time.Duration, kind Kind) (Client, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exist := s.types[t.CachePrefix()]; exist {
		return nil, fmt.Errorf(`前缀[%s]已注册`, t.CachePrefix())
	}

	s.types[t.CachePrefix()] = newTypeInfo(t, expireTime, kind)

	return &client{
		cache: cache.NewLoadable(func(ctx context.Context, key interface{}) (interface{}, error) {
			return t.Load(ctx, key)
		}, cache.New(store.NewRedis(s.redisClient, &store.Options{
			Expiration: expireTime,
		}))),
		t: t,
	}, nil
}

type client struct {
	cache *cache.LoadableCache
	t     Type
}

func (c client) Get(key string) (record interface{}, err error) {
	record = c.t.New()

	var (
		value interface{}
	)

	if value, err = c.cache.Get(bg, c.t.CachePrefix()+Delimiter+key); err != nil {
		return nil, errors.Wrap(err, `从redis获取失败`)
	}

	switch x := value.(type) {
	case []byte:
		return record, json.Unmarshal(x, record)
	default:
		return value, nil
	}
}

func (c client) Invalidate(key string) error {
	return c.cache.Delete(bg, c.t.CachePrefix()+Delimiter+key)
}

var (
	bg = context.Background()
)
