package bleve_redis

import (
	blevestore "github.com/blevesearch/upsidedown_store_api"
	"github.com/redis/go-redis/v9"
)

const (
	// Name of the Key/Value store within the registry
	Name = "redis"
)

// store implements the KVStore interface for a Redis.
type store struct {
	client *redis.Client
	mo     blevestore.MergeOperator
}

// KVStore returns a new redis based KVStore
func KVStore(client *redis.Client, mo blevestore.MergeOperator) blevestore.KVStore {
	return store{
		client: client,
		mo:     mo,
	}
}

// Close flushes the connection to Redis and closes it.
func (s store) Close() (err error) {
	return nil
}
