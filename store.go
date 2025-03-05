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
	options *redis.Options
	mo      blevestore.MergeOperator
}

// KVStore returns a new redis based KVStore
func KVStore(options *redis.Options, mo blevestore.MergeOperator) blevestore.KVStore {
	return store{
		options: options,
		mo:      mo,
	}
}

func (s store) getClient() *redis.Client {
	client := redis.NewClient(s.options)
	return client
}

// Close flushes the connection to Redis and closes it.
func (s store) Close() (err error) {
	return nil
}

func (s store) Reader() (blevestore.KVReader, error) {
	return reader{
		client: s.getClient(),
	}, nil
}

func (s store) Writer() (blevestore.KVWriter, error) {
	return writer{
		client: s.getClient(),
		mo:     s.mo,
	}, nil
}
