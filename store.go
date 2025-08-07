package bleve_redis

import (
	"context"

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

func (s store) getClient() (*redis.Client, error) {
	client := redis.NewClient(s.options)
	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Close flushes the connection to Redis and closes it.
func (s store) Close() (err error) {
	return nil
}

func (s store) Reader() (blevestore.KVReader, error) {
	client, err := s.getClient()
	if err != nil {
		return nil, err
	}
	return reader{client: client}, nil
}

func (s store) Writer() (blevestore.KVWriter, error) {
	client, err := s.getClient()
	if err != nil {
		return nil, err
	}
	return writer{
		client: client,
		mo:     s.mo,
	}, nil
}
