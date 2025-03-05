package bleve_redis

import (
	"context"
	"errors"
	"fmt"
	blevestore "github.com/blevesearch/upsidedown_store_api"
	"github.com/redis/go-redis/v9"
)

type reader struct {
	client *redis.Client
}

func (r reader) Get(key []byte) ([]byte, error) {
	result, err := r.client.Get(context.Background(), string(key)).Bytes()
	if err != nil {
		// handle key not found case explicitly
		if errors.Is(err, redis.Nil) {
			return nil, nil // Bleve expects nil, nil if the key is not found
		}
		return nil, err
	}
	return result, nil
}

func (r reader) MultiGet(keys [][]byte) ([][]byte, error) {
	var keysStr []string
	for _, key := range keys {
		keysStr = append(keysStr, string(key))
	}

	cmd := r.client.MGet(context.Background(), keysStr...)
	if cmd.Err() != nil && !errors.Is(cmd.Err(), redis.Nil) {
		return nil, cmd.Err()
	}

	results := make([][]byte, len(keys))
	for i, val := range cmd.Val() {
		if val != nil {
			results[i] = []byte(val.(string))
		} else {
			results[i] = nil
		}
	}

	return results, nil
}

func (r reader) PrefixIterator(prefix []byte) blevestore.KVIterator {
	iter := &iterator{
		client: r.client,
	}

	prefixStr := string(prefix)
	err := iter.scan(prefixStr)
	if err != nil {
		fmt.Printf("failed to scan redis with prefix: %v", err)
	}

	return iter
}

func (r reader) RangeIterator(start, end []byte) blevestore.KVIterator {
	iter := &iterator{
		client: r.client,
	}

	err := iter.scanRange(string(start), string(end))
	if err != nil {
		fmt.Printf("failed to range scan redis: %v", err)
	}

	return iter
}

func (r reader) Close() (err error) {
	return r.client.Close()
}
