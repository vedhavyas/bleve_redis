package bleve_redis

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
)

type iterator struct {
	client *redis.Client
	keys   []string
	index  int
}

func (iter *iterator) scan(prefix string) error {
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = iter.client.Scan(context.Background(), cursor, prefix+"*", 100).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("failed to scan prefix keys: %v", err)
		}

		for _, key := range keys {
			iter.keys = append(iter.keys, key)
		}

		if cursor == 0 {
			break
		}
	}

	sort.Strings(iter.keys)
	return nil
}

func (iter *iterator) scanRange(start, end string) error {
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = iter.client.Scan(context.Background(), cursor, "*", 10).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		for _, key := range keys {
			// Since keys are lexicographically ordered, only add keys in the (start, end) range
			if key >= start && key <= end {
				iter.keys = append(iter.keys, key)
			}
		}

		if cursor == 0 {
			break
		}
	}

	sort.Strings(iter.keys)
	return nil
}

func (iter *iterator) Seek(key []byte) {
	iter.index = sort.SearchStrings(iter.keys, string(key))
}

func (iter *iterator) Next() {
	iter.index++
}

func (iter *iterator) Key() []byte {
	return []byte(iter.keys[iter.index])
}

func (iter *iterator) Value() []byte {
	value, _ := iter.client.Get(context.Background(), iter.keys[iter.index]).Result()
	return []byte(value)
}

func (iter *iterator) Valid() bool {
	return iter.index < len(iter.keys)
}

func (iter *iterator) Current() ([]byte, []byte, bool) {
	if iter.Valid() {
		return iter.Key(), iter.Value(), true
	}
	return nil, nil, false
}

func (iter *iterator) Close() error {
	iter.keys = nil
	return nil
}
