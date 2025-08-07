package bleve_redis

import (
	"context"

	blevestore "github.com/blevesearch/upsidedown_store_api"
	"github.com/redis/go-redis/v9"
)

type batch struct {
	client   *redis.Client
	pipeline redis.Pipeliner
	merge    *blevestore.EmulatedMerge
	mo       blevestore.MergeOperator
}

// Set adds a set operation to the batch
func (b *batch) Set(key, val []byte) {
	b.pipeline.Set(context.Background(), string(key), val, 0)
}

// Delete adds a delete operation to the batch
func (b *batch) Delete(key []byte) {
	b.pipeline.Del(context.Background(), string(key))
}

// Merge stub. Redis doesn't support merge operation natively.
// You need a custom resolution function for concurrent updates and this is beyond the batch scope.
func (b *batch) Merge(key, val []byte) {
	b.merge.Merge(key, val)
}

// Reset resets the batch
func (b *batch) Reset() {
	b.pipeline.Discard()
	b.pipeline = b.client.TxPipeline()
	b.merge = blevestore.NewEmulatedMerge(b.mo)
}

// Close  resets it
func (b *batch) Close() error {
	b.Reset()
	b.pipeline = nil
	b.merge = nil
	return nil
}

func (b *batch) Execute() error {
	_, err := b.pipeline.Exec(context.Background())
	return err
}
