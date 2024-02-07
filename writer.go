package bleve_redis

import (
	"context"
	"errors"
	"fmt"
	blevestore "github.com/blevesearch/upsidedown_store_api"
	"github.com/redis/go-redis/v9"
)

func (s store) Writer() (blevestore.KVWriter, error) {
	return s, nil
}

func (s store) NewBatch() blevestore.KVBatch {
	b := batch{
		client:   s.client,
		pipeline: s.client.TxPipeline(),
		merge:    blevestore.NewEmulatedMerge(s.mo),
		mo:       s.mo,
	}
	return &b
}

func (s store) NewBatchEx(options blevestore.KVBatchOptions) ([]byte, blevestore.KVBatch, error) {
	return make([]byte, options.TotalBytes), s.NewBatch(), nil
}

func (s store) ExecuteBatch(b blevestore.KVBatch) error {
	batch, ok := b.(*batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	// first process merges
	for k, mergeOps := range batch.merge.Merges {
		existingVal, err := s.client.Get(context.Background(), k).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		kb := []byte(k)
		mergedVal, fullMergeOk := batch.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}

		// add the final merge to this batch
		batch.Set(kb, mergedVal)
	}

	// now execute the batch
	return batch.Execute()
}
