package bleve_redis

import (
	"context"
	"errors"
	"fmt"

	blevestore "github.com/blevesearch/upsidedown_store_api"
	"github.com/redis/go-redis/v9"
)

type writer struct {
	client *redis.Client
	mo     blevestore.MergeOperator
}

func (w writer) NewBatch() blevestore.KVBatch {
	b := batch{
		client:   w.client,
		pipeline: w.client.TxPipeline(),
		merge:    blevestore.NewEmulatedMerge(w.mo),
		mo:       w.mo,
	}
	return &b
}

func (w writer) NewBatchEx(options blevestore.KVBatchOptions) ([]byte, blevestore.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w writer) ExecuteBatch(b blevestore.KVBatch) error {
	batch, ok := b.(*batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	// first process merges
	for k, mergeOps := range batch.merge.Merges {
		existingVal, err := w.client.Get(context.Background(), k).Bytes()
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

func (w writer) Close() error {
	return w.client.Close()
}
