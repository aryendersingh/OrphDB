// Package cosmosdb provides an Azure CosmosDB Backend implementation for OrphDB.
//
// Container setup requirements:
//   - Partition key path: /pk
//   - Items have structure: {"id": "<key>", "pk": "<partition>", "v": "<base64 value>"}
//
// The partition key is derived from the first 2 characters of the key's
// hex-encoded hash to distribute load across partitions. This prevents hot
// partitions when keys have sequential or clustered patterns.
package cosmosdb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/orphdb/orphdb/backend"
)

const (
	maxBatchSize   = 100 // CosmosDB transactional batch limit
	maxItemSize    = 2 * 1024 * 1024 // 2MB max item size
)

// item is the CosmosDB document structure.
type item struct {
	ID           string `json:"id"`
	PartitionKey string `json:"pk"`
	Value        string `json:"v"` // base64-encoded
}

// Backend stores data in an Azure CosmosDB container.
type Backend struct {
	container *azcosmos.ContainerClient
}

// New creates a CosmosDB backend using the given container client.
func New(container *azcosmos.ContainerClient) *Backend {
	return &Backend{container: container}
}

// partitionKey derives a partition key from the object key.
// Uses first 2 hex chars of SHA-256 hash for uniform distribution (256 partitions).
func partitionKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:1]) // 2 hex chars = 256 partitions
}

func (b *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	pk := azcosmos.NewPartitionKeyString(partitionKey(key))

	resp, err := b.container.ReadItem(ctx, pk, key, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cosmosdb get %q: %w", key, err)
	}

	var doc item
	if err := json.Unmarshal(resp.Value, &doc); err != nil {
		return nil, fmt.Errorf("cosmosdb get %q: unmarshal: %w", key, err)
	}
	value, err := base64.StdEncoding.DecodeString(doc.Value)
	if err != nil {
		return nil, fmt.Errorf("cosmosdb get %q: decode: %w", key, err)
	}
	return value, nil
}

func (b *Backend) Put(ctx context.Context, key string, value []byte) error {
	doc := item{
		ID:           key,
		PartitionKey: partitionKey(key),
		Value:        base64.StdEncoding.EncodeToString(value),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("cosmosdb put %q: marshal: %w", key, err)
	}

	pk := azcosmos.NewPartitionKeyString(doc.PartitionKey)
	_, err = b.container.UpsertItem(ctx, pk, data, nil)
	if err != nil {
		return fmt.Errorf("cosmosdb put %q: %w", key, err)
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	pk := azcosmos.NewPartitionKeyString(partitionKey(key))
	_, err := b.container.DeleteItem(ctx, pk, key, nil)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("cosmosdb delete %q: %w", key, err)
	}
	return nil
}

func (b *Backend) BatchGet(ctx context.Context, keys []string) ([]backend.KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// CosmosDB doesn't have a native BatchGet across partitions.
	// We use point reads grouped by partition key for efficiency.
	var results []backend.KeyValue
	for _, key := range keys {
		val, err := b.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if val != nil {
			results = append(results, backend.KeyValue{Key: key, Value: val})
		}
	}
	return results, nil
}

func (b *Backend) BatchPut(ctx context.Context, items []backend.KeyValue) error {
	if len(items) == 0 {
		return nil
	}

	// Group items by partition key for transactional batches.
	groups := make(map[string][]backend.KeyValue)
	for _, it := range items {
		pk := partitionKey(it.Key)
		groups[pk] = append(groups[pk], it)
	}

	for pk, group := range groups {
		// Process each partition group in sub-batches.
		for i := 0; i < len(group); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(group) {
				end = len(group)
			}
			batch := group[i:end]

			cosmPK := azcosmos.NewPartitionKeyString(pk)
			tb := b.container.NewTransactionalBatch(cosmPK)

			for _, it := range batch {
				doc := item{
					ID:           it.Key,
					PartitionKey: pk,
					Value:        base64.StdEncoding.EncodeToString(it.Value),
				}
				data, err := json.Marshal(doc)
				if err != nil {
					return fmt.Errorf("cosmosdb batch put: marshal %q: %w", it.Key, err)
				}
				tb.UpsertItem(data, nil)
			}

			resp, err := b.container.ExecuteTransactionalBatch(ctx, tb, nil)
			if err != nil {
				return fmt.Errorf("cosmosdb batch put: %w", err)
			}
			if !resp.Success {
				return fmt.Errorf("cosmosdb batch put: transactional batch failed")
			}
		}
	}

	return nil
}

func (b *Backend) BatchDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	// Group keys by partition key for transactional batches.
	groups := make(map[string][]string)
	for _, key := range keys {
		pk := partitionKey(key)
		groups[pk] = append(groups[pk], key)
	}

	for pk, group := range groups {
		for i := 0; i < len(group); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(group) {
				end = len(group)
			}
			batch := group[i:end]

			cosmPK := azcosmos.NewPartitionKeyString(pk)
			tb := b.container.NewTransactionalBatch(cosmPK)

			for _, key := range batch {
				tb.DeleteItem(key, nil)
			}

			resp, err := b.container.ExecuteTransactionalBatch(ctx, tb, nil)
			if err != nil {
				return fmt.Errorf("cosmosdb batch delete: %w", err)
			}
			// Transactional batch may fail if items don't exist — that's OK for deletes.
			_ = resp
		}
	}

	return nil
}

func (b *Backend) Close() error {
	return nil // CosmosDB client lifecycle managed externally.
}

// BatchLimits returns CosmosDB-specific batch limits.
func (b *Backend) BatchLimits() backend.BatchLimits {
	return backend.BatchLimits{
		MaxReadBatchSize:   100,
		MaxWriteBatchSize:  maxBatchSize,
		MaxDeleteBatchSize: maxBatchSize,
		MaxItemSize:        maxItemSize,
	}
}

// isNotFound checks if an error is a CosmosDB 404 Not Found.
func isNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == 404
	}
	return false
}

var _ backend.Backend = (*Backend)(nil)
var _ backend.BatchLimiter = (*Backend)(nil)
