// Package backend defines the interface that all OrphDB storage backends must implement.
package backend

import "context"

// KeyValue represents a single key-value pair.
type KeyValue struct {
	Key   string
	Value []byte
}

// Backend is the storage interface that all OrphDB backends must implement.
// Implementations must be safe for concurrent use.
//
// Keys are opaque byte strings from the backend's perspective. The store layer
// handles chunking, compression, and metadata encoding before calling backend methods.
type Backend interface {
	// Get retrieves a single value by key.
	// Returns nil, nil if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// Put stores a single key-value pair.
	Put(ctx context.Context, key string, value []byte) error

	// Delete removes a single key.
	// Returns nil if the key does not exist.
	Delete(ctx context.Context, key string) error

	// BatchGet retrieves multiple values by key.
	// Missing keys are omitted from the result (no error).
	// The returned slice may be in any order.
	BatchGet(ctx context.Context, keys []string) ([]KeyValue, error)

	// BatchPut stores multiple key-value pairs atomically per sub-batch.
	// The backend may impose limits on batch size; the store layer handles splitting.
	BatchPut(ctx context.Context, items []KeyValue) error

	// BatchDelete removes multiple keys.
	// Missing keys are silently ignored.
	BatchDelete(ctx context.Context, keys []string) error

	// Close releases any resources held by the backend.
	Close() error
}

// BatchLimits describes the batch size limits of a backend.
// The store layer uses these to split operations into appropriately-sized sub-batches.
type BatchLimits struct {
	MaxReadBatchSize  int // Max keys per BatchGet call
	MaxWriteBatchSize int // Max items per BatchPut call
	MaxDeleteBatchSize int // Max keys per BatchDelete call
	MaxItemSize       int // Max size of a single value in bytes
}

// BatchLimiter is optionally implemented by backends that have specific batch limits.
type BatchLimiter interface {
	BatchLimits() BatchLimits
}
