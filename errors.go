package orphdb

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound is returned when a key does not exist in the store.
	ErrNotFound = errors.New("orphdb: key not found")

	// ErrStoreClosed is returned when operations are attempted on a closed store.
	ErrStoreClosed = errors.New("orphdb: store is closed")

	// ErrKeyEmpty is returned when an empty key is provided.
	ErrKeyEmpty = errors.New("orphdb: key must not be empty")

	// ErrValueTooLarge is returned when a value exceeds the maximum allowed size.
	ErrValueTooLarge = errors.New("orphdb: value exceeds maximum size")

	// ErrPipelineFull is returned when the async write pipeline cannot accept more items.
	ErrPipelineFull = errors.New("orphdb: write pipeline is full")

	// ErrBatchTooLarge is returned when a batch exceeds the maximum allowed size.
	ErrBatchTooLarge = errors.New("orphdb: batch exceeds maximum size")
)

// BatchError contains errors from a batch operation, keyed by the item that failed.
type BatchError struct {
	// Errors maps keys to their individual errors.
	Errors map[string]error
}

func (e *BatchError) Error() string {
	if len(e.Errors) == 0 {
		return "orphdb: batch operation failed"
	}
	return fmt.Sprintf("orphdb: batch operation failed for %d keys", len(e.Errors))
}

// HasErrors returns true if any individual operations failed.
func (e *BatchError) HasErrors() bool {
	return len(e.Errors) > 0
}

// ChunkError wraps an error that occurred during chunked object operations.
type ChunkError struct {
	Key      string
	ChunkIdx int
	Err      error
}

func (e *ChunkError) Error() string {
	return fmt.Sprintf("orphdb: chunk error for key %q chunk %d: %v", e.Key, e.ChunkIdx, e.Err)
}

func (e *ChunkError) Unwrap() error {
	return e.Err
}
