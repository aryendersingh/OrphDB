// Package memory provides an in-memory Backend implementation for testing
// and embedded use cases.
package memory

import (
	"context"
	"sync"

	"github.com/orphdb/orphdb/backend"
)

// Backend is a thread-safe in-memory key-value store.
// It is primarily intended for testing and development.
type Backend struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New creates a new in-memory backend.
func New() *Backend {
	return &Backend{
		data: make(map[string][]byte, 1024),
	}
}

func (b *Backend) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.RLock()
	val, ok := b.data[key]
	b.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

func (b *Backend) Put(_ context.Context, key string, value []byte) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	b.mu.Lock()
	b.data[key] = cp
	b.mu.Unlock()
	return nil
}

func (b *Backend) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.data, key)
	b.mu.Unlock()
	return nil
}

func (b *Backend) BatchGet(_ context.Context, keys []string) ([]backend.KeyValue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	results := make([]backend.KeyValue, 0, len(keys))
	for _, key := range keys {
		if val, ok := b.data[key]; ok {
			cp := make([]byte, len(val))
			copy(cp, val)
			results = append(results, backend.KeyValue{Key: key, Value: cp})
		}
	}
	return results, nil
}

func (b *Backend) BatchPut(_ context.Context, items []backend.KeyValue) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, item := range items {
		cp := make([]byte, len(item.Value))
		copy(cp, item.Value)
		b.data[item.Key] = cp
	}
	return nil
}

func (b *Backend) BatchDelete(_ context.Context, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, key := range keys {
		delete(b.data, key)
	}
	return nil
}

func (b *Backend) Close() error {
	b.mu.Lock()
	b.data = nil
	b.mu.Unlock()
	return nil
}

// Len returns the number of stored entries (for testing).
func (b *Backend) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.data)
}

// Ensure Backend implements backend.Backend.
var _ backend.Backend = (*Backend)(nil)
