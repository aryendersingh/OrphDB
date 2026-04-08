package orphdb

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/orphdb/orphdb/backend"
)

// Store is a portable, high-performance object store backed by a cloud KV store.
//
// It provides transparent compression, chunking for large objects, in-memory
// caching for hot reads, and a buffered write pipeline for high throughput.
//
// Store is safe for concurrent use.
type Store struct {
	backend  backend.Backend
	cache    *cache
	writer   *chunkWriter
	reader   *chunkReader
	pipeline *pipeline
	batch    *batchOrchestrator
	cfg      Config
	metrics  MetricsCollector
	closed   atomic.Bool
}

// New creates a new Store with the given backend and options.
//
// The store is immediately ready for use — there is no initialization phase,
// which keeps cold-start latency minimal.
func New(b backend.Backend, opts ...Option) *Store {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	// Respect backend-specific batch limits if available.
	if limiter, ok := b.(backend.BatchLimiter); ok {
		limits := limiter.BatchLimits()
		if limits.MaxReadBatchSize > 0 {
			cfg.Batch.ReadSubBatchSize = limits.MaxReadBatchSize
		}
		if limits.MaxWriteBatchSize > 0 {
			cfg.Batch.WriteSubBatchSize = limits.MaxWriteBatchSize
			cfg.Pipeline.MaxBatchSize = limits.MaxWriteBatchSize
		}
		if limits.MaxItemSize > 0 && limits.MaxItemSize < cfg.ChunkSize {
			cfg.ChunkSize = limits.MaxItemSize
		}
	}

	m := cfg.Metrics
	if m == nil {
		m = nopMetrics{}
	}

	w := &chunkWriter{
		backend:     b,
		chunkSize:   cfg.ChunkSize,
		compression: cfg.Compression,
		metrics:     m,
	}

	r := &chunkReader{
		backend:     b,
		compression: cfg.Compression,
		metrics:     m,
		concurrency: cfg.Batch.MaxConcurrency,
	}

	s := &Store{
		backend:  b,
		cache:    newCache(cfg.Cache),
		writer:   w,
		reader:   r,
		pipeline: newPipeline(w, cfg.Pipeline, m),
		batch:    newBatchOrchestrator(b, r, w, cfg.Batch, m),
		cfg:      cfg,
		metrics:  m,
	}

	return s
}

// Put stores a value for the given key. The write is synchronous.
func (s *Store) Put(ctx context.Context, key string, value []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}
	if int64(len(value)) > s.cfg.MaxValueSize {
		return ErrValueTooLarge
	}

	start := time.Now()

	if err := s.writer.writeObject(ctx, key, value); err != nil {
		return err
	}

	s.cache.Put(key, value)

	if s.metrics != nil {
		s.metrics.ObserveLatency(MetricPut, time.Since(start))
	}
	return nil
}

// PutAsync queues a write for asynchronous processing via the write pipeline.
// Returns a channel that will receive nil on success or an error.
//
// The value is cached immediately for read-after-write consistency.
// Call Flush to ensure all async writes have been persisted.
func (s *Store) PutAsync(ctx context.Context, key string, value []byte) <-chan error {
	errCh := make(chan error, 1)
	if err := s.checkOpen(); err != nil {
		errCh <- err
		return errCh
	}
	if key == "" {
		errCh <- ErrKeyEmpty
		return errCh
	}
	if int64(len(value)) > s.cfg.MaxValueSize {
		errCh <- ErrValueTooLarge
		return errCh
	}

	// Cache immediately for read-after-write consistency.
	s.cache.Put(key, value)

	return s.pipeline.Submit(key, value)
}

// Get retrieves a value by key.
// Returns ErrNotFound if the key does not exist.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, ErrKeyEmpty
	}

	start := time.Now()

	// Check cache first.
	if val, ok := s.cache.Get(key); ok {
		if s.metrics != nil {
			s.metrics.IncrCounter(MetricCacheHit, 1)
			s.metrics.ObserveLatency(MetricGet, time.Since(start))
		}
		return val, nil
	}
	if s.metrics != nil {
		s.metrics.IncrCounter(MetricCacheMiss, 1)
	}

	// Cache miss — read from backend.
	val, err := s.reader.readObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, ErrNotFound
	}

	// Populate cache.
	s.cache.Put(key, val)

	if s.metrics != nil {
		s.metrics.ObserveLatency(MetricGet, time.Since(start))
	}
	return val, nil
}

// Delete removes a key from the store.
// Returns nil even if the key does not exist.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}

	start := time.Now()

	s.cache.Delete(key)

	if err := deleteObject(ctx, s.backend, key); err != nil {
		return err
	}

	if s.metrics != nil {
		s.metrics.ObserveLatency(MetricDelete, time.Since(start))
	}
	return nil
}

// BatchGet retrieves multiple values by key.
// Missing keys are omitted from the result (no error).
// Results may be returned in any order.
func (s *Store) BatchGet(ctx context.Context, keys []string) ([]KeyValue, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	start := time.Now()

	// Partition keys into cache hits and misses.
	var (
		results []KeyValue
		misses  []string
	)

	for _, key := range keys {
		if val, ok := s.cache.Get(key); ok {
			results = append(results, KeyValue{Key: key, Value: val})
			if s.metrics != nil {
				s.metrics.IncrCounter(MetricCacheHit, 1)
			}
		} else {
			misses = append(misses, key)
			if s.metrics != nil {
				s.metrics.IncrCounter(MetricCacheMiss, 1)
			}
		}
	}

	// Fetch cache misses from backend.
	if len(misses) > 0 {
		fetched, err := s.batch.batchGet(ctx, misses)
		if err != nil {
			return nil, err
		}
		for _, kv := range fetched {
			results = append(results, KeyValue{Key: kv.Key, Value: kv.Value})
			s.cache.Put(kv.Key, kv.Value)
		}
	}

	if s.metrics != nil {
		s.metrics.ObserveLatency(MetricBatchGet, time.Since(start))
	}
	return results, nil
}

// BatchPut stores multiple key-value pairs synchronously.
func (s *Store) BatchPut(ctx context.Context, items []KeyValue) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	start := time.Now()

	backendItems := make([]backend.KeyValue, len(items))
	for i, item := range items {
		if item.Key == "" {
			return ErrKeyEmpty
		}
		backendItems[i] = backend.KeyValue{Key: item.Key, Value: item.Value}
	}

	if err := s.batch.batchPut(ctx, backendItems); err != nil {
		return err
	}

	// Update cache.
	for _, item := range items {
		s.cache.Put(item.Key, item.Value)
	}

	if s.metrics != nil {
		s.metrics.ObserveLatency(MetricBatchPut, time.Since(start))
	}
	return nil
}

// BatchDelete removes multiple keys from the store.
func (s *Store) BatchDelete(ctx context.Context, keys []string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	for _, key := range keys {
		s.cache.Delete(key)
	}

	return s.batch.batchDelete(ctx, keys)
}

// Flush blocks until all pending async writes have been persisted.
func (s *Store) Flush(ctx context.Context) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.pipeline.Flush(ctx)
}

// Close flushes pending writes, releases resources, and closes the backend.
// After Close returns, all Store methods will return ErrStoreClosed.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrStoreClosed
	}

	// Stop the pipeline (drains remaining writes).
	pErr := s.pipeline.Close()

	// Close the backend.
	bErr := s.backend.Close()

	if pErr != nil {
		return pErr
	}
	return bErr
}

// Stats returns current store statistics.
type StoreStats struct {
	Pipeline PipelineStats
}

// Stats returns current store statistics.
func (s *Store) Stats() StoreStats {
	return StoreStats{
		Pipeline: s.pipeline.Stats(),
	}
}

func (s *Store) checkOpen() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	return nil
}

// KeyValue is a key-value pair for batch operations.
type KeyValue struct {
	Key   string
	Value []byte
}
