package orphdb

import (
	"context"
	"sync"
	"time"

	"github.com/orphdb/orphdb/backend"
	"golang.org/x/sync/errgroup"
)

// writeRequest represents an item queued for async writing.
type writeRequest struct {
	key   string
	value []byte
	errCh chan error
}

// pipeline is an async write pipeline that buffers and batches writes.
type pipeline struct {
	writer  *chunkWriter
	cfg     PipelineConfig
	metrics MetricsCollector

	incoming  chan writeRequest
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newPipeline(w *chunkWriter, cfg PipelineConfig, m MetricsCollector) *pipeline {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100_000
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 25
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Millisecond
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 64
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 50 * time.Millisecond
	}

	p := &pipeline{
		writer:   w,
		cfg:      cfg,
		metrics:  m,
		incoming: make(chan writeRequest, cfg.BufferSize),
	}

	p.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go p.flushWorker()
	}

	return p
}

// Submit queues a write for async processing. Returns ErrPipelineFull if the buffer is full.
func (p *pipeline) Submit(key string, value []byte) <-chan error {
	errCh := make(chan error, 1)
	select {
	case p.incoming <- writeRequest{key: key, value: value, errCh: errCh}:
	default:
		errCh <- ErrPipelineFull
	}
	return errCh
}

// flushWorker drains the incoming channel, batches items, and writes them.
func (p *pipeline) flushWorker() {
	defer p.wg.Done()

	batch := make([]writeRequest, 0, p.cfg.MaxBatchSize)
	timer := time.NewTimer(p.cfg.FlushInterval)
	defer timer.Stop()

	for {
		batch = batch[:0]

		// Wait for the first item.
		req, ok := <-p.incoming
		if !ok {
			return // Channel closed, exit.
		}
		batch = append(batch, req)

		// Fill the batch without blocking, up to max size or flush interval.
		timer.Reset(p.cfg.FlushInterval)
	collect:
		for len(batch) < p.cfg.MaxBatchSize {
			select {
			case req, ok := <-p.incoming:
				if !ok {
					// Channel closed — flush what we have and exit.
					p.flushBatch(batch)
					return
				}
				batch = append(batch, req)
			case <-timer.C:
				break collect
			}
		}

		p.flushBatch(batch)
	}
}

// flushBatch writes a batch of items to the backend with retries.
func (p *pipeline) flushBatch(batch []writeRequest) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	// Filter out sentinel (Flush) requests — they have empty keys.
	var items []backend.KeyValue
	for _, req := range batch {
		if req.key != "" {
			items = append(items, backend.KeyValue{Key: req.key, Value: req.value})
		}
	}

	var err error
	if len(items) > 0 {
		for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
			if attempt > 0 {
				backoff := p.cfg.RetryBackoff * time.Duration(1<<uint(attempt-1))
				time.Sleep(backoff)
			}
			err = p.writer.writeBatch(context.Background(), items)
			if err == nil {
				break
			}
		}
	}

	if p.metrics != nil {
		p.metrics.ObserveLatency(MetricPipelineFlush, time.Since(start))
		p.metrics.IncrCounter(MetricBackendPut, int64(len(items)))
	}

	for _, req := range batch {
		if req.key != "" {
			req.errCh <- err
		} else {
			req.errCh <- nil // Sentinel — Flush() complete.
		}
	}
}

// Flush blocks until all currently queued items have been written.
func (p *pipeline) Flush(ctx context.Context) error {
	errCh := make(chan error, 1)
	select {
	case p.incoming <- writeRequest{key: "", value: nil, errCh: errCh}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the pipeline and waits for all pending writes to complete.
func (p *pipeline) Close() error {
	p.closeOnce.Do(func() {
		close(p.incoming)
		p.wg.Wait()
	})
	return nil
}

// Len returns the number of items currently buffered in the pipeline.
func (p *pipeline) Len() int {
	return len(p.incoming)
}

// PipelineStats contains pipeline statistics.
type PipelineStats struct {
	BufferLen int
	BufferCap int
	Workers   int
}

// Stats returns current pipeline statistics.
func (p *pipeline) Stats() PipelineStats {
	return PipelineStats{
		BufferLen: len(p.incoming),
		BufferCap: cap(p.incoming),
		Workers:   p.cfg.Workers,
	}
}

// batchOrchestrator manages parallel batch operations against the backend.
type batchOrchestrator struct {
	backend backend.Backend
	reader  *chunkReader
	writer  *chunkWriter
	cfg     BatchConfig
	metrics MetricsCollector
}

func newBatchOrchestrator(b backend.Backend, r *chunkReader, w *chunkWriter, cfg BatchConfig, m MetricsCollector) *batchOrchestrator {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 128
	}
	if cfg.ReadSubBatchSize <= 0 {
		cfg.ReadSubBatchSize = 100
	}
	if cfg.WriteSubBatchSize <= 0 {
		cfg.WriteSubBatchSize = 25
	}
	return &batchOrchestrator{
		backend: b,
		reader:  r,
		writer:  w,
		cfg:     cfg,
		metrics: m,
	}
}

// batchGet fetches multiple keys in parallel sub-batches.
func (o *batchOrchestrator) batchGet(ctx context.Context, keys []string) ([]backend.KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	start := time.Now()
	defer func() {
		if o.metrics != nil {
			o.metrics.ObserveLatency(MetricBatchGet, time.Since(start))
		}
	}()

	if len(keys) <= o.cfg.ReadSubBatchSize {
		return o.reader.readBatch(ctx, keys)
	}

	subBatches := splitKeys(keys, o.cfg.ReadSubBatchSize)
	sem := make(chan struct{}, o.cfg.MaxConcurrency)

	var mu sync.Mutex
	var allResults []backend.KeyValue

	g, gctx := errgroup.WithContext(ctx)

	for _, batch := range subBatches {
		batch := batch
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			results, err := o.reader.readBatch(gctx, batch)
			if err != nil {
				return err
			}
			mu.Lock()
			allResults = append(allResults, results...)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return allResults, nil
}

// batchPut writes multiple items in parallel sub-batches.
func (o *batchOrchestrator) batchPut(ctx context.Context, items []backend.KeyValue) error {
	if len(items) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		if o.metrics != nil {
			o.metrics.ObserveLatency(MetricBatchPut, time.Since(start))
		}
	}()

	if len(items) <= o.cfg.WriteSubBatchSize {
		return o.writer.writeBatch(ctx, items)
	}

	subBatches := splitItems(items, o.cfg.WriteSubBatchSize)
	sem := make(chan struct{}, o.cfg.MaxConcurrency)
	g, gctx := errgroup.WithContext(ctx)

	for _, batch := range subBatches {
		batch := batch
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			return o.writer.writeBatch(gctx, batch)
		})
	}

	return g.Wait()
}

// batchDelete removes multiple keys in parallel.
func (o *batchOrchestrator) batchDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	sem := make(chan struct{}, o.cfg.MaxConcurrency)
	g, gctx := errgroup.WithContext(ctx)

	for _, key := range keys {
		key := key
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			return deleteObject(gctx, o.backend, key)
		})
	}

	return g.Wait()
}

// splitKeys splits a key slice into sub-batches of at most size n.
func splitKeys(keys []string, n int) [][]string {
	if n <= 0 {
		return [][]string{keys}
	}
	batches := make([][]string, 0, (len(keys)+n-1)/n)
	for i := 0; i < len(keys); i += n {
		end := i + n
		if end > len(keys) {
			end = len(keys)
		}
		batches = append(batches, keys[i:end])
	}
	return batches
}

// splitItems splits a KeyValue slice into sub-batches of at most size n.
func splitItems(items []backend.KeyValue, n int) [][]backend.KeyValue {
	if n <= 0 {
		return [][]backend.KeyValue{items}
	}
	batches := make([][]backend.KeyValue, 0, (len(items)+n-1)/n)
	for i := 0; i < len(items); i += n {
		end := i + n
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}
