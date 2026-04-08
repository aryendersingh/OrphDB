package orphdb

import "time"

// Compression specifies the compression algorithm.
type Compression int

const (
	CompressionNone   Compression = iota
	CompressionSnappy             // Fast compression, moderate ratio
	CompressionZstd               // Better ratio, slightly slower
)

// Config holds the store configuration.
type Config struct {
	// Cache configuration. Nil disables caching.
	Cache *CacheConfig

	// Compression algorithm for stored values.
	Compression Compression

	// ChunkSize is the maximum size of a single KV entry in bytes.
	// Objects larger than this are split into chunks.
	// Defaults to 256KB, which fits within all cloud KV store limits.
	ChunkSize int

	// MaxValueSize is the maximum total size of a single object in bytes.
	// Defaults to 5GB.
	MaxValueSize int64

	// Pipeline configuration for async writes.
	Pipeline PipelineConfig

	// Batch configuration for parallel operations.
	Batch BatchConfig

	// Metrics collector. Nil disables metrics.
	Metrics MetricsCollector
}

// CacheConfig configures the in-memory LRU cache.
type CacheConfig struct {
	// MaxBytes is the maximum cache size in bytes. Defaults to 256MB.
	MaxBytes int64

	// NumShards is the number of cache shards for reducing lock contention.
	// Must be a power of 2. Defaults to 256.
	NumShards int

	// TTL is the default time-to-live for cached entries. Defaults to 5 minutes.
	TTL time.Duration

	// OnEvict is called when an entry is evicted from cache.
	OnEvict func(key string, size int)
}

// PipelineConfig configures the async write pipeline.
type PipelineConfig struct {
	// BufferSize is the write pipeline buffer capacity.
	// Defaults to 100,000.
	BufferSize int

	// MaxBatchSize is the maximum number of items per batch write.
	// Defaults to 25 (DynamoDB's BatchWriteItem limit).
	MaxBatchSize int

	// FlushInterval is the maximum time to wait before flushing a partial batch.
	// Defaults to 10ms.
	FlushInterval time.Duration

	// Workers is the number of concurrent pipeline flush workers.
	// Defaults to 64.
	Workers int

	// MaxRetries is the number of retry attempts for failed writes.
	// Defaults to 3.
	MaxRetries int

	// RetryBackoff is the base delay between retries (exponential backoff).
	// Defaults to 50ms.
	RetryBackoff time.Duration
}

// BatchConfig configures parallel batch operations.
type BatchConfig struct {
	// MaxConcurrency is the maximum number of concurrent sub-batch operations.
	// Defaults to 128.
	MaxConcurrency int

	// SubBatchSize is the number of items per sub-batch sent to the backend.
	// Should match the backend's native batch limit.
	// Defaults to 25 for writes, 100 for reads.
	ReadSubBatchSize  int
	WriteSubBatchSize int
}

// DefaultConfig returns a Config with production-ready defaults.
func DefaultConfig() Config {
	return Config{
		Cache: &CacheConfig{
			MaxBytes:  256 * 1024 * 1024, // 256MB
			NumShards: 256,
			TTL:       5 * time.Minute,
		},
		Compression:  CompressionSnappy,
		ChunkSize:    256 * 1024,        // 256KB
		MaxValueSize: 5 * 1024 * 1024 * 1024, // 5GB
		Pipeline: PipelineConfig{
			BufferSize:    100_000,
			MaxBatchSize:  25,
			FlushInterval: 10 * time.Millisecond,
			Workers:       64,
			MaxRetries:    3,
			RetryBackoff:  50 * time.Millisecond,
		},
		Batch: BatchConfig{
			MaxConcurrency:    128,
			ReadSubBatchSize:  100,
			WriteSubBatchSize: 25,
		},
	}
}

// Option is a functional option for configuring the Store.
type Option func(*Config)

// WithCache enables caching with the given configuration.
func WithCache(c CacheConfig) Option {
	return func(cfg *Config) { cfg.Cache = &c }
}

// WithNoCache disables the in-memory cache.
func WithNoCache() Option {
	return func(cfg *Config) { cfg.Cache = nil }
}

// WithCompression sets the compression algorithm.
func WithCompression(c Compression) Option {
	return func(cfg *Config) { cfg.Compression = c }
}

// WithChunkSize sets the maximum chunk size in bytes.
func WithChunkSize(n int) Option {
	return func(cfg *Config) { cfg.ChunkSize = n }
}

// WithPipeline sets the write pipeline configuration.
func WithPipeline(p PipelineConfig) Option {
	return func(cfg *Config) { cfg.Pipeline = p }
}

// WithBatch sets the batch operation configuration.
func WithBatch(b BatchConfig) Option {
	return func(cfg *Config) { cfg.Batch = b }
}

// WithMetrics sets the metrics collector.
func WithMetrics(m MetricsCollector) Option {
	return func(cfg *Config) { cfg.Metrics = m }
}

// WithWorkers sets the number of pipeline workers.
func WithWorkers(n int) Option {
	return func(cfg *Config) { cfg.Pipeline.Workers = n }
}

// WithBufferSize sets the write pipeline buffer size.
func WithBufferSize(n int) Option {
	return func(cfg *Config) { cfg.Pipeline.BufferSize = n }
}
