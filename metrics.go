package orphdb

import "time"

// MetricsCollector receives operational metrics from the store.
// Implementations must be safe for concurrent use.
type MetricsCollector interface {
	// ObserveLatency records a latency observation for an operation.
	ObserveLatency(op string, d time.Duration)

	// IncrCounter increments a named counter.
	IncrCounter(name string, delta int64)

	// SetGauge sets a named gauge to a value.
	SetGauge(name string, value float64)
}

// Metric operation names.
const (
	MetricGet           = "get"
	MetricPut           = "put"
	MetricDelete        = "delete"
	MetricBatchGet      = "batch_get"
	MetricBatchPut      = "batch_put"
	MetricBatchDelete   = "batch_delete"
	MetricCacheHit      = "cache_hit"
	MetricCacheMiss     = "cache_miss"
	MetricPipelineFlush = "pipeline_flush"
	MetricChunkRead     = "chunk_read"
	MetricChunkWrite    = "chunk_write"
	MetricCompressSize  = "compress_bytes"
	MetricBackendGet    = "backend_get"
	MetricBackendPut    = "backend_put"
)

// nopMetrics is a no-op metrics collector.
type nopMetrics struct{}

func (nopMetrics) ObserveLatency(string, time.Duration) {}
func (nopMetrics) IncrCounter(string, int64)            {}
func (nopMetrics) SetGauge(string, float64)             {}
