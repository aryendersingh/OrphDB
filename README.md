# OrphDB

A portable, high-performance object store backed by cloud key-value stores. OrphDB provides a unified API across AWS DynamoDB, GCP Cloud Bigtable, and Azure CosmosDB with transparent compression, chunking, caching, and a buffered write pipeline.

## Performance

Measured on an in-memory backend (network-bound backends will vary):

| Operation | Measured | Target |
|-----------|----------|--------|
| Async writes/sec | 647,000 | 100,000 |
| 10-key batch get (hot) | 1.4 us | < 50 ms |
| 5000-key batch get (hot) | 338 us | < 1 s |
| Single put | 639 ns/op | -- |
| Single get (cache hit) | 67 ns/op | -- |

## Architecture

```
+-------------------------------------------+
|              OrphDB Store                 |
+-----------------+-------------------------+
| Write Pipeline  |  Read Path              |
| - Buffering     |  - Sharded LRU Cache    |
| - Batching      |  - Parallel Fetch       |
| - Compression   |  - Chunk Reassembly     |
| - Chunking      |  - Decompression        |
+-----------------+-------------------------+
|          Backend Interface                |
+----------+----------+---------+-----------+
| DynamoDB | Bigtable |CosmosDB |  Memory   |
+----------+----------+---------+-----------+
```

**Design highlights:**

- **Zero cold-start overhead** -- no index loading; the KV store is the source of truth
- **256-shard LRU cache** with TTL for sub-microsecond hot reads
- **64-worker async write pipeline** with batching and exponential retry
- **Automatic chunking** for objects exceeding KV store item size limits
- **Snappy/Zstd compression** with a self-describing header format
- **Backend auto-tuning** via the `BatchLimiter` interface -- batch sizes and chunk thresholds adapt to each provider's limits

## Installation

```
go get github.com/orphdb/orphdb
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/orphdb/orphdb"
    "github.com/orphdb/orphdb/backend/memory"
)

func main() {
    // Create a store with an in-memory backend.
    store := orphdb.New(memory.New())
    defer store.Close()

    ctx := context.Background()

    // Put a value.
    if err := store.Put(ctx, "user:1", []byte(`{"name":"alice"}`)); err != nil {
        log.Fatal(err)
    }

    // Get it back.
    val, err := store.Get(ctx, "user:1")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(string(val)) // {"name":"alice"}
}
```

## Using Cloud Backends

### AWS DynamoDB

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    ddb "github.com/orphdb/orphdb/backend/dynamodb"
)

cfg, _ := config.LoadDefaultConfig(context.Background())
client := dynamodb.NewFromConfig(cfg)

store := orphdb.New(ddb.New(client, "my-table"))
```

Table schema: partition key `pk` (String), value attribute `v` (Binary).

### GCP Cloud Bigtable

```go
import (
    "cloud.google.com/go/bigtable"
    bt "github.com/orphdb/orphdb/backend/bigtable"
)

client, _ := bigtable.NewClient(ctx, "my-project", "my-instance")

store := orphdb.New(bt.New(client, "my-table"))
```

Requires a column family `d` (configurable via `bt.WithColumnFamily`).

### Azure CosmosDB

```go
import (
    "github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
    cosmos "github.com/orphdb/orphdb/backend/cosmosdb"
)

client, _ := azcosmos.NewClientFromConnectionString(connStr, nil)
container, _ := client.NewContainer("my-db", "my-container")

store := orphdb.New(cosmos.New(container))
```

Container partition key path: `/pk`.

## Configuration

OrphDB ships with production-ready defaults. Customize via functional options:

```go
store := orphdb.New(backend,
    // Cache: 512MB across 256 shards, 10-minute TTL.
    orphdb.WithCache(orphdb.CacheConfig{
        MaxBytes:  512 * 1024 * 1024,
        NumShards: 256,
        TTL:       10 * time.Minute,
    }),

    // Compression algorithm (CompressionNone, CompressionSnappy, CompressionZstd).
    orphdb.WithCompression(orphdb.CompressionSnappy),

    // Chunk size for large objects (default 256KB).
    orphdb.WithChunkSize(256 * 1024),

    // Async write pipeline tuning.
    orphdb.WithPipeline(orphdb.PipelineConfig{
        BufferSize:    100_000,
        MaxBatchSize:  25,
        FlushInterval: 10 * time.Millisecond,
        Workers:       64,
        MaxRetries:    3,
        RetryBackoff:  50 * time.Millisecond,
    }),

    // Batch operation tuning.
    orphdb.WithBatch(orphdb.BatchConfig{
        MaxConcurrency:    128,
        ReadSubBatchSize:  100,
        WriteSubBatchSize: 25,
    }),

    // Plug in your own metrics collector.
    orphdb.WithMetrics(myCollector),
)
```

### Disabling features

```go
orphdb.WithNoCache()                         // disable caching
orphdb.WithCompression(orphdb.CompressionNone) // disable compression
```

## Async Writes

For maximum throughput, use the async write pipeline:

```go
// Submit writes without blocking on the backend.
errCh := store.PutAsync(ctx, "key", value)

// The value is cached immediately -- reads return it before the
// backend write completes (read-after-write consistency).
val, _ := store.Get(ctx, "key") // returns value immediately

// Wait for persistence if needed.
err := <-errCh

// Or flush everything at once.
store.Flush(ctx)
```

## Batch Operations

```go
// Write 1000 items in parallel sub-batches.
items := make([]orphdb.KeyValue, 1000)
for i := range items {
    items[i] = orphdb.KeyValue{Key: keys[i], Value: values[i]}
}
store.BatchPut(ctx, items)

// Read 5000 keys -- automatically split into concurrent sub-batches
// sized to match the backend's limits.
results, err := store.BatchGet(ctx, keys)

// Delete in bulk.
store.BatchDelete(ctx, keys)
```

## Custom Backends

Implement the `backend.Backend` interface:

```go
type Backend interface {
    Get(ctx context.Context, key string) ([]byte, error)
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
    BatchGet(ctx context.Context, keys []string) ([]KeyValue, error)
    BatchPut(ctx context.Context, items []KeyValue) error
    BatchDelete(ctx context.Context, keys []string) error
    Close() error
}
```

Optionally implement `BatchLimiter` to advertise batch size and item size limits so the store layer can auto-tune:

```go
type BatchLimiter interface {
    BatchLimits() BatchLimits
}
```

## Error Handling

```go
val, err := store.Get(ctx, "missing-key")
if errors.Is(err, orphdb.ErrNotFound) {
    // key does not exist
}
if errors.Is(err, orphdb.ErrStoreClosed) {
    // store was closed
}
```

Sentinel errors: `ErrNotFound`, `ErrStoreClosed`, `ErrKeyEmpty`, `ErrValueTooLarge`, `ErrPipelineFull`, `ErrBatchTooLarge`.

## Observability

Implement `MetricsCollector` to receive latency observations, counters, and gauges:

```go
type MetricsCollector interface {
    ObserveLatency(op string, d time.Duration)
    IncrCounter(name string, delta int64)
    SetGauge(name string, value float64)
}
```

Operations emitted: `get`, `put`, `delete`, `batch_get`, `batch_put`, `cache_hit`, `cache_miss`, `pipeline_flush`, `chunk_read`, `chunk_write`.

## Testing

```
go test ./...
go test -race ./...
go test -bench=. -benchmem ./...
```

## License

MIT
