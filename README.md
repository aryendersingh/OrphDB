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

## Cloud Provider Setup

### AWS DynamoDB

#### 1. Create the table

**AWS CLI:**

```bash
aws dynamodb create-table \
    --table-name orphdb \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-1
```

**Terraform:**

```hcl
resource "aws_dynamodb_table" "orphdb" {
  name         = "orphdb"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  # Optional: enable point-in-time recovery for production.
  point_in_time_recovery {
    enabled = true
  }
}
```

Schema details:
- Partition key: `pk` (String) -- stores the object key
- No sort key required
- Value stored in attribute `v` (Binary)
- On-demand billing is recommended; for sustained workloads, provision throughput to match your target (e.g. 100K WCU / 100K RCU)

#### 2. Configure credentials

```bash
# Option A: environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1

# Option B: shared credentials file (~/.aws/credentials)
aws configure

# Option C: IAM role (EC2, ECS, Lambda -- automatic)
```

Required IAM permissions:

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:DeleteItem",
    "dynamodb:BatchGetItem",
    "dynamodb:BatchWriteItem"
  ],
  "Resource": "arn:aws:dynamodb:*:*:table/orphdb"
}
```

#### 3. Connect

```go
import (
    "context"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    ddb "github.com/orphdb/orphdb/backend/dynamodb"
    "github.com/orphdb/orphdb"
)

cfg, err := config.LoadDefaultConfig(context.Background(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}

client := dynamodb.NewFromConfig(cfg)
store := orphdb.New(ddb.New(client, "orphdb"))
defer store.Close()
```

---

### GCP Cloud Bigtable

#### 1. Create the instance and table

**gcloud CLI:**

```bash
# Create a Bigtable instance (if you don't have one).
gcloud bigtable instances create orphdb-instance \
    --display-name="OrphDB" \
    --cluster-config=id=orphdb-cluster,zone=us-central1-a,nodes=3

# Create the table with the required column family.
cbt -instance orphdb-instance createtable orphdb
cbt -instance orphdb-instance createfamily orphdb d
```

**Terraform:**

```hcl
resource "google_bigtable_instance" "orphdb" {
  name                = "orphdb-instance"
  deletion_protection = false

  cluster {
    cluster_id   = "orphdb-cluster"
    zone         = "us-central1-a"
    num_nodes    = 3
    storage_type = "SSD"
  }
}

resource "google_bigtable_table" "orphdb" {
  name          = "orphdb"
  instance_name = google_bigtable_instance.orphdb.name

  column_family {
    family = "d"
  }
}
```

Schema details:
- Column family: `d` (configurable via `bt.WithColumnFamily`)
- Column qualifier: `v` stores the value
- Each row key maps to one object key
- Bigtable handles large batch operations natively (up to 100K mutations per call)

#### 2. Configure credentials

```bash
# Option A: application default credentials (recommended for local dev)
gcloud auth application-default login

# Option B: service account key file
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Option C: workload identity (GKE -- automatic)
```

Required IAM role: `roles/bigtable.user` on the instance.

#### 3. Connect

```go
import (
    "context"

    "cloud.google.com/go/bigtable"
    bt "github.com/orphdb/orphdb/backend/bigtable"
    "github.com/orphdb/orphdb"
)

client, err := bigtable.NewClient(context.Background(), "my-gcp-project", "orphdb-instance")
if err != nil {
    log.Fatal(err)
}

store := orphdb.New(bt.New(client, "orphdb"))
defer store.Close()
```

To use a custom column family:

```go
store := orphdb.New(bt.New(client, "orphdb", bt.WithColumnFamily("my_cf")))
```

---

### Azure CosmosDB

#### 1. Create the account, database, and container

**Azure CLI:**

```bash
# Create a CosmosDB account (NoSQL API).
az cosmosdb create \
    --name orphdb-account \
    --resource-group my-rg \
    --kind GlobalDocumentDB \
    --default-consistency-level Session

# Create a database.
az cosmosdb sql database create \
    --account-name orphdb-account \
    --resource-group my-rg \
    --name orphdb-db

# Create a container with /pk as partition key.
az cosmosdb sql container create \
    --account-name orphdb-account \
    --resource-group my-rg \
    --database-name orphdb-db \
    --name orphdb \
    --partition-key-path /pk \
    --throughput 400  # or use --max-throughput 4000 for autoscale
```

**Terraform:**

```hcl
resource "azurerm_cosmosdb_account" "orphdb" {
  name                = "orphdb-account"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = azurerm_resource_group.rg.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "orphdb" {
  name                = "orphdb-db"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.orphdb.name
}

resource "azurerm_cosmosdb_sql_container" "orphdb" {
  name                = "orphdb"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.orphdb.name
  database_name       = azurerm_cosmosdb_sql_database.orphdb.name
  partition_key_paths = ["/pk"]

  # Autoscale throughput (optional).
  autoscale_settings {
    max_throughput = 4000
  }
}
```

Schema details:
- Partition key path: `/pk`
- Document structure: `{"id": "<key>", "pk": "<partition_hash>", "v": "<base64-encoded value>"}`
- The partition key is derived from the SHA-256 hash of the object key, giving 256 logical partitions for uniform load distribution
- Max item size: 2MB (larger objects are automatically chunked by OrphDB)

#### 2. Configure credentials

```bash
# Option A: connection string
export COSMOSDB_CONNECTION_STRING="AccountEndpoint=https://orphdb-account.documents.azure.com:443/;AccountKey=..."

# Option B: Azure CLI login (for DefaultAzureCredential)
az login
```

#### 3. Connect

**Using a connection string:**

```go
import (
    "os"

    "github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
    cosmos "github.com/orphdb/orphdb/backend/cosmosdb"
    "github.com/orphdb/orphdb"
)

connStr := os.Getenv("COSMOSDB_CONNECTION_STRING")
client, err := azcosmos.NewClientFromConnectionString(connStr, nil)
if err != nil {
    log.Fatal(err)
}

container, err := client.NewContainer("orphdb-db", "orphdb")
if err != nil {
    log.Fatal(err)
}

store := orphdb.New(cosmos.New(container))
defer store.Close()
```

**Using Azure AD (DefaultAzureCredential):**

```go
import (
    "github.com/Azure/azure-sdk-for-go/sdk/azidentity"
    "github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

cred, err := azidentity.NewDefaultAzureCredential(nil)
if err != nil {
    log.Fatal(err)
}

client, err := azcosmos.NewClient("https://orphdb-account.documents.azure.com:443/", cred, nil)
if err != nil {
    log.Fatal(err)
}

container, err := client.NewContainer("orphdb-db", "orphdb")
if err != nil {
    log.Fatal(err)
}

store := orphdb.New(cosmos.New(container))
defer store.Close()
```

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

### Unit tests (no cloud credentials required)

All unit tests run against the in-memory backend:

```bash
# Run all tests.
go test ./...

# With the race detector.
go test -race ./...

# Verbose output.
go test -v ./...

# Run only the core store tests.
go test -v -run 'TestPutGet|TestBatchPutGet|TestLargeValueChunking' ./...
```

### Benchmarks

```bash
# Run all benchmarks.
go test -bench=. -benchmem ./...

# Run a specific benchmark.
go test -bench=BenchmarkGet_Hot -benchmem -benchtime=5s ./...
```

### Throughput and latency validation

The test suite includes throughput and latency tests that validate the performance targets. These are skipped with `-short`:

```bash
# Run performance validation tests.
go test -v -run 'TestThroughput|TestLatency' ./...

# Skip them in CI with -short.
go test -short ./...
```

Expected output:

```
=== RUN   TestThroughput_Writes
    bench_test.go: Wrote 100000 items in 154ms (647722 writes/sec)
--- PASS: TestThroughput_Writes

=== RUN   TestLatency_BatchGet10
    bench_test.go: 10-key batch get (hot): 1.417us
--- PASS: TestLatency_BatchGet10

=== RUN   TestLatency_BatchGet5000
    bench_test.go: 5000-key batch get (hot): 338.083us, returned 5000 items
--- PASS: TestLatency_BatchGet5000
```

### Integration testing against cloud backends

To run against a real backend, write a short test file or main program that constructs the cloud backend and exercises the store. The unit test suite intentionally avoids requiring cloud credentials.

**Example: DynamoDB integration test**

```bash
# Ensure credentials and the table exist, then:
AWS_REGION=us-east-1 go test -v -run TestIntegration ./integration/...
```

```go
//go:build integration

package integration

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    ddb "github.com/orphdb/orphdb/backend/dynamodb"
    "github.com/orphdb/orphdb"
)

func TestIntegrationDynamoDB(t *testing.T) {
    ctx := context.Background()
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        t.Skip("no AWS credentials:", err)
    }

    client := dynamodb.NewFromConfig(cfg)
    store := orphdb.New(ddb.New(client, "orphdb-test"))
    defer store.Close()

    // Put.
    if err := store.Put(ctx, "integration-key", []byte("hello")); err != nil {
        t.Fatal(err)
    }

    // Get.
    val, err := store.Get(ctx, "integration-key")
    if err != nil {
        t.Fatal(err)
    }
    if string(val) != "hello" {
        t.Fatalf("got %q, want %q", val, "hello")
    }

    // Clean up.
    store.Delete(ctx, "integration-key")
}
```

**Local DynamoDB for testing:**

```bash
# Run DynamoDB Local with Docker.
docker run -d -p 8000:8000 amazon/dynamodb-local

# Create the table.
aws dynamodb create-table \
    --table-name orphdb-test \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url http://localhost:8000
```

```go
// Connect to DynamoDB Local.
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
)
client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})
```

**Bigtable emulator for testing:**

```bash
# Start the Bigtable emulator.
gcloud beta emulators bigtable start --host-port=localhost:8086

# In another terminal, set the environment variable.
export BIGTABLE_EMULATOR_HOST=localhost:8086

# Create table and column family using cbt.
cbt -instance emulator-instance createtable orphdb-test
cbt -instance emulator-instance createfamily orphdb-test d
```

The Go Bigtable client automatically detects `BIGTABLE_EMULATOR_HOST` and connects to the emulator.

**CosmosDB emulator for testing:**

```bash
# Run the CosmosDB emulator with Docker.
docker run -d -p 8081:8081 -p 10250-10256:10250-10256 \
    mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest
```

Connection string for the emulator:

```
AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
```

## License

MIT
