// Package orphdb provides a portable, high-performance object store backed by
// cloud key-value stores. It supports AWS DynamoDB, GCP Cloud Bigtable, and
// Azure CosmosDB as storage backends.
//
// OrphDB is designed for workloads requiring:
//   - Low cold-start latency with an extremely large key space
//   - ~100,000 writes/second throughput
//   - Batch reads of 5,000 keys in under 1 second
//   - Single-digit millisecond hot reads for small key sets
//
// Architecture:
//
//	┌─────────────────────────────────────────┐
//	│              OrphDB Store               │
//	├────────────────┬────────────────────────┤
//	│ Write Pipeline │  Read Path             │
//	│ • Buffering    │  • Sharded LRU Cache   │
//	│ • Batching     │  • Parallel Fetch      │
//	│ • Compression  │  • Chunk Reassembly    │
//	│ • Chunking     │  • Decompression       │
//	├────────────────┴────────────────────────┤
//	│          Backend Interface              │
//	├──────────┬──────────┬─────────┬─────────┤
//	│ DynamoDB │ Bigtable │CosmosDB │ Memory  │
//	└──────────┴──────────┴─────────┴─────────┘
package orphdb
