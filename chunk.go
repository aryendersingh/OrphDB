package orphdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/orphdb/orphdb/backend"
	"golang.org/x/sync/errgroup"
)

// Chunk key format:
//   Single object (fits in one KV entry):  "d:{key}"
//   Chunked object metadata:                "m:{key}"
//   Chunked object data:                    "c:{key}:{chunk_index_hex}"
//
// Metadata format (stored at m:{key}):
//   [4 bytes: chunk count, LE]
//   [8 bytes: total size, LE]
//   [32 bytes: SHA-256 of original value]

const (
	prefixData  = "d:"
	prefixMeta  = "m:"
	prefixChunk = "c:"
)

// chunkWriter handles splitting objects into chunks and writing them.
type chunkWriter struct {
	backend     backend.Backend
	chunkSize   int
	compression Compression
	metrics     MetricsCollector
}

// chunkReader handles reading and reassembling chunked objects.
type chunkReader struct {
	backend     backend.Backend
	compression Compression
	metrics     MetricsCollector
	concurrency int
}

// writeObject writes a single object, chunking if necessary.
func (w *chunkWriter) writeObject(ctx context.Context, key string, value []byte) error {
	compressed := compressValue(value, w.compression)

	// Fits in a single entry — store directly.
	if len(compressed) <= w.chunkSize {
		return w.backend.Put(ctx, prefixData+key, compressed)
	}

	// Large object: chunk it.
	return w.writeChunked(ctx, key, value, compressed)
}

// writeChunked splits a compressed value into chunks and writes metadata + chunks.
func (w *chunkWriter) writeChunked(ctx context.Context, key string, original, compressed []byte) error {
	numChunks := (len(compressed) + w.chunkSize - 1) / w.chunkSize
	checksum := sha256.Sum256(original)

	if w.metrics != nil {
		w.metrics.IncrCounter(MetricChunkWrite, int64(numChunks))
	}

	// Build metadata.
	meta := make([]byte, 44)
	binary.LittleEndian.PutUint32(meta[0:4], uint32(numChunks))
	binary.LittleEndian.PutUint64(meta[4:12], uint64(len(original)))
	copy(meta[12:44], checksum[:])

	// Build all KV pairs: metadata + chunks.
	items := make([]backend.KeyValue, 0, numChunks+1)

	// First, delete any old single-entry data key (in case this was previously unchunked).
	// We do this by writing the metadata and chunks, then deleting the old key.
	items = append(items, backend.KeyValue{
		Key:   prefixMeta + key,
		Value: meta,
	})

	for i := 0; i < numChunks; i++ {
		start := i * w.chunkSize
		end := start + w.chunkSize
		if end > len(compressed) {
			end = len(compressed)
		}
		chunkKey := fmt.Sprintf("%s%s:%04x", prefixChunk, key, i)
		items = append(items, backend.KeyValue{
			Key:   chunkKey,
			Value: compressed[start:end],
		})
	}

	// Write all items.
	if err := w.backend.BatchPut(ctx, items); err != nil {
		return fmt.Errorf("orphdb: write chunks for %q: %w", key, err)
	}

	// Clean up the old direct-data key if it existed.
	_ = w.backend.Delete(ctx, prefixData+key)

	return nil
}

// writeBatch writes multiple objects, handling chunking as needed.
func (w *chunkWriter) writeBatch(ctx context.Context, items []backend.KeyValue) error {
	var (
		direct  []backend.KeyValue
		chunked []backend.KeyValue
	)

	for _, item := range items {
		compressed := compressValue(item.Value, w.compression)
		if len(compressed) <= w.chunkSize {
			direct = append(direct, backend.KeyValue{
				Key:   prefixData + item.Key,
				Value: compressed,
			})
		} else {
			chunked = append(chunked, item)
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	// Write all direct items in one batch.
	if len(direct) > 0 {
		g.Go(func() error {
			return w.backend.BatchPut(gctx, direct)
		})
	}

	// Write chunked items concurrently.
	for _, item := range chunked {
		item := item
		g.Go(func() error {
			return w.writeObject(gctx, item.Key, item.Value)
		})
	}

	return g.Wait()
}

// readObject reads a single object, reassembling chunks if necessary.
func (r *chunkReader) readObject(ctx context.Context, key string) ([]byte, error) {
	// Try direct read first (common case for small objects).
	data, err := r.backend.Get(ctx, prefixData+key)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return decompressValue(data)
	}

	// Not found as direct — check for chunked metadata.
	return r.readChunked(ctx, key)
}

// readChunked reads a chunked object by fetching metadata and all chunks.
func (r *chunkReader) readChunked(ctx context.Context, key string) ([]byte, error) {
	meta, err := r.backend.Get(ctx, prefixMeta+key)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil // Not found.
	}
	if len(meta) < 44 {
		return nil, fmt.Errorf("orphdb: corrupt metadata for key %q", key)
	}

	numChunks := int(binary.LittleEndian.Uint32(meta[0:4]))
	origSize := binary.LittleEndian.Uint64(meta[4:12])
	var expectedHash [32]byte
	copy(expectedHash[:], meta[12:44])

	if r.metrics != nil {
		r.metrics.IncrCounter(MetricChunkRead, int64(numChunks))
	}

	// Fetch all chunk keys.
	chunkKeys := make([]string, numChunks)
	for i := 0; i < numChunks; i++ {
		chunkKeys[i] = fmt.Sprintf("%s%s:%04x", prefixChunk, key, i)
	}

	chunks, err := r.backend.BatchGet(ctx, chunkKeys)
	if err != nil {
		return nil, fmt.Errorf("orphdb: read chunks for %q: %w", key, err)
	}

	if len(chunks) != numChunks {
		return nil, fmt.Errorf("orphdb: incomplete chunks for %q: got %d, want %d", key, len(chunks), numChunks)
	}

	// Sort chunks by key to ensure correct order.
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Key < chunks[j].Key
	})

	// Reassemble compressed data.
	var totalSize int
	for _, c := range chunks {
		totalSize += len(c.Value)
	}
	compressed := make([]byte, 0, totalSize)
	for _, c := range chunks {
		compressed = append(compressed, c.Value...)
	}

	// Decompress.
	result, err := decompressValue(compressed)
	if err != nil {
		return nil, fmt.Errorf("orphdb: decompress chunks for %q: %w", key, err)
	}

	// Verify integrity.
	actualHash := sha256.Sum256(result)
	if actualHash != expectedHash {
		return nil, fmt.Errorf("orphdb: checksum mismatch for key %q: expected %s, got %s",
			key, hex.EncodeToString(expectedHash[:8]), hex.EncodeToString(actualHash[:8]))
	}
	_ = origSize // validated implicitly by decompression + hash

	return result, nil
}

// readBatch reads multiple objects concurrently, handling both direct and chunked objects.
func (r *chunkReader) readBatch(ctx context.Context, keys []string) ([]backend.KeyValue, error) {
	// First, try to fetch all keys as direct entries.
	directKeys := make([]string, len(keys))
	for i, k := range keys {
		directKeys[i] = prefixData + k
	}

	directResults, err := r.backend.BatchGet(ctx, directKeys)
	if err != nil {
		return nil, err
	}

	// Build a set of found keys.
	found := make(map[string][]byte, len(directResults))
	for _, kv := range directResults {
		// Strip prefix to get original key.
		origKey := kv.Key[len(prefixData):]
		val, err := decompressValue(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("orphdb: decompress %q: %w", origKey, err)
		}
		found[origKey] = val
	}

	// Find keys that weren't in direct results — they might be chunked.
	var missingKeys []string
	for _, k := range keys {
		if _, ok := found[k]; !ok {
			missingKeys = append(missingKeys, k)
		}
	}

	// Fetch chunked objects concurrently.
	if len(missingKeys) > 0 {
		concurrency := r.concurrency
		if concurrency <= 0 {
			concurrency = 64
		}
		sem := make(chan struct{}, concurrency)

		var mu sync.Mutex
		g, gctx := errgroup.WithContext(ctx)

		for _, k := range missingKeys {
			k := k
			sem <- struct{}{}
			g.Go(func() error {
				defer func() { <-sem }()
				val, err := r.readChunked(gctx, k)
				if err != nil {
					return err
				}
				if val != nil {
					mu.Lock()
					found[k] = val
					mu.Unlock()
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}

	// Build result.
	result := make([]backend.KeyValue, 0, len(found))
	for k, v := range found {
		result = append(result, backend.KeyValue{Key: k, Value: v})
	}
	return result, nil
}

// deleteObject deletes a single object, including all chunks.
func deleteObject(ctx context.Context, b backend.Backend, key string) error {
	// Try to read metadata to find chunks.
	meta, err := b.Get(ctx, prefixMeta+key)
	if err != nil {
		return err
	}

	keysToDelete := []string{prefixData + key, prefixMeta + key}

	if meta != nil && len(meta) >= 4 {
		numChunks := int(binary.LittleEndian.Uint32(meta[0:4]))
		for i := 0; i < numChunks; i++ {
			keysToDelete = append(keysToDelete, fmt.Sprintf("%s%s:%04x", prefixChunk, key, i))
		}
	}

	return b.BatchDelete(ctx, keysToDelete)
}
