package orphdb_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orphdb/orphdb"
	"github.com/orphdb/orphdb/backend/memory"
)

func newTestStore(opts ...orphdb.Option) *orphdb.Store {
	b := memory.New()
	defaults := []orphdb.Option{
		orphdb.WithCompression(orphdb.CompressionSnappy),
		orphdb.WithChunkSize(1024), // small chunks for testing
		orphdb.WithPipeline(orphdb.PipelineConfig{
			BufferSize:    1000,
			MaxBatchSize:  10,
			FlushInterval: 5 * time.Millisecond,
			Workers:       4,
			MaxRetries:    1,
			RetryBackoff:  time.Millisecond,
		}),
	}
	return orphdb.New(b, append(defaults, opts...)...)
}

func TestPutGet(t *testing.T) {
	s := newTestStore()
	defer s.Close()

	ctx := context.Background()

	err := s.Put(ctx, "hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := s.Get(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "world" {
		t.Fatalf("got %q, want %q", val, "world")
	}
}

func TestGetNotFound(t *testing.T) {
	s := newTestStore()
	defer s.Close()

	_, err := s.Get(context.Background(), "missing")
	if err != orphdb.ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	s.Put(ctx, "key", []byte("val"))
	s.Delete(ctx, "key")

	_, err := s.Get(ctx, "key")
	if err != orphdb.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestDeleteNonexistent(t *testing.T) {
	s := newTestStore()
	defer s.Close()

	if err := s.Delete(context.Background(), "nope"); err != nil {
		t.Fatalf("delete nonexistent should not error: %v", err)
	}
}

func TestEmptyKey(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	if err := s.Put(ctx, "", []byte("val")); err != orphdb.ErrKeyEmpty {
		t.Fatalf("expected ErrKeyEmpty, got %v", err)
	}
	if _, err := s.Get(ctx, ""); err != orphdb.ErrKeyEmpty {
		t.Fatalf("expected ErrKeyEmpty, got %v", err)
	}
}

func TestLargeValueChunking(t *testing.T) {
	s := newTestStore(orphdb.WithChunkSize(256)) // very small chunks
	defer s.Close()
	ctx := context.Background()

	// Create a value that will require multiple chunks.
	val := make([]byte, 4096)
	rand.Read(val)

	if err := s.Put(ctx, "big", val); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(val) {
		t.Fatalf("size mismatch: got %d, want %d", len(got), len(val))
	}
	for i := range val {
		if got[i] != val[i] {
			t.Fatalf("byte %d: got %02x, want %02x", i, got[i], val[i])
		}
	}
}

func TestLargeValueChunkingDelete(t *testing.T) {
	s := newTestStore(orphdb.WithChunkSize(256))
	defer s.Close()
	ctx := context.Background()

	val := make([]byte, 4096)
	rand.Read(val)

	s.Put(ctx, "big", val)
	s.Delete(ctx, "big")

	_, err := s.Get(ctx, "big")
	if err != orphdb.ErrNotFound {
		t.Fatalf("expected ErrNotFound after deleting chunked object, got %v", err)
	}
}

func TestBatchPutGet(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	items := make([]orphdb.KeyValue, 100)
	for i := range items {
		items[i] = orphdb.KeyValue{
			Key:   fmt.Sprintf("batch-key-%04d", i),
			Value: []byte(fmt.Sprintf("value-%04d", i)),
		}
	}

	if err := s.BatchPut(ctx, items); err != nil {
		t.Fatal(err)
	}

	keys := make([]string, len(items))
	for i := range items {
		keys[i] = items[i].Key
	}

	results, err := s.BatchGet(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != len(items) {
		t.Fatalf("got %d results, want %d", len(results), len(items))
	}

	found := make(map[string]string, len(results))
	for _, r := range results {
		found[r.Key] = string(r.Value)
	}
	for _, item := range items {
		if got, ok := found[item.Key]; !ok {
			t.Fatalf("missing key %q", item.Key)
		} else if got != string(item.Value) {
			t.Fatalf("key %q: got %q, want %q", item.Key, got, string(item.Value))
		}
	}
}

func TestBatchGetPartialMiss(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	s.Put(ctx, "exists", []byte("yes"))

	results, err := s.BatchGet(ctx, []string{"exists", "missing1", "missing2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Key != "exists" {
		t.Fatalf("got key %q, want %q", results[0].Key, "exists")
	}
}

func TestBatchDelete(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		s.Put(ctx, fmt.Sprintf("del-%d", i), []byte("v"))
	}

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("del-%d", i)
	}

	if err := s.BatchDelete(ctx, keys); err != nil {
		t.Fatal(err)
	}

	results, err := s.BatchGet(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results after batch delete, got %d", len(results))
	}
}

func TestPutAsync(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	errCh := s.PutAsync(ctx, "async-key", []byte("async-value"))

	// Value should be available immediately via cache.
	val, err := s.Get(ctx, "async-key")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "async-value" {
		t.Fatalf("got %q, want %q", val, "async-value")
	}

	// Wait for write to complete.
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestFlush(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		s.PutAsync(ctx, fmt.Sprintf("flush-%d", i), []byte("v"))
	}

	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// After flush, create a new store with the same backend to verify persistence.
	// (In this test, cache already has it, so this mainly tests pipeline flush.)
}

func TestConcurrentAccess(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	const numKeys = 100
	const numGoroutines = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < numKeys; i++ {
				key := fmt.Sprintf("concurrent-%d-%d", id, i)
				val := []byte(fmt.Sprintf("val-%d-%d", id, i))

				if err := s.Put(ctx, key, val); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				got, err := s.Get(ctx, key)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if string(got) != string(val) {
					t.Errorf("got %q, want %q", got, val)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}

func TestStoreClosed(t *testing.T) {
	s := newTestStore()
	s.Close()

	ctx := context.Background()
	if _, err := s.Get(ctx, "key"); err != orphdb.ErrStoreClosed {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
	if err := s.Put(ctx, "key", []byte("v")); err != orphdb.ErrStoreClosed {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
}

func TestCompressionNone(t *testing.T) {
	s := newTestStore(orphdb.WithCompression(orphdb.CompressionNone))
	defer s.Close()
	ctx := context.Background()

	val := []byte("uncompressed data here")
	s.Put(ctx, "nocomp", val)

	got, err := s.Get(ctx, "nocomp")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(val) {
		t.Fatalf("got %q, want %q", got, val)
	}
}

func TestCompressionZstd(t *testing.T) {
	s := newTestStore(orphdb.WithCompression(orphdb.CompressionZstd))
	defer s.Close()
	ctx := context.Background()

	val := make([]byte, 2048)
	rand.Read(val)

	s.Put(ctx, "zstd", val)

	got, err := s.Get(ctx, "zstd")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(val) {
		t.Fatalf("size mismatch: got %d, want %d", len(got), len(val))
	}
}

func TestNoCache(t *testing.T) {
	s := newTestStore(orphdb.WithNoCache())
	defer s.Close()
	ctx := context.Background()

	s.Put(ctx, "nocache", []byte("value"))

	got, err := s.Get(ctx, "nocache")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "value" {
		t.Fatalf("got %q, want %q", got, "value")
	}
}

func TestOverwrite(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	s.Put(ctx, "ow", []byte("first"))
	s.Put(ctx, "ow", []byte("second"))

	got, err := s.Get(ctx, "ow")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}
}

func TestLargeBatchGet(t *testing.T) {
	s := newTestStore()
	defer s.Close()
	ctx := context.Background()

	const n = 5000
	items := make([]orphdb.KeyValue, n)
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		items[i] = orphdb.KeyValue{
			Key:   fmt.Sprintf("large-batch-%06d", i),
			Value: []byte(fmt.Sprintf("v%06d", i)),
		}
		keys[i] = items[i].Key
	}

	if err := s.BatchPut(ctx, items); err != nil {
		t.Fatal(err)
	}

	results, err := s.BatchGet(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
}
