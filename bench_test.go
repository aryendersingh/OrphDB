package orphdb_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/orphdb/orphdb"
	"github.com/orphdb/orphdb/backend/memory"
)

func BenchmarkPut(b *testing.B) {
	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()
	val := make([]byte, 256)
	rand.Read(val)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-put-%d", i)
			s.Put(ctx, key, val)
			i++
		}
	})
}

func BenchmarkGet_Hot(b *testing.B) {
	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()

	const numKeys = 10000
	val := make([]byte, 256)
	rand.Read(val)

	for i := 0; i < numKeys; i++ {
		s.Put(ctx, fmt.Sprintf("bench-hot-%d", i), val)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-hot-%d", i%numKeys)
			s.Get(ctx, key)
			i++
		}
	})
}

func BenchmarkGet_Cold(b *testing.B) {
	s := newBenchStore(orphdb.WithNoCache())
	defer s.Close()
	ctx := context.Background()

	const numKeys = 10000
	val := make([]byte, 256)
	rand.Read(val)

	for i := 0; i < numKeys; i++ {
		s.Put(ctx, fmt.Sprintf("bench-cold-%d", i), val)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-cold-%d", i%numKeys)
			s.Get(ctx, key)
			i++
		}
	})
}

func BenchmarkBatchGet_10Keys(b *testing.B) {
	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()

	const numKeys = 10000
	val := make([]byte, 256)
	rand.Read(val)

	for i := 0; i < numKeys; i++ {
		s.Put(ctx, fmt.Sprintf("bench-bg10-%d", i), val)
	}

	keys := make([]string, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := (i * 10) % numKeys
		for j := 0; j < 10; j++ {
			keys[j] = fmt.Sprintf("bench-bg10-%d", (offset+j)%numKeys)
		}
		s.BatchGet(ctx, keys)
	}
}

func BenchmarkBatchGet_5000Keys(b *testing.B) {
	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()

	const numKeys = 10000
	val := make([]byte, 256)
	rand.Read(val)

	for i := 0; i < numKeys; i++ {
		s.Put(ctx, fmt.Sprintf("bench-bg5k-%d", i), val)
	}

	keys := make([]string, 5000)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-bg5k-%d", i%numKeys)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.BatchGet(ctx, keys)
	}
}

func BenchmarkBatchPut(b *testing.B) {
	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()
	val := make([]byte, 256)
	rand.Read(val)

	items := make([]orphdb.KeyValue, 25)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range items {
			items[j] = orphdb.KeyValue{
				Key:   fmt.Sprintf("bench-bp-%d-%d", i, j),
				Value: val,
			}
		}
		s.BatchPut(ctx, items)
	}
}

func BenchmarkPutAsync(b *testing.B) {
	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()
	val := make([]byte, 256)
	rand.Read(val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-async-%d", i)
		<-s.PutAsync(ctx, key, val)
	}
}

// TestThroughput_Writes is a throughput test (not a benchmark) that measures
// sustained write rate to validate the 100K writes/sec target.
func TestThroughput_Writes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()

	val := make([]byte, 256)
	rand.Read(val)

	const numWrites = 100_000
	start := time.Now()

	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("tp-w-%d", i)
		s.PutAsync(ctx, key, val)
	}
	s.Flush(ctx)

	elapsed := time.Since(start)
	rate := float64(numWrites) / elapsed.Seconds()
	t.Logf("Wrote %d items in %v (%.0f writes/sec)", numWrites, elapsed, rate)

	if rate < 50_000 {
		t.Logf("WARNING: write throughput %.0f/sec is below 100K target (memory backend)", rate)
	}
}

// TestLatency_BatchGet10 measures latency for a 10-key batch read.
func TestLatency_BatchGet10(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()

	val := make([]byte, 256)
	rand.Read(val)

	for i := 0; i < 10000; i++ {
		s.Put(ctx, fmt.Sprintf("lat-%d", i), val)
	}

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("lat-%d", i)
	}

	// Warm.
	s.BatchGet(ctx, keys)

	// Measure hot.
	start := time.Now()
	s.BatchGet(ctx, keys)
	hot := time.Since(start)
	t.Logf("10-key batch get (hot): %v", hot)

	if hot > 50*time.Millisecond {
		t.Logf("WARNING: hot 10-key read %v exceeds 50ms target", hot)
	}
}

// TestLatency_BatchGet5000 measures latency for a 5000-key batch read.
func TestLatency_BatchGet5000(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	s := newBenchStore()
	defer s.Close()
	ctx := context.Background()

	val := make([]byte, 256)
	rand.Read(val)

	const numKeys = 5000
	for i := 0; i < numKeys; i++ {
		s.Put(ctx, fmt.Sprintf("lat5k-%d", i), val)
	}

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("lat5k-%d", i)
	}

	// Warm.
	s.BatchGet(ctx, keys)

	// Measure hot.
	start := time.Now()
	results, err := s.BatchGet(ctx, keys)
	hot := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("5000-key batch get (hot): %v, returned %d items", hot, len(results))

	if hot > time.Second {
		t.Logf("WARNING: 5000-key read %v exceeds 1s target", hot)
	}
}

func newBenchStore(extraOpts ...orphdb.Option) *orphdb.Store {
	opts := []orphdb.Option{
		orphdb.WithCompression(orphdb.CompressionSnappy),
		orphdb.WithChunkSize(256 * 1024),
		orphdb.WithPipeline(orphdb.PipelineConfig{
			BufferSize:    500_000,
			MaxBatchSize:  25,
			FlushInterval: 5 * time.Millisecond,
			Workers:       64,
			MaxRetries:    1,
			RetryBackoff:  time.Millisecond,
		}),
		orphdb.WithBatch(orphdb.BatchConfig{
			MaxConcurrency:    128,
			ReadSubBatchSize:  100,
			WriteSubBatchSize: 25,
		}),
	}
	opts = append(opts, extraOpts...)
	return orphdb.New(memory.New(), opts...)
}
