package orphdb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCacheBasic(t *testing.T) {
	c := newCache(&CacheConfig{
		MaxBytes:  1024 * 1024,
		NumShards: 4,
		TTL:       time.Minute,
	})

	c.Put("key1", []byte("value1"))

	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(val) != "value1" {
		t.Fatalf("got %q, want %q", val, "value1")
	}
}

func TestCacheMiss(t *testing.T) {
	c := newCache(&CacheConfig{
		MaxBytes:  1024 * 1024,
		NumShards: 4,
		TTL:       time.Minute,
	})

	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestCacheEviction(t *testing.T) {
	c := newCache(&CacheConfig{
		MaxBytes:  512, // Very small cache
		NumShards: 1,
		TTL:       time.Minute,
	})

	// Fill cache beyond capacity.
	for i := 0; i < 100; i++ {
		c.Put(fmt.Sprintf("key-%d", i), make([]byte, 64))
	}

	// Some early entries should be evicted.
	misses := 0
	for i := 0; i < 100; i++ {
		if _, ok := c.Get(fmt.Sprintf("key-%d", i)); !ok {
			misses++
		}
	}
	if misses == 0 {
		t.Fatal("expected some evictions")
	}
}

func TestCacheTTL(t *testing.T) {
	now := time.Now()
	c := newCache(&CacheConfig{
		MaxBytes:  1024 * 1024,
		NumShards: 1,
		TTL:       100 * time.Millisecond,
	})
	c.now = func() time.Time { return now }

	c.Put("ttl-key", []byte("value"))

	// Should be present.
	if _, ok := c.Get("ttl-key"); !ok {
		t.Fatal("expected cache hit")
	}

	// Advance time past TTL.
	c.now = func() time.Time { return now.Add(200 * time.Millisecond) }

	if _, ok := c.Get("ttl-key"); ok {
		t.Fatal("expected cache miss after TTL")
	}
}

func TestCacheUpdate(t *testing.T) {
	c := newCache(&CacheConfig{
		MaxBytes:  1024 * 1024,
		NumShards: 4,
		TTL:       time.Minute,
	})

	c.Put("key", []byte("v1"))
	c.Put("key", []byte("v2"))

	val, ok := c.Get("key")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(val) != "v2" {
		t.Fatalf("got %q, want %q", val, "v2")
	}
}

func TestCacheDelete(t *testing.T) {
	c := newCache(&CacheConfig{
		MaxBytes:  1024 * 1024,
		NumShards: 4,
		TTL:       time.Minute,
	})

	c.Put("key", []byte("value"))
	c.Delete("key")

	if _, ok := c.Get("key"); ok {
		t.Fatal("expected miss after delete")
	}
}

func TestCacheNil(t *testing.T) {
	// A nil cache should not panic.
	var c *cache
	c.Put("key", []byte("val"))
	_, ok := c.Get("key")
	if ok {
		t.Fatal("nil cache should always miss")
	}
	c.Delete("key") // should not panic
}

func TestCacheConcurrent(t *testing.T) {
	c := newCache(&CacheConfig{
		MaxBytes:  10 * 1024 * 1024,
		NumShards: 16,
		TTL:       time.Minute,
	})

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("c-%d-%d", id, i)
				c.Put(key, []byte("v"))
				c.Get(key)
				c.Delete(key)
			}
		}(g)
	}
	wg.Wait()
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct{ in, want int }{
		{0, 1}, {1, 1}, {2, 2}, {3, 4}, {4, 4},
		{5, 8}, {7, 8}, {8, 8}, {9, 16}, {255, 256}, {256, 256},
	}
	for _, tc := range tests {
		got := nextPowerOf2(tc.in)
		if got != tc.want {
			t.Errorf("nextPowerOf2(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
