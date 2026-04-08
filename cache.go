package orphdb

import (
	"container/list"
	"sync"
	"time"
)

// cache is a sharded LRU cache with TTL support.
// Sharding reduces lock contention for concurrent access.
type cache struct {
	shards    []*cacheShard
	shardMask uint64
	ttl       time.Duration
	maxBytes  int64
	onEvict   func(key string, size int)
	now       func() time.Time // for testing
}

type cacheShard struct {
	mu       sync.Mutex
	items    map[string]*list.Element
	evictList *list.List
	curBytes int64
	maxBytes int64
}

type cacheEntry struct {
	key       string
	value     []byte
	size      int64  // total memory accounting (key + value + overhead)
	expiresAt time.Time
}

func newCache(cfg *CacheConfig) *cache {
	if cfg == nil {
		return nil
	}
	numShards := cfg.NumShards
	if numShards <= 0 {
		numShards = 256
	}
	// Round up to next power of 2.
	numShards = nextPowerOf2(numShards)

	perShardBytes := cfg.MaxBytes / int64(numShards)
	if perShardBytes < 1024 {
		perShardBytes = 1024
	}

	shards := make([]*cacheShard, numShards)
	for i := range shards {
		shards[i] = &cacheShard{
			items:     make(map[string]*list.Element, 256),
			evictList: list.New(),
			maxBytes:  perShardBytes,
		}
	}

	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	return &cache{
		shards:    shards,
		shardMask: uint64(numShards - 1),
		ttl:       ttl,
		maxBytes:  cfg.MaxBytes,
		onEvict:   cfg.OnEvict,
		now:       time.Now,
	}
}

// getShard returns the shard for a given key using FNV-1a distribution.
func (c *cache) getShard(key string) *cacheShard {
	h := fnv1a(key)
	return c.shards[h&c.shardMask]
}

// Get retrieves a value from the cache. Returns nil if not found or expired.
func (c *cache) Get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	shard := c.getShard(key)
	shard.mu.Lock()
	elem, ok := shard.items[key]
	if !ok {
		shard.mu.Unlock()
		return nil, false
	}
	entry := elem.Value.(*cacheEntry)
	if c.now().After(entry.expiresAt) {
		// Expired — remove it.
		shard.removeElement(elem)
		shard.mu.Unlock()
		if c.onEvict != nil {
			c.onEvict(key, len(entry.value))
		}
		return nil, false
	}
	// Move to front (most recently used).
	shard.evictList.MoveToFront(elem)
	val := entry.value
	shard.mu.Unlock()
	return val, true
}

// Put adds a value to the cache. Evicts LRU entries if necessary.
func (c *cache) Put(key string, value []byte) {
	if c == nil {
		return
	}
	shard := c.getShard(key)
	entrySize := int64(len(key)) + int64(len(value)) + 128 // 128 bytes overhead estimate

	shard.mu.Lock()
	// Update existing entry.
	if elem, ok := shard.items[key]; ok {
		old := elem.Value.(*cacheEntry)
		shard.curBytes -= old.size
		old.value = value
		old.size = entrySize
		old.expiresAt = c.now().Add(c.ttl)
		shard.curBytes += entrySize
		shard.evictList.MoveToFront(elem)
		shard.evict()
		shard.mu.Unlock()
		return
	}

	// New entry.
	entry := &cacheEntry{
		key:       key,
		value:     value,
		size:      entrySize,
		expiresAt: c.now().Add(c.ttl),
	}
	elem := shard.evictList.PushFront(entry)
	shard.items[key] = elem
	shard.curBytes += entrySize
	shard.evict()
	shard.mu.Unlock()
}

// Delete removes a key from the cache.
func (c *cache) Delete(key string) {
	if c == nil {
		return
	}
	shard := c.getShard(key)
	shard.mu.Lock()
	if elem, ok := shard.items[key]; ok {
		shard.removeElement(elem)
	}
	shard.mu.Unlock()
}

// evict removes LRU entries until the shard is under its max size.
// Must be called with shard.mu held.
func (s *cacheShard) evict() {
	for s.curBytes > s.maxBytes {
		tail := s.evictList.Back()
		if tail == nil {
			break
		}
		s.removeElement(tail)
	}
}

// removeElement removes an element from the shard. Must be called with shard.mu held.
func (s *cacheShard) removeElement(elem *list.Element) {
	entry := elem.Value.(*cacheEntry)
	s.evictList.Remove(elem)
	delete(s.items, entry.key)
	s.curBytes -= entry.size
}

// fnv1a computes a fast FNV-1a hash of a string.
func fnv1a(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// nextPowerOf2 returns the smallest power of 2 >= n.
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
