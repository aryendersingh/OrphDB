package memory_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/orphdb/orphdb/backend"
	"github.com/orphdb/orphdb/backend/memory"
)

func TestMemoryBackendBasic(t *testing.T) {
	b := memory.New()
	defer b.Close()
	ctx := context.Background()

	// Put + Get.
	if err := b.Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	val, err := b.Get(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v1" {
		t.Fatalf("got %q, want %q", val, "v1")
	}

	// Get missing key.
	val, err = b.Get(ctx, "missing")
	if err != nil || val != nil {
		t.Fatalf("expected nil, nil for missing key, got %v, %v", val, err)
	}

	// Delete.
	if err := b.Delete(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	val, err = b.Get(ctx, "k1")
	if err != nil || val != nil {
		t.Fatalf("expected nil after delete, got %v, %v", val, err)
	}
}

func TestMemoryBackendBatch(t *testing.T) {
	b := memory.New()
	defer b.Close()
	ctx := context.Background()

	items := make([]backend.KeyValue, 50)
	for i := range items {
		items[i] = backend.KeyValue{
			Key:   fmt.Sprintf("bk-%d", i),
			Value: []byte(fmt.Sprintf("bv-%d", i)),
		}
	}

	if err := b.BatchPut(ctx, items); err != nil {
		t.Fatal(err)
	}

	if b.Len() != 50 {
		t.Fatalf("got %d items, want 50", b.Len())
	}

	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("bk-%d", i)
	}

	results, err := b.BatchGet(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 50 {
		t.Fatalf("got %d results, want 50", len(results))
	}

	// Batch delete.
	if err := b.BatchDelete(ctx, keys[:25]); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 25 {
		t.Fatalf("got %d items after partial delete, want 25", b.Len())
	}
}

func TestMemoryBackendValueIsolation(t *testing.T) {
	b := memory.New()
	defer b.Close()
	ctx := context.Background()

	original := []byte("original")
	b.Put(ctx, "iso", original)

	// Mutate the original slice.
	original[0] = 'X'

	val, _ := b.Get(ctx, "iso")
	if string(val) != "original" {
		t.Fatalf("stored value was mutated: got %q", val)
	}

	// Mutate the returned slice.
	val[0] = 'Y'
	val2, _ := b.Get(ctx, "iso")
	if string(val2) != "original" {
		t.Fatalf("returned value mutation leaked: got %q", val2)
	}
}
