// Package bigtable provides a GCP Cloud Bigtable Backend implementation for OrphDB.
//
// Table setup requirements:
//   - A column family named "d" (configurable via ColumnFamily option)
//   - Column qualifier "v" stores the value
//
// Bigtable supports very large batch operations and cell sizes up to 100MB,
// making it well suited for high-throughput workloads.
package bigtable

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/orphdb/orphdb/backend"
)

const (
	defaultColumnFamily = "d"
	columnQualifier     = "v"

	maxBatchMutate = 100_000 // Bigtable MutateRows limit
	maxItemSize    = 10 * 1024 * 1024 // Recommended max cell size: 10MB
)

// Backend stores data in a GCP Cloud Bigtable table.
type Backend struct {
	client       *bigtable.Client
	table        *bigtable.Table
	columnFamily string
}

// Option configures the Bigtable backend.
type Option func(*Backend)

// WithColumnFamily sets the column family name. Defaults to "d".
func WithColumnFamily(cf string) Option {
	return func(b *Backend) { b.columnFamily = cf }
}

// New creates a Bigtable backend using the given client and table name.
func New(client *bigtable.Client, tableName string, opts ...Option) *Backend {
	b := &Backend{
		client:       client,
		table:        client.Open(tableName),
		columnFamily: defaultColumnFamily,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	row, err := b.table.ReadRow(ctx, key,
		bigtable.RowFilter(bigtable.ChainFilters(
			bigtable.FamilyFilter(b.columnFamily),
			bigtable.ColumnFilter(columnQualifier),
			bigtable.LatestNFilter(1),
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("bigtable get %q: %w", key, err)
	}
	if row == nil {
		return nil, nil
	}
	cells := row[b.columnFamily]
	if len(cells) == 0 {
		return nil, nil
	}
	return cells[0].Value, nil
}

func (b *Backend) Put(ctx context.Context, key string, value []byte) error {
	mut := bigtable.NewMutation()
	mut.Set(b.columnFamily, columnQualifier, bigtable.Now(), value)
	if err := b.table.Apply(ctx, key, mut); err != nil {
		return fmt.Errorf("bigtable put %q: %w", key, err)
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	if err := b.table.Apply(ctx, key, mut); err != nil {
		return fmt.Errorf("bigtable delete %q: %w", key, err)
	}
	return nil
}

func (b *Backend) BatchGet(ctx context.Context, keys []string) ([]backend.KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	rowList := bigtable.RowList(keys)
	filter := bigtable.RowFilter(bigtable.ChainFilters(
		bigtable.FamilyFilter(b.columnFamily),
		bigtable.ColumnFilter(columnQualifier),
		bigtable.LatestNFilter(1),
	))

	var results []backend.KeyValue
	var readErr error

	err := b.table.ReadRows(ctx, rowList, func(row bigtable.Row) bool {
		cells := row[b.columnFamily]
		if len(cells) == 0 {
			return true
		}
		results = append(results, backend.KeyValue{
			Key:   row.Key(),
			Value: cells[0].Value,
		})
		return true
	}, filter)

	if err != nil {
		return nil, fmt.Errorf("bigtable batch get: %w", err)
	}
	if readErr != nil {
		return nil, readErr
	}
	return results, nil
}

func (b *Backend) BatchPut(ctx context.Context, items []backend.KeyValue) error {
	if len(items) == 0 {
		return nil
	}

	// Process in sub-batches of maxBatchMutate.
	for i := 0; i < len(items); i += maxBatchMutate {
		end := i + maxBatchMutate
		if end > len(items) {
			end = len(items)
		}
		batch := items[i:end]

		keys := make([]string, len(batch))
		muts := make([]*bigtable.Mutation, len(batch))

		for j, item := range batch {
			keys[j] = item.Key
			mut := bigtable.NewMutation()
			mut.Set(b.columnFamily, columnQualifier, bigtable.Now(), item.Value)
			muts[j] = mut
		}

		errs, err := b.table.ApplyBulk(ctx, keys, muts)
		if err != nil {
			return fmt.Errorf("bigtable batch put: %w", err)
		}
		if errs != nil {
			// Return the first error found.
			for _, e := range errs {
				if e != nil {
					return fmt.Errorf("bigtable batch put: %w", e)
				}
			}
		}
	}

	return nil
}

func (b *Backend) BatchDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	rowKeys := make([]string, len(keys))
	muts := make([]*bigtable.Mutation, len(keys))

	for i, key := range keys {
		rowKeys[i] = key
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		muts[i] = mut
	}

	errs, err := b.table.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		return fmt.Errorf("bigtable batch delete: %w", err)
	}
	if errs != nil {
		for _, e := range errs {
			if e != nil {
				return fmt.Errorf("bigtable batch delete: %w", e)
			}
		}
	}
	return nil
}

func (b *Backend) Close() error {
	return b.client.Close()
}

// BatchLimits returns Bigtable-specific batch limits.
func (b *Backend) BatchLimits() backend.BatchLimits {
	return backend.BatchLimits{
		MaxReadBatchSize:   10_000, // Bigtable handles large reads well
		MaxWriteBatchSize:  maxBatchMutate,
		MaxDeleteBatchSize: maxBatchMutate,
		MaxItemSize:        maxItemSize,
	}
}

var _ backend.Backend = (*Backend)(nil)
var _ backend.BatchLimiter = (*Backend)(nil)
