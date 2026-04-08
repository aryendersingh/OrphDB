// Package dynamodb provides an AWS DynamoDB Backend implementation for OrphDB.
//
// Table schema requirements:
//   - Partition key: "pk" (String)
//   - No sort key required
//   - Value stored in attribute "v" (Binary)
//
// The table should be provisioned with on-demand capacity or sufficient
// provisioned throughput for the expected workload.
package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/orphdb/orphdb/backend"
)

const (
	attrPK    = "pk"
	attrValue = "v"

	maxBatchGet    = 100
	maxBatchWrite  = 25
	maxItemSize    = 400 * 1024 // 400KB DynamoDB item size limit
)

// Backend stores data in an AWS DynamoDB table.
type Backend struct {
	client    *dynamodb.Client
	tableName string
}

// New creates a DynamoDB backend using the given client and table name.
func New(client *dynamodb.Client, tableName string) *Backend {
	return &Backend{
		client:    client,
		tableName: tableName,
	}
}

func (b *Backend) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &b.tableName,
		Key: map[string]types.AttributeValue{
			attrPK: &types.AttributeValueMemberS{Value: key},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb get %q: %w", key, err)
	}
	if out.Item == nil {
		return nil, nil
	}
	val, ok := out.Item[attrValue].(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("dynamodb get %q: unexpected attribute type", key)
	}
	return val.Value, nil
}

func (b *Backend) Put(ctx context.Context, key string, value []byte) error {
	_, err := b.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &b.tableName,
		Item: map[string]types.AttributeValue{
			attrPK:    &types.AttributeValueMemberS{Value: key},
			attrValue: &types.AttributeValueMemberB{Value: value},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb put %q: %w", key, err)
	}
	return nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &b.tableName,
		Key: map[string]types.AttributeValue{
			attrPK: &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb delete %q: %w", key, err)
	}
	return nil
}

func (b *Backend) BatchGet(ctx context.Context, keys []string) ([]backend.KeyValue, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var allResults []backend.KeyValue

	// Process in sub-batches of maxBatchGet.
	for i := 0; i < len(keys); i += maxBatchGet {
		end := i + maxBatchGet
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		reqKeys := make([]map[string]types.AttributeValue, len(batch))
		for j, key := range batch {
			reqKeys[j] = map[string]types.AttributeValue{
				attrPK: &types.AttributeValueMemberS{Value: key},
			}
		}

		input := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				b.tableName: {
					Keys:           reqKeys,
					ConsistentRead: aws.Bool(true),
				},
			},
		}

		for {
			out, err := b.client.BatchGetItem(ctx, input)
			if err != nil {
				return nil, fmt.Errorf("dynamodb batch get: %w", err)
			}

			for _, item := range out.Responses[b.tableName] {
				pk, ok := item[attrPK].(*types.AttributeValueMemberS)
				if !ok {
					continue
				}
				val, ok := item[attrValue].(*types.AttributeValueMemberB)
				if !ok {
					continue
				}
				allResults = append(allResults, backend.KeyValue{
					Key:   pk.Value,
					Value: val.Value,
				})
			}

			// Handle unprocessed keys (throttling).
			unprocessed := out.UnprocessedKeys[b.tableName]
			if len(unprocessed.Keys) == 0 {
				break
			}
			input.RequestItems = map[string]types.KeysAndAttributes{
				b.tableName: unprocessed,
			}
		}
	}

	return allResults, nil
}

func (b *Backend) BatchPut(ctx context.Context, items []backend.KeyValue) error {
	if len(items) == 0 {
		return nil
	}

	// Process in sub-batches of maxBatchWrite.
	for i := 0; i < len(items); i += maxBatchWrite {
		end := i + maxBatchWrite
		if end > len(items) {
			end = len(items)
		}
		batch := items[i:end]

		requests := make([]types.WriteRequest, len(batch))
		for j, item := range batch {
			requests[j] = types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						attrPK:    &types.AttributeValueMemberS{Value: item.Key},
						attrValue: &types.AttributeValueMemberB{Value: item.Value},
					},
				},
			}
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				b.tableName: requests,
			},
		}

		for {
			out, err := b.client.BatchWriteItem(ctx, input)
			if err != nil {
				return fmt.Errorf("dynamodb batch put: %w", err)
			}

			unprocessed := out.UnprocessedItems[b.tableName]
			if len(unprocessed) == 0 {
				break
			}
			input.RequestItems = map[string][]types.WriteRequest{
				b.tableName: unprocessed,
			}
		}
	}

	return nil
}

func (b *Backend) BatchDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	for i := 0; i < len(keys); i += maxBatchWrite {
		end := i + maxBatchWrite
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		requests := make([]types.WriteRequest, len(batch))
		for j, key := range batch {
			requests[j] = types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						attrPK: &types.AttributeValueMemberS{Value: key},
					},
				},
			}
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				b.tableName: requests,
			},
		}

		for {
			out, err := b.client.BatchWriteItem(ctx, input)
			if err != nil {
				return fmt.Errorf("dynamodb batch delete: %w", err)
			}

			unprocessed := out.UnprocessedItems[b.tableName]
			if len(unprocessed) == 0 {
				break
			}
			input.RequestItems = map[string][]types.WriteRequest{
				b.tableName: unprocessed,
			}
		}
	}

	return nil
}

func (b *Backend) Close() error {
	return nil // DynamoDB client doesn't need explicit close.
}

// BatchLimits returns DynamoDB-specific batch limits.
func (b *Backend) BatchLimits() backend.BatchLimits {
	return backend.BatchLimits{
		MaxReadBatchSize:   maxBatchGet,
		MaxWriteBatchSize:  maxBatchWrite,
		MaxDeleteBatchSize: maxBatchWrite,
		MaxItemSize:        maxItemSize,
	}
}

var _ backend.Backend = (*Backend)(nil)
var _ backend.BatchLimiter = (*Backend)(nil)
