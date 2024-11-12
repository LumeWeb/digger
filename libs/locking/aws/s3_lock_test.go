package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

// mockS3Client implements S3Client interface for testing
type mockS3Client struct {
	objects map[string][]byte
}

var _ S3Client = (*mockS3Client)(nil) // Verify mockS3Client implements S3Client

func (m *mockS3Client) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, nil
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.objects == nil {
		m.objects = make(map[string][]byte)
	}
	body, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*params.Key] = body
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if data, ok := m.objects[*params.Key]; ok {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader(data)),
		}, nil
	}
	return nil, &types.NoSuchKey{}
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	delete(m.objects, *params.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3Lock_Lock(t *testing.T) {
	client := &mockS3Client{}
	lock, err := NewS3Lock(client, "test-bucket")
	assert.NoError(t, err)

	// Test successful lock
	acquired, err := lock.Lock(123, "test-resource")
	assert.NoError(t, err)
	assert.True(t, acquired)

	// Test lock already exists
	acquired, err = lock.Lock(456, "test-resource")
	assert.NoError(t, err)
	assert.False(t, acquired)
}

func TestS3Lock_Unlock(t *testing.T) {
	client := &mockS3Client{}
	lock, err := NewS3Lock(client, "test-bucket")
	assert.NoError(t, err)

	// Create a lock first
	content := LockContent{
		TransactionId: 123,
		CreatedAt:     time.Now(),
	}
	contentBytes, _ := json.Marshal(content)
	client.objects = map[string][]byte{
		"test-resource": contentBytes,
	}

	// Test successful unlock
	unlocked, err := lock.Unlock("test-resource")
	assert.NoError(t, err)
	assert.True(t, unlocked)

	// Test unlock non-existent lock
	unlocked, err = lock.Unlock("test-resource")
	assert.NoError(t, err)
	assert.False(t, unlocked)
}

func TestS3Lock_GetLock(t *testing.T) {
	client := &mockS3Client{}
	lock, err := NewS3Lock(client, "test-bucket")
	assert.NoError(t, err)

	// Test get non-existent lock
	id, err := lock.GetLock("test-resource")
	assert.NoError(t, err)
	assert.Nil(t, id)

	// Create a lock
	content := LockContent{
		TransactionId: 123,
		CreatedAt:     time.Now(),
	}
	contentBytes, _ := json.Marshal(content)
	client.objects = map[string][]byte{
		"test-resource": contentBytes,
	}

	// Test get existing lock
	id, err = lock.GetLock("test-resource")
	assert.NoError(t, err)
	assert.NotNil(t, id)
	assert.Equal(t, 123, *id)
}
