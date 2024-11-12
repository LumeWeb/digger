package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Client defines the interface for S3 operations needed by S3Lock
type S3Client interface {
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type S3Lock struct {
	client     S3Client
	bucketName string
}

type LockContent struct {
	TransactionId int       `json:"transaction_id"`
	CreatedAt     time.Time `json:"created_at"`
}

func NewS3Lock(client S3Client, bucketName string) (*S3Lock, error) {
	// Verify bucket exists and is accessible
	_, err := client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to access bucket %s: %v", bucketName, err)
	}

	return &S3Lock{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (s *S3Lock) Lock(transactionId int, resource string) (bool, error) {
	// Check if lock already exists
	existingLock, err := s.GetLock(resource)
	if err != nil {
		return false, err
	}
	if existingLock != nil {
		return false, nil
	}

	lockContent := LockContent{
		TransactionId: transactionId,
		CreatedAt:     time.Now(),
	}

	contentBytes, err := json.Marshal(lockContent)
	if err != nil {
		return false, fmt.Errorf("failed to marshal lock content: %v", err)
	}

	_, err = s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(resource),
		Body:   bytes.NewReader(contentBytes),
	})
	if err != nil {
		return false, fmt.Errorf("failed to create lock: %v", err)
	}

	return true, nil
}

func (s *S3Lock) Unlock(resource string) (bool, error) {
	// Check if lock exists first
	existingLock, err := s.GetLock(resource)
	if err != nil {
		return false, err
	}
	if existingLock == nil {
		return false, nil
	}

	_, err = s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(resource),
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete lock: %v", err)
	}

	return true, nil
}

func (s *S3Lock) GetLock(resource string) (*int, error) {
	result, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(resource),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get lock: %v", err)
	}
	defer result.Body.Close()

	var lockContent LockContent
	if err := json.NewDecoder(result.Body).Decode(&lockContent); err != nil {
		return nil, fmt.Errorf("failed to decode lock content: %v", err)
	}

	return &lockContent.TransactionId, nil
}
