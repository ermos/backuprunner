package backuprunner

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Storage implements Storage interface for S3-compatible storage
type S3Storage struct {
	client       *s3.Client
	uploader     *manager.Uploader
	bucket       string
	backupPrefix string
}

// NewS3Storage creates a new S3 storage instance
// Compatible with AWS S3, GCP Cloud Storage, MinIO, and other S3-compatible services
func NewS3Storage(endpoint, region, bucket, accessKey, secretKey string, pathStyle bool, backupPrefix string) (*S3Storage, error) {
	if bucket == "" {
		return nil, fmt.Errorf("S3 bucket name is required")
	}

	ctx := context.Background()

	// Build config options
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(region))

	// Set credentials if provided
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Build S3 client options
	var s3Opts []func(*s3.Options)

	// Set custom endpoint for MinIO, R2, etc.
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	// Set path style for MinIO and other S3-compatible services
	if pathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)

	return &S3Storage{
		client:       client,
		uploader:     manager.NewUploader(client),
		bucket:       bucket,
		backupPrefix: backupPrefix,
	}, nil
}

// Upload uploads a file to S3
func (s *S3Storage) Upload(ctx context.Context, sourcePath string, backupName string) error {
	file, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer func(file *os.File) {
		if errFileClose := file.Close(); err != nil {
			log.Printf("failed to close source file: %v", errFileClose)
		}
	}(file)

	key := s.getKey(backupName)

	_, err = s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// List returns all backup files in the S3 bucket with the configured prefix
func (s *S3Storage) List(ctx context.Context) ([]string, error) {
	prefix := s.backupPrefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var backups []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil {
				name := filepath.Base(*obj.Key)
				if isBackupFile(name) {
					backups = append(backups, name)
				}
			}
		}
	}

	// Sort by name (oldest first)
	sort.Strings(backups)

	return backups, nil
}

// Delete removes a backup from S3
func (s *S3Storage) Delete(ctx context.Context, backupName string) error {
	key := s.getKey(backupName)

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete S3 object: %w", err)
	}

	return nil
}

// Type returns the storage type name
func (s *S3Storage) Type() string {
	return "s3"
}

func (s *S3Storage) ApplyRetentionPolicy(ctx context.Context, retentionCount int) error {
	return applyRetentionPolicy(ctx, s, retentionCount)
}

// getKey returns the full S3 key for a backup name
func (s *S3Storage) getKey(backupName string) string {
	if s.backupPrefix == "" {
		return backupName
	}
	return strings.TrimSuffix(s.backupPrefix, "/") + "/" + backupName
}
