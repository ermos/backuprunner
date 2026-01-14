package backuprunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCPStorage implements Storage interface for Google Cloud Storage
type GCPStorage struct {
	client       *storage.Client
	bucket       string
	backupPrefix string
}

// NewGCPStorage creates a new GCP Cloud Storage instance
// Uses service account JSON file for authentication
func NewGCPStorage(credentialsFile, bucket, backupPrefix string) (*GCPStorage, error) {
	if bucket == "" {
		return nil, fmt.Errorf("GCP bucket name is required")
	}

	ctx := context.Background()
	var client *storage.Client
	var err error

	if credentialsFile != "" {
		// Use service account JSON file
		client, err = storage.NewClient(ctx, option.WithAuthCredentialsFile(option.ServiceAccount, credentialsFile))
	} else {
		// Use default credentials (GOOGLE_APPLICATION_CREDENTIALS env var or metadata server)
		client, err = storage.NewClient(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create GCP storage client: %w", err)
	}

	return &GCPStorage{
		client:       client,
		bucket:       bucket,
		backupPrefix: backupPrefix,
	}, nil
}

// Upload uploads a file to GCP Cloud Storage
func (s *GCPStorage) Upload(ctx context.Context, sourcePath string, backupName string) error {
	file, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer func(file *os.File) {
		if errFileClose := file.Close(); err != nil {
			log.Printf("failed to close file %q: %v", sourcePath, errFileClose)
		}
	}(file)

	objectName := s.getObjectName(backupName)
	obj := s.client.Bucket(s.bucket).Object(objectName)

	writer := obj.NewWriter(ctx)
	defer func(writer *storage.Writer) {
		if errWriterClose := writer.Close(); err != nil {
			log.Printf("failed to close GCS writer for object %q: %v", objectName, errWriterClose)
		}
	}(writer)

	if _, err = io.Copy(writer, file); err != nil {
		return fmt.Errorf("failed to upload to GCS: %w", err)
	}

	return writer.Close()
}

// List returns all backup files in the GCS bucket with the configured prefix
func (s *GCPStorage) List(ctx context.Context) ([]string, error) {
	prefix := s.backupPrefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	query := &storage.Query{Prefix: prefix}
	it := s.client.Bucket(s.bucket).Objects(ctx, query)

	var backups []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list GCS objects: %w", err)
		}

		name := filepath.Base(attrs.Name)
		if isBackupFile(name) {
			backups = append(backups, name)
		}
	}

	// Sort by name (oldest first)
	sort.Strings(backups)

	return backups, nil
}

// Delete removes a backup from GCS
func (s *GCPStorage) Delete(ctx context.Context, backupName string) error {
	objectName := s.getObjectName(backupName)
	obj := s.client.Bucket(s.bucket).Object(objectName)

	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete GCS object: %w", err)
	}

	return nil
}

// Type returns the storage type name
func (s *GCPStorage) Type() string {
	return "gcp"
}

func (s *GCPStorage) ApplyRetentionPolicy(ctx context.Context, retentionCount int) error {
	return applyRetentionPolicy(ctx, s, retentionCount)
}

// Close closes the GCP client
func (s *GCPStorage) Close() error {
	return s.client.Close()
}

// getObjectName returns the full GCS object name for a backup
func (s *GCPStorage) getObjectName(backupName string) string {
	if s.backupPrefix == "" {
		return backupName
	}
	return strings.TrimSuffix(s.backupPrefix, "/") + "/" + backupName
}
