package backuprunner

import (
	"context"
	"fmt"
)

// Storage interface defines methods for backup storage
type Storage interface {
	// Upload uploads a backup file to the storage
	Upload(ctx context.Context, sourcePath string, backupName string) error
	// List returns a list of backup names in the storage
	List(ctx context.Context) ([]string, error)
	// Delete removes a backup from the storage
	Delete(ctx context.Context, backupName string) error
	// Type returns the storage type name
	Type() string
	// ApplyRetentionPolicy enforces the retention policy by deleting old backups
	ApplyRetentionPolicy(ctx context.Context, retentionCount int) error
}

// New creates a new storage instance based on configuration
func New(cfg *Config) (Storage, error) {
	switch cfg.StorageType {
	case "local":
		return NewLocalStorage(cfg.LocalBackupPath)
	case "s3":
		return NewS3Storage(
			cfg.S3Endpoint,
			cfg.S3Region,
			cfg.S3Bucket,
			cfg.S3AccessKey,
			cfg.S3SecretKey,
			cfg.S3PathStyle,
			cfg.S3BackupPrefix,
		)
	case "gcp":
		return NewGCPStorage(
			cfg.GCPCredentialsFile,
			cfg.GCPBucket,
			cfg.GCPBackupPrefix,
		)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s (supported: local, s3, gcp)", cfg.StorageType)
	}
}
