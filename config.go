package backuprunner

import (
	"errors"
	"strings"

	"github.com/ermos/dotenv"
)

type Config struct {
	// Backup configuration
	BackupCron    string `env:"BACKUP_CRON" required:"true"`
	BackupOnStart bool   `env:"BACKUP_ON_START" default:"false"`
	BackupTimeout int    `env:"BACKUP_TIMEOUT" default:"30"` // Timeout in minutes

	// Storage configuration
	StorageType string `env:"STORAGE_TYPE" default:"local"`

	// Local storage configuration
	LocalBackupPath string `env:"LOCAL_BACKUP_PATH" default:"/backups"`

	// S3 storage configuration (compatible with AWS S3, MinIO, etc.)
	S3Endpoint     string `env:"S3_ENDPOINT"`
	S3Region       string `env:"S3_REGION" default:"us-east-1"`
	S3Bucket       string `env:"S3_BUCKET"`
	S3AccessKey    string `env:"S3_ACCESS_KEY"`
	S3SecretKey    string `env:"S3_SECRET_KEY"`
	S3PathStyle    bool   `env:"S3_PATH_STYLE" default:"false"`
	S3BackupPrefix string `env:"S3_BACKUP_PREFIX"`

	// GCP Cloud Storage configuration (native API with service account)
	GCSBucket          string `env:"GCS_BUCKET"` // Format: gs://bucket-name/prefix
	GCPCredentialsFile string `env:"GCP_CREDENTIALS_FILE"`

	// Parsed GCP values (not from env, computed from GCS_BUCKET)
	GCPBucket       string
	GCPBackupPrefix string

	// Backup retention
	RetentionCount int `env:"RETENTION_COUNT" default:"0"`
}

func ConfigValidate(c *Config) error {
	if c.GCSBucket != "" {
		c.GCPBucket, c.GCPBackupPrefix = parseGCSUri(c.GCSBucket)
	}
	
	switch c.StorageType {
	case "s3":
		if c.S3Bucket == "" {
			return errors.New("S3_BUCKET is required when STORAGE_TYPE is 's3'")
		}
	case "gcp":
		if c.GCPBucket == "" {
			return errors.New("GCS_BUCKET is required when STORAGE_TYPE is 'gcp' (format: gs://bucket-name/prefix)")
		}
	case "local":
		// No additional validation needed
	default:
		return errors.New("STORAGE_TYPE must be 'local', 's3', or 'gcp'")
	}
	return nil
}

// parseGCSUri parses a GCS URI like "gs://bucket-name/path/to/prefix"
// Returns the bucket name and the prefix (path within the bucket)
func parseGCSUri(uri string) (bucket, prefix string) {
	// Remove gs:// prefix
	uri = strings.TrimPrefix(uri, "gs://")

	// Split by first /
	parts := strings.SplitN(uri, "/", 2)
	bucket = parts[0]

	if len(parts) > 1 {
		prefix = parts[1]
	}

	return bucket, prefix
}
