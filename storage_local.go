package backuprunner

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LocalStorage implements Storage interface for local filesystem
type LocalStorage struct {
	basePath string
}

// NewLocalStorage creates a new local storage instance
func NewLocalStorage(basePath string) (*LocalStorage, error) {
	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	return &LocalStorage{
		basePath: basePath,
	}, nil
}

// Upload copies a file to the local backup directory
func (s *LocalStorage) Upload(ctx context.Context, sourcePath string, backupName string) error {
	destPath := filepath.Join(s.basePath, backupName)

	// Open source file
	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer func(src *os.File) {
		if errSrcClose := src.Close(); err != nil {
			log.Printf("failed to close source file: %v", errSrcClose)
		}
	}(src)

	// Create destination file
	dst, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer func(dst *os.File) {
		if errDstClose := dst.Close(); err != nil {
			log.Printf("failed to close destination file: %v", errDstClose)
		}
	}(dst)

	// Copy with context cancellation support
	done := make(chan error, 1)
	go func() {
		_, errIoCopy := io.Copy(dst, src)
		done <- errIoCopy
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-done:
		if err != nil {
			return fmt.Errorf("failed to copy file: %w", err)
		}
	}

	return nil
}

// List returns all backup files in the directory
func (s *LocalStorage) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	var backups []string
	for _, entry := range entries {
		if !entry.IsDir() && isBackupFile(entry.Name()) {
			backups = append(backups, entry.Name())
		}
	}

	// Sort by name (which includes timestamp, so oldest first)
	sort.Strings(backups)

	return backups, nil
}

// Delete removes a backup file
func (s *LocalStorage) Delete(ctx context.Context, backupName string) error {
	filePath := filepath.Join(s.basePath, backupName)
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete backup: %w", err)
	}
	return nil
}

// Type returns the storage type name
func (s *LocalStorage) Type() string {
	return "local"
}

func (s *LocalStorage) ApplyRetentionPolicy(ctx context.Context, retentionCount int) error {
	return applyRetentionPolicy(ctx, s, retentionCount)
}

// isBackupFile checks if a filename is a PostgreSQL backup file
func isBackupFile(name string) bool {
	return strings.HasPrefix(name, "pg-backup_") &&
		(strings.HasSuffix(name, ".dump") ||
			strings.HasSuffix(name, ".sql") ||
			strings.HasSuffix(name, ".sql.gz") ||
			strings.HasSuffix(name, ".tar"))
}
