package backuprunner

import (
	"context"
	"fmt"
	"log"
)

func applyRetentionPolicy(ctx context.Context, s Storage, retentionCount int) error {
	log.Printf("Applying retention policy (keeping %d backups)...", retentionCount)

	backups, err := s.List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	if len(backups) <= retentionCount {
		log.Printf("Current backup count (%d) within retention limit", len(backups))
		return nil
	}

	// Delete oldest backups (list is sorted oldest first)
	toDelete := len(backups) - retentionCount
	for i := 0; i < toDelete; i++ {
		log.Printf("Deleting old backup: %s", backups[i])
		if err = s.Delete(ctx, backups[i]); err != nil {
			log.Printf("Warning: failed to delete %s: %v", backups[i], err)
		}
	}

	log.Printf("Retention policy applied, deleted %d old backup(s)", toDelete)
	return nil
}
