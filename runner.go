package backuprunner

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
)

func Run(b Backup) error {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Starting %s Backup Service...", b.Name())

	// Load configuration
	cfg, err := b.Config()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	log.Printf("Configuration loaded:")
	for _, v := range b.ExtraConfigLogInfo() {
		log.Printf("  %s", v)
	}
	log.Printf("  Backup schedule: %s", cfg.BackupCron)
	log.Printf("  Storage type: %s", cfg.StorageType)
	log.Printf("  Retention count: %d", cfg.RetentionCount)

	// Initialize storage
	store, err := New(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %v", err)
	}
	log.Printf("Storage initialized: %s", store.Type())

	// Set storage
	err = b.SetStorage(store)
	if err != nil {
		return fmt.Errorf("failed to set storage: %v", err)
	}

	// Test PostgreSQL connection
	if err = b.TestConnection(context.Background()); err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}
	log.Println("PostgreSQL connection verified")

	// Run backup on start if configured
	if cfg.BackupOnStart {
		log.Println("Running initial backup on startup...")
		if err = b.Run(context.Background()); err != nil {
			log.Printf("Initial backup failed: %v", err)
		}
	}

	// Setup cron scheduler (standard 5-field format: minute hour dom month dow)
	c := cron.New()

	// Add backup job
	entryID, err := c.AddFunc(cfg.BackupCron, func() {
		log.Println("Cron triggered backup job")
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.BackupTimeout)*time.Minute)
		defer cancel()

		if err = b.Run(ctx); err != nil {
			log.Printf("Backup failed: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to add cron job: %v", err)
	}
	log.Printf("Cron job registered with ID: %d", entryID)

	// Start cron scheduler
	c.Start()
	log.Println("Cron scheduler started, waiting for scheduled jobs...")

	// Print next scheduled run time
	entries := c.Entries()
	if len(entries) > 0 {
		log.Printf("Next backup scheduled at: %s", entries[0].Next.Format("2006-01-02 15:04:05"))
	}

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal %s, shutting down...", sig)

	// Stop cron scheduler
	ctx := c.Stop()
	<-ctx.Done()

	log.Println("Shutdown complete")
	return nil
}
