package backuprunner

import "context"

type Backup interface {
	Name() string
	Config() (*Config, error)
	ExtraConfigLogInfo() []string
	SetStorage(s Storage) error
	TestConnection(ctx context.Context) error
	Run(ctx context.Context) error
}
