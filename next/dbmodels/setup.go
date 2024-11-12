package dbmodels

import (
	"fmt"
	"github.com/diggerhq/digger/next/models_generated"
	slogGorm "github.com/orandin/slog-gorm"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	_ "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"log/slog"
	"os"
	"strings"
)

type DatabaseConfig struct {
	Dialect string
	DSN     string
}

type Database struct {
	GormDB *gorm.DB
	Query  *models_generated.Query
}

var DB *Database

func getDBConfig() (*DatabaseConfig, error) {
	dialect := strings.ToLower(os.Getenv("DIGGER_DB_DIALECT"))
	if dialect == "" {
		dialect = "postgres" // Default to postgres for backwards compatibility
	}

	switch dialect {
	case "postgres":
		dsn := os.Getenv("DIGGER_DATABASE_URL")
		if dsn == "" {
			return nil, fmt.Errorf("DIGGER_DATABASE_URL environment variable is required for postgres")
		}
		return &DatabaseConfig{Dialect: dialect, DSN: dsn}, nil
	case "sqlite":
		dsn := os.Getenv("DIGGER_SQLITE_PATH")
		if dsn == "" {
			dsn = "digger.db" // Default SQLite database name
		}
		return &DatabaseConfig{Dialect: dialect, DSN: dsn}, nil
	default:
		return nil, fmt.Errorf("unsupported database dialect: %s", dialect)
	}
}

func ConnectDatabase() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("gorm", true)
	gormLogger := slogGorm.New(
		slogGorm.WithHandler(logger.Handler()),
		slogGorm.WithTraceAll(),
		slogGorm.SetLogLevel(slogGorm.DefaultLogType, slog.LevelInfo),
		slogGorm.WithContextValue("gorm", "true"),
	)

	config, err := getDBConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to get database config: %v", err))
	}

	var dialector gorm.Dialector
	switch config.Dialect {
	case "postgres":
		dialector = postgres.Open(config.DSN)
	case "sqlite":
		dialector = sqlite.Open(config.DSN)
	}

	database, err := gorm.Open(dialector, &gorm.Config{
		Logger: gormLogger,
	})

	if err != nil {
		panic(fmt.Sprintf("Failed to connect to database: %v", err))
	}

	query := models_generated.Use(database)
	DB = &Database{
		Query:  query,
		GormDB: database,
	}
}
