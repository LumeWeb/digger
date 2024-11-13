package models

import (
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	_ "gorm.io/driver/postgres"
	_ "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"net/url"
	"os"
)

type Database struct {
	GormDB *gorm.DB
}

var DEFAULT_ORG_NAME = "digger"

// var DB *gorm.DB
var DB *Database

func ConnectDatabase() {
	dbUrl := os.Getenv("DATABASE_URL")
	var database *gorm.DB
	var err error

	u, err := url.Parse(dbUrl)
	if err != nil {
		panic("Invalid database URL: " + err.Error())
	}

	switch u.Scheme {
	case "sqlite":
		database, err = gorm.Open(sqlite.Open(u.Host+u.Path), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Info),
		})
	case "postgres", "postgresql":
		database, err = gorm.Open(postgres.Open(dbUrl), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Info),
		})
	default:
		panic("Unsupported database type: " + u.Scheme)
	}

	if err != nil {
		panic("Failed to connect to database!")
	}

	DB = &Database{GormDB: database}

	// data and fixtures added
	orgNumberOne, err := DB.GetOrganisation(DEFAULT_ORG_NAME)
	if orgNumberOne == nil {
		log.Print("No default found, creating default organisation")
		DB.CreateOrganisation("digger", "", DEFAULT_ORG_NAME)
	}

}
