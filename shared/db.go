package shared

import (
	"database/sql"
	"log"
	"os"
)

func GetDB() (*sql.DB, func()) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:example@localhost:5432/postgres?sslmode=disable"
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}

	return db, func() { db.Close() }
}
