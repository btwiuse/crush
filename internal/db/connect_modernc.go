//go:build (darwin && (amd64 || arm64)) || (freebsd && (amd64 || arm64)) || (linux && (386 || amd64 || arm || arm64 || loong64 || ppc64le || riscv64 || s390x)) || (windows && (386 || amd64 || arm64))

package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"

	sqlite "modernc.org/sqlite"
)

// backupConn is satisfied by the unexported *conn type from modernc.org/sqlite.
// It exposes the SQLite online-backup API.
type backupConn interface {
	NewBackup(dstUri string) (*sqlite.Backup, error)
	NewRestore(srcUri string) (*sqlite.Backup, error)
}

// openDB opens a named in-memory SQLite database. If an on-disk database
// file already exists at dbPath, its contents are restored into memory
// before returning.
func openDB(dbPath string) (*sql.DB, error) {
	name := memDBName(dbPath)

	// Build DSN for a named in-memory database with cache=shared so the
	// database is shared among all connections opened with the same name.
	params := url.Values{}
	for key, val := range memoryPragmas {
		params.Add("_pragma", fmt.Sprintf("%s(%s)", key, val))
	}
	params.Set("_txlock", "immediate")
	params.Set("mode", "memory")
	params.Set("cache", "shared")

	dsn := fmt.Sprintf("file:/%s?%s", name, params.Encode())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory database: %w", err)
	}

	// Restore from the on-disk database if it already exists.
	if _, statErr := os.Stat(dbPath); statErr == nil {
		if err := restoreDB(db, dbPath); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to restore database from %s: %w", dbPath, err)
		}
	}

	return db, nil
}

// runBackup executes a backup/restore operation to completion and releases
// all associated resources.
func runBackup(b *sqlite.Backup) error {
	_, err := b.Step(-1)
	if ferr := b.Finish(); ferr != nil && err == nil {
		err = ferr
	}
	return err
}

// restoreDB copies the contents of the SQLite file at srcPath into the
// in-memory database represented by db.
func restoreDB(db *sql.DB, srcPath string) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(c any) error {
		bc, ok := c.(backupConn)
		if !ok {
			return fmt.Errorf("sqlite driver does not support restore (got %T)", c)
		}
		backup, err := bc.NewRestore(srcPath)
		if err != nil {
			return err
		}
		return runBackup(backup)
	})
}

// saveDB writes the in-memory database to the given file path using the
// SQLite online-backup API.
func saveDB(db *sql.DB, destPath string) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(c any) error {
		bc, ok := c.(backupConn)
		if !ok {
			return fmt.Errorf("sqlite driver does not support backup (got %T)", c)
		}
		backup, err := bc.NewBackup(destPath)
		if err != nil {
			return err
		}
		return runBackup(backup)
	})
}
