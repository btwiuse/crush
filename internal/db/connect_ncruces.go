//go:build !((darwin && (amd64 || arm64)) || (freebsd && (amd64 || arm64)) || (linux && (386 || amd64 || arm || arm64 || loong64 || ppc64le || riscv64 || s390x)) || (windows && (386 || amd64 || arm64)))

package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

// openDB opens a named in-memory SQLite database backed by the memdb VFS. If
// an on-disk database file already exists at dbPath, its raw bytes are loaded
// into the memdb before the connection is opened.
func openDB(dbPath string) (*sql.DB, error) {
	name := memDBName(dbPath)

	// Read the existing on-disk database, if present, so we can seed the
	// in-memory database with its contents.
	var data []byte
	if raw, err := os.ReadFile(dbPath); err == nil {
		data = raw
	}

	// Create the named shared-memory database from the existing bytes.
	// Passing nil creates an empty database. memdb.Create also handles
	// converting WAL-mode files to rollback-journal format.
	memdb.Create(name, data)

	// Use BEGIN IMMEDIATE so writers acquire the reserved lock up front,
	// preventing deferred-to-writer upgrade deadlocks.
	dsn := fmt.Sprintf("file:/%s?vfs=memdb&_txlock=immediate", name)
	db, err := driver.Open(dsn, func(c *sqlite3.Conn) error {
		for key, val := range memoryPragmas {
			if err := c.Exec(fmt.Sprintf("PRAGMA %s = %s;", key, val)); err != nil {
				return fmt.Errorf("failed to set pragma %q: %w", key, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory database: %w", err)
	}

	return db, nil
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
		// sql.Conn.Raw delivers the driver-internal *conn value. The ncruces
		// driver exposes it via the exported driver.Conn interface whose
		// Raw() method returns the underlying *sqlite3.Conn.
		dc, ok := c.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type %T", c)
		}
		return dc.Raw().Backup("main", destPath)
	})
}
