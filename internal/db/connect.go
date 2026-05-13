package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"hash/fnv"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
)

// backupInterval controls how frequently the in-memory database is flushed
// to disk.
const backupInterval = 5 * time.Minute

var (
	// memoryPragmas are a subset of pragmas compatible with in-memory SQLite
	// databases. WAL journal mode, synchronous, and page_size do not apply to
	// in-memory databases.
	memoryPragmas = map[string]string{
		"foreign_keys":  "ON",
		"cache_size":    "-8000",
		"secure_delete": "ON",
		"busy_timeout":  "30000",
	}

	gooseInitOnce sync.Once
	gooseInitErr  error
)

// memDBName derives a stable, URL-safe name for an in-memory database from
// the on-disk file path. Each unique path produces a unique name.
func memDBName(dbPath string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(dbPath))
	return fmt.Sprintf("crush%016x", h.Sum64())
}

//go:embed migrations/*.sql
var FS embed.FS

func init() {
	goose.SetBaseFS(FS)

	if testing.Testing() {
		goose.SetLogger(goose.NopLogger())
	}
}

// connEntry holds a shared database connection and its reference count.
type connEntry struct {
	db         *sql.DB
	dbPath     string
	refCount   int
	stopBackup context.CancelFunc
}

var (
	pool   = make(map[string]*connEntry)
	poolMu sync.Mutex
)

// Connect opens a SQLite database connection for the given data
// directory and runs migrations. If a connection to the same database
// file already exists, the existing connection is returned with its
// reference count incremented. Callers must pair each Connect with a
// [Release] when they no longer need the connection.
func Connect(ctx context.Context, dataDir string) (*sql.DB, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("data.dir is not set")
	}

	dbPath := filepath.Join(dataDir, "crush.db")

	// Resolve to an absolute path so that different relative paths to
	// the same file share a single connection.
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		absPath = dbPath
	}

	poolMu.Lock()
	defer poolMu.Unlock()

	if entry, ok := pool[absPath]; ok {
		entry.refCount++
		return entry.db, nil
	}

	conn, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}

	// Serialize all access through a single connection. The in-memory
	// database is not shared across connections (each connection to a
	// named shared-cache database is serialized at the SQLite level
	// anyway), and holding a single idle connection ensures the database
	// is never garbage-collected by the sql.DB pool while the entry
	// lives in our pool.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	if err = conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := initGoose(); err != nil {
		conn.Close()
		slog.Error("Failed to initialize goose", "error", err)
		return nil, fmt.Errorf("failed to initialize goose: %w", err)
	}

	if err := goose.Up(conn, "migrations"); err != nil {
		conn.Close()
		slog.Error("Failed to apply migrations", "error", err)
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	bctx, bcancel := context.WithCancel(context.Background())
	pool[absPath] = &connEntry{db: conn, dbPath: dbPath, refCount: 1, stopBackup: bcancel}
	go periodicBackup(bctx, conn, dbPath)
	return conn, nil
}

// Release decrements the reference count for the database at the given
// data directory. When the count reaches zero the in-memory database is
// flushed to disk, the underlying connection is closed, and the entry is
// removed from the pool.
func Release(dataDir string) error {
	dbPath := filepath.Join(dataDir, "crush.db")
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		absPath = dbPath
	}

	poolMu.Lock()
	defer poolMu.Unlock()

	entry, ok := pool[absPath]
	if !ok {
		return nil
	}

	entry.refCount--
	if entry.refCount > 0 {
		return nil
	}

	// Stop the periodic backup goroutine before flushing.
	entry.stopBackup()

	// Flush the in-memory database to disk one final time.
	if err := saveDB(entry.db, entry.dbPath); err != nil {
		slog.Warn("Failed to flush database to disk on release", "error", err)
	}

	delete(pool, absPath)
	return entry.db.Close()
}

// ResetPool closes all pooled connections and clears the pool. This is
// intended for use in tests to ensure a clean state between test cases.
func ResetPool() {
	poolMu.Lock()
	defer poolMu.Unlock()
	for path, entry := range pool {
		if entry.stopBackup != nil {
			entry.stopBackup()
		}
		entry.db.Close()
		delete(pool, path)
	}
}

func initGoose() error {
	gooseInitOnce.Do(func() {
		goose.SetBaseFS(FS)
		gooseInitErr = goose.SetDialect("sqlite3")
	})

	return gooseInitErr
}

// periodicBackup flushes the in-memory database to disk on a fixed interval
// until ctx is cancelled.
func periodicBackup(ctx context.Context, db *sql.DB, dbPath string) {
	t := time.NewTicker(backupInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := saveDB(db, dbPath); err != nil {
				slog.Warn("Failed to write database to disk", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
