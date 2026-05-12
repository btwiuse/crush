// Package db provides the data access layer for Crush.
// This file contains the filesystem-based Querier implementation.
package db

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// messageRecord is the on-disk representation of a Message.
// The extra Seq field preserves insertion order when multiple messages
// share the same CreatedAt second (which is common in tests).
type messageRecord struct {
	Message
	Seq int64 `json:"_seq"`
}

// FSQuerier implements the Querier interface using the filesystem.
// Each record is stored as a separate JSON file under a data directory.
//
// Directory structure:
//
//	<dataDir>/
//	  sessions/<id>.json
//	  messages/<sessionID>/<id>.json
//	  files/<id>.json
//	  read_files/<sessionID>/<hex(path)>.json
type FSQuerier struct {
	dataDir string
	mu      sync.RWMutex
	seq     int64 // monotonically increasing creation counter
}

// NewFSQuerier creates a new FSQuerier that stores data under dataDir.
// The required subdirectories are created if they do not already exist.
func NewFSQuerier(dataDir string) (*FSQuerier, error) {
	for _, subDir := range []string{
		filepath.Join(dataDir, "sessions"),
		filepath.Join(dataDir, "messages"),
		filepath.Join(dataDir, "files"),
		filepath.Join(dataDir, "read_files"),
	} {
		if err := os.MkdirAll(subDir, 0o700); err != nil {
			return nil, fmt.Errorf("creating directory %s: %w", subDir, err)
		}
	}
	return &FSQuerier{dataDir: dataDir, seq: time.Now().UnixNano()}, nil
}

// Compile-time assertion that FSQuerier satisfies the Querier interface.
var _ Querier = (*FSQuerier)(nil)

// ---------- context helper ----------

// ctxErr returns ctx.Err() if the context is already done, otherwise nil.
func ctxErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// ---------- path helpers ----------

func (q *FSQuerier) sessionPath(id string) string {
	return filepath.Join(q.dataDir, "sessions", id+".json")
}

func (q *FSQuerier) messagePath(sessionID, id string) string {
	return filepath.Join(q.dataDir, "messages", sessionID, id+".json")
}

func (q *FSQuerier) filePath(id string) string {
	return filepath.Join(q.dataDir, "files", id+".json")
}

func (q *FSQuerier) readFilePath(sessionID, path string) string {
	encoded := hex.EncodeToString([]byte(path))
	return filepath.Join(q.dataDir, "read_files", sessionID, encoded+".json")
}

// ---------- generic JSON helpers (no lock) ----------

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeJSONFile(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ---------- internal helpers (called with lock already held) ----------

func (q *FSQuerier) readSessionNoLock(id string) (Session, error) {
	var s Session
	if err := readJSONFile(q.sessionPath(id), &s); err != nil {
		if os.IsNotExist(err) {
			return Session{}, sql.ErrNoRows
		}
		return Session{}, err
	}
	return s, nil
}

func (q *FSQuerier) writeSessionNoLock(s Session) error {
	return writeJSONFile(q.sessionPath(s.ID), s)
}

func (q *FSQuerier) listAllSessionsNoLock() ([]Session, error) {
	dir := filepath.Join(q.dataDir, "sessions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	sessions := make([]Session, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var s Session
		if err := readJSONFile(filepath.Join(dir, e.Name()), &s); err != nil {
			continue
		}
		sessions = append(sessions, s)
	}
	return sessions, nil
}

func (q *FSQuerier) readMessageNoLock(sessionID, id string) (Message, error) {
	var rec messageRecord
	if err := readJSONFile(q.messagePath(sessionID, id), &rec); err != nil {
		if os.IsNotExist(err) {
			return Message{}, sql.ErrNoRows
		}
		return Message{}, err
	}
	return rec.Message, nil
}

func (q *FSQuerier) listMessagesForSessionNoLock(sessionID string) ([]Message, error) {
	dir := filepath.Join(q.dataDir, "messages", sessionID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	type seqMsg struct {
		msg Message
		seq int64
	}
	records := make([]seqMsg, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var rec messageRecord
		if err := readJSONFile(filepath.Join(dir, e.Name()), &rec); err != nil {
			continue
		}
		records = append(records, seqMsg{msg: rec.Message, seq: rec.Seq})
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].seq != records[j].seq {
			return records[i].seq < records[j].seq
		}
		return records[i].msg.ID < records[j].msg.ID
	})
	msgs := make([]Message, len(records))
	for i, r := range records {
		msgs[i] = r.msg
	}
	return msgs, nil
}

func (q *FSQuerier) listAllMessagesNoLock() ([]Message, error) {
	baseDir := filepath.Join(q.dataDir, "messages")
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var msgs []Message
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sessionMsgs, err := q.listMessagesForSessionNoLock(e.Name())
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, sessionMsgs...)
	}
	return msgs, nil
}

func (q *FSQuerier) readFileNoLock(id string) (File, error) {
	var f File
	if err := readJSONFile(q.filePath(id), &f); err != nil {
		if os.IsNotExist(err) {
			return File{}, sql.ErrNoRows
		}
		return File{}, err
	}
	return f, nil
}

func (q *FSQuerier) listAllFilesNoLock() ([]File, error) {
	dir := filepath.Join(q.dataDir, "files")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	files := make([]File, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var f File
		if err := readJSONFile(filepath.Join(dir, e.Name()), &f); err != nil {
			continue
		}
		files = append(files, f)
	}
	return files, nil
}

func (q *FSQuerier) listReadFilesForSessionNoLock(sessionID string) ([]ReadFile, error) {
	dir := filepath.Join(q.dataDir, "read_files", sessionID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	result := make([]ReadFile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var rf ReadFile
		if err := readJSONFile(filepath.Join(dir, e.Name()), &rf); err != nil {
			continue
		}
		result = append(result, rf)
	}
	return result, nil
}

// ---------- Session methods ----------

// CreateSession creates a new session.
func (q *FSQuerier) CreateSession(ctx context.Context, arg CreateSessionParams) (Session, error) {
	if err := ctxErr(ctx); err != nil {
		return Session{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().Unix()
	s := Session{
		ID:               arg.ID,
		ParentSessionID:  arg.ParentSessionID,
		Title:            arg.Title,
		MessageCount:     arg.MessageCount,
		PromptTokens:     arg.PromptTokens,
		CompletionTokens: arg.CompletionTokens,
		Cost:             arg.Cost,
		UpdatedAt:        now,
		CreatedAt:        now,
	}
	if err := q.writeSessionNoLock(s); err != nil {
		return Session{}, err
	}
	return s, nil
}

// DeleteSession removes a session by ID.
func (q *FSQuerier) DeleteSession(ctx context.Context, id string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := os.Remove(q.sessionPath(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// GetSessionByID returns a session by ID.
func (q *FSQuerier) GetSessionByID(ctx context.Context, id string) (Session, error) {
	if err := ctxErr(ctx); err != nil {
		return Session{}, err
	}
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.readSessionNoLock(id)
}

// GetLastSession returns the most recently updated session.
func (q *FSQuerier) GetLastSession(ctx context.Context) (Session, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return Session{}, err
	}
	if len(sessions) == 0 {
		return Session{}, sql.ErrNoRows
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt > sessions[j].UpdatedAt
	})
	return sessions[0], nil
}

// ListSessions returns all top-level sessions ordered by updated_at descending.
func (q *FSQuerier) ListSessions(ctx context.Context) ([]Session, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return nil, err
	}

	// Filter to top-level sessions only.
	result := sessions[:0]
	for _, s := range sessions {
		if !s.ParentSessionID.Valid {
			result = append(result, s)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].UpdatedAt > result[j].UpdatedAt
	})
	return result, nil
}

// RenameSession updates the title of a session.
func (q *FSQuerier) RenameSession(ctx context.Context, arg RenameSessionParams) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	s, err := q.readSessionNoLock(arg.ID)
	if err != nil {
		return err
	}
	s.Title = arg.Title
	return q.writeSessionNoLock(s)
}

// UpdateSession updates a session's fields and returns the updated session.
func (q *FSQuerier) UpdateSession(ctx context.Context, arg UpdateSessionParams) (Session, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	s, err := q.readSessionNoLock(arg.ID)
	if err != nil {
		return Session{}, err
	}
	s.Title = arg.Title
	s.PromptTokens = arg.PromptTokens
	s.CompletionTokens = arg.CompletionTokens
	s.SummaryMessageID = arg.SummaryMessageID
	s.Cost = arg.Cost
	s.Todos = arg.Todos
	s.UpdatedAt = time.Now().Unix()
	if err := q.writeSessionNoLock(s); err != nil {
		return Session{}, err
	}
	return s, nil
}

// UpdateSessionTitleAndUsage atomically increments usage counters.
func (q *FSQuerier) UpdateSessionTitleAndUsage(ctx context.Context, arg UpdateSessionTitleAndUsageParams) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	s, err := q.readSessionNoLock(arg.ID)
	if err != nil {
		return err
	}
	s.Title = arg.Title
	s.PromptTokens += arg.PromptTokens
	s.CompletionTokens += arg.CompletionTokens
	s.Cost += arg.Cost
	s.UpdatedAt = time.Now().Unix()
	return q.writeSessionNoLock(s)
}

// ---------- Message methods ----------

// CreateMessage creates a new message and increments the session message count.
func (q *FSQuerier) CreateMessage(ctx context.Context, arg CreateMessageParams) (Message, error) {
	if err := ctxErr(ctx); err != nil {
		return Message{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	q.seq++
	now := time.Now().Unix()
	m := Message{
		ID:               arg.ID,
		SessionID:        arg.SessionID,
		Role:             arg.Role,
		Parts:            arg.Parts,
		Model:            arg.Model,
		CreatedAt:        now,
		UpdatedAt:        now,
		Provider:         arg.Provider,
		IsSummaryMessage: arg.IsSummaryMessage,
	}

	rec := messageRecord{Message: m, Seq: q.seq}
	if err := writeJSONFile(q.messagePath(arg.SessionID, arg.ID), rec); err != nil {
		return Message{}, err
	}

	// Mirror the SQLite trigger: increment the session's message_count.
	if s, err := q.readSessionNoLock(arg.SessionID); err == nil {
		s.MessageCount++
		_ = q.writeSessionNoLock(s)
	}

	return m, nil
}

// DeleteMessage removes a message by ID.
func (q *FSQuerier) DeleteMessage(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Find the message across all sessions.
	baseDir := filepath.Join(q.dataDir, "messages")
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p := q.messagePath(e.Name(), id)
		if _, err := os.Stat(p); err == nil {
			sessionID := e.Name()
			if removeErr := os.Remove(p); removeErr != nil && !os.IsNotExist(removeErr) {
				return removeErr
			}
			// Mirror the SQLite trigger: decrement the session's message_count.
			if s, err := q.readSessionNoLock(sessionID); err == nil {
				if s.MessageCount > 0 {
					s.MessageCount--
				}
				_ = q.writeSessionNoLock(s)
			}
			return nil
		}
	}
	return nil
}

// DeleteSessionMessages removes all messages for a session.
func (q *FSQuerier) DeleteSessionMessages(ctx context.Context, sessionID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	dir := filepath.Join(q.dataDir, "messages", sessionID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	count := int64(0)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
		count++
	}

	// Mirror the trigger: set message_count to 0 after deleting all.
	if s, err := q.readSessionNoLock(sessionID); err == nil {
		s.MessageCount -= count
		if s.MessageCount < 0 {
			s.MessageCount = 0
		}
		_ = q.writeSessionNoLock(s)
	}
	return nil
}

// GetMessage returns a message by ID.
func (q *FSQuerier) GetMessage(ctx context.Context, id string) (Message, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	baseDir := filepath.Join(q.dataDir, "messages")
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return Message{}, sql.ErrNoRows
		}
		return Message{}, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := q.readMessageNoLock(e.Name(), id)
		if err == nil {
			return m, nil
		}
	}
	return Message{}, sql.ErrNoRows
}

// ListMessagesBySession returns all messages for a session in creation order.
func (q *FSQuerier) ListMessagesBySession(ctx context.Context, sessionID string) ([]Message, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	q.mu.RLock()
	defer q.mu.RUnlock()

	// listMessagesForSessionNoLock already returns messages sorted by Seq (creation order).
	return q.listMessagesForSessionNoLock(sessionID)
}

// ListUserMessagesBySession returns user messages for a session ordered by created_at desc.
func (q *FSQuerier) ListUserMessagesBySession(ctx context.Context, sessionID string) ([]Message, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	msgs, err := q.listMessagesForSessionNoLock(sessionID)
	if err != nil {
		return nil, err
	}
	result := msgs[:0]
	for _, m := range msgs {
		if m.Role == "user" {
			result = append(result, m)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result, nil
}

// ListAllUserMessages returns all user messages across all sessions ordered by created_at desc.
func (q *FSQuerier) ListAllUserMessages(ctx context.Context) ([]Message, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	all, err := q.listAllMessagesNoLock()
	if err != nil {
		return nil, err
	}
	result := all[:0]
	for _, m := range all {
		if m.Role == "user" {
			result = append(result, m)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result, nil
}

// UpdateMessage updates the parts and finished_at fields of a message.
func (q *FSQuerier) UpdateMessage(ctx context.Context, arg UpdateMessageParams) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	// Find the message across all sessions.
	baseDir := filepath.Join(q.dataDir, "messages")
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return sql.ErrNoRows
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var rec messageRecord
		if readErr := readJSONFile(q.messagePath(e.Name(), arg.ID), &rec); readErr != nil {
			continue
		}
		rec.Message.Parts = arg.Parts
		rec.Message.FinishedAt = arg.FinishedAt
		rec.Message.UpdatedAt = time.Now().Unix()
		return writeJSONFile(q.messagePath(e.Name(), arg.ID), rec)
	}
	return sql.ErrNoRows
}

// ---------- File methods ----------

// CreateFile creates a new file record.
func (q *FSQuerier) CreateFile(ctx context.Context, arg CreateFileParams) (File, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().Unix()
	f := File{
		ID:        arg.ID,
		SessionID: arg.SessionID,
		Path:      arg.Path,
		Content:   arg.Content,
		Version:   arg.Version,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := writeJSONFile(q.filePath(arg.ID), f); err != nil {
		return File{}, err
	}
	return f, nil
}

// DeleteFile removes a file by ID.
func (q *FSQuerier) DeleteFile(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := os.Remove(q.filePath(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DeleteSessionFiles removes all file records for a session.
func (q *FSQuerier) DeleteSessionFiles(ctx context.Context, sessionID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	files, err := q.listAllFilesNoLock()
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.SessionID != sessionID {
			continue
		}
		if err := os.Remove(q.filePath(f.ID)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// GetFile returns a file record by ID.
func (q *FSQuerier) GetFile(ctx context.Context, id string) (File, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.readFileNoLock(id)
}

// GetFileByPathAndSession returns the latest version of a file for a given path and session.
func (q *FSQuerier) GetFileByPathAndSession(ctx context.Context, arg GetFileByPathAndSessionParams) (File, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	files, err := q.listAllFilesNoLock()
	if err != nil {
		return File{}, err
	}

	// Filter by path and session, then pick the one with max version (then max created_at).
	var matches []File
	for _, f := range files {
		if f.Path == arg.Path && f.SessionID == arg.SessionID {
			matches = append(matches, f)
		}
	}
	if len(matches) == 0 {
		return File{}, sql.ErrNoRows
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Version != matches[j].Version {
			return matches[i].Version > matches[j].Version
		}
		return matches[i].CreatedAt > matches[j].CreatedAt
	})
	return matches[0], nil
}

// ListFilesByPath returns all file records for a path ordered by version desc, created_at desc.
func (q *FSQuerier) ListFilesByPath(ctx context.Context, path string) ([]File, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	files, err := q.listAllFilesNoLock()
	if err != nil {
		return nil, err
	}
	var result []File
	for _, f := range files {
		if f.Path == path {
			result = append(result, f)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Version != result[j].Version {
			return result[i].Version > result[j].Version
		}
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result, nil
}

// ListFilesBySession returns all file records for a session ordered by version asc, created_at asc.
func (q *FSQuerier) ListFilesBySession(ctx context.Context, sessionID string) ([]File, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	files, err := q.listAllFilesNoLock()
	if err != nil {
		return nil, err
	}
	var result []File
	for _, f := range files {
		if f.SessionID == sessionID {
			result = append(result, f)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Version != result[j].Version {
			return result[i].Version < result[j].Version
		}
		return result[i].CreatedAt < result[j].CreatedAt
	})
	return result, nil
}

// ListLatestSessionFiles returns the latest-version file records for the given session.
// For each file path, it finds the globally latest version, then filters to records
// belonging to the given session.
func (q *FSQuerier) ListLatestSessionFiles(ctx context.Context, sessionID string) ([]File, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	files, err := q.listAllFilesNoLock()
	if err != nil {
		return nil, err
	}

	type key struct{ version, createdAt int64 }
	// For each path find the globally latest (max version, then max created_at).
	latest := make(map[string]key)
	for _, f := range files {
		k := key{f.Version, f.CreatedAt}
		if prev, ok := latest[f.Path]; !ok ||
			k.version > prev.version ||
			(k.version == prev.version && k.createdAt > prev.createdAt) {
			latest[f.Path] = k
		}
	}

	var result []File
	for _, f := range files {
		if f.SessionID != sessionID {
			continue
		}
		lk := latest[f.Path]
		if f.Version == lk.version && f.CreatedAt == lk.createdAt {
			result = append(result, f)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Path < result[j].Path
	})
	return result, nil
}

// ListNewFiles is not implemented in the filesystem backend (no is_new column).
func (q *FSQuerier) ListNewFiles(_ context.Context) ([]File, error) {
	return nil, nil
}

// ---------- ReadFile methods ----------

// GetFileRead returns the read record for a session+path combination.
func (q *FSQuerier) GetFileRead(ctx context.Context, arg GetFileReadParams) (ReadFile, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var rf ReadFile
	if err := readJSONFile(q.readFilePath(arg.SessionID, arg.Path), &rf); err != nil {
		if os.IsNotExist(err) {
			return ReadFile{}, sql.ErrNoRows
		}
		return ReadFile{}, err
	}
	return rf, nil
}

// ListSessionReadFiles returns all read-file records for a session, ordered by read_at desc.
func (q *FSQuerier) ListSessionReadFiles(ctx context.Context, sessionID string) ([]ReadFile, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result, err := q.listReadFilesForSessionNoLock(sessionID)
	if err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ReadAt > result[j].ReadAt
	})
	return result, nil
}

// RecordFileRead upserts a read-file record (insert or update read_at).
func (q *FSQuerier) RecordFileRead(ctx context.Context, arg RecordFileReadParams) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	rf := ReadFile{
		SessionID: arg.SessionID,
		Path:      arg.Path,
		ReadAt:    time.Now().Unix(),
	}
	return writeJSONFile(q.readFilePath(arg.SessionID, arg.Path), rf)
}

// ---------- Stats methods ----------

// GetAverageResponseTime returns the mean response time in seconds for assistant messages.
func (q *FSQuerier) GetAverageResponseTime(ctx context.Context) (int64, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	msgs, err := q.listAllMessagesNoLock()
	if err != nil {
		return 0, err
	}
	var total int64
	var count int64
	for _, m := range msgs {
		if m.Role != "assistant" {
			continue
		}
		if !m.FinishedAt.Valid || m.FinishedAt.Int64 <= m.CreatedAt {
			continue
		}
		total += m.FinishedAt.Int64 - m.CreatedAt
		count++
	}
	if count == 0 {
		return 0, nil
	}
	return total / count, nil
}

// GetHourDayHeatmap returns session counts grouped by day-of-week and hour.
func (q *FSQuerier) GetHourDayHeatmap(ctx context.Context) ([]GetHourDayHeatmapRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return nil, err
	}

	type cell struct{ dow, hour int64 }
	counts := make(map[cell]int64)
	for _, s := range sessions {
		if s.ParentSessionID.Valid {
			continue
		}
		t := time.Unix(s.CreatedAt, 0)
		c := cell{int64(t.Weekday()), int64(t.Hour())}
		counts[c]++
	}

	rows := make([]GetHourDayHeatmapRow, 0, len(counts))
	for c, n := range counts {
		rows = append(rows, GetHourDayHeatmapRow{
			DayOfWeek:    c.dow,
			Hour:         c.hour,
			SessionCount: n,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].DayOfWeek != rows[j].DayOfWeek {
			return rows[i].DayOfWeek < rows[j].DayOfWeek
		}
		return rows[i].Hour < rows[j].Hour
	})
	return rows, nil
}

// GetRecentActivity returns daily session activity for the last 30 days.
func (q *FSQuerier) GetRecentActivity(ctx context.Context) ([]GetRecentActivityRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().AddDate(0, 0, -30).Unix()

	type dayStats struct {
		count  int64
		tokens float64
		cost   float64
	}
	byDay := make(map[string]*dayStats)
	for _, s := range sessions {
		if s.ParentSessionID.Valid {
			continue
		}
		if s.CreatedAt < cutoff {
			continue
		}
		day := time.Unix(s.CreatedAt, 0).UTC().Format("2006-01-02")
		if byDay[day] == nil {
			byDay[day] = &dayStats{}
		}
		byDay[day].count++
		byDay[day].tokens += float64(s.PromptTokens + s.CompletionTokens)
		byDay[day].cost += s.Cost
	}

	rows := make([]GetRecentActivityRow, 0, len(byDay))
	for day, stats := range byDay {
		rows = append(rows, GetRecentActivityRow{
			Day:          day,
			SessionCount: stats.count,
			TotalTokens:  sql.NullFloat64{Float64: stats.tokens, Valid: true},
			Cost:         sql.NullFloat64{Float64: stats.cost, Valid: true},
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Day.(string) < rows[j].Day.(string)
	})
	return rows, nil
}

// GetToolUsage returns tool call counts extracted from message parts.
func (q *FSQuerier) GetToolUsage(ctx context.Context) ([]GetToolUsageRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	msgs, err := q.listAllMessagesNoLock()
	if err != nil {
		return nil, err
	}

	counts := make(map[string]int64)
	for _, m := range msgs {
		counts = extractToolCalls(m.Parts, counts)
	}

	rows := make([]GetToolUsageRow, 0, len(counts))
	for name, cnt := range counts {
		rows = append(rows, GetToolUsageRow{ToolName: name, CallCount: cnt})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].CallCount > rows[j].CallCount
	})
	return rows, nil
}

// extractToolCalls parses a JSON parts string and accumulates tool call counts.
func extractToolCalls(partsJSON string, counts map[string]int64) map[string]int64 {
	// The parts field is a JSON array of {"type":"tool_call","data":{"name":"...",...}}.
	var parts []struct {
		Type string `json:"type"`
		Data struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(partsJSON), &parts); err != nil {
		return counts
	}
	for _, p := range parts {
		if p.Type == "tool_call" && p.Data.Name != "" {
			counts[p.Data.Name]++
		}
	}
	return counts
}

// GetTotalStats returns aggregate statistics across all top-level sessions.
func (q *FSQuerier) GetTotalStats(ctx context.Context) (GetTotalStatsRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return GetTotalStatsRow{}, err
	}

	var (
		totalSessions         int64
		totalPromptTokens     int64
		totalCompletionTokens int64
		totalCost             float64
		totalMessages         int64
	)
	for _, s := range sessions {
		if s.ParentSessionID.Valid {
			continue
		}
		totalSessions++
		totalPromptTokens += s.PromptTokens
		totalCompletionTokens += s.CompletionTokens
		totalCost += s.Cost
		totalMessages += s.MessageCount
	}

	var avgTokens, avgMessages float64
	if totalSessions > 0 {
		avgTokens = float64(totalPromptTokens+totalCompletionTokens) / float64(totalSessions)
		avgMessages = float64(totalMessages) / float64(totalSessions)
	}

	return GetTotalStatsRow{
		TotalSessions:         totalSessions,
		TotalPromptTokens:     totalPromptTokens,
		TotalCompletionTokens: totalCompletionTokens,
		TotalCost:             totalCost,
		TotalMessages:         totalMessages,
		AvgTokensPerSession:   avgTokens,
		AvgMessagesPerSession: avgMessages,
	}, nil
}

// GetUsageByDay returns token and cost totals grouped by calendar day, ordered by day desc.
func (q *FSQuerier) GetUsageByDay(ctx context.Context) ([]GetUsageByDayRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return nil, err
	}

	type dayStats struct {
		prompt     float64
		completion float64
		cost       float64
		count      int64
	}
	byDay := make(map[string]*dayStats)
	for _, s := range sessions {
		if s.ParentSessionID.Valid {
			continue
		}
		day := time.Unix(s.CreatedAt, 0).UTC().Format("2006-01-02")
		if byDay[day] == nil {
			byDay[day] = &dayStats{}
		}
		byDay[day].prompt += float64(s.PromptTokens)
		byDay[day].completion += float64(s.CompletionTokens)
		byDay[day].cost += s.Cost
		byDay[day].count++
	}

	rows := make([]GetUsageByDayRow, 0, len(byDay))
	for day, stats := range byDay {
		rows = append(rows, GetUsageByDayRow{
			Day:              day,
			PromptTokens:     sql.NullFloat64{Float64: stats.prompt, Valid: true},
			CompletionTokens: sql.NullFloat64{Float64: stats.completion, Valid: true},
			Cost:             sql.NullFloat64{Float64: stats.cost, Valid: true},
			SessionCount:     stats.count,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Day.(string) > rows[j].Day.(string)
	})
	return rows, nil
}

// GetUsageByDayOfWeek returns session counts grouped by day of week.
func (q *FSQuerier) GetUsageByDayOfWeek(ctx context.Context) ([]GetUsageByDayOfWeekRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return nil, err
	}

	type dowStats struct {
		count      int64
		prompt     float64
		completion float64
	}
	byDow := make(map[int64]*dowStats)
	for _, s := range sessions {
		if s.ParentSessionID.Valid {
			continue
		}
		dow := int64(time.Unix(s.CreatedAt, 0).Weekday())
		if byDow[dow] == nil {
			byDow[dow] = &dowStats{}
		}
		byDow[dow].count++
		byDow[dow].prompt += float64(s.PromptTokens)
		byDow[dow].completion += float64(s.CompletionTokens)
	}

	rows := make([]GetUsageByDayOfWeekRow, 0, len(byDow))
	for dow, stats := range byDow {
		rows = append(rows, GetUsageByDayOfWeekRow{
			DayOfWeek:        dow,
			SessionCount:     stats.count,
			PromptTokens:     sql.NullFloat64{Float64: stats.prompt, Valid: true},
			CompletionTokens: sql.NullFloat64{Float64: stats.completion, Valid: true},
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].DayOfWeek < rows[j].DayOfWeek
	})
	return rows, nil
}

// GetUsageByHour returns session counts grouped by hour of day.
func (q *FSQuerier) GetUsageByHour(ctx context.Context) ([]GetUsageByHourRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	sessions, err := q.listAllSessionsNoLock()
	if err != nil {
		return nil, err
	}

	counts := make(map[int64]int64)
	for _, s := range sessions {
		if s.ParentSessionID.Valid {
			continue
		}
		hour := int64(time.Unix(s.CreatedAt, 0).Hour())
		counts[hour]++
	}

	rows := make([]GetUsageByHourRow, 0, len(counts))
	for hour, cnt := range counts {
		rows = append(rows, GetUsageByHourRow{Hour: hour, SessionCount: cnt})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Hour < rows[j].Hour
	})
	return rows, nil
}

// GetUsageByModel returns message counts grouped by model and provider.
func (q *FSQuerier) GetUsageByModel(ctx context.Context) ([]GetUsageByModelRow, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	msgs, err := q.listAllMessagesNoLock()
	if err != nil {
		return nil, err
	}

	type key struct{ model, provider string }
	counts := make(map[key]int64)
	for _, m := range msgs {
		if m.Role != "assistant" {
			continue
		}
		model := m.Model.String
		if model == "" {
			model = "unknown"
		}
		provider := m.Provider.String
		if provider == "" {
			provider = "unknown"
		}
		counts[key{model, provider}]++
	}

	rows := make([]GetUsageByModelRow, 0, len(counts))
	for k, cnt := range counts {
		rows = append(rows, GetUsageByModelRow{
			Model:        k.model,
			Provider:     k.provider,
			MessageCount: cnt,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].MessageCount > rows[j].MessageCount
	})
	return rows, nil
}
