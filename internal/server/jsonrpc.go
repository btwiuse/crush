package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
)

// JSON-RPC 2.0 error codes.
const (
	jrpcParseError     = -32700
	jrpcMethodNotFound = -32601
	jrpcInvalidParams  = -32602
	jrpcInternalError  = -32603
)

// jrpcError represents a JSON-RPC 2.0 error object.
type jrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *jrpcError) Error() string { return e.Message }

// jrpcMessage is a generic JSON-RPC 2.0 message used for both
// requests/responses and notifications.
type jrpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"` // nil for notifications
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jrpcError      `json:"error,omitempty"`
}

// jrpcHandler processes a JSON-RPC method call and returns the result or
// an error. Returning a nil result with no error means the method succeeded
// with no response body.
type jrpcHandler func(ctx context.Context, params json.RawMessage) (any, error)

// jrpcRouter routes JSON-RPC method names to handlers.
type jrpcRouter map[string]jrpcHandler

func (r jrpcRouter) Handle(method string, h jrpcHandler) {
	r[method] = h
}

// jrpcConn manages a single WebSocket connection with JSON-RPC protocol.
// It provides thread-safe writes and manages event subscriptions.
type jrpcConn struct {
	conn   MsgConn
	wmu    sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	// incremented for each request sent from the server side
	// (currently unused; server-initiated requests are not yet needed).
	seqID atomic.Int64

	done chan struct{}
}

// serveJRPC handles the WebSocket connection's read loop. It reads
// JSON-RPC 2.0 messages, dispatches them to registered handlers, and
// writes responses back.
func serveJRPC(ctx context.Context, conn MsgConn, router jrpcRouter) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jc := &jrpcConn{
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	defer close(jc.done)

	slog.Debug("WebSocket connection established")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := conn.ReadMsg()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Debug("WebSocket read error", "error", err)
			return
		}

		var msg jrpcMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Debug("Failed to parse JSON-RPC message", "error", err)
			return
		}

		// Parse error: invalid JSON
		if msg.JSONRPC != "2.0" && msg.ID != nil {
			jc.writeError(*msg.ID, jrpcParseError, "Parse error")
			continue
		}

		// Notification: JSON-RPC request without an ID (client-to-server
		// notification, currently not used but handled gracefully).
		if msg.ID == nil {
			continue
		}

		id := *msg.ID

		// Find and invoke the handler.
		handler, ok := router[msg.Method]
		if !ok {
			jc.writeError(id, jrpcMethodNotFound, fmt.Sprintf("Method not found: %s", msg.Method))
			continue
		}

		slog.Info("JSON-RPC request", "method", msg.Method)

		result, err := handler(ctx, msg.Params)
		if err != nil {
			var jErr *jrpcError
			if errors.As(err, &jErr) {
				jc.writeError(id, jErr.Code, jErr.Message)
			} else {
				jc.writeError(id, jrpcInternalError, err.Error())
			}
			continue
		}

		jc.writeResult(id, result)
	}
}

func (jc *jrpcConn) writeResult(id int64, result any) {
	var raw json.RawMessage
	if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			slog.Error("Failed to marshal JSON-RPC result", "error", err)
			jc.writeError(id, jrpcInternalError, "Internal error")
			return
		}
		raw = b
	}

	msg := jrpcMessage{
		JSONRPC: "2.0",
		ID:      &id,
		Result:  raw,
	}

	jc.wmu.Lock()
	defer jc.wmu.Unlock()
	if err := jc.conn.WriteMsg(MustMarshal(msg)); err != nil {
		slog.Debug("Failed to write JSON-RPC response", "error", err)
	}
}

func (jc *jrpcConn) writeError(id int64, code int, message string) {
	msg := jrpcMessage{
		JSONRPC: "2.0",
		ID:      &id,
		Error:   &jrpcError{Code: code, Message: message},
	}

	jc.wmu.Lock()
	defer jc.wmu.Unlock()
	if err := jc.conn.WriteMsg(MustMarshal(msg)); err != nil {
		slog.Debug("Failed to write JSON-RPC error", "error", err)
	}
}

// notify sends a server-to-client JSON-RPC notification (no ID).
func (jc *jrpcConn) notify(method string, params any) {
	raw, err := json.Marshal(params)
	if err != nil {
		slog.Error("Failed to marshal notification params", "error", err)
		return
	}

	msg := jrpcMessage{
		JSONRPC: "2.0",
		Method:  method,
		Params:  raw,
	}

	jc.wmu.Lock()
	defer jc.wmu.Unlock()
	if err := jc.conn.WriteMsg(MustMarshal(msg)); err != nil {
		slog.Debug("Failed to write JSON-RPC notification", "error", err)
	}
}

// handleWebSocket is the HTTP handler that upgrades to WebSocket and
// starts the JSON-RPC read loop.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	conn := NewWSConn(wsConn)
	defer conn.Close()

	ctx := context.WithValue(r.Context(), jrpcConnKey, &jrpcConn{
		conn:   conn,
		ctx:    r.Context(),
		cancel: func() {},
		done:   make(chan struct{}),
	})
	serveJRPC(ctx, conn, s.jrpc)
}
