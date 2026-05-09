package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/crush/internal/client"
	jrpc "github.com/charmbracelet/crush/internal/jsonrpc"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// echoServer returns a test HTTP server that upgrades to WebSocket and
// echoes back JSON-RPC responses.
func echoServer(t *testing.T, handler func(*websocket.Conn, jrpc.Request)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req jrpc.Request
			require.NoError(t, json.Unmarshal(msg, &req))
			handler(conn, req)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// tcpWSClient creates a WSClient connected to a test TCP server.
func tcpWSClient(t *testing.T, srv *httptest.Server) *client.WSClient {
	t.Helper()
	addr := strings.TrimPrefix(srv.URL, "http://")
	c, err := client.NewWSClient("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestWSClient_Call_Success(t *testing.T) {
	t.Parallel()

	srv := echoServer(t, func(conn *websocket.Conn, req jrpc.Request) {
		resp, _ := jrpc.OKResponse(req.ID, "pong")
		_ = conn.WriteJSON(resp)
	})

	c := tcpWSClient(t, srv)
	var result string
	err := c.Call(context.Background(), "ping", nil, &result)
	require.NoError(t, err)
	require.Equal(t, "pong", result)
}

func TestWSClient_Call_Error(t *testing.T) {
	t.Parallel()

	srv := echoServer(t, func(conn *websocket.Conn, req jrpc.Request) {
		resp := jrpc.ErrResponse(req.ID, jrpc.CodeMethodNotFound, "method not found: ping")
		_ = conn.WriteJSON(resp)
	})

	c := tcpWSClient(t, srv)
	err := c.Call(context.Background(), "ping", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "method not found")
}

func TestWSClient_Call_Context_Cancelled(t *testing.T) {
	t.Parallel()

	// Server never responds.
	srv := echoServer(t, func(_ *websocket.Conn, _ jrpc.Request) {})

	c := tcpWSClient(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := c.Call(ctx, "slow", nil, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWSClient_Subscribe_Notification(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()

		// Consume the subscribe call and reply.
		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		var req jrpc.Request
		require.NoError(t, json.Unmarshal(msg, &req))
		resp, _ := jrpc.OKResponse(req.ID, nil)
		_ = conn.WriteJSON(resp)

		// Push a notification.
		notif, _ := jrpc.NewNotification(jrpc.MethodEventPush, map[string]interface{}{
			"type":    "agent_event",
			"payload": json.RawMessage(`{}`),
		})
		_ = conn.WriteJSON(notif)

		// Keep connection open.
		time.Sleep(500 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)

	addr := strings.TrimPrefix(srv.URL, "http://")
	c, err := client.NewWSClient("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	events, err := c.SubscribeEvents(ctx, "ws1")
	require.NoError(t, err)

	select {
	case ev := <-events:
		require.NotNil(t, ev)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event notification")
	}
}

func TestWSClient_Concurrent_Calls(t *testing.T) {
	t.Parallel()

	srv := echoServer(t, func(conn *websocket.Conn, req jrpc.Request) {
		// Decode the expected number and echo it back as result.
		resp, _ := jrpc.OKResponse(req.ID, req.Params)
		_ = conn.WriteJSON(resp)
	})

	c := tcpWSClient(t, srv)

	const n = 10
	errc := make(chan error, n)
	for i := range n {
		go func(i int) {
			var result json.RawMessage
			err := c.Call(context.Background(), "echo", i, &result)
			errc <- err
		}(i)
	}

	for range n {
		require.NoError(t, <-errc)
	}
}
