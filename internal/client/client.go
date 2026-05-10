package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/proto"
	"github.com/charmbracelet/crush/internal/server"
	"github.com/coder/websocket"
)

// DummyHost is used to satisfy the HTTP client's Host requirement.
const DummyHost = "api.crush.localhost"

// jrpcRequest is a JSON-RPC 2.0 request.
type jrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// jrpcResponse is a JSON-RPC 2.0 response or notification.
type jrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"` // nil for notifications
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"` // notifications only
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

// Client represents a JSON-RPC client connected to a Crush server.
type Client struct {
	conn    *websocket.Conn
	wmu     sync.Mutex
	path    string
	network string
	addr    string

	seqID   atomic.Int64
	mu      sync.RWMutex
	pending map[int64]chan<- *jrpcResponse

	// eventCh receives event notifications as json.RawMessage (pubsub.Payload).
	eventCh chan any

	closeOnce sync.Once
	closed    chan struct{}
}

// DefaultClient creates a new [Client] connected to the default server address.
func DefaultClient(path string) (*Client, error) {
	host, err := server.ParseHostURL(server.DefaultHost())
	if err != nil {
		return nil, err
	}
	return NewClient(path, host.Scheme, host.Host)
}

// NewClient creates a new [Client] connected to the server at the given
// network and address.
func NewClient(path, network, address string) (*Client, error) {
	c := &Client{
		path:    path,
		network: network,
		addr:    ensurePort(address, network),
		pending: make(map[int64]chan<- *jrpcResponse),
		eventCh: make(chan any, 100),
		closed:  make(chan struct{}),
	}
	if err := c.dial(); err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	return c, nil
}

// ensurePort adds a default port to address when missing, based on the
// network scheme. Unix and named-pipe addresses are returned as-is.
func ensurePort(address, network string) string {
	switch network {
	case "unix", "npipe":
		return address
	}
	// No port → add default.
	if _, _, err := net.SplitHostPort(address); err != nil {
		switch network {
		case "https":
			return net.JoinHostPort(address, "443")
		default:
			return net.JoinHostPort(address, "80")
		}
	}
	return address
}

// Path returns the client's workspace filesystem path.
func (c *Client) Path() string {
	return c.path
}

// dial establishes the WebSocket connection.
func (c *Client) dial() error {
	var wsURL string
	var hc *http.Client

	switch c.network {
	case "unix", "npipe":
		tr := &http.Transport{DialContext: c.dialer}
		tr.DisableCompression = true
		hc = &http.Client{Transport: tr}
		wsURL = "ws://" + DummyHost + "/v1/rpc"
	case "https":
		tr := &http.Transport{DialContext: c.dialer}
		hc = &http.Client{Transport: tr}
		wsURL = "wss://" + c.addr + "/v1/rpc"
	default:
		// http, tcp, or any other TCP-based scheme
		tr := &http.Transport{DialContext: c.dialer}
		hc = &http.Client{Transport: tr}
		wsURL = "ws://" + c.addr + "/v1/rpc"
	}

	conn, err := dialWebSocket(context.Background(), wsURL, hc)
	if err != nil {
		return err
	}

	c.conn = conn
	go c.readLoop()
	return nil
}

func (c *Client) dialer(ctx context.Context, network, address string) (net.Conn, error) {
	d := net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	switch c.network {
	case "npipe":
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return dialPipeContext(ctx, c.addr)
	case "unix":
		return d.DialContext(ctx, "unix", c.addr)
	default:
		// http, https, tcp — dial using the stored address
		return d.DialContext(ctx, "tcp", c.addr)
	}
}

// readLoop reads messages from the WebSocket and dispatches them as
// responses (matched to pending requests) or event notifications.
func (c *Client) readLoop() {
	defer func() {
		c.closeOnce.Do(func() {
			close(c.closed)
		})
	}()

	for {
		_, msg, err := c.conn.Read(context.Background())
		if err != nil {
			var wsErr websocket.CloseError
			if errors.As(err, &wsErr) {
				slog.Debug("WebSocket closed", "code", wsErr.Code, "reason", wsErr.Reason)
			} else {
				slog.Debug("WebSocket read error", "error", err)
			}
			// Cancel all pending requests.
			c.mu.Lock()
			for id, ch := range c.pending {
				close(ch)
				delete(c.pending, id)
			}
			c.mu.Unlock()
			return
		}

		var resp jrpcResponse
		if err := json.Unmarshal(msg, &resp); err != nil {
			slog.Debug("Failed to parse JSON-RPC message", "error", err)
			continue
		}

		if resp.ID != nil {
			// Response to a pending request.
			c.mu.RLock()
			ch, ok := c.pending[*resp.ID]
			c.mu.RUnlock()
			if ok {
				select {
				case ch <- &resp:
				default:
				}
			}
		} else if resp.Method == "event" {
			// Event notification.
			select {
			case c.eventCh <- resp.Params:
			default:
				slog.Debug("Dropping event: channel full")
			}
		}
	}
}

// call sends a JSON-RPC request and waits for the response.
func (c *Client) call(ctx context.Context, method string, params, result any) error {
	var rawParams json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("failed to marshal params: %w", err)
		}
		rawParams = b
	}

	id := c.seqID.Add(1)

	ch := make(chan *jrpcResponse, 1)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	req := jrpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  rawParams,
	}

	c.wmu.Lock()
	err := c.conn.Write(ctx, websocket.MessageText, mustMarshal(req))
	c.wmu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	select {
	case resp := <-ch:
		if resp == nil {
			return errors.New("connection closed")
		}
		if resp.Error != nil {
			var jrpcErr struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(resp.Error, &jrpcErr); err != nil {
				return fmt.Errorf("JSON-RPC error (code: %d)", jrpcErr.Code)
			}
			return fmt.Errorf("JSON-RPC error: %s", jrpcErr.Message)
		}
		if result != nil && len(resp.Result) > 0 {
			if err := json.Unmarshal(resp.Result, result); err != nil {
				return fmt.Errorf("failed to decode result: %w", err)
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("connection closed")
	}
}

// GetGlobalConfig retrieves the server's configuration.
func (c *Client) GetGlobalConfig(ctx context.Context) (*config.Config, error) {
	var cfg config.Config
	if err := c.call(ctx, "system.config", nil, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Health checks the server's health status.
func (c *Client) Health(ctx context.Context) error {
	return c.call(ctx, "system.health", nil, nil)
}

// VersionInfo retrieves the server's version information.
func (c *Client) VersionInfo(ctx context.Context) (*proto.VersionInfo, error) {
	var vi proto.VersionInfo
	if err := c.call(ctx, "system.version", nil, &vi); err != nil {
		return nil, err
	}
	return &vi, nil
}

// ShutdownServer sends a shutdown request to the server.
func (c *Client) ShutdownServer(ctx context.Context) error {
	return c.call(ctx, "system.control", proto.ServerControl{Command: "shutdown"}, nil)
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	if c.conn != nil {
		return c.conn.CloseNow()
	}
	return nil
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
