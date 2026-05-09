package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	jrpc "github.com/charmbracelet/crush/internal/jsonrpc"
	"github.com/charmbracelet/crush/internal/proto"
	"github.com/charmbracelet/crush/internal/pubsub"
	"github.com/gorilla/websocket"
)

// pending holds state for an in-flight JSON-RPC call.
type pending struct {
	ch  chan *jrpc.Response
	err error
}

// WSClient maintains a single WebSocket connection to a Crush server
// and multiplexes all JSON-RPC 2.0 requests over it. It also receives
// server-push notifications (events) and dispatches them to subscribers.
type WSClient struct {
	network string
	addr    string

	mu     sync.Mutex
	conn   *websocket.Conn
	closed bool

	// In-flight requests keyed by ID string.
	inflightMu sync.Mutex
	inflight   map[string]*pending

	nextID atomic.Int64

	// eventSubs receive push-notification payloads.
	subsMu sync.RWMutex
	subs   []chan any
}

// NewWSClient creates a new WebSocket client connecting to the given
// network/address using the same dialing conventions as [Client].
func NewWSClient(network, addr string) (*WSClient, error) {
	c := &WSClient{
		network:  network,
		addr:     addr,
		inflight: make(map[string]*pending),
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	go c.readLoop()
	return c, nil
}

// wsURL builds the WebSocket URL for the server.
func (c *WSClient) wsURL() string {
	host := c.addr
	if c.network == "unix" || c.network == "npipe" {
		host = DummyHost
	}
	return fmt.Sprintf("ws://%s/v1/ws", host)
}

// dial returns a net.Conn using the same logic as the HTTP client dialer.
func (c *WSClient) dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	switch c.network {
	case "npipe":
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return dialPipeContext(ctx2, c.addr)
	case "unix":
		return d.DialContext(ctx, "unix", c.addr)
	default:
		return d.DialContext(ctx, "tcp", c.addr)
	}
}

// connect establishes the WebSocket connection.
func (c *WSClient) connect() error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 30 * time.Second,
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return c.dial(ctx)
		},
	}

	reqHeader := http.Header{}
	if c.network == "unix" || c.network == "npipe" {
		reqHeader.Set("Host", DummyHost)
	}

	conn, _, err := dialer.Dial(c.wsURL(), reqHeader)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

// readLoop reads messages from the WebSocket and routes them.
func (c *WSClient) readLoop() {
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn == nil {
			return
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Debug("WSClient read error", "error", err)
			}
			// Fail all in-flight requests.
			c.failAll(err)
			return
		}

		// Peek to distinguish response (has "id") from notification (no "id").
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(msg, &raw); err != nil {
			slog.Warn("WSClient: failed to parse message", "error", err)
			continue
		}

		idRaw, hasID := raw["id"]
		methodRaw := raw["method"]

		if hasID && string(idRaw) != "null" {
			// It's a response.
			var resp jrpc.Response
			if err := json.Unmarshal(msg, &resp); err != nil {
				slog.Warn("WSClient: failed to parse response", "error", err)
				continue
			}
			c.deliverResponse(&resp)
		} else if methodRaw != nil {
			// It's a notification.
			var notif jrpc.Notification
			if err := json.Unmarshal(msg, &notif); err != nil {
				slog.Warn("WSClient: failed to parse notification", "error", err)
				continue
			}
			c.dispatchNotification(&notif)
		}
	}
}

// deliverResponse routes a JSON-RPC response to the waiting Call.
func (c *WSClient) deliverResponse(resp *jrpc.Response) {
	key := resp.ID.String()
	c.inflightMu.Lock()
	p, ok := c.inflight[key]
	if ok {
		delete(c.inflight, key)
	}
	c.inflightMu.Unlock()
	if ok {
		p.ch <- resp
	}
}

// failAll fails all pending in-flight calls with the given error.
func (c *WSClient) failAll(err error) {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()
	for key, p := range c.inflight {
		p.err = err
		close(p.ch)
		delete(c.inflight, key)
	}
}

// dispatchNotification fans out a server-push notification to all
// registered event subscribers.
func (c *WSClient) dispatchNotification(notif *jrpc.Notification) {
	if notif.Method != jrpc.MethodEventPush {
		return
	}
	var payload pubsub.Payload
	if err := json.Unmarshal(notif.Params, &payload); err != nil {
		slog.Warn("WSClient: failed to parse event notification", "error", err)
		return
	}
	ev := decodeEventPayload(payload)
	if ev == nil {
		return
	}
	c.subsMu.RLock()
	defer c.subsMu.RUnlock()
	for _, ch := range c.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// decodeEventPayload converts a pubsub.Payload into the concrete typed
// event value expected by [workspace.ClientWorkspace].
func decodeEventPayload(p pubsub.Payload) any {
	switch p.Type {
	case pubsub.PayloadTypeLSPEvent:
		var e pubsub.Event[proto.LSPEvent]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypeMCPEvent:
		var e pubsub.Event[proto.MCPEvent]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypePermissionRequest:
		var e pubsub.Event[proto.PermissionRequest]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypePermissionNotification:
		var e pubsub.Event[proto.PermissionNotification]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypeMessage:
		var e pubsub.Event[proto.Message]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypeSession:
		var e pubsub.Event[proto.Session]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypeFile:
		var e pubsub.Event[proto.File]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	case pubsub.PayloadTypeAgentEvent:
		var e pubsub.Event[proto.AgentEvent]
		_ = json.Unmarshal(p.Payload, &e)
		return e
	}
	return nil
}

// Call performs a synchronous JSON-RPC call and decodes the result
// into dst. If dst is nil the result is discarded.
func (c *WSClient) Call(ctx context.Context, method string, params, dst any) error {
	id := jrpc.NumberID(c.nextID.Add(1))
	req, err := jrpc.NewRequest(id, method, params)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	ch := make(chan *jrpc.Response, 1)
	key := id.String()
	c.inflightMu.Lock()
	c.inflight[key] = &pending{ch: ch}
	c.inflightMu.Unlock()

	// Send.
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		c.inflightMu.Lock()
		delete(c.inflight, key)
		c.inflightMu.Unlock()
		return fmt.Errorf("websocket: not connected")
	}
	c.mu.Lock()
	err = c.conn.WriteJSON(req)
	c.mu.Unlock()
	if err != nil {
		c.inflightMu.Lock()
		delete(c.inflight, key)
		c.inflightMu.Unlock()
		return fmt.Errorf("websocket write: %w", err)
	}

	// Wait.
	select {
	case <-ctx.Done():
		c.inflightMu.Lock()
		delete(c.inflight, key)
		c.inflightMu.Unlock()
		return ctx.Err()
	case resp, ok := <-ch:
		if !ok {
			return fmt.Errorf("websocket: connection closed")
		}
		if resp.Error != nil {
			return fmt.Errorf("rpc %s: %s (code %d)", method, resp.Error.Message, resp.Error.Code)
		}
		if dst != nil {
			if err := json.Unmarshal(resp.Result, dst); err != nil {
				return fmt.Errorf("decode result: %w", err)
			}
		}
		return nil
	}
}

// Subscribe registers an event channel and returns it along with a
// cancel function to unregister it.
func (c *WSClient) Subscribe() (<-chan any, func()) {
	ch := make(chan any, 100)
	c.subsMu.Lock()
	c.subs = append(c.subs, ch)
	c.subsMu.Unlock()
	cancel := func() {
		c.subsMu.Lock()
		defer c.subsMu.Unlock()
		for i, s := range c.subs {
			if s == ch {
				c.subs = append(c.subs[:i], c.subs[i+1:]...)
				break
			}
		}
		close(ch)
	}
	return ch, cancel
}

// SubscribeEvents subscribes to server-push events for a workspace
// and returns a channel that receives the typed events. This call
// sends the events.subscribe JSON-RPC request.
func (c *WSClient) SubscribeEvents(ctx context.Context, workspaceID string) (<-chan any, error) {
	events := make(chan any, 100)
	raw, cancel := c.Subscribe()

	// Ask the server to start sending events for this workspace.
	if err := c.Call(ctx, jrpc.MethodEventsSubscribe, map[string]string{"id": workspaceID}, nil); err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer close(events)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-raw:
				if !ok {
					return
				}
				select {
				case events <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return events, nil
}

// Close closes the underlying WebSocket connection.
func (c *WSClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil && !c.closed {
		c.closed = true
		err := c.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		_ = err
		return c.conn.Close()
	}
	return nil
}

// wsURL constructs the WebSocket URL for the given network/address.
// Exported so tests can reuse it.
func WSEndpoint(network, addr string) *url.URL {
	host := addr
	if network == "unix" || network == "npipe" {
		host = DummyHost
	}
	return &url.URL{Scheme: "ws", Host: host, Path: "/v1/ws"}
}
