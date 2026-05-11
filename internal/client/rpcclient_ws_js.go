//go:build js

package client

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

func dialWebSocket(ctx context.Context, wsURL string, hc *http.Client) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{})
	return conn, err
}
