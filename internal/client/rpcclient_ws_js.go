//go:build js

package client

import (
	"context"
	"net/http"

	"github.com/charmbracelet/crush/internal/server"
	"github.com/coder/websocket"
)

func dialWebSocket(ctx context.Context, wsURL string, hc *http.Client) (server.MsgConn, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{})
	if err != nil {
		return nil, err
	}
	return server.NewWSConn(conn), nil
}
