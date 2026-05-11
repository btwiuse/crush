package server

import (
	"context"
	"encoding/json"

	"github.com/coder/websocket"
)

// MsgConn is a message-oriented connection. Each ReadMsg call returns a single
// complete message, unlike net.Conn which operates on byte streams.
type MsgConn interface {
	ReadMsg() ([]byte, error)
	WriteMsg([]byte) error
	Close() error
}

// wsConn adapts a *websocket.Conn to the MsgConn interface.
type wsConn struct {
	conn *websocket.Conn
}

func (w *wsConn) ReadMsg() ([]byte, error) {
	_, msg, err := w.conn.Read(context.Background())
	return msg, err
}

func (w *wsConn) WriteMsg(msg []byte) error {
	return w.conn.Write(context.Background(), websocket.MessageText, msg)
}

func (w *wsConn) Close() error {
	return w.conn.CloseNow()
}

// NewWSConn wraps a WebSocket connection as a MsgConn and disables the
// read limit so messages of any size are accepted.
func NewWSConn(conn *websocket.Conn) MsgConn {
	conn.SetReadLimit(-1)
	return &wsConn{conn: conn}
}

// MustMarshal panics if json.Marshal fails. Use for values that should never
// fail to marshal, such as well-known structs with only primitive fields.
func MustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
