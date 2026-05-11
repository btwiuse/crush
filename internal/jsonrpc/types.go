// Package jsonrpc implements a minimal JSON-RPC 2.0 protocol layer used
// by the Crush WebSocket transport.
package jsonrpc

import (
	"bytes"
	"encoding/json"
)

// Version is the JSON-RPC protocol version string.
const Version = "2.0"

// ID is a JSON-RPC request/response identifier. It can be a string,
// number, or null (for notifications).
type ID struct {
	str   string
	num   int64
	isStr bool
	isNum bool
}

// StringID creates an ID from a string.
func StringID(s string) ID { return ID{str: s, isStr: true} }

// NumberID creates an ID from an integer.
func NumberID(n int64) ID { return ID{num: n, isNum: true} }

// MarshalJSON implements the json.Marshaler interface.
func (id ID) MarshalJSON() ([]byte, error) {
	switch {
	case id.isStr:
		return json.Marshal(id.str)
	case id.isNum:
		return json.Marshal(id.num)
	default:
		return []byte("null"), nil
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (id *ID) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	// Try number first.
	var num int64
	if err := json.Unmarshal(data, &num); err == nil {
		id.num = num
		id.isNum = true
		return nil
	}
	// Fall back to string.
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	id.str = str
	id.isStr = true
	return nil
}

// String returns a string representation of the ID (for use as a map
// key).
func (id ID) String() string {
	if id.isStr {
		return id.str
	}
	if id.isNum {
		b, _ := json.Marshal(id.num)
		return string(b)
	}
	return ""
}

// IsZero reports whether the ID is the zero value (null).
func (id ID) IsZero() bool { return !id.isStr && !id.isNum }

// Request is a JSON-RPC 2.0 request message. When ID is zero, it is a
// notification (no response expected).
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      ID              `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the request is a notification (no ID).
func (r *Request) IsNotification() bool { return r.ID.IsZero() }

// Response is a JSON-RPC 2.0 response message.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      ID              `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Notification is a JSON-RPC 2.0 notification sent by the server to
// push events to the client without a corresponding request.
type Notification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *Error) Error() string { return e.Message }

// Standard JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// NewRequest creates a new JSON-RPC 2.0 request with the given ID,
// method, and parameters.
func NewRequest(id ID, method string, params any) (*Request, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	return &Request{
		JSONRPC: Version,
		ID:      id,
		Method:  method,
		Params:  raw,
	}, nil
}

// NewNotification creates a new JSON-RPC 2.0 notification.
func NewNotification(method string, params any) (*Notification, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	return &Notification{
		JSONRPC: Version,
		Method:  method,
		Params:  raw,
	}, nil
}

// OKResponse creates a success response.
func OKResponse(id ID, result any) (*Response, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &Response{
		JSONRPC: Version,
		ID:      id,
		Result:  raw,
	}, nil
}

// ErrResponse creates an error response.
func ErrResponse(id ID, code int, message string) *Response {
	return &Response{
		JSONRPC: Version,
		ID:      id,
		Error:   &Error{Code: code, Message: message},
	}
}
