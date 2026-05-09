package jsonrpc_test

import (
	"encoding/json"
	"testing"

	"github.com/charmbracelet/crush/internal/jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestIDMarshal(t *testing.T) {
	t.Parallel()

	t.Run("string", func(t *testing.T) {
		t.Parallel()
		id := jsonrpc.StringID("abc")
		b, err := json.Marshal(id)
		require.NoError(t, err)
		require.Equal(t, `"abc"`, string(b))
		require.Equal(t, "abc", id.String())
	})

	t.Run("number", func(t *testing.T) {
		t.Parallel()
		id := jsonrpc.NumberID(42)
		b, err := json.Marshal(id)
		require.NoError(t, err)
		require.Equal(t, "42", string(b))
		require.Equal(t, "42", id.String())
	})

	t.Run("zero", func(t *testing.T) {
		t.Parallel()
		var id jsonrpc.ID
		b, err := json.Marshal(id)
		require.NoError(t, err)
		require.Equal(t, "null", string(b))
		require.True(t, id.IsZero())
	})
}

func TestIDUnmarshal(t *testing.T) {
	t.Parallel()

	t.Run("string", func(t *testing.T) {
		t.Parallel()
		var id jsonrpc.ID
		require.NoError(t, json.Unmarshal([]byte(`"hello"`), &id))
		require.Equal(t, "hello", id.String())
		require.False(t, id.IsZero())
	})

	t.Run("number", func(t *testing.T) {
		t.Parallel()
		var id jsonrpc.ID
		require.NoError(t, json.Unmarshal([]byte("7"), &id))
		require.Equal(t, "7", id.String())
		require.False(t, id.IsZero())
	})

	t.Run("null", func(t *testing.T) {
		t.Parallel()
		var id jsonrpc.ID
		require.NoError(t, json.Unmarshal([]byte("null"), &id))
		require.True(t, id.IsZero())
	})
}

func TestNewRequest(t *testing.T) {
	t.Parallel()

	id := jsonrpc.NumberID(1)
	req, err := jsonrpc.NewRequest(id, jsonrpc.MethodAgentInfo, map[string]string{"id": "ws1"})
	require.NoError(t, err)
	require.Equal(t, jsonrpc.Version, req.JSONRPC)
	require.Equal(t, jsonrpc.MethodAgentInfo, req.Method)
	require.False(t, req.IsNotification())

	// Verify params round-trips.
	var params map[string]string
	require.NoError(t, json.Unmarshal(req.Params, &params))
	require.Equal(t, "ws1", params["id"])
}

func TestNewNotification(t *testing.T) {
	t.Parallel()

	notif, err := jsonrpc.NewNotification(jsonrpc.MethodEventPush, map[string]string{"type": "lsp_event"})
	require.NoError(t, err)
	require.Equal(t, jsonrpc.Version, notif.JSONRPC)
	require.Equal(t, jsonrpc.MethodEventPush, notif.Method)
}

func TestOKResponse(t *testing.T) {
	t.Parallel()

	id := jsonrpc.StringID("req-1")
	result := map[string]bool{"is_ready": true}
	resp, err := jsonrpc.OKResponse(id, result)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	var decoded map[string]bool
	require.NoError(t, json.Unmarshal(resp.Result, &decoded))
	require.True(t, decoded["is_ready"])
}

func TestErrResponse(t *testing.T) {
	t.Parallel()

	id := jsonrpc.NumberID(99)
	resp := jsonrpc.ErrResponse(id, jsonrpc.CodeMethodNotFound, "method not found: foo.bar")
	require.NotNil(t, resp.Error)
	require.Equal(t, jsonrpc.CodeMethodNotFound, resp.Error.Code)
	require.Contains(t, resp.Error.Message, "foo.bar")
	require.Nil(t, resp.Result)
}

func TestRequestIsNotification(t *testing.T) {
	t.Parallel()

	notification, err := jsonrpc.NewRequest(jsonrpc.ID{}, jsonrpc.MethodEventsSubscribe, nil)
	require.NoError(t, err)
	require.True(t, notification.IsNotification())

	req, err := jsonrpc.NewRequest(jsonrpc.NumberID(1), jsonrpc.MethodEventsSubscribe, nil)
	require.NoError(t, err)
	require.False(t, req.IsNotification())
}

func TestErrorImplementsError(t *testing.T) {
	t.Parallel()

	e := &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "boom"}
	require.Equal(t, "boom", e.Error())
}

func TestRequestRoundTrip(t *testing.T) {
	t.Parallel()

	orig, err := jsonrpc.NewRequest(jsonrpc.StringID("x"), jsonrpc.MethodWorkspaceList, nil)
	require.NoError(t, err)

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var decoded jsonrpc.Request
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, orig.JSONRPC, decoded.JSONRPC)
	require.Equal(t, orig.Method, decoded.Method)
	require.Equal(t, orig.ID.String(), decoded.ID.String())
}

func TestResponseRoundTrip(t *testing.T) {
	t.Parallel()

	orig, err := jsonrpc.OKResponse(jsonrpc.NumberID(5), "hello")
	require.NoError(t, err)

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var decoded jsonrpc.Response
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Nil(t, decoded.Error)
	require.Equal(t, orig.ID.String(), decoded.ID.String())

	var result string
	require.NoError(t, json.Unmarshal(decoded.Result, &result))
	require.Equal(t, "hello", result)
}
