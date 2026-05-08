package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestEnsureServerTCPHealthy(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	parsed, err := url.Parse(srv.URL)
	require.NoError(t, err)

	hostURL := &url.URL{
		Scheme: "tcp",
		Host:   parsed.Host,
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	err = ensureServer(cmd, hostURL)
	require.NoError(t, err)
}

func TestEnsureServerTCPUnreachable(t *testing.T) {
	t.Parallel()

	hostURL := &url.URL{
		Scheme: "tcp",
		Host:   "127.0.0.1:1",
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	err := ensureServer(cmd, hostURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to connect to remote crush server")
}
