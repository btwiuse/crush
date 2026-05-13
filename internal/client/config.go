package client

import (
	"context"
	"fmt"

	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/oauth"
	"github.com/charmbracelet/crush/internal/proto"
)

// SetConfigField sets a config key/value pair on the server.
func (c *Client) SetConfigField(ctx context.Context, id string, scope config.Scope, key string, value any) error {
	params := struct {
		ID    string       `json:"id"`
		Scope config.Scope `json:"scope"`
		Key   string       `json:"key"`
		Value any          `json:"value"`
	}{ID: id, Scope: scope, Key: key, Value: value}
	if err := c.call(ctx, "workspace.config.set", params, nil); err != nil {
		return fmt.Errorf("failed to set config field: %w", err)
	}
	return nil
}

// RemoveConfigField removes a config key on the server.
func (c *Client) RemoveConfigField(ctx context.Context, id string, scope config.Scope, key string) error {
	params := struct {
		ID    string       `json:"id"`
		Scope config.Scope `json:"scope"`
		Key   string       `json:"key"`
	}{ID: id, Scope: scope, Key: key}
	if err := c.call(ctx, "workspace.config.remove", params, nil); err != nil {
		return fmt.Errorf("failed to remove config field: %w", err)
	}
	return nil
}

// UpdatePreferredModel updates the preferred model on the server.
func (c *Client) UpdatePreferredModel(ctx context.Context, id string, scope config.Scope, modelType config.SelectedModelType, model config.SelectedModel) error {
	params := struct {
		ID        string                 `json:"id"`
		Scope     config.Scope           `json:"scope"`
		ModelType config.SelectedModelType `json:"model_type"`
		Model     config.SelectedModel   `json:"model"`
	}{ID: id, Scope: scope, ModelType: modelType, Model: model}
	if err := c.call(ctx, "workspace.config.model", params, nil); err != nil {
		return fmt.Errorf("failed to update preferred model: %w", err)
	}
	return nil
}

// SetCompactMode sets compact mode on the server.
func (c *Client) SetCompactMode(ctx context.Context, id string, scope config.Scope, enabled bool) error {
	params := struct {
		ID      string       `json:"id"`
		Scope   config.Scope `json:"scope"`
		Enabled bool         `json:"enabled"`
	}{ID: id, Scope: scope, Enabled: enabled}
	if err := c.call(ctx, "workspace.config.compact", params, nil); err != nil {
		return fmt.Errorf("failed to set compact mode: %w", err)
	}
	return nil
}

// SetProviderAPIKey sets a provider API key on the server.
func (c *Client) SetProviderAPIKey(ctx context.Context, id string, scope config.Scope, providerID string, apiKey any) error {
	params := struct {
		ID         string       `json:"id"`
		Scope      config.Scope `json:"scope"`
		ProviderID string       `json:"provider_id"`
		APIKey     any          `json:"api_key"`
	}{ID: id, Scope: scope, ProviderID: providerID, APIKey: apiKey}
	if err := c.call(ctx, "workspace.config.providerKey", params, nil); err != nil {
		return fmt.Errorf("failed to set provider API key: %w", err)
	}
	return nil
}

// ImportCopilot attempts to import a GitHub Copilot token on the server.
func (c *Client) ImportCopilot(ctx context.Context, id string) (*oauth.Token, bool, error) {
	params := struct {
		ID string `json:"id"`
	}{ID: id}
	var resp proto.ImportCopilotResponse
	if err := c.call(ctx, "workspace.config.importCopilot", params, &resp); err != nil {
		return nil, false, fmt.Errorf("failed to import copilot: %w", err)
	}
	token, _ := resp.Token.(*oauth.Token)
	return token, resp.Success, nil
}

// RefreshOAuthToken refreshes an OAuth token for a provider on the server.
func (c *Client) RefreshOAuthToken(ctx context.Context, id string, scope config.Scope, providerID string) error {
	params := struct {
		ID         string       `json:"id"`
		Scope      config.Scope `json:"scope"`
		ProviderID string       `json:"provider_id"`
	}{ID: id, Scope: scope, ProviderID: providerID}
	if err := c.call(ctx, "workspace.config.refreshOAuth", params, nil); err != nil {
		return fmt.Errorf("failed to refresh OAuth token: %w", err)
	}
	return nil
}

// ProjectNeedsInitialization checks if the project needs initialization.
func (c *Client) ProjectNeedsInitialization(ctx context.Context, id string) (bool, error) {
	var resp proto.ProjectNeedsInitResponse
	params := struct {
		ID string `json:"id"`
	}{ID: id}
	if err := c.call(ctx, "workspace.project.needsInit", params, &resp); err != nil {
		return false, fmt.Errorf("failed to check project init: %w", err)
	}
	return resp.NeedsInit, nil
}

// MarkProjectInitialized marks the project as initialized on the server.
func (c *Client) MarkProjectInitialized(ctx context.Context, id string) error {
	params := struct {
		ID string `json:"id"`
	}{ID: id}
	if err := c.call(ctx, "workspace.project.init", params, nil); err != nil {
		return fmt.Errorf("failed to mark project initialized: %w", err)
	}
	return nil
}

// GetInitializePrompt retrieves the initialization prompt from the server.
func (c *Client) GetInitializePrompt(ctx context.Context, id string) (string, error) {
	params := struct {
		ID string `json:"id"`
	}{ID: id}
	var resp proto.ProjectInitPromptResponse
	if err := c.call(ctx, "workspace.project.initPrompt", params, &resp); err != nil {
		return "", fmt.Errorf("failed to get init prompt: %w", err)
	}
	return resp.Prompt, nil
}

// MCPResourceContents holds the contents of an MCP resource.
type MCPResourceContents struct {
	URI      string `json:"uri"`
	MIMEType string `json:"mime_type,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     []byte `json:"blob,omitempty"`
}

// EnableDockerMCP enables the Docker MCP server on the workspace.
func (c *Client) EnableDockerMCP(ctx context.Context, id string) error {
	params := struct {
		ID string `json:"id"`
	}{ID: id}
	if err := c.call(ctx, "workspace.mcp.docker.enable", params, nil); err != nil {
		return fmt.Errorf("failed to enable docker MCP: %w", err)
	}
	return nil
}

// DisableDockerMCP disables the Docker MCP server on the workspace.
func (c *Client) DisableDockerMCP(ctx context.Context, id string) error {
	params := struct {
		ID string `json:"id"`
	}{ID: id}
	if err := c.call(ctx, "workspace.mcp.docker.disable", params, nil); err != nil {
		return fmt.Errorf("failed to disable docker MCP: %w", err)
	}
	return nil
}

// RefreshMCPTools refreshes tools for a named MCP server.
func (c *Client) RefreshMCPTools(ctx context.Context, id, name string) error {
	params := struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{ID: id, Name: name}
	if err := c.call(ctx, "workspace.mcp.refreshTools", params, nil); err != nil {
		return fmt.Errorf("failed to refresh MCP tools: %w", err)
	}
	return nil
}

// ReadMCPResource reads a resource from a named MCP server.
func (c *Client) ReadMCPResource(ctx context.Context, id, name, uri string) ([]MCPResourceContents, error) {
	params := struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		URI  string `json:"uri"`
	}{ID: id, Name: name, URI: uri}
	var contents []MCPResourceContents
	if err := c.call(ctx, "workspace.mcp.readResource", params, &contents); err != nil {
		return nil, fmt.Errorf("failed to read MCP resource: %w", err)
	}
	return contents, nil
}

// GetMCPPrompt retrieves a prompt from a named MCP server.
func (c *Client) GetMCPPrompt(ctx context.Context, id, clientID, promptID string, args map[string]string) (string, error) {
	params := struct {
		ID       string            `json:"id"`
		ClientID string            `json:"client_id"`
		PromptID string            `json:"prompt_id"`
		Args     map[string]string `json:"args"`
	}{ID: id, ClientID: clientID, PromptID: promptID, Args: args}
	var resp proto.MCPGetPromptResponse
	if err := c.call(ctx, "workspace.mcp.getPrompt", params, &resp); err != nil {
		return "", fmt.Errorf("failed to get MCP prompt: %w", err)
	}
	return resp.Prompt, nil
}
