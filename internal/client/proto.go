package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/message"
	"github.com/charmbracelet/crush/internal/proto"
	"github.com/charmbracelet/crush/internal/pubsub"
	"github.com/charmbracelet/x/powernap/pkg/lsp/protocol"
)

// ListWorkspaces retrieves all workspaces from the server.
func (c *Client) ListWorkspaces(ctx context.Context) ([]proto.Workspace, error) {
	var workspaces []proto.Workspace
	if err := c.call(ctx, "workspaces.list", nil, &workspaces); err != nil {
		return nil, fmt.Errorf("failed to list workspaces: %w", err)
	}
	return workspaces, nil
}

// CreateWorkspace creates a new workspace on the server.
func (c *Client) CreateWorkspace(ctx context.Context, ws proto.Workspace) (*proto.Workspace, error) {
	var created proto.Workspace
	if err := c.call(ctx, "workspaces.create", ws, &created); err != nil {
		return nil, fmt.Errorf("failed to create workspace: %w", err)
	}
	return &created, nil
}

// GetWorkspace retrieves a workspace from the server.
func (c *Client) GetWorkspace(ctx context.Context, id string) (*proto.Workspace, error) {
	var ws proto.Workspace
	if err := c.call(ctx, "workspace.get", workspaceParams{ID: id}, &ws); err != nil {
		return nil, fmt.Errorf("failed to get workspace: %w", err)
	}
	return &ws, nil
}

// DeleteWorkspace deletes a workspace on the server.
func (c *Client) DeleteWorkspace(ctx context.Context, id string) error {
	if err := c.call(ctx, "workspaces.delete", workspaceParams{ID: id}, nil); err != nil {
		return fmt.Errorf("failed to delete workspace: %w", err)
	}
	return nil
}

// SubscribeEvents subscribes to events for a workspace.
func (c *Client) SubscribeEvents(ctx context.Context, id string) (<-chan any, error) {
	// Subscribe on the server.
	if err := c.call(ctx, "events.subscribe", workspaceParams{ID: id}, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to events: %w", err)
	}

	// Return the shared event channel. Events are JSON-RPC notifications
	// with method "event", each carrying a pubsub.Payload as params.
	events := make(chan any, 100)

	go func() {
		defer close(events)

		// Use the client's event channel which receives raw JSON of
		// pubsub.Payload from the read loop.
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-c.eventCh:
				if !ok {
					return
				}

				// ev is json.RawMessage (the params of the event notification)
				raw, ok := ev.(json.RawMessage)
				if !ok {
					continue
				}

				var p pubsub.Payload
				if err := json.Unmarshal(raw, &p); err != nil {
					slog.Error("Unmarshaling event envelope", "error", err)
					continue
				}

				unwrapped := unwrapEvent(p)
				if unwrapped != nil {
					sendEvent(ctx, events, unwrapped)
				}
			}
		}
	}()

	return events, nil
}

// unwrapEvent converts a pubsub.Payload back into a typed event.
func unwrapEvent(p pubsub.Payload) any {
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
	default:
		slog.Warn("Unknown event type", "type", p.Type)
		return nil
	}
}

func sendEvent(ctx context.Context, evc chan any, ev any) {
	select {
	case evc <- ev:
	case <-ctx.Done():
		return
	}
}

// GetLSPDiagnostics retrieves LSP diagnostics for a specific LSP client.
func (c *Client) GetLSPDiagnostics(ctx context.Context, id string, lspName string) (map[protocol.DocumentURI][]protocol.Diagnostic, error) {
	var diagnostics map[protocol.DocumentURI][]protocol.Diagnostic
	if err := c.call(ctx, "workspace.lsps.diagnostics", lspDiagnosticsParams{ID: id, LSP: lspName}, &diagnostics); err != nil {
		return nil, fmt.Errorf("failed to get LSP diagnostics: %w", err)
	}
	return diagnostics, nil
}

// GetLSPs retrieves the LSP client states for a workspace.
func (c *Client) GetLSPs(ctx context.Context, id string) (map[string]proto.LSPClientInfo, error) {
	var lsps map[string]proto.LSPClientInfo
	if err := c.call(ctx, "workspace.lsps", workspaceParams{ID: id}, &lsps); err != nil {
		return nil, fmt.Errorf("failed to get LSPs: %w", err)
	}
	return lsps, nil
}

// MCPGetStates retrieves the MCP client states for a workspace.
func (c *Client) MCPGetStates(ctx context.Context, id string) (map[string]proto.MCPClientInfo, error) {
	var states map[string]proto.MCPClientInfo
	if err := c.call(ctx, "workspace.mcp.states", workspaceParams{ID: id}, &states); err != nil {
		return nil, fmt.Errorf("failed to get MCP states: %w", err)
	}
	return states, nil
}

// MCPRefreshPrompts refreshes prompts for a named MCP client.
func (c *Client) MCPRefreshPrompts(ctx context.Context, id, name string) error {
	if err := c.call(ctx, "workspace.mcp.refreshPrompts", mcpNameParams{ID: id, Name: name}, nil); err != nil {
		return fmt.Errorf("failed to refresh MCP prompts: %w", err)
	}
	return nil
}

// MCPRefreshResources refreshes resources for a named MCP client.
func (c *Client) MCPRefreshResources(ctx context.Context, id, name string) error {
	if err := c.call(ctx, "workspace.mcp.refreshResources", mcpNameParams{ID: id, Name: name}, nil); err != nil {
		return fmt.Errorf("failed to refresh MCP resources: %w", err)
	}
	return nil
}

// GetAgentSessionQueuedPrompts retrieves the number of queued prompts.
func (c *Client) GetAgentSessionQueuedPrompts(ctx context.Context, id string, sessionID string) (int, error) {
	var count int
	if err := c.call(ctx, "workspace.agent.session.prompts.queued", sessionParams{ID: id, SessionID: sessionID}, &count); err != nil {
		return 0, fmt.Errorf("failed to get queued prompts: %w", err)
	}
	return count, nil
}

// ClearAgentSessionQueuedPrompts clears the queued prompts.
func (c *Client) ClearAgentSessionQueuedPrompts(ctx context.Context, id string, sessionID string) error {
	if err := c.call(ctx, "workspace.agent.session.prompts.clear", sessionParams{ID: id, SessionID: sessionID}, nil); err != nil {
		return fmt.Errorf("failed to clear queued prompts: %w", err)
	}
	return nil
}

// GetAgentInfo retrieves the agent status for a workspace.
func (c *Client) GetAgentInfo(ctx context.Context, id string) (*proto.AgentInfo, error) {
	var info proto.AgentInfo
	if err := c.call(ctx, "workspace.agent", workspaceParams{ID: id}, &info); err != nil {
		return nil, fmt.Errorf("failed to get agent info: %w", err)
	}
	return &info, nil
}

// UpdateAgent triggers an agent model update.
func (c *Client) UpdateAgent(ctx context.Context, id string) error {
	if err := c.call(ctx, "workspace.agent.update", workspaceParams{ID: id}, nil); err != nil {
		return fmt.Errorf("failed to update agent: %w", err)
	}
	return nil
}

// SendMessage sends a message to the agent.
func (c *Client) SendMessage(ctx context.Context, id string, sessionID, prompt string, attachments ...message.Attachment) error {
	protoAttachments := make([]proto.Attachment, len(attachments))
	for i, a := range attachments {
		protoAttachments[i] = proto.Attachment{
			FilePath: a.FilePath,
			FileName: a.FileName,
			MimeType: a.MimeType,
			Content:  a.Content,
		}
	}
	params := agentSendParams{
		ID: id,
		Msg: proto.AgentMessage{
			SessionID:   sessionID,
			Prompt:      prompt,
			Attachments: protoAttachments,
		},
	}
	if err := c.call(ctx, "workspace.agent.send", params, nil); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	return nil
}

// GetAgentSessionInfo retrieves the agent session info.
func (c *Client) GetAgentSessionInfo(ctx context.Context, id string, sessionID string) (*proto.AgentSession, error) {
	var info proto.AgentSession
	if err := c.call(ctx, "workspace.agent.session", sessionParams{ID: id, SessionID: sessionID}, &info); err != nil {
		return nil, fmt.Errorf("failed to get agent session info: %w", err)
	}
	return &info, nil
}

// AgentSummarizeSession requests a session summarization.
func (c *Client) AgentSummarizeSession(ctx context.Context, id string, sessionID string) error {
	if err := c.call(ctx, "workspace.agent.session.summarize", sessionParams{ID: id, SessionID: sessionID}, nil); err != nil {
		return fmt.Errorf("failed to summarize session: %w", err)
	}
	return nil
}

// InitiateAgentProcessing triggers agent initialization.
func (c *Client) InitiateAgentProcessing(ctx context.Context, id string) error {
	if err := c.call(ctx, "workspace.agent.init", workspaceParams{ID: id}, nil); err != nil {
		return fmt.Errorf("failed to initiate agent processing: %w", err)
	}
	return nil
}

// ListMessages retrieves all messages for a session.
func (c *Client) ListMessages(ctx context.Context, id string, sessionID string) ([]proto.Message, error) {
	var msgs []proto.Message
	if err := c.call(ctx, "workspace.sessions.messages", sessionParams{ID: id, SessionID: sessionID}, &msgs); err != nil {
		return nil, fmt.Errorf("failed to get messages: %w", err)
	}
	return msgs, nil
}

// GetSession retrieves a specific session.
func (c *Client) GetSession(ctx context.Context, id string, sessionID string) (*proto.Session, error) {
	var sess proto.Session
	if err := c.call(ctx, "workspace.sessions.get", sessionParams{ID: id, SessionID: sessionID}, &sess); err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
	}
	return &sess, nil
}

// ListSessionHistoryFiles retrieves history files for a session.
func (c *Client) ListSessionHistoryFiles(ctx context.Context, id string, sessionID string) ([]proto.File, error) {
	var files []proto.File
	if err := c.call(ctx, "workspace.sessions.history", sessionParams{ID: id, SessionID: sessionID}, &files); err != nil {
		return nil, fmt.Errorf("failed to get session history: %w", err)
	}
	return files, nil
}

// CreateSession creates a new session.
func (c *Client) CreateSession(ctx context.Context, id string, title string) (*proto.Session, error) {
	var sess proto.Session
	params := struct {
		ID    string `json:"id"`
		Title string `json:"title"`
	}{ID: id, Title: title}
	if err := c.call(ctx, "workspace.sessions.create", params, &sess); err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	return &sess, nil
}

// ListSessions lists all sessions in a workspace.
func (c *Client) ListSessions(ctx context.Context, id string) ([]proto.Session, error) {
	var sessions []proto.Session
	if err := c.call(ctx, "workspace.sessions.list", workspaceParams{ID: id}, &sessions); err != nil {
		return nil, fmt.Errorf("failed to get sessions: %w", err)
	}
	return sessions, nil
}

// GrantPermission grants a permission on a workspace.
func (c *Client) GrantPermission(ctx context.Context, id string, req proto.PermissionGrant) error {
	params := struct {
		ID         string               `json:"id"`
		Permission proto.PermissionGrant `json:"permission"`
	}{ID: id, Permission: req}
	if err := c.call(ctx, "workspace.permissions.grant", params, nil); err != nil {
		return fmt.Errorf("failed to grant permission: %w", err)
	}
	return nil
}

// SetPermissionsSkipRequests sets the skip-requests flag.
func (c *Client) SetPermissionsSkipRequests(ctx context.Context, id string, skip bool) error {
	params := struct {
		ID   string `json:"id"`
		Skip bool   `json:"skip"`
	}{ID: id, Skip: skip}
	if err := c.call(ctx, "workspace.permissions.skip.set", params, nil); err != nil {
		return fmt.Errorf("failed to set permissions skip: %w", err)
	}
	return nil
}

// GetPermissionsSkipRequests retrieves the skip-requests flag.
func (c *Client) GetPermissionsSkipRequests(ctx context.Context, id string) (bool, error) {
	var skipReq proto.PermissionSkipRequest
	if err := c.call(ctx, "workspace.permissions.skip.get", workspaceParams{ID: id}, &skipReq); err != nil {
		return false, fmt.Errorf("failed to get permissions skip: %w", err)
	}
	return skipReq.Skip, nil
}

// GetConfig retrieves the workspace-specific configuration.
func (c *Client) GetConfig(ctx context.Context, id string) (*config.Config, error) {
	var cfg config.Config
	if err := c.call(ctx, "workspace.config.get", workspaceParams{ID: id}, &cfg); err != nil {
		return nil, fmt.Errorf("failed to get config: %w", err)
	}
	return &cfg, nil
}

// SaveSession updates a session.
func (c *Client) SaveSession(ctx context.Context, id string, sess proto.Session) (*proto.Session, error) {
	params := struct {
		ID      string       `json:"id"`
		Session proto.Session `json:"session"`
	}{ID: id, Session: sess}
	var saved proto.Session
	if err := c.call(ctx, "workspace.sessions.update", params, &saved); err != nil {
		return nil, fmt.Errorf("failed to save session: %w", err)
	}
	return &saved, nil
}

// DeleteSession deletes a session from a workspace.
func (c *Client) DeleteSession(ctx context.Context, id string, sessionID string) error {
	if err := c.call(ctx, "workspace.sessions.delete", sessionParams{ID: id, SessionID: sessionID}, nil); err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}
	return nil
}

// ListUserMessages retrieves user-role messages for a session.
func (c *Client) ListUserMessages(ctx context.Context, id string, sessionID string) ([]proto.Message, error) {
	var msgs []proto.Message
	if err := c.call(ctx, "workspace.sessions.messages.user", sessionParams{ID: id, SessionID: sessionID}, &msgs); err != nil {
		return nil, fmt.Errorf("failed to get user messages: %w", err)
	}
	return msgs, nil
}

// ListAllUserMessages retrieves all user-role messages across sessions.
func (c *Client) ListAllUserMessages(ctx context.Context, id string) ([]proto.Message, error) {
	var msgs []proto.Message
	if err := c.call(ctx, "workspace.messages.all", workspaceParams{ID: id}, &msgs); err != nil {
		return nil, fmt.Errorf("failed to get all user messages: %w", err)
	}
	return msgs, nil
}

// CancelAgentSession cancels an ongoing agent operation.
func (c *Client) CancelAgentSession(ctx context.Context, id string, sessionID string) error {
	if err := c.call(ctx, "workspace.agent.session.cancel", sessionParams{ID: id, SessionID: sessionID}, nil); err != nil {
		return fmt.Errorf("failed to cancel agent session: %w", err)
	}
	return nil
}

// GetAgentSessionQueuedPromptsList retrieves the list of queued prompts.
func (c *Client) GetAgentSessionQueuedPromptsList(ctx context.Context, id string, sessionID string) ([]string, error) {
	var prompts []string
	if err := c.call(ctx, "workspace.agent.session.prompts.list", sessionParams{ID: id, SessionID: sessionID}, &prompts); err != nil {
		return nil, fmt.Errorf("failed to get queued prompts list: %w", err)
	}
	return prompts, nil
}

// GetDefaultSmallModel retrieves the default small model for a provider.
func (c *Client) GetDefaultSmallModel(ctx context.Context, id string, providerID string) (*config.SelectedModel, error) {
	params := struct {
		ID         string `json:"id"`
		ProviderID string `json:"provider_id"`
	}{ID: id, ProviderID: providerID}
	var model config.SelectedModel
	if err := c.call(ctx, "workspace.agent.defaultSmallModel", params, &model); err != nil {
		return nil, fmt.Errorf("failed to get default small model: %w", err)
	}
	return &model, nil
}

// FileTrackerRecordRead records a file read.
func (c *Client) FileTrackerRecordRead(ctx context.Context, id string, sessionID, path string) error {
	params := struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
		Path      string `json:"path"`
	}{ID: id, SessionID: sessionID, Path: path}
	if err := c.call(ctx, "workspace.filetracker.read", params, nil); err != nil {
		return fmt.Errorf("failed to record file read: %w", err)
	}
	return nil
}

// FileTrackerLastReadTime returns the last read time for a file.
func (c *Client) FileTrackerLastReadTime(ctx context.Context, id string, sessionID, path string) (time.Time, error) {
	params := struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
		Path      string `json:"path"`
	}{ID: id, SessionID: sessionID, Path: path}
	var t time.Time
	if err := c.call(ctx, "workspace.filetracker.lastRead", params, &t); err != nil {
		return time.Time{}, fmt.Errorf("failed to get last read time: %w", err)
	}
	return t, nil
}

// FileTrackerListReadFiles returns the list of read files for a session.
func (c *Client) FileTrackerListReadFiles(ctx context.Context, id string, sessionID string) ([]string, error) {
	var files []string
	if err := c.call(ctx, "workspace.filetracker.files", sessionParams{ID: id, SessionID: sessionID}, &files); err != nil {
		return nil, fmt.Errorf("failed to get read files: %w", err)
	}
	return files, nil
}

// LSPStart starts an LSP server for a path.
func (c *Client) LSPStart(ctx context.Context, id string, path string) error {
	params := struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	}{ID: id, Path: path}
	if err := c.call(ctx, "workspace.lsps.start", params, nil); err != nil {
		return fmt.Errorf("failed to start LSP: %w", err)
	}
	return nil
}

// LSPStopAll stops all LSP servers for a workspace.
func (c *Client) LSPStopAll(ctx context.Context, id string) error {
	if err := c.call(ctx, "workspace.lsps.stop", workspaceParams{ID: id}, nil); err != nil {
		return fmt.Errorf("failed to stop LSPs: %w", err)
	}
	return nil
}

// -- Type aliases for JSON-RPC param structs --

type workspaceParams struct {
	ID string `json:"id"`
}

type sessionParams struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
}

type lspDiagnosticsParams struct {
	ID  string `json:"id"`
	LSP string `json:"lsp"`
}

type mcpNameParams struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type agentSendParams struct {
	ID  string             `json:"id"`
	Msg proto.AgentMessage `json:"message"`
}

