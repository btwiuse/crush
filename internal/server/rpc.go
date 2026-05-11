package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/charmbracelet/crush/internal/backend"
	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/message"
	"github.com/charmbracelet/crush/internal/proto"
	"github.com/charmbracelet/crush/internal/pubsub"
	"github.com/charmbracelet/crush/internal/session"
)

// registerRPCHandlers registers all JSON-RPC method handlers on the given
// router.
func (s *Server) registerRPCHandlers(router jrpcRouter) {
	// System
	router.Handle("system.health", jrpcHandler(s.handleGetHealth))
	router.Handle("system.version", jrpcHandler(s.handleGetVersion))
	router.Handle("system.config", jrpcHandler(s.handleGetConfig))
	router.Handle("system.control", jrpcHandler(s.handlePostControl))

	// Workspaces
	router.Handle("workspaces.list", jrpcHandler(s.handleGetWorkspaces))
	router.Handle("workspace.get", jrpcHandler(s.handleGetWorkspace))
	router.Handle("workspaces.create", jrpcHandler(s.handlePostWorkspaces))
	router.Handle("workspaces.delete", jrpcHandler(s.handleDeleteWorkspaces))

	// Workspace config
	router.Handle("workspace.config.get", jrpcHandler(s.handleGetWorkspaceConfig))
	router.Handle("workspace.config.set", jrpcHandler(s.handlePostWorkspaceConfigSet))
	router.Handle("workspace.config.remove", jrpcHandler(s.handlePostWorkspaceConfigRemove))
	router.Handle("workspace.config.model", jrpcHandler(s.handlePostWorkspaceConfigModel))
	router.Handle("workspace.config.compact", jrpcHandler(s.handlePostWorkspaceConfigCompact))
	router.Handle("workspace.config.providerKey", jrpcHandler(s.handlePostWorkspaceConfigProviderKey))
	router.Handle("workspace.config.importCopilot", jrpcHandler(s.handlePostWorkspaceConfigImportCopilot))
	router.Handle("workspace.config.refreshOAuth", jrpcHandler(s.handlePostWorkspaceConfigRefreshOAuth))
	router.Handle("workspace.providers", jrpcHandler(s.handleGetWorkspaceProviders))

	// Sessions
	router.Handle("workspace.sessions.list", jrpcHandler(s.handleGetWorkspaceSessions))
	router.Handle("workspace.sessions.create", jrpcHandler(s.handlePostWorkspaceSessions))
	router.Handle("workspace.sessions.get", jrpcHandler(s.handleGetWorkspaceSession))
	router.Handle("workspace.sessions.update", jrpcHandler(s.handlePutWorkspaceSession))
	router.Handle("workspace.sessions.delete", jrpcHandler(s.handleDeleteWorkspaceSession))
	router.Handle("workspace.sessions.history", jrpcHandler(s.handleGetWorkspaceSessionHistory))
	router.Handle("workspace.sessions.messages", jrpcHandler(s.handleGetWorkspaceSessionMessages))
	router.Handle("workspace.sessions.messages.user", jrpcHandler(s.handleGetWorkspaceSessionUserMessages))
	router.Handle("workspace.messages.all", jrpcHandler(s.handleGetWorkspaceAllUserMessages))

	// File tracker
	router.Handle("workspace.filetracker.read", jrpcHandler(s.handlePostWorkspaceFileTrackerRead))
	router.Handle("workspace.filetracker.lastRead", jrpcHandler(s.handleGetWorkspaceFileTrackerLastRead))
	router.Handle("workspace.filetracker.files", jrpcHandler(s.handleGetWorkspaceSessionFileTrackerFiles))

	// LSP
	router.Handle("workspace.lsps", jrpcHandler(s.handleGetWorkspaceLSPs))
	router.Handle("workspace.lsps.diagnostics", jrpcHandler(s.handleGetWorkspaceLSPDiagnostics))
	router.Handle("workspace.lsps.start", jrpcHandler(s.handlePostWorkspaceLSPStart))
	router.Handle("workspace.lsps.stop", jrpcHandler(s.handlePostWorkspaceLSPStopAll))

	// Permissions
	router.Handle("workspace.permissions.skip.get", jrpcHandler(s.handleGetWorkspacePermissionsSkip))
	router.Handle("workspace.permissions.skip.set", jrpcHandler(s.handlePostWorkspacePermissionsSkip))
	router.Handle("workspace.permissions.grant", jrpcHandler(s.handlePostWorkspacePermissionsGrant))

	// Agent
	router.Handle("workspace.agent", jrpcHandler(s.handleGetWorkspaceAgent))
	router.Handle("workspace.agent.send", jrpcHandler(s.handlePostWorkspaceAgent))
	router.Handle("workspace.agent.init", jrpcHandler(s.handlePostWorkspaceAgentInit))
	router.Handle("workspace.agent.update", jrpcHandler(s.handlePostWorkspaceAgentUpdate))
	router.Handle("workspace.agent.session", jrpcHandler(s.handleGetWorkspaceAgentSession))
	router.Handle("workspace.agent.session.cancel", jrpcHandler(s.handlePostWorkspaceAgentSessionCancel))
	router.Handle("workspace.agent.session.prompts.queued", jrpcHandler(s.handleGetWorkspaceAgentSessionPromptQueued))
	router.Handle("workspace.agent.session.prompts.list", jrpcHandler(s.handleGetWorkspaceAgentSessionPromptList))
	router.Handle("workspace.agent.session.prompts.clear", jrpcHandler(s.handlePostWorkspaceAgentSessionPromptClear))
	router.Handle("workspace.agent.session.summarize", jrpcHandler(s.handlePostWorkspaceAgentSessionSummarize))
	router.Handle("workspace.agent.defaultSmallModel", jrpcHandler(s.handleGetWorkspaceAgentDefaultSmallModel))

	// Project
	router.Handle("workspace.project.needsInit", jrpcHandler(s.handleGetWorkspaceProjectNeedsInit))
	router.Handle("workspace.project.init", jrpcHandler(s.handlePostWorkspaceProjectInit))
	router.Handle("workspace.project.initPrompt", jrpcHandler(s.handleGetWorkspaceProjectInitPrompt))

	// MCP
	router.Handle("workspace.mcp.refreshTools", jrpcHandler(s.handlePostWorkspaceMCPRefreshTools))
	router.Handle("workspace.mcp.readResource", jrpcHandler(s.handlePostWorkspaceMCPReadResource))
	router.Handle("workspace.mcp.getPrompt", jrpcHandler(s.handlePostWorkspaceMCPGetPrompt))
	router.Handle("workspace.mcp.states", jrpcHandler(s.handleGetWorkspaceMCPStates))
	router.Handle("workspace.mcp.refreshPrompts", jrpcHandler(s.handlePostWorkspaceMCPRefreshPrompts))
	router.Handle("workspace.mcp.refreshResources", jrpcHandler(s.handlePostWorkspaceMCPRefreshResources))
	router.Handle("workspace.mcp.docker.enable", jrpcHandler(s.handlePostWorkspaceMCPEnableDocker))
	router.Handle("workspace.mcp.docker.disable", jrpcHandler(s.handlePostWorkspaceMCPDisableDocker))

	// Events
	router.Handle("events.subscribe", jrpcHandler(s.handleSubscribeEvents))
	router.Handle("events.unsubscribe", jrpcHandler(s.handleUnsubscribeEvents))
}

// -- Parameter types --

type workspaceIDParams struct {
	ID string `json:"id"`
}

type sessionIDParams struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
}

// mapErr maps backend errors to JSON-RPC error responses.
func mapErr(err error) error {
	switch {
	case
		err == nil:
		return nil
	case
		isNotFound(err):
		return &jrpcError{Code: -32001, Message: err.Error()}
	case
		isBadRequest(err):
		return &jrpcError{Code: -32002, Message: err.Error()}
	default:
		return err
	}
}

func isNotFound(err error) bool {
	return err == backend.ErrWorkspaceNotFound ||
		err == backend.ErrLSPClientNotFound
}

func isBadRequest(err error) bool {
	return err == backend.ErrAgentNotInitialized ||
		err == backend.ErrPathRequired ||
		err == backend.ErrInvalidPermissionAction ||
		err == backend.ErrUnknownCommand
}

// -- System handlers --

func (s *Server) handleGetHealth(_ context.Context, _ json.RawMessage) (any, error) {
	return nil, nil
}

func (s *Server) handleGetVersion(_ context.Context, _ json.RawMessage) (any, error) {
	return s.backend.VersionInfo(), nil
}

func (s *Server) handleGetConfig(_ context.Context, _ json.RawMessage) (any, error) {
	return s.backend.Config(), nil
}

func (s *Server) handlePostControl(_ context.Context, params json.RawMessage) (any, error) {
	var req proto.ServerControl
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}

	switch req.Command {
	case "shutdown":
		s.backend.Shutdown()
		return nil, nil
	default:
		return nil, &jrpcError{Code: -32002, Message: "unknown command"}
	}
}

// -- Workspace handlers --

func (s *Server) handleGetWorkspaces(_ context.Context, _ json.RawMessage) (any, error) {
	return s.backend.ListWorkspaces(), nil
}

func (s *Server) handleGetWorkspace(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	ws, err := s.backend.GetWorkspaceProto(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return ws, nil
}

func (s *Server) handlePostWorkspaces(_ context.Context, params json.RawMessage) (any, error) {
	var args proto.Workspace
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	_, result, err := s.backend.CreateWorkspace(args)
	if err != nil {
		return nil, mapErr(err)
	}
	return result, nil
}

func (s *Server) handleDeleteWorkspaces(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	s.backend.DeleteWorkspace(p.ID)
	return nil, nil
}

// -- Config handlers --

func (s *Server) handleGetWorkspaceConfig(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	cfg, err := s.backend.GetWorkspaceConfig(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return cfg, nil
}

func (s *Server) handleGetWorkspaceProviders(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	providers, err := s.backend.GetWorkspaceProviders(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return providers, nil
}

func (s *Server) handlePostWorkspaceConfigSet(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID    string       `json:"id"`
		Scope config.Scope `json:"scope"`
		Key   string       `json:"key"`
		Value any          `json:"value"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.SetConfigField(req.ID, req.Scope, req.Key, req.Value); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceConfigRemove(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID    string       `json:"id"`
		Scope config.Scope `json:"scope"`
		Key   string       `json:"key"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.RemoveConfigField(req.ID, req.Scope, req.Key); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceConfigModel(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID        string                 `json:"id"`
		Scope     config.Scope           `json:"scope"`
		ModelType config.SelectedModelType `json:"model_type"`
		Model     config.SelectedModel   `json:"model"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.UpdatePreferredModel(req.ID, req.Scope, req.ModelType, req.Model); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceConfigCompact(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID      string       `json:"id"`
		Scope   config.Scope `json:"scope"`
		Enabled bool         `json:"enabled"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.SetCompactMode(req.ID, req.Scope, req.Enabled); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceConfigProviderKey(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID         string       `json:"id"`
		Scope      config.Scope `json:"scope"`
		ProviderID string       `json:"provider_id"`
		APIKey     any          `json:"api_key"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.SetProviderAPIKey(req.ID, req.Scope, req.ProviderID, req.APIKey); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceConfigImportCopilot(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	token, ok, err := s.backend.ImportCopilot(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return proto.ImportCopilotResponse{Token: token, Success: ok}, nil
}

func (s *Server) handlePostWorkspaceConfigRefreshOAuth(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID         string       `json:"id"`
		Scope      config.Scope `json:"scope"`
		ProviderID string       `json:"provider_id"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.RefreshOAuthToken(ctx, req.ID, req.Scope, req.ProviderID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

// -- Session handlers --

func (s *Server) handleGetWorkspaceSessions(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	sessions, err := s.backend.ListSessions(ctx, p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	result := make([]proto.Session, len(sessions))
	for i, se := range sessions {
		result[i] = sessionToProto(se)
	}
	return result, nil
}

func (s *Server) handlePostWorkspaceSessions(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID    string `json:"id"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	se, err := s.backend.CreateSession(ctx, req.ID, req.Title)
	if err != nil {
		return nil, mapErr(err)
	}
	return sessionToProto(se), nil
}

func (s *Server) handleGetWorkspaceSession(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	se, err := s.backend.GetSession(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return sessionToProto(se), nil
}

func (s *Server) handlePutWorkspaceSession(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID      string         `json:"id"`
		Session session.Session `json:"session"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	saved, err := s.backend.SaveSession(ctx, req.ID, req.Session)
	if err != nil {
		return nil, mapErr(err)
	}
	return sessionToProto(saved), nil
}

func (s *Server) handleDeleteWorkspaceSession(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.DeleteSession(ctx, p.ID, p.SessionID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handleGetWorkspaceSessionHistory(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	history, err := s.backend.ListSessionHistory(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return history, nil
}

func (s *Server) handleGetWorkspaceSessionMessages(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	messages, err := s.backend.ListSessionMessages(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return messagesToProto(messages), nil
}

func (s *Server) handleGetWorkspaceSessionUserMessages(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	messages, err := s.backend.ListUserMessages(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return messagesToProto(messages), nil
}

func (s *Server) handleGetWorkspaceAllUserMessages(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	messages, err := s.backend.ListAllUserMessages(ctx, p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return messagesToProto(messages), nil
}

// -- File tracker handlers --

func (s *Server) handlePostWorkspaceFileTrackerRead(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
		Path      string `json:"path"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.FileTrackerRecordRead(ctx, req.ID, req.SessionID, req.Path); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handleGetWorkspaceFileTrackerLastRead(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
		Path      string `json:"path"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	t, err := s.backend.FileTrackerLastReadTime(ctx, req.ID, req.SessionID, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	return t, nil
}

func (s *Server) handleGetWorkspaceSessionFileTrackerFiles(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	files, err := s.backend.FileTrackerListReadFiles(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return files, nil
}

// -- LSP handlers --

func (s *Server) handleGetWorkspaceLSPs(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	states, err := s.backend.GetLSPStates(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	result := make(map[string]proto.LSPClientInfo, len(states))
	for k, v := range states {
		result[k] = proto.LSPClientInfo{
			Name:            v.Name,
			State:           v.State,
			Error:           v.Error,
			DiagnosticCount: v.DiagnosticCount,
			ConnectedAt:     v.ConnectedAt,
		}
	}
	return result, nil
}

func (s *Server) handleGetWorkspaceLSPDiagnostics(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID  string `json:"id"`
		LSP string `json:"lsp"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	diagnostics, err := s.backend.GetLSPDiagnostics(req.ID, req.LSP)
	if err != nil {
		return nil, mapErr(err)
	}
	return diagnostics, nil
}

func (s *Server) handlePostWorkspaceLSPStart(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.LSPStart(ctx, req.ID, req.Path); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceLSPStopAll(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.LSPStopAll(ctx, p.ID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

// -- Permission handlers --

func (s *Server) handleGetWorkspacePermissionsSkip(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	skip, err := s.backend.GetPermissionsSkip(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return proto.PermissionSkipRequest{Skip: skip}, nil
}

func (s *Server) handlePostWorkspacePermissionsSkip(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Skip bool   `json:"skip"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.SetPermissionsSkip(req.ID, req.Skip); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspacePermissionsGrant(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID         string                 `json:"id"`
		Permission proto.PermissionGrant  `json:"permission"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.GrantPermission(req.ID, req.Permission); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

// -- Agent handlers --

func (s *Server) handleGetWorkspaceAgent(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	info, err := s.backend.GetAgentInfo(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return info, nil
}

func (s *Server) handlePostWorkspaceAgent(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID  string            `json:"id"`
		Msg proto.AgentMessage `json:"message"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}

	// Run the agent in a goroutine so the JSON-RPC read loop stays free
	// to process other requests (e.g. permission grants) while the agent
	// is waiting. The ctx stays valid until the WebSocket closes.
	go func() {
		if err := s.backend.SendMessage(ctx, req.ID, req.Msg); err != nil {
			slog.Error("Agent run failed", "error", err)
		}
	}()
	return nil, nil
}

func (s *Server) handlePostWorkspaceAgentInit(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	go func() {
		if err := s.backend.InitAgent(ctx, p.ID); err != nil {
			slog.Error("Agent init failed", "error", err)
		}
	}()
	return nil, nil
}

func (s *Server) handlePostWorkspaceAgentUpdate(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	go func() {
		if err := s.backend.UpdateAgent(ctx, p.ID); err != nil {
			slog.Error("Agent update failed", "error", err)
		}
	}()
	return nil, nil
}

func (s *Server) handleGetWorkspaceAgentSession(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	agentSession, err := s.backend.GetAgentSession(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return agentSession, nil
}

func (s *Server) handlePostWorkspaceAgentSessionCancel(_ context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.CancelSession(p.ID, p.SessionID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handleGetWorkspaceAgentSessionPromptQueued(_ context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	queued, err := s.backend.QueuedPrompts(p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return queued, nil
}

func (s *Server) handlePostWorkspaceAgentSessionPromptClear(_ context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.ClearQueue(p.ID, p.SessionID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceAgentSessionSummarize(ctx context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	go func() {
		if err := s.backend.SummarizeSession(ctx, p.ID, p.SessionID); err != nil {
			slog.Error("Session summarization failed", "error", err)
		}
	}()
	return nil, nil
}

func (s *Server) handleGetWorkspaceAgentSessionPromptList(_ context.Context, params json.RawMessage) (any, error) {
	var p sessionIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	prompts, err := s.backend.QueuedPromptsList(p.ID, p.SessionID)
	if err != nil {
		return nil, mapErr(err)
	}
	return prompts, nil
}

func (s *Server) handleGetWorkspaceAgentDefaultSmallModel(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID         string `json:"id"`
		ProviderID string `json:"provider_id"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	model, err := s.backend.GetDefaultSmallModel(req.ID, req.ProviderID)
	if err != nil {
		return nil, mapErr(err)
	}
	return model, nil
}

// -- Project handlers --

func (s *Server) handleGetWorkspaceProjectNeedsInit(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	needs, err := s.backend.ProjectNeedsInitialization(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return proto.ProjectNeedsInitResponse{NeedsInit: needs}, nil
}

func (s *Server) handlePostWorkspaceProjectInit(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.MarkProjectInitialized(p.ID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handleGetWorkspaceProjectInitPrompt(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	prompt, err := s.backend.InitializePrompt(p.ID)
	if err != nil {
		return nil, mapErr(err)
	}
	return proto.ProjectInitPromptResponse{Prompt: prompt}, nil
}

// -- MCP handlers --

func (s *Server) handlePostWorkspaceMCPRefreshTools(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.RefreshMCPTools(ctx, req.ID, req.Name); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceMCPReadResource(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		URI  string `json:"uri"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	contents, err := s.backend.ReadMCPResource(ctx, req.ID, req.Name, req.URI)
	if err != nil {
		return nil, mapErr(err)
	}
	return contents, nil
}

func (s *Server) handlePostWorkspaceMCPGetPrompt(_ context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID       string            `json:"id"`
		ClientID string            `json:"client_id"`
		PromptID string            `json:"prompt_id"`
		Args     map[string]string `json:"args"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	prompt, err := s.backend.GetMCPPrompt(req.ID, req.ClientID, req.PromptID, req.Args)
	if err != nil {
		return nil, mapErr(err)
	}
	return proto.MCPGetPromptResponse{Prompt: prompt}, nil
}

func (s *Server) handleGetWorkspaceMCPStates(_ context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	states := s.backend.MCPGetStates(p.ID)
	result := make(map[string]proto.MCPClientInfo, len(states))
	for k, v := range states {
		result[k] = proto.MCPClientInfo{
			Name:          v.Name,
			State:         proto.MCPState(v.State),
			Error:         v.Error,
			ToolCount:     v.Counts.Tools,
			PromptCount:   v.Counts.Prompts,
			ResourceCount: v.Counts.Resources,
			ConnectedAt:   v.ConnectedAt,
		}
	}
	return result, nil
}

func (s *Server) handlePostWorkspaceMCPRefreshPrompts(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	s.backend.MCPRefreshPrompts(ctx, req.ID, req.Name)
	return nil, nil
}

func (s *Server) handlePostWorkspaceMCPRefreshResources(ctx context.Context, params json.RawMessage) (any, error) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	s.backend.MCPRefreshResources(ctx, req.ID, req.Name)
	return nil, nil
}

func (s *Server) handlePostWorkspaceMCPEnableDocker(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.EnableDockerMCP(ctx, p.ID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

func (s *Server) handlePostWorkspaceMCPDisableDocker(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}
	if err := s.backend.DisableDockerMCP(p.ID); err != nil {
		return nil, mapErr(err)
	}
	return nil, nil
}

// -- Event handlers --

func (s *Server) handleSubscribeEvents(ctx context.Context, params json.RawMessage) (any, error) {
	var p workspaceIDParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &jrpcError{Code: jrpcInvalidParams, Message: "Invalid params"}
	}

	// Find the WebSocket connection from the context. The connection is
	// stored in the context by the WebSocket handler.
	jc, ok := ctx.Value(jrpcConnKey).(*jrpcConn)
	if !ok {
		return nil, &jrpcError{Code: jrpcInternalError, Message: "Not a WebSocket connection"}
	}

	events, err := s.backend.SubscribeEvents(ctx, p.ID)
	if err != nil {
		return nil, mapErr(err)
	}

	go func() {
		defer func() {
			// Drain the channel if the connection is closed.
			for range events {
			}
		}()

		// Debounce: skip message "updated" events if the same message
		// was sent within the last 100ms. This prevents flooding the
		// client during rapid streaming deltas.
		const debounceInterval = 100 * time.Millisecond
		lastMsgSent := make(map[string]time.Time)

		// Push current agent state immediately.
		if info, err := s.backend.GetAgentInfo(p.ID); err == nil {
			wrapped := wrapEvent(pubsub.Event[proto.AgentInfo]{
				Type:    pubsub.UpdatedEvent,
				Payload: info,
			})
			if wrapped != nil {
				jc.notify("event", wrapped)
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}

				// Debounce message update events — but always send the
				// final state (message with a FinishPart) so the client
				// never misses the terminal update.
				if msgEvent, ok := ev.Payload.(pubsub.Event[message.Message]); ok && msgEvent.Type == pubsub.UpdatedEvent {
					// Always let through the final message state.
					if msgEvent.Payload.FinishPart() == nil {
						now := time.Now()
						if last, exists := lastMsgSent[msgEvent.Payload.ID]; exists && now.Sub(last) < debounceInterval {
							continue
						}
						lastMsgSent[msgEvent.Payload.ID] = now
					}
				}

				wrapped := wrapEvent(ev.Payload)
				if wrapped == nil {
					continue
				}
				jc.notify("event", wrapped)
			}
		}
	}()

	return nil, nil
}

// jrpcConnKey is the context key for the WebSocket connection.
type jrpcConnKeyType struct{}

var jrpcConnKey = jrpcConnKeyType{}

func (s *Server) handleUnsubscribeEvents(ctx context.Context, _ json.RawMessage) (any, error) {
	// The ctx is already tied to the connection, so when the connection
	// closes, all event goroutines are cancelled automatically.
	slog.Debug("Event unsubscribed")
	return nil, nil
}

// -- Helper functions (reused from events.go) --
// Note: sessionToProto, fileToProto, messageToProto, messagesToProto
// are defined in events.go
