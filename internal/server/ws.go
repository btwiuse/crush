package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/charmbracelet/crush/internal/backend"
	jrpc "github.com/charmbracelet/crush/internal/jsonrpc"
	"github.com/charmbracelet/crush/internal/proto"
	"github.com/charmbracelet/crush/internal/session"
	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// wsConn wraps a *websocket.Conn with a mutex so concurrent writers
// are safe.
type wsConn struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (c *wsConn) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

// controllerWS handles JSON-RPC 2.0 traffic over a WebSocket
// connection for a single client.
type controllerWS struct {
	backend *backend.Backend
	conn    *wsConn
	logger  *slog.Logger

	// nextID is used to generate server-initiated request IDs (not
	// currently needed since the server only sends notifications).
	nextID atomic.Int64

	// subscribed tracks workspace IDs the client is subscribed to for
	// event push.
	subMu      sync.Mutex
	subscribed map[string]context.CancelFunc
}

// handleWebSocket upgrades the HTTP connection to WebSocket and then
// handles JSON-RPC 2.0 messages until the connection is closed.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	raw, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logError(r, "WebSocket upgrade failed", "error", err)
		return
	}
	defer raw.Close()

	c := &controllerWS{
		backend:    s.backend,
		conn:       &wsConn{conn: raw},
		logger:     s.logger,
		subscribed: make(map[string]context.CancelFunc),
	}
	c.serve(r.Context())
}

// serve reads JSON-RPC requests from the WebSocket connection and
// dispatches them.
func (c *controllerWS) serve(ctx context.Context) {
	for {
		_, msg, err := c.conn.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Debug("WebSocket read error", "error", err)
			}
			break
		}

		var req jrpc.Request
		if err := json.Unmarshal(msg, &req); err != nil {
			_ = c.conn.writeJSON(jrpc.ErrResponse(jrpc.ID{}, jrpc.CodeParseError, "parse error: "+err.Error()))
			continue
		}

		if req.JSONRPC != jrpc.Version {
			_ = c.conn.writeJSON(jrpc.ErrResponse(req.ID, jrpc.CodeInvalidRequest, "invalid JSON-RPC version"))
			continue
		}

		// Dispatch in a goroutine so one slow call doesn't block others.
		go c.dispatch(ctx, req)
	}

	// Cancel all active event subscriptions when the connection closes.
	c.subMu.Lock()
	for _, cancel := range c.subscribed {
		cancel()
	}
	c.subMu.Unlock()
}

// dispatch routes a single request to its handler.
func (c *controllerWS) dispatch(ctx context.Context, req jrpc.Request) {
	var (
		result any
		rpcErr *jrpc.Error
	)

	switch req.Method {
	// --- System ---
	case jrpc.MethodHealth:
		result = "ok"
	case jrpc.MethodVersion:
		result = c.backend.VersionInfo()
	case jrpc.MethodConfig:
		result = c.backend.Config()
	case jrpc.MethodControl:
		rpcErr = c.handleControl(req)

	// --- Workspaces ---
	case jrpc.MethodWorkspaceList:
		result = c.backend.ListWorkspaces()
	case jrpc.MethodWorkspaceGet:
		result, rpcErr = c.handleWorkspaceGet(req)
	case jrpc.MethodWorkspaceCreate:
		result, rpcErr = c.handleWorkspaceCreate(req)
	case jrpc.MethodWorkspaceDelete:
		rpcErr = c.handleWorkspaceDelete(req)
	case jrpc.MethodWorkspaceConfig:
		result, rpcErr = c.handleWorkspaceConfig(req)
	case jrpc.MethodWorkspaceProviders:
		result, rpcErr = c.handleWorkspaceProviders(req)

	// --- Sessions ---
	case jrpc.MethodSessionList:
		result, rpcErr = c.handleSessionList(ctx, req)
	case jrpc.MethodSessionGet:
		result, rpcErr = c.handleSessionGet(ctx, req)
	case jrpc.MethodSessionCreate:
		result, rpcErr = c.handleSessionCreate(ctx, req)
	case jrpc.MethodSessionSave:
		result, rpcErr = c.handleSessionSave(ctx, req)
	case jrpc.MethodSessionDelete:
		rpcErr = c.handleSessionDelete(ctx, req)

	// --- Messages ---
	case jrpc.MethodMessageList:
		result, rpcErr = c.handleMessageList(ctx, req)
	case jrpc.MethodMessageListUser:
		result, rpcErr = c.handleMessageListUser(ctx, req)
	case jrpc.MethodMessageListAllUser:
		result, rpcErr = c.handleMessageListAllUser(ctx, req)

	// --- Agent ---
	case jrpc.MethodAgentInfo:
		result, rpcErr = c.handleAgentInfo(req)
	case jrpc.MethodAgentSend:
		rpcErr = c.handleAgentSend(ctx, req)
	case jrpc.MethodAgentInit:
		rpcErr = c.handleAgentInit(ctx, req)
	case jrpc.MethodAgentUpdate:
		rpcErr = c.handleAgentUpdate(ctx, req)
	case jrpc.MethodAgentSessionGet:
		result, rpcErr = c.handleAgentSessionGet(ctx, req)
	case jrpc.MethodAgentSessionCancel:
		rpcErr = c.handleAgentSessionCancel(req)
	case jrpc.MethodAgentSessionSummarize:
		rpcErr = c.handleAgentSessionSummarize(ctx, req)
	case jrpc.MethodAgentQueuedCount:
		result, rpcErr = c.handleAgentQueuedCount(req)
	case jrpc.MethodAgentQueuedList:
		result, rpcErr = c.handleAgentQueuedList(req)
	case jrpc.MethodAgentQueueClear:
		rpcErr = c.handleAgentQueueClear(req)
	case jrpc.MethodAgentDefaultSmallModel:
		result, rpcErr = c.handleAgentDefaultSmallModel(req)

	// --- Permissions ---
	case jrpc.MethodPermissionsGrant:
		rpcErr = c.handlePermissionsGrant(req)
	case jrpc.MethodPermissionsSkip:
		rpcErr = c.handlePermissionsSkip(req)
	case jrpc.MethodPermissionsGetSkip:
		result, rpcErr = c.handlePermissionsGetSkip(req)

	// --- FileTracker ---
	case jrpc.MethodFileTrackerRead:
		rpcErr = c.handleFileTrackerRead(ctx, req)
	case jrpc.MethodFileTrackerLastRead:
		result, rpcErr = c.handleFileTrackerLastRead(ctx, req)
	case jrpc.MethodFileTrackerList:
		result, rpcErr = c.handleFileTrackerList(ctx, req)

	// --- History ---
	case jrpc.MethodHistoryList:
		result, rpcErr = c.handleHistoryList(ctx, req)

	// --- LSP ---
	case jrpc.MethodLSPList:
		result, rpcErr = c.handleLSPList(req)
	case jrpc.MethodLSPDiagnostics:
		result, rpcErr = c.handleLSPDiagnostics(req)
	case jrpc.MethodLSPStart:
		rpcErr = c.handleLSPStart(ctx, req)
	case jrpc.MethodLSPStopAll:
		rpcErr = c.handleLSPStopAll(ctx, req)

	// --- Config mutations ---
	case jrpc.MethodConfigSet:
		rpcErr = c.handleConfigSet(req)
	case jrpc.MethodConfigRemove:
		rpcErr = c.handleConfigRemove(req)
	case jrpc.MethodConfigModel:
		rpcErr = c.handleConfigModel(req)
	case jrpc.MethodConfigCompact:
		rpcErr = c.handleConfigCompact(req)
	case jrpc.MethodConfigProviderKey:
		rpcErr = c.handleConfigProviderKey(req)
	case jrpc.MethodConfigImportCopilot:
		result, rpcErr = c.handleConfigImportCopilot(req)
	case jrpc.MethodConfigRefreshOAuth:
		rpcErr = c.handleConfigRefreshOAuth(ctx, req)

	// --- Project ---
	case jrpc.MethodProjectNeedsInit:
		result, rpcErr = c.handleProjectNeedsInit(req)
	case jrpc.MethodProjectInit:
		rpcErr = c.handleProjectInit(req)
	case jrpc.MethodProjectInitPrompt:
		result, rpcErr = c.handleProjectInitPrompt(req)

	// --- MCP ---
	case jrpc.MethodMCPStates:
		result, rpcErr = c.handleMCPStates(req)
	case jrpc.MethodMCPRefreshTools:
		rpcErr = c.handleMCPRefreshTools(ctx, req)
	case jrpc.MethodMCPRefreshPrompts:
		rpcErr = c.handleMCPRefreshPrompts(ctx, req)
	case jrpc.MethodMCPRefreshResources:
		rpcErr = c.handleMCPRefreshResources(ctx, req)
	case jrpc.MethodMCPReadResource:
		result, rpcErr = c.handleMCPReadResource(ctx, req)
	case jrpc.MethodMCPGetPrompt:
		result, rpcErr = c.handleMCPGetPrompt(req)
	case jrpc.MethodMCPEnableDocker:
		rpcErr = c.handleMCPEnableDocker(ctx, req)
	case jrpc.MethodMCPDisableDocker:
		rpcErr = c.handleMCPDisableDocker(req)

	// --- Events ---
	case jrpc.MethodEventsSubscribe:
		rpcErr = c.handleEventsSubscribe(ctx, req)
	case jrpc.MethodEventsUnsubscribe:
		rpcErr = c.handleEventsUnsubscribe(req)

	default:
		rpcErr = &jrpc.Error{Code: jrpc.CodeMethodNotFound, Message: fmt.Sprintf("method not found: %s", req.Method)}
	}

	// Notifications do not get a response.
	if req.IsNotification() {
		return
	}

	var resp *jrpc.Response
	if rpcErr != nil {
		resp = jrpc.ErrResponse(req.ID, rpcErr.Code, rpcErr.Message)
	} else {
		var err error
		resp, err = jrpc.OKResponse(req.ID, result)
		if err != nil {
			resp = jrpc.ErrResponse(req.ID, jrpc.CodeInternalError, "marshal error: "+err.Error())
		}
	}
	if err := c.conn.writeJSON(resp); err != nil {
		slog.Debug("WebSocket write error", "error", err)
	}
}

// --- helper to decode params ---

func decodeParams(req jrpc.Request, dst any) *jrpc.Error {
	if err := json.Unmarshal(req.Params, dst); err != nil {
		return &jrpc.Error{Code: jrpc.CodeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	return nil
}

func backendErr(err error) *jrpc.Error {
	switch {
	case err == nil:
		return nil
	default:
		return &jrpc.Error{Code: jrpc.CodeInternalError, Message: err.Error()}
	}
}

// --- System handlers ---

func (c *controllerWS) handleControl(req jrpc.Request) *jrpc.Error {
	var cmd proto.ServerControl
	if e := decodeParams(req, &cmd); e != nil {
		return e
	}
	switch cmd.Command {
	case "shutdown":
		c.backend.Shutdown()
	default:
		return &jrpc.Error{Code: jrpc.CodeInvalidParams, Message: "unknown command: " + cmd.Command}
	}
	return nil
}

// --- Workspace handlers ---

type workspaceIDParams struct {
	ID string `json:"id"`
}

func (c *controllerWS) handleWorkspaceGet(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	ws, err := c.backend.GetWorkspaceProto(p.ID)
	return ws, backendErr(err)
}

func (c *controllerWS) handleWorkspaceCreate(req jrpc.Request) (any, *jrpc.Error) {
	var args proto.Workspace
	if e := decodeParams(req, &args); e != nil {
		return nil, e
	}
	_, result, err := c.backend.CreateWorkspace(args)
	return result, backendErr(err)
}

func (c *controllerWS) handleWorkspaceDelete(req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	c.backend.DeleteWorkspace(p.ID)
	return nil
}

func (c *controllerWS) handleWorkspaceConfig(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	cfg, err := c.backend.GetWorkspaceConfig(p.ID)
	return cfg, backendErr(err)
}

func (c *controllerWS) handleWorkspaceProviders(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	result, err := c.backend.GetWorkspaceProviders(p.ID)
	return result, backendErr(err)
}

// --- Session handlers ---

func (c *controllerWS) handleSessionList(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	sessions, err := c.backend.ListSessions(ctx, p.ID)
	if err != nil {
		return nil, backendErr(err)
	}
	result := make([]proto.Session, len(sessions))
	for i, s := range sessions {
		result[i] = sessionToProto(s)
	}
	return result, nil
}

func (c *controllerWS) handleSessionGet(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	sess, err := c.backend.GetSession(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, backendErr(err)
	}
	return sessionToProto(sess), nil
}

func (c *controllerWS) handleSessionCreate(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID    string `json:"id"`
		Title string `json:"title"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	sess, err := c.backend.CreateSession(ctx, p.ID, p.Title)
	if err != nil {
		return nil, backendErr(err)
	}
	return sessionToProto(sess), nil
}

func (c *controllerWS) handleSessionSave(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID      string        `json:"id"`
		Session proto.Session `json:"session"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	internal := session.Session{
		ID:               p.Session.ID,
		ParentSessionID:  p.Session.ParentSessionID,
		Title:            p.Session.Title,
		SummaryMessageID: p.Session.SummaryMessageID,
		MessageCount:     p.Session.MessageCount,
		PromptTokens:     p.Session.PromptTokens,
		CompletionTokens: p.Session.CompletionTokens,
		Cost:             p.Session.Cost,
		CreatedAt:        p.Session.CreatedAt,
		UpdatedAt:        p.Session.UpdatedAt,
	}
	sess, err := c.backend.SaveSession(ctx, p.ID, internal)
	if err != nil {
		return nil, backendErr(err)
	}
	return sessionToProto(sess), nil
}

func (c *controllerWS) handleSessionDelete(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.DeleteSession(ctx, p.ID, p.SessionID))
}

// --- Message handlers ---

func (c *controllerWS) handleMessageList(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	msgs, err := c.backend.ListSessionMessages(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, backendErr(err)
	}
	return messagesToProto(msgs), nil
}

func (c *controllerWS) handleMessageListUser(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	msgs, err := c.backend.ListUserMessages(ctx, p.ID, p.SessionID)
	if err != nil {
		return nil, backendErr(err)
	}
	return messagesToProto(msgs), nil
}

func (c *controllerWS) handleMessageListAllUser(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	msgs, err := c.backend.ListAllUserMessages(ctx, p.ID)
	if err != nil {
		return nil, backendErr(err)
	}
	return messagesToProto(msgs), nil
}

// --- Agent handlers ---

func (c *controllerWS) handleAgentInfo(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	info, err := c.backend.GetAgentInfo(p.ID)
	return info, backendErr(err)
}

func (c *controllerWS) handleAgentSend(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string             `json:"id"`
		Msg proto.AgentMessage `json:"msg"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.SendMessage(ctx, p.ID, p.Msg))
}

func (c *controllerWS) handleAgentInit(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.InitAgent(ctx, p.ID))
}

func (c *controllerWS) handleAgentUpdate(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.UpdateAgent(ctx, p.ID))
}

func (c *controllerWS) handleAgentSessionGet(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	sess, err := c.backend.GetAgentSession(ctx, p.ID, p.SessionID)
	return sess, backendErr(err)
}

func (c *controllerWS) handleAgentSessionCancel(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.CancelSession(p.ID, p.SessionID))
}

func (c *controllerWS) handleAgentSessionSummarize(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.SummarizeSession(ctx, p.ID, p.SessionID))
}

func (c *controllerWS) handleAgentQueuedCount(req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	count, err := c.backend.QueuedPrompts(p.ID, p.SessionID)
	return count, backendErr(err)
}

func (c *controllerWS) handleAgentQueuedList(req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	list, err := c.backend.QueuedPromptsList(p.ID, p.SessionID)
	return list, backendErr(err)
}

func (c *controllerWS) handleAgentQueueClear(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.ClearQueue(p.ID, p.SessionID))
}

func (c *controllerWS) handleAgentDefaultSmallModel(req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID         string `json:"id"`
		ProviderID string `json:"provider_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	model, err := c.backend.GetDefaultSmallModel(p.ID, p.ProviderID)
	return model, backendErr(err)
}

// --- Permission handlers ---

func (c *controllerWS) handlePermissionsGrant(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID    string                `json:"id"`
		Grant proto.PermissionGrant `json:"grant"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.GrantPermission(p.ID, p.Grant))
}

func (c *controllerWS) handlePermissionsSkip(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID   string `json:"id"`
		Skip bool   `json:"skip"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.SetPermissionsSkip(p.ID, p.Skip))
}

func (c *controllerWS) handlePermissionsGetSkip(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	skip, err := c.backend.GetPermissionsSkip(p.ID)
	return skip, backendErr(err)
}

// --- FileTracker handlers ---

func (c *controllerWS) handleFileTrackerRead(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
		Path      string `json:"path"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.FileTrackerRecordRead(ctx, p.ID, p.SessionID, p.Path))
}

func (c *controllerWS) handleFileTrackerLastRead(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
		Path      string `json:"path"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	t, err := c.backend.FileTrackerLastReadTime(ctx, p.ID, p.SessionID, p.Path)
	return t, backendErr(err)
}

func (c *controllerWS) handleFileTrackerList(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	files, err := c.backend.FileTrackerListReadFiles(ctx, p.ID, p.SessionID)
	return files, backendErr(err)
}

// --- History handler ---

func (c *controllerWS) handleHistoryList(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID        string `json:"id"`
		SessionID string `json:"session_id"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	files, err := c.backend.ListSessionHistory(ctx, p.ID, p.SessionID)
	return files, backendErr(err)
}

// --- LSP handlers ---

func (c *controllerWS) handleLSPList(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	states, err := c.backend.GetLSPStates(p.ID)
	if err != nil {
		return nil, backendErr(err)
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

func (c *controllerWS) handleLSPDiagnostics(req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID      string `json:"id"`
		LSPName string `json:"lsp_name"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	diags, err := c.backend.GetLSPDiagnostics(p.ID, p.LSPName)
	return diags, backendErr(err)
}

func (c *controllerWS) handleLSPStart(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.LSPStart(ctx, p.ID, p.Path))
}

func (c *controllerWS) handleLSPStopAll(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.LSPStopAll(ctx, p.ID))
}

// --- Config mutation handlers ---

func (c *controllerWS) handleConfigSet(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string                 `json:"id"`
		Req proto.ConfigSetRequest `json:"req"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.SetConfigField(p.ID, p.Req.Scope, p.Req.Key, p.Req.Value))
}

func (c *controllerWS) handleConfigRemove(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string                    `json:"id"`
		Req proto.ConfigRemoveRequest `json:"req"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.RemoveConfigField(p.ID, p.Req.Scope, p.Req.Key))
}

func (c *controllerWS) handleConfigModel(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string                   `json:"id"`
		Req proto.ConfigModelRequest `json:"req"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.UpdatePreferredModel(p.ID, p.Req.Scope, p.Req.ModelType, p.Req.Model))
}

func (c *controllerWS) handleConfigCompact(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string                     `json:"id"`
		Req proto.ConfigCompactRequest `json:"req"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.SetCompactMode(p.ID, p.Req.Scope, p.Req.Enabled))
}

func (c *controllerWS) handleConfigProviderKey(req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string                         `json:"id"`
		Req proto.ConfigProviderKeyRequest `json:"req"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.SetProviderAPIKey(p.ID, p.Req.Scope, p.Req.ProviderID, p.Req.APIKey))
}

func (c *controllerWS) handleConfigImportCopilot(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	token, ok, err := c.backend.ImportCopilot(p.ID)
	if err != nil {
		return nil, backendErr(err)
	}
	return proto.ImportCopilotResponse{Token: token, Success: ok}, nil
}

func (c *controllerWS) handleConfigRefreshOAuth(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID  string                          `json:"id"`
		Req proto.ConfigRefreshOAuthRequest `json:"req"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.RefreshOAuthToken(ctx, p.ID, p.Req.Scope, p.Req.ProviderID))
}

// --- Project handlers ---

func (c *controllerWS) handleProjectNeedsInit(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	needs, err := c.backend.ProjectNeedsInitialization(p.ID)
	if err != nil {
		return nil, backendErr(err)
	}
	return proto.ProjectNeedsInitResponse{NeedsInit: needs}, nil
}

func (c *controllerWS) handleProjectInit(req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.MarkProjectInitialized(p.ID))
}

func (c *controllerWS) handleProjectInitPrompt(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	prompt, err := c.backend.InitializePrompt(p.ID)
	if err != nil {
		return nil, backendErr(err)
	}
	return proto.ProjectInitPromptResponse{Prompt: prompt}, nil
}

// --- MCP handlers ---

func (c *controllerWS) handleMCPStates(req jrpc.Request) (any, *jrpc.Error) {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	states := c.backend.MCPGetStates(p.ID)
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

func (c *controllerWS) handleMCPRefreshTools(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.RefreshMCPTools(ctx, p.ID, p.Name))
}

func (c *controllerWS) handleMCPRefreshPrompts(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	c.backend.MCPRefreshPrompts(ctx, p.ID, p.Name)
	return nil
}

func (c *controllerWS) handleMCPRefreshResources(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	c.backend.MCPRefreshResources(ctx, p.ID, p.Name)
	return nil
}

func (c *controllerWS) handleMCPReadResource(ctx context.Context, req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		URI  string `json:"uri"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	contents, err := c.backend.ReadMCPResource(ctx, p.ID, p.Name, p.URI)
	return contents, backendErr(err)
}

func (c *controllerWS) handleMCPGetPrompt(req jrpc.Request) (any, *jrpc.Error) {
	var p struct {
		ID       string            `json:"id"`
		ClientID string            `json:"client_id"`
		PromptID string            `json:"prompt_id"`
		Args     map[string]string `json:"args"`
	}
	if e := decodeParams(req, &p); e != nil {
		return nil, e
	}
	prompt, err := c.backend.GetMCPPrompt(p.ID, p.ClientID, p.PromptID, p.Args)
	if err != nil {
		return nil, backendErr(err)
	}
	return proto.MCPGetPromptResponse{Prompt: prompt}, nil
}

func (c *controllerWS) handleMCPEnableDocker(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.EnableDockerMCP(ctx, p.ID))
}

func (c *controllerWS) handleMCPDisableDocker(req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	return backendErr(c.backend.DisableDockerMCP(p.ID))
}

// --- Event subscription handlers ---

// handleEventsSubscribe subscribes the WebSocket client to push
// notifications for a workspace. Events are forwarded as JSON-RPC
// notifications with method "events.push".
func (c *controllerWS) handleEventsSubscribe(ctx context.Context, req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}

	c.subMu.Lock()
	if _, already := c.subscribed[p.ID]; already {
		c.subMu.Unlock()
		return nil // Already subscribed — no error.
	}
	subCtx, cancel := context.WithCancel(ctx)
	c.subscribed[p.ID] = cancel
	c.subMu.Unlock()

	events, err := c.backend.SubscribeEvents(subCtx, p.ID)
	if err != nil {
		cancel()
		c.subMu.Lock()
		delete(c.subscribed, p.ID)
		c.subMu.Unlock()
		return backendErr(err)
	}

	go func() {
		for ev := range events {
			wrapped := wrapEvent(ev.Payload)
			if wrapped == nil {
				continue
			}
			notif, err := jrpc.NewNotification(jrpc.MethodEventPush, wrapped)
			if err != nil {
				slog.Error("Failed to create event notification",
					"workspace", p.ID, "event_type", fmt.Sprintf("%T", ev.Payload), "error", err)
				continue
			}
			if err := c.conn.writeJSON(notif); err != nil {
				slog.Debug("WebSocket notification write error", "error", err)
				break
			}
		}
	}()

	return nil
}

func (c *controllerWS) handleEventsUnsubscribe(req jrpc.Request) *jrpc.Error {
	var p workspaceIDParams
	if e := decodeParams(req, &p); e != nil {
		return e
	}
	c.subMu.Lock()
	if cancel, ok := c.subscribed[p.ID]; ok {
		cancel()
		delete(c.subscribed, p.ID)
	}
	c.subMu.Unlock()
	return nil
}
