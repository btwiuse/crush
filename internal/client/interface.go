package client

import (
	"context"
	"time"

	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/message"
	"github.com/charmbracelet/crush/internal/oauth"
	"github.com/charmbracelet/crush/internal/proto"
	"github.com/charmbracelet/x/powernap/pkg/lsp/protocol"
)

// ServerClient is the interface implemented by both the HTTP Client and the
// WebSocket RpcClient. It provides the full set of operations that the
// Crush server supports.
type ServerClient interface {
	Path() string
	GetGlobalConfig(ctx context.Context) (*config.Config, error)
	Health(ctx context.Context) error
	VersionInfo(ctx context.Context) (*proto.VersionInfo, error)
	ShutdownServer(ctx context.Context) error

	// Workspace lifecycle.
	ListWorkspaces(ctx context.Context) ([]proto.Workspace, error)
	CreateWorkspace(ctx context.Context, ws proto.Workspace) (*proto.Workspace, error)
	GetWorkspace(ctx context.Context, id string) (*proto.Workspace, error)
	DeleteWorkspace(ctx context.Context, id string) error
	SubscribeEvents(ctx context.Context, id string) (<-chan any, error)

	// Sessions.
	CreateSession(ctx context.Context, id string, title string) (*proto.Session, error)
	GetSession(ctx context.Context, id string, sessionID string) (*proto.Session, error)
	SaveSession(ctx context.Context, id string, sess proto.Session) (*proto.Session, error)
	DeleteSession(ctx context.Context, id string, sessionID string) error
	ListSessions(ctx context.Context, id string) ([]proto.Session, error)
	ListSessionHistoryFiles(ctx context.Context, id string, sessionID string) ([]proto.File, error)

	// Messages.
	ListMessages(ctx context.Context, id string, sessionID string) ([]proto.Message, error)
	ListUserMessages(ctx context.Context, id string, sessionID string) ([]proto.Message, error)
	ListAllUserMessages(ctx context.Context, id string) ([]proto.Message, error)

	// Agent.
	SendMessage(ctx context.Context, id string, sessionID, prompt string, attachments ...message.Attachment) error
	GetAgentInfo(ctx context.Context, id string) (*proto.AgentInfo, error)
	GetAgentSessionInfo(ctx context.Context, id string, sessionID string) (*proto.AgentSession, error)
	UpdateAgent(ctx context.Context, id string) error
	InitiateAgentProcessing(ctx context.Context, id string) error
	CancelAgentSession(ctx context.Context, id string, sessionID string) error
	AgentSummarizeSession(ctx context.Context, id string, sessionID string) error
	GetAgentSessionQueuedPrompts(ctx context.Context, id string, sessionID string) (int, error)
	GetAgentSessionQueuedPromptsList(ctx context.Context, id string, sessionID string) ([]string, error)
	ClearAgentSessionQueuedPrompts(ctx context.Context, id string, sessionID string) error
	GetDefaultSmallModel(ctx context.Context, id string, providerID string) (*config.SelectedModel, error)

	// Permissions.
	GrantPermission(ctx context.Context, id string, req proto.PermissionGrant) error
	SetPermissionsSkipRequests(ctx context.Context, id string, skip bool) error
	GetPermissionsSkipRequests(ctx context.Context, id string) (bool, error)

	// File tracker.
	FileTrackerRecordRead(ctx context.Context, id string, sessionID, path string) error
	FileTrackerLastReadTime(ctx context.Context, id string, sessionID, path string) (time.Time, error)
	FileTrackerListReadFiles(ctx context.Context, id string, sessionID string) ([]string, error)

	// LSP.
	GetLSPDiagnostics(ctx context.Context, id string, lspName string) (map[protocol.DocumentURI][]protocol.Diagnostic, error)
	GetLSPs(ctx context.Context, id string) (map[string]proto.LSPClientInfo, error)
	LSPStart(ctx context.Context, id string, path string) error
	LSPStopAll(ctx context.Context, id string) error

	// Config.
	GetConfig(ctx context.Context, id string) (*config.Config, error)
	SetConfigField(ctx context.Context, id string, scope config.Scope, key string, value any) error
	RemoveConfigField(ctx context.Context, id string, scope config.Scope, key string) error
	UpdatePreferredModel(ctx context.Context, id string, scope config.Scope, modelType config.SelectedModelType, model config.SelectedModel) error
	SetCompactMode(ctx context.Context, id string, scope config.Scope, enabled bool) error
	SetProviderAPIKey(ctx context.Context, id string, scope config.Scope, providerID string, apiKey any) error
	ImportCopilot(ctx context.Context, id string) (*oauth.Token, bool, error)
	RefreshOAuthToken(ctx context.Context, id string, scope config.Scope, providerID string) error

	// Project.
	ProjectNeedsInitialization(ctx context.Context, id string) (bool, error)
	MarkProjectInitialized(ctx context.Context, id string) error
	GetInitializePrompt(ctx context.Context, id string) (string, error)

	// MCP.
	MCPGetStates(ctx context.Context, id string) (map[string]proto.MCPClientInfo, error)
	MCPRefreshPrompts(ctx context.Context, id, name string) error
	MCPRefreshResources(ctx context.Context, id, name string) error
	RefreshMCPTools(ctx context.Context, id, name string) error
	ReadMCPResource(ctx context.Context, id, name, uri string) ([]MCPResourceContents, error)
	GetMCPPrompt(ctx context.Context, id, clientID, promptID string, args map[string]string) (string, error)
	EnableDockerMCP(ctx context.Context, id string) error
	DisableDockerMCP(ctx context.Context, id string) error
}
