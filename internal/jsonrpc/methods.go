package jsonrpc

// Method names for all JSON-RPC 2.0 operations exposed over the
// WebSocket endpoint.
const (
	// System.
	MethodHealth  = "system.health"
	MethodVersion = "system.version"
	MethodConfig  = "system.config"
	MethodControl = "system.control"

	// Workspaces.
	MethodWorkspaceList   = "workspace.list"
	MethodWorkspaceGet    = "workspace.get"
	MethodWorkspaceCreate = "workspace.create"
	MethodWorkspaceDelete = "workspace.delete"
	MethodWorkspaceConfig = "workspace.config"

	// Sessions.
	MethodSessionList   = "session.list"
	MethodSessionGet    = "session.get"
	MethodSessionCreate = "session.create"
	MethodSessionSave   = "session.save"
	MethodSessionDelete = "session.delete"

	// Messages.
	MethodMessageList        = "message.list"
	MethodMessageListUser    = "message.list_user"
	MethodMessageListAllUser = "message.list_all_user"

	// Agent.
	MethodAgentInfo              = "agent.info"
	MethodAgentSend              = "agent.send"
	MethodAgentInit              = "agent.init"
	MethodAgentUpdate            = "agent.update"
	MethodAgentSessionGet        = "agent.session.get"
	MethodAgentSessionCancel     = "agent.session.cancel"
	MethodAgentSessionSummarize  = "agent.session.summarize"
	MethodAgentQueuedCount       = "agent.queue.count"
	MethodAgentQueuedList        = "agent.queue.list"
	MethodAgentQueueClear        = "agent.queue.clear"
	MethodAgentDefaultSmallModel = "agent.default_small_model"

	// Permissions.
	MethodPermissionsGrant   = "permissions.grant"
	MethodPermissionsSkip    = "permissions.skip"
	MethodPermissionsGetSkip = "permissions.get_skip"

	// FileTracker.
	MethodFileTrackerRead     = "filetracker.read"
	MethodFileTrackerLastRead = "filetracker.last_read"
	MethodFileTrackerList     = "filetracker.list"

	// History.
	MethodHistoryList = "history.list"

	// LSP.
	MethodLSPList        = "lsp.list"
	MethodLSPDiagnostics = "lsp.diagnostics"
	MethodLSPStart       = "lsp.start"
	MethodLSPStopAll     = "lsp.stop_all"

	// Config mutations.
	MethodConfigSet           = "config.set"
	MethodConfigRemove        = "config.remove"
	MethodConfigModel         = "config.model"
	MethodConfigCompact       = "config.compact"
	MethodConfigProviderKey   = "config.provider_key"
	MethodConfigImportCopilot = "config.import_copilot"
	MethodConfigRefreshOAuth  = "config.refresh_oauth"

	// Project.
	MethodProjectNeedsInit  = "project.needs_init"
	MethodProjectInit       = "project.init"
	MethodProjectInitPrompt = "project.init_prompt"

	// MCP.
	MethodMCPStates           = "mcp.states"
	MethodMCPRefreshTools     = "mcp.refresh_tools"
	MethodMCPRefreshPrompts   = "mcp.refresh_prompts"
	MethodMCPRefreshResources = "mcp.refresh_resources"
	MethodMCPReadResource     = "mcp.read_resource"
	MethodMCPGetPrompt        = "mcp.get_prompt"
	MethodMCPEnableDocker     = "mcp.docker.enable"
	MethodMCPDisableDocker    = "mcp.docker.disable"

	// Providers.
	MethodWorkspaceProviders = "workspace.providers"

	// Event subscription (server-to-client notifications use the same
	// payload type constants from pubsub, but these method names are
	// used for the subscription call and for push notifications).
	MethodEventsSubscribe   = "events.subscribe"
	MethodEventsUnsubscribe = "events.unsubscribe"
	MethodEventPush         = "events.push"
)
