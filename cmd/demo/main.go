// Package main is a minimal standalone demo of the Crush TUI.
//
// It stubs out file access and other backend services with mock data
// but calls the configured LLM provider (deepseek) for real model
// responses for a more interactive demo experience.
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"charm.land/fantasy"
	"charm.land/fantasy/providers/openaicompat"
	"github.com/btwiuse/boba"
	tea "charm.land/bubbletea/v2"
	mcptools "github.com/charmbracelet/crush/internal/agent/tools/mcp"
	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/csync"
	"github.com/charmbracelet/crush/internal/history"
	"github.com/charmbracelet/crush/internal/lsp"
	"github.com/charmbracelet/crush/internal/message"
	"github.com/charmbracelet/crush/internal/oauth"
	"github.com/charmbracelet/crush/internal/permission"
	"github.com/charmbracelet/crush/internal/program"
	"github.com/charmbracelet/crush/internal/pubsub"
	"github.com/charmbracelet/crush/internal/session"
	"github.com/charmbracelet/crush/internal/ui/common"
	ui "github.com/charmbracelet/crush/internal/ui/model"
	"github.com/charmbracelet/crush/internal/ui/styles"
	"github.com/charmbracelet/crush/internal/workspace"
	"github.com/google/uuid"
)

func main() {
	ws := newMockWorkspace()
	sty := styles.ThemeForProvider("")
	com := &common.Common{
		Workspace: ws,
		Styles:    &sty,
	}

	model := ui.New(com, "", false)

	program := boba.NewProgram(
		model,
		tea.WithFilter(ui.MouseEventFilter),
	)

	// Subscribe sends workspace events to the TUI as tea.Msgs.
	// The mock workspace does nothing here, but the call is required
	// to satisfy the interface.
	go ws.Subscribe(program)

	if _, err := program.Run(); err != nil {
		os.Exit(1)
	}
}

// ── mock config ──────────────────────────────────────────────────────────────

// newMockConfig returns a minimal *config.Config that satisfies:
//   - IsConfigured() == true  (at least one enabled provider)
//   - non-nil Options and Options.TUI  (required by ui.New)
func newMockConfig() *config.Config {
	providers := csync.NewMap[string, config.ProviderConfig]()
	providers.Set("mock", config.ProviderConfig{
		ID:   "mock",
		Name: "Mock Provider (demo)",
	})

	return &config.Config{
		Providers: providers,
		Models: map[config.SelectedModelType]config.SelectedModel{
			config.SelectedModelTypeLarge: {
				Provider: "deepseek",
				Model:    "deepseek-chat",
			},
			config.SelectedModelTypeSmall: {
				Provider: "deepseek",
				Model:    "deepseek-chat",
			},
		},
		RecentModels: map[config.SelectedModelType][]config.SelectedModel{},
		MCP:          config.MCPs{},
		LSP:          config.LSPs{},
		Options: &config.Options{
			TUI: &config.TUIOptions{},
		},
		Agents: map[string]config.Agent{
			config.AgentCoder: {
				ID:   config.AgentCoder,
				Name: "Coder",
			},
		},
	}
}

// ── mock workspace ───────────────────────────────────────────────────────────

type mockWorkspace struct {
	cfg             *config.Config
	prog            program.Program
	mu              sync.Mutex
	sessionMessages map[string][]message.Message
}

func newMockWorkspace() *mockWorkspace {
	return &mockWorkspace{
		cfg:             newMockConfig(),
		sessionMessages: make(map[string][]message.Message),
	}
}

// -- Sessions --

var mockSession = session.Session{
	ID:        "mock-session-id",
	Title:     "title",
	CreatedAt: time.Now().UnixMilli(),
	UpdatedAt: time.Now().UnixMilli(),
}

func (w *mockWorkspace) CreateSession(_ context.Context, title string) (session.Session, error) {
	mockSession.Title = title
	return mockSession, nil
}

func (w *mockWorkspace) GetSession(_ context.Context, _ string) (session.Session, error) {
	return mockSession, nil
}

func (w *mockWorkspace) ListSessions(_ context.Context) ([]session.Session, error) {
	return []session.Session{mockSession}, nil
}

func (w *mockWorkspace) SaveSession(_ context.Context, sess session.Session) (session.Session, error) {
	return sess, nil
}

func (w *mockWorkspace) DeleteSession(_ context.Context, _ string) error { return nil }

func (w *mockWorkspace) CreateAgentToolSessionID(messageID, toolCallID string) string {
	return messageID + ":" + toolCallID
}

func (w *mockWorkspace) ParseAgentToolSessionID(sessionID string) (string, string, bool) {
	return "", "", false
}

// -- Messages --

func (w *mockWorkspace) ListMessages(_ context.Context, sessionID string) ([]message.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	msgs := w.sessionMessages[sessionID]
	result := make([]message.Message, len(msgs))
	copy(result, msgs)
	return result, nil
}

func (w *mockWorkspace) ListUserMessages(_ context.Context, _ string) ([]message.Message, error) {
	return nil, nil
}

func (w *mockWorkspace) ListAllUserMessages(_ context.Context) ([]message.Message, error) {
	return nil, nil
}

// -- Agent --

func (w *mockWorkspace) AgentRun(ctx context.Context, sessionID, content string, _ ...message.Attachment) error {
	// Load existing messages from previous turns.
	w.mu.Lock()
	existing := make([]message.Message, len(w.sessionMessages[sessionID]))
	copy(existing, w.sessionMessages[sessionID])
	w.mu.Unlock()

	// Publish user message.
	userMsg := message.Message{
		ID:        uuid.New().String(),
		SessionID: sessionID,
		Role:      message.User,
		Parts:     []message.ContentPart{message.TextContent{Text: content}},
		CreatedAt: time.Now().UnixMilli(),
	}
	w.prog.Send(pubsub.Event[message.Message]{
		Type:    pubsub.CreatedEvent,
		Payload: userMsg,
	})
	w.mu.Lock()
	w.sessionMessages[sessionID] = append(w.sessionMessages[sessionID], userMsg)
	w.mu.Unlock()

	// Publish an empty assistant message.
	assistantID := uuid.New().String()
	assistantMsg := message.Message{
		ID:        assistantID,
		SessionID: sessionID,
		Role:      message.Assistant,
		Parts:     []message.ContentPart{},
		CreatedAt: time.Now().UnixMilli(),
	}
	w.prog.Send(pubsub.Event[message.Message]{
		Type:    pubsub.CreatedEvent,
		Payload: assistantMsg,
	})

	// Build full fantasy prompt: previous turns + current user message.
	var fantasyMsgs []fantasy.Message
	for _, m := range existing {
		fantasyMsgs = append(fantasyMsgs, m.ToAIMessage()...)
	}
	fantasyMsgs = append(fantasyMsgs, fantasy.Message{
		Role: fantasy.MessageRoleUser,
		Content: []fantasy.MessagePart{
			fantasy.TextPart{Text: content},
		},
	})

	// Build the DeepSeek provider and call the LLM.
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	provider, err := openaicompat.New(
		openaicompat.WithBaseURL("https://api.deepseek.com/v1"),
		openaicompat.WithAPIKey(apiKey),
	)
	if err != nil {
		return fmt.Errorf("creating deepseek provider: %w", err)
	}

	llm, err := provider.LanguageModel(ctx, "deepseek-chat")
	if err != nil {
		return fmt.Errorf("getting language model: %w", err)
	}

	stream, err := llm.Stream(ctx, fantasy.Call{
		Prompt: fantasyMsgs,
	})
	if err != nil {
		return fmt.Errorf("starting LLM stream: %w", err)
	}

	sentFinish := false
	for part := range stream {
		switch part.Type {
		case fantasy.StreamPartTypeTextDelta:
			assistantMsg.AppendContent(part.Delta)
			w.prog.Send(pubsub.Event[message.Message]{
				Type:    pubsub.UpdatedEvent,
				Payload: assistantMsg,
			})
		case fantasy.StreamPartTypeError:
			return fmt.Errorf("stream error: %w", part.Error)
		case fantasy.StreamPartTypeFinish:
			sentFinish = true
			assistantMsg.AddFinish(message.FinishReasonEndTurn, "", "")
			w.prog.Send(pubsub.Event[message.Message]{
				Type:    pubsub.UpdatedEvent,
				Payload: assistantMsg,
			})
		}
	}

	if !sentFinish {
		assistantMsg.AddFinish(message.FinishReasonEndTurn, "", "")
		w.prog.Send(pubsub.Event[message.Message]{
			Type:    pubsub.UpdatedEvent,
			Payload: assistantMsg,
		})
	}

	// Save the assistant message for future turns.
	w.mu.Lock()
	w.sessionMessages[sessionID] = append(w.sessionMessages[sessionID], assistantMsg)
	w.mu.Unlock()

	return nil
}

func (w *mockWorkspace) AgentCancel(_ string) {}

func (w *mockWorkspace) AgentIsBusy() bool { return false }

func (w *mockWorkspace) AgentIsSessionBusy(_ string) bool { return false }

func (w *mockWorkspace) AgentModel() workspace.AgentModel {
	return workspace.AgentModel{}
}

func (w *mockWorkspace) AgentIsReady() bool { return true }

func (w *mockWorkspace) AgentQueuedPrompts(_ string) int { return 0 }

func (w *mockWorkspace) AgentQueuedPromptsList(_ string) []string { return nil }

func (w *mockWorkspace) AgentClearQueue(_ string) {}

func (w *mockWorkspace) AgentSummarize(_ context.Context, _ string) error { return nil }

func (w *mockWorkspace) UpdateAgentModel(_ context.Context) error { return nil }

func (w *mockWorkspace) InitCoderAgent(_ context.Context) error { return nil }

func (w *mockWorkspace) GetDefaultSmallModel(_ string) config.SelectedModel {
	return config.SelectedModel{}
}

// -- Permissions --

func (w *mockWorkspace) PermissionGrant(_ permission.PermissionRequest) {}

func (w *mockWorkspace) PermissionGrantPersistent(_ permission.PermissionRequest) {}

func (w *mockWorkspace) PermissionDeny(_ permission.PermissionRequest) {}

func (w *mockWorkspace) PermissionSkipRequests() bool { return false }

func (w *mockWorkspace) PermissionSetSkipRequests(_ bool) {}

// -- FileTracker --

func (w *mockWorkspace) FileTrackerRecordRead(_ context.Context, _, _ string) {}

func (w *mockWorkspace) FileTrackerLastReadTime(_ context.Context, _, _ string) time.Time {
	return time.Time{}
}

func (w *mockWorkspace) FileTrackerListReadFiles(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

// -- History --

func (w *mockWorkspace) ListSessionHistory(_ context.Context, _ string) ([]history.File, error) {
	return nil, nil
}

// -- LSP --

func (w *mockWorkspace) LSPStart(_ context.Context, _ string) {}

func (w *mockWorkspace) LSPStopAll(_ context.Context) {}

func (w *mockWorkspace) LSPGetStates() map[string]workspace.LSPClientInfo {
	return map[string]workspace.LSPClientInfo{}
}

func (w *mockWorkspace) LSPGetDiagnosticCounts(_ string) lsp.DiagnosticCounts {
	return lsp.DiagnosticCounts{}
}

// -- Config --

func (w *mockWorkspace) Config() *config.Config { return w.cfg }

func (w *mockWorkspace) WorkingDir() string {
	if dir, err := os.Getwd(); err == nil {
		return dir
	}
	return "."
}

func (w *mockWorkspace) Resolver() config.VariableResolver {
	return config.IdentityResolver()
}

// -- Config mutations --

func (w *mockWorkspace) UpdatePreferredModel(_ config.Scope, _ config.SelectedModelType, _ config.SelectedModel) error {
	return nil
}

func (w *mockWorkspace) SetCompactMode(_ config.Scope, _ bool) error { return nil }

func (w *mockWorkspace) SetProviderAPIKey(_ config.Scope, _ string, _ any) error { return nil }

func (w *mockWorkspace) SetConfigField(_ config.Scope, _ string, _ any) error { return nil }

func (w *mockWorkspace) RemoveConfigField(_ config.Scope, _ string) error { return nil }

func (w *mockWorkspace) ImportCopilot() (*oauth.Token, bool) { return nil, false }

func (w *mockWorkspace) RefreshOAuthToken(_ context.Context, _ config.Scope, _ string) error {
	return nil
}

// -- Project lifecycle --

func (w *mockWorkspace) ProjectNeedsInitialization() (bool, error) { return false, nil }

func (w *mockWorkspace) MarkProjectInitialized() error { return nil }

func (w *mockWorkspace) InitializePrompt() (string, error) { return "", nil }

// -- MCP --

func (w *mockWorkspace) MCPGetStates() map[string]mcptools.ClientInfo {
	return map[string]mcptools.ClientInfo{}
}

func (w *mockWorkspace) MCPRefreshPrompts(_ context.Context, _ string) {}

func (w *mockWorkspace) MCPRefreshResources(_ context.Context, _ string) {}

func (w *mockWorkspace) RefreshMCPTools(_ context.Context, _ string) {}

func (w *mockWorkspace) ReadMCPResource(_ context.Context, _, _ string) ([]workspace.MCPResourceContents, error) {
	return nil, nil
}

func (w *mockWorkspace) GetMCPPrompt(_, _ string, _ map[string]string) (string, error) {
	return "", nil
}

func (w *mockWorkspace) EnableDockerMCP(_ context.Context) error { return nil }

func (w *mockWorkspace) DisableDockerMCP() error { return nil }

// -- Events --

func (w *mockWorkspace) Subscribe(prog program.Program) {
	w.prog = prog
}

func (w *mockWorkspace) Shutdown() {}
