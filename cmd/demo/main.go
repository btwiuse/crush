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
	"strings"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/catwalk/pkg/catwalk"
	"charm.land/fantasy"
	"charm.land/fantasy/providers/anthropic"
	"charm.land/fantasy/providers/openaicompat"
	"github.com/btwiuse/boba"
	mcptools "github.com/charmbracelet/crush/internal/agent/tools/mcp"
	"github.com/charmbracelet/crush/internal/config"
	"github.com/charmbracelet/crush/internal/csync"
	"github.com/charmbracelet/crush/internal/history"
	"github.com/charmbracelet/crush/internal/lsp"
	"github.com/charmbracelet/crush/internal/message"
	"github.com/charmbracelet/crush/internal/oauth"
	"github.com/charmbracelet/crush/internal/permission"
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

// newMockConfig returns a *config.Config preconfigured for the demo.
func newMockConfig() *config.Config {
	providers := csync.NewMap[string, config.ProviderConfig]()
	providers.Set("deepseek", config.ProviderConfig{
		ID:      "deepseek",
		Name:    "DeepSeek",
		Type:    catwalk.TypeOpenAICompat,
		APIKey:  "$DEEPSEEK_API_KEY",
		BaseURL: "https://api.deepseek.com/v1",
		Models: []catwalk.Model{
			{
				ID:            "deepseek-chat",
				Name:          "DeepSeek Chat",
				ContextWindow: 1000000,
			},
			{
				ID:            "deepseek-reasoner",
				Name:          "DeepSeek Reasoner",
				ContextWindow: 1000000,
			},
		},
	})

	var transparent = false
	return &config.Config{
		Providers: providers,
		Models: map[config.SelectedModelType]config.SelectedModel{
			config.SelectedModelTypeSmall: {
				Provider: "deepseek",
				Model:    "deepseek-chat",
			},
			config.SelectedModelTypeLarge: {
				Provider: "deepseek",
				Model:    "deepseek-chat",
			},
		},
		RecentModels: map[config.SelectedModelType][]config.SelectedModel{
			config.SelectedModelTypeSmall: {
				{Provider: "deepseek", Model: "deepseek-chat"},
			},
			config.SelectedModelTypeLarge: {
				{Provider: "deepseek", Model: "deepseek-chat"},
			},
		},
		MCP: config.MCPs{},
		LSP: config.LSPs{},
		Options: &config.Options{
			TUI: &config.TUIOptions{
				CompactMode: true,
				Transparent: &transparent,
			},
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
	prog            boba.Program
	mu              sync.Mutex
	sessions        map[string]session.Session
	sessionMessages map[string][]message.Message
	sessionSeq      int
}

func newMockWorkspace() *mockWorkspace {
	return &mockWorkspace{
		cfg:             newMockConfig(),
		sessions:        make(map[string]session.Session),
		sessionMessages: make(map[string][]message.Message),
	}
}

// -- Sessions --

func (w *mockWorkspace) newSessionID() string {
	w.sessionSeq++
	return fmt.Sprintf("mock-session-%d", w.sessionSeq)
}

func (w *mockWorkspace) CreateSession(ctx context.Context, title string) (session.Session, error) {
	if title == "" {
		t, err := w.generateTitle(ctx)
		if err == nil && t != "" {
			title = t
		}
	}
	if title == "" {
		title = "Session"
	}
	now := time.Now().Unix()
	sess := session.Session{
		ID:        w.newSessionID(),
		Title:     title,
		CreatedAt: now,
		UpdatedAt: now,
	}
	w.mu.Lock()
	w.sessions[sess.ID] = sess
	w.mu.Unlock()
	return sess, nil
}

func (w *mockWorkspace) GetSession(_ context.Context, id string) (session.Session, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	sess, ok := w.sessions[id]
	if !ok {
		return session.Session{}, fmt.Errorf("session %q not found", id)
	}
	return sess, nil
}

func (w *mockWorkspace) ListSessions(_ context.Context) ([]session.Session, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]session.Session, 0, len(w.sessions))
	for _, s := range w.sessions {
		result = append(result, s)
	}
	return result, nil
}

func (w *mockWorkspace) SaveSession(_ context.Context, sess session.Session) (session.Session, error) {
	w.mu.Lock()
	w.sessions[sess.ID] = sess
	w.mu.Unlock()
	return sess, nil
}

func (w *mockWorkspace) DeleteSession(_ context.Context, id string) error {
	w.mu.Lock()
	delete(w.sessions, id)
	delete(w.sessionMessages, id)
	w.mu.Unlock()
	return nil
}

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

func (w *mockWorkspace) ListUserMessages(_ context.Context, sessionID string) ([]message.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	msgs := w.sessionMessages[sessionID]
	var result []message.Message
	for _, m := range msgs {
		if m.Role == message.User {
			result = append(result, m)
		}
	}
	return result, nil
}

func (w *mockWorkspace) ListAllUserMessages(_ context.Context) ([]message.Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var result []message.Message
	for _, msgs := range w.sessionMessages {
		for _, m := range msgs {
			if m.Role == message.User {
				result = append(result, m)
			}
		}
	}
	return result, nil
}

// -- Agent --

// resolveValue resolves $VAR and ${VAR} references from the environment.
func resolveValue(v string) string {
	if !strings.HasPrefix(v, "$") {
		return v
	}
	name := strings.TrimPrefix(v, "$")
	name = strings.Trim(name, "{}")
	return os.Getenv(name)
}

// buildProvider reads the large model entry from the config, resolves
// credentials, and creates the corresponding fantasy provider.  It returns
// the provider and the model ID string to pass to
// provider.LanguageModel(ctx, modelID).
func (w *mockWorkspace) buildProvider(modelType config.SelectedModelType) (fantasy.Provider, string, error) {
	modelCfg, ok := w.cfg.Models[modelType]
	if !ok {
		return nil, "", fmt.Errorf("no model configured for %q", modelType)
	}

	providerCfg, ok := w.cfg.Providers.Get(modelCfg.Provider)
	if !ok {
		return nil, "", fmt.Errorf("provider %q not found in config", modelCfg.Provider)
	}

	apiKey := resolveValue(providerCfg.APIKey)
	baseURL := resolveValue(providerCfg.BaseURL)
	switch providerCfg.Type {
	case catwalk.TypeOpenAICompat, "":
		p, err := openaicompat.New(
			openaicompat.WithBaseURL(baseURL),
			openaicompat.WithAPIKey(apiKey),
		)
		if err != nil {
			return nil, "", fmt.Errorf("creating openai-compat provider: %w", err)
		}
		return p, modelCfg.Model, nil
	case catwalk.TypeAnthropic:
		p, err := anthropic.New(
			anthropic.WithBaseURL(baseURL),
			anthropic.WithAPIKey(apiKey),
		)
		if err != nil {
			return nil, "", fmt.Errorf("creating anthropic provider: %w", err)
		}
		return p, modelCfg.Model, nil
	default:
		return nil, "", fmt.Errorf("unsupported provider type %q in demo", providerCfg.Type)
	}
}

func (w *mockWorkspace) generateTitle(ctx context.Context) (string, error) {
	provider, modelID, err := w.buildProvider(config.SelectedModelTypeSmall)
	if err != nil {
		return "", err
	}
	llm, err := provider.LanguageModel(ctx, modelID)
	if err != nil {
		return "", err
	}
	stream, err := llm.Stream(ctx, fantasy.Call{
		Prompt: []fantasy.Message{
			{
				Role:    fantasy.MessageRoleUser,
				Content: []fantasy.MessagePart{fantasy.TextPart{Text: "Generate a short session title under 5 words. Respond with only the title, no explanation or punctuation."}},
			},
		},
	})
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for part := range stream {
		if part.Type == fantasy.StreamPartTypeTextDelta {
			sb.WriteString(part.Delta)
		}
	}
	return strings.TrimSpace(sb.String()), nil
}

func (w *mockWorkspace) AgentRun(ctx context.Context, sessionID, content string, attachments ...message.Attachment) error {
	// Load existing messages from previous turns.
	w.mu.Lock()
	existing := make([]message.Message, len(w.sessionMessages[sessionID]))
	copy(existing, w.sessionMessages[sessionID])
	w.mu.Unlock()

	// Build user message parts including text attachments.
	userParts := []message.ContentPart{message.TextContent{Text: content}}
	for _, a := range attachments {
		if a.IsText() {
			userParts = append(userParts, message.TextContent{Text: string(a.Content)})
		}
	}

	// Publish user message.
	userMsg := message.Message{
		ID:        uuid.New().String(),
		SessionID: sessionID,
		Role:      message.User,
		Parts:     userParts,
		CreatedAt: time.Now().Unix(),
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
	modelCfg := w.cfg.Models[config.SelectedModelTypeLarge]
	assistantMsg := message.Message{
		ID:        assistantID,
		SessionID: sessionID,
		Role:      message.Assistant,
		Parts:     []message.ContentPart{},
		Provider:  modelCfg.Provider,
		Model:     modelCfg.Model,
		CreatedAt: time.Now().Unix(),
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
			fantasy.TextPart{Text: message.PromptWithTextAttachments(content, attachments)},
		},
	})

	// Build the provider from config and call the LLM.
	provider, modelID, err := w.buildProvider(config.SelectedModelTypeLarge)
	if err != nil {
		return err
	}

	llm, err := provider.LanguageModel(ctx, modelID)
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
		case fantasy.StreamPartTypeReasoningDelta:
			assistantMsg.AppendReasoningContent(part.Delta)
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
	modelCfg := w.cfg.Models[config.SelectedModelTypeLarge]
	catwalkCfg := w.cfg.GetModel(modelCfg.Provider, modelCfg.Model)
	if catwalkCfg == nil {
		return workspace.AgentModel{}
	}
	return workspace.AgentModel{
		CatwalkCfg: *catwalkCfg,
		ModelCfg:   modelCfg,
	}
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

func (w *mockWorkspace) UpdatePreferredModel(_ config.Scope, modelType config.SelectedModelType, model config.SelectedModel) error {
	w.mu.Lock()
	w.cfg.Models[modelType] = model
	w.cfg.RecentModels[modelType] = append([]config.SelectedModel{model}, w.cfg.RecentModels[modelType]...)
	w.mu.Unlock()
	return nil
}

func (w *mockWorkspace) SetCompactMode(_ config.Scope, enabled bool) error {
	if w.cfg.Options != nil && w.cfg.Options.TUI != nil {
		w.mu.Lock()
		w.cfg.Options.TUI.CompactMode = enabled
		w.mu.Unlock()
	}
	return nil
}

func (w *mockWorkspace) SetProviderAPIKey(_ config.Scope, providerID string, apiKey any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if pc, ok := w.cfg.Providers.Get(providerID); ok {
		keyStr, _ := apiKey.(string)
		pc.APIKey = keyStr
		w.cfg.Providers.Set(providerID, pc)
	}
	return nil
}

func (w *mockWorkspace) SetConfigField(_ config.Scope, _ string, _ any) error {
	// Generic JSON-path field mutations are not needed for the demo.
	return nil
}

func (w *mockWorkspace) RemoveConfigField(_ config.Scope, _ string) error {
	return nil
}

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

func (w *mockWorkspace) Subscribe(prog boba.Program) {
	w.prog = prog
}

func (w *mockWorkspace) Shutdown() {}
