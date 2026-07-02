/*
 * Copyright 2026 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package agentsmd provides a middleware that automatically injects Agents.md
// file contents into model input messages. The injection is transient — content
// is prepended at model call time and never persisted to conversation state,
// so it is naturally excluded from summarization / compression.
//
// Package agentsmd 提供一个中间件，会自动将 Agents.md 文件内容注入到模型输入消息中。该注入是临时的——内容会在模型调用时前置，且永不持久化到对话状态，因此会自然地从摘要/压缩中排除。
package agentsmd

import (
	"context"
	"fmt"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

// Config defines the configuration for the agentsmd middleware.
// Config 定义 agentsmd 中间件的配置。
type Config struct {
	// Backend provides file access for loading Agents.md files.
	// Implementations can use local filesystem, remote storage, or any other backend.
	// Required.
	//
	// Backend 提供加载 Agents.md 文件的文件访问能力。实现可以使用本地文件系统、远程存储或任何其他后端。必需。
	Backend Backend

	// AgentsMDFiles specifies the ordered list of Agents.md file paths to load.
	// Files are loaded and injected in the given order.
	// Supports @import syntax inside files for recursive inclusion (max depth 5).
	//
	// AgentsMDFiles 指定要加载的 Agents.md 文件路径有序列表。文件会按给定顺序加载并注入。支持文件内 @import 语法进行递归包含（最大深度 5）。
	AgentsMDFiles []string

	// AllAgentsMDMaxBytes limits the total byte size of all loaded Agents.md content.
	// Files are loaded in order; once the cumulative size exceeds this limit,
	// remaining files are skipped. Each individual file is always loaded in full.
	// 0 means no limit.
	//
	// AllAgentsMDMaxBytes 限制所有已加载 Agents.md 内容的总字节大小。文件按顺序加载；一旦累计大小超过该限制，剩余文件将被跳过。每个单独文件始终会完整加载。0 表示无限制。
	AllAgentsMDMaxBytes int

	// OnLoadWarning is an optional callback invoked when a non-fatal error occurs
	// during Agents.md file loading (e.g. file not found, circular @import, depth
	// exceeded). If nil, warnings are logged via log.Printf.
	//
	// Note: Backend.Read errors other than os.ErrNotExist (e.g. permission denied,
	// I/O errors) are NOT treated as warnings and will abort the loading process.
	//
	// OnLoadWarning 是可选回调，在 Agents.md 文件加载期间发生非致命错误时调用（例如文件未找到、循环 @import、超过深度）。如果为 nil，则通过 log.Printf 记录警告。
	// 注意：除 os.ErrNotExist 以外的 Backend.Read 错误（例如权限拒绝、I/O 错误）不会被视为警告，并会中止加载过程。
	OnLoadWarning func(filePath string, err error)
}

// NewTyped creates a generic agentsmd middleware that injects Agents.md content into every
// model call. The content is loaded from the configured file paths via Backend
// on each model invocation.
//
// This is the generic constructor that supports both *schema.Message and *schema.AgenticMessage.
//
// Recommended: place this middleware AFTER the summarization middleware, so that
// Agents.md content is excluded from summarization/compression.
//
// NewTyped 创建一个泛型 agentsmd 中间件，将 Agents.md 内容注入到每次模型调用中。内容会在每次模型调用时通过 Backend 从配置的文件路径加载。
// 这是同时支持 *schema.Message 和 *schema.AgenticMessage 的泛型构造函数。
// 建议：将此中间件放在摘要中间件之后，使 Agents.md 内容从摘要/压缩中排除。
func NewTyped[M adk.MessageType](_ context.Context, cfg *Config) (adk.TypedChatModelAgentMiddleware[M], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &typedMiddleware[M]{
		loader: newLoaderConfig(cfg.Backend, cfg.AgentsMDFiles, cfg.AllAgentsMDMaxBytes, cfg.OnLoadWarning),
	}, nil
}

// New creates an agentsmd middleware that injects Agents.md content into every
// model call. The content is loaded from the configured file paths via Backend
// on each model invocation.
//
// Recommended: place this middleware AFTER the summarization middleware, so that
// Agents.md content is excluded from summarization/compression.
//
// New 创建一个 agentsmd 中间件，将 Agents.md 内容注入到每次模型调用中。内容会在每次模型调用时通过 Backend 从配置的文件路径加载。
// 建议：将此中间件放在摘要中间件之后，使 Agents.md 内容从摘要/压缩中排除。
func New(ctx context.Context, cfg *Config) (adk.ChatModelAgentMiddleware, error) {
	return NewTyped[*schema.Message](ctx, cfg)
}

type typedMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
	loader *loaderConfig
}

const agentsMDCacheKey = "__agentsmd_content_cache__"
const agentsMDExtraKey = "__agentsmd_content__"

// BeforeModelRewriteState injects Agents.md content as a User message before
// the first User message in the conversation. The injected message is tagged
// with an Extra key so that repeated invocations are idempotent.
//
// BeforeModelRewriteState 将 Agents.md 内容作为 User 消息注入到对话中的第一条 User 消息之前。注入的消息会带有一个 Extra key，使重复调用保持幂等。
func (m *typedMiddleware[M]) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[M], _ *adk.TypedModelContext[M]) (context.Context, *adk.TypedChatModelAgentState[M], error) {
	// Idempotent: if we already injected, return early.
	// 幂等：如果已经注入过，则提前返回。
	for _, msg := range state.Messages {
		if hasAgentsMDExtra(msg) {
			return ctx, state, nil
		}
	}

	content, err := m.loadContent(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if content == "" {
		return ctx, state, nil
	}

	nState := *state
	nState.Messages = typedInsertBeforeFirstUser(state.Messages, content)
	return ctx, &nState, nil
}

// hasAgentsMDExtra checks whether a message has the agentsmd extra key set.
// hasAgentsMDExtra 检查消息是否设置了 agentsmd extra key。
func hasAgentsMDExtra[M adk.MessageType](msg M) bool {
	switch v := any(msg).(type) {
	case *schema.Message:
		if v.Extra != nil {
			if _, ok := v.Extra[agentsMDExtraKey]; ok {
				return true
			}
		}
	case *schema.AgenticMessage:
		if v.Extra != nil {
			if _, ok := v.Extra[agentsMDExtraKey]; ok {
				return true
			}
		}
	}
	return false
}

// typedInsertBeforeFirstUser inserts a user message with agentsmd content before the first User message.
// typedInsertBeforeFirstUser 在第一条 User 消息前插入一条包含 agentsmd 内容的用户消息。
func typedInsertBeforeFirstUser[M adk.MessageType](msgs []M, content string) []M {
	newMsg := makeUserMsgWithExtra[M](content)
	result := make([]M, 0, len(msgs)+1)
	for i, msg := range msgs {
		if isUserRole(msg) {
			result = append(result, newMsg)
			result = append(result, msgs[i:]...)
			return result
		}
		result = append(result, msg)
	}
	result = append(result, newMsg)
	return result
}

func isUserRole[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.User
	case *schema.AgenticMessage:
		return m.Role == schema.AgenticRoleTypeUser
	}
	return false
}

func makeUserMsgWithExtra[M adk.MessageType](content string) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		msg := schema.UserMessage(content)
		msg.Extra = map[string]any{agentsMDExtraKey: true}
		return any(msg).(M)
	case *schema.AgenticMessage:
		msg := schema.UserAgenticMessage(content)
		msg.Extra = map[string]any{agentsMDExtraKey: true}
		return any(msg).(M)
	}
	panic("unreachable")
}

// loadContent retrieves the Agents.md content, using a per-Run cache to avoid
// reloading on every model call within the same Run().
//
// loadContent 获取 Agents.md 内容，并使用每个 Run 的缓存，避免在同一 Run() 内每次模型调用都重新加载。
func (m *typedMiddleware[M]) loadContent(ctx context.Context) (string, error) {
	if cached, found, err := adk.GetRunLocalValue(ctx, agentsMDCacheKey); err == nil && found {
		if s, ok := cached.(string); ok {
			return s, nil
		}
	}

	content, err := m.loader.load(ctx)
	if err != nil {
		return "", fmt.Errorf("[agentsmd]: failed to load agent files: %w", err)
	}

	if content != "" {
		_ = adk.SetRunLocalValue(ctx, agentsMDCacheKey, content)
	}

	return content, nil
}

func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("[agentsmd]: config is required")
	}
	if c.Backend == nil {
		return fmt.Errorf("[agentsmd]: backend is required")
	}
	if len(c.AgentsMDFiles) == 0 {
		return fmt.Errorf("[agentsmd]: at least one agent file path is required")
	}
	if c.AllAgentsMDMaxBytes < 0 {
		return fmt.Errorf("[agentsmd]: AllAgentMDDocsMaxBytes must be non-negative")
	}
	return nil
}
