/*
 * Copyright 2025 CloudWeGo Authors
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

// Package patchtoolcalls provides a middleware that patches dangling tool calls in the message history.
// Package patchtoolcalls 提供一个 middleware，用于修补消息历史中悬空的工具调用。
package patchtoolcalls

import (
	"context"
	"fmt"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/schema"
)

// Config defines the configuration options for the patch tool calls middleware.
// Config 定义 patch tool calls middleware 的配置选项。
type Config struct {
	// PatchedContentGenerator is an optional custom function to generate the content
	// of patched tool messages. If not provided, a default message will be used.
	//
	// Parameters:
	//   - ctx: the context for the operation
	//   - toolName: the name of the tool that was called
	//   - toolCallID: the id of the tool call
	//
	// Returns:
	//   - string: the content to use for the patched tool message
	//   - error: any error that occurred during generation
	//
	// PatchedContentGenerator 是一个可选的自定义函数，用于生成修补后的工具消息内容。
	// 如果未提供，将使用默认消息。
	// 参数：
	// - ctx：操作的 context
	// - toolName：被调用工具的名称
	// - toolCallID：工具调用的 id
	// 返回：
	// - string：用于修补后的工具消息的内容
	// - error：生成过程中发生的任何错误
	PatchedContentGenerator func(ctx context.Context, toolName, toolCallID string) (string, error)
}

// NewTyped creates a new generic patch tool calls middleware.
//
// The middleware scans the message history before each model invocation and inserts
// placeholder tool messages for any tool calls that don't have corresponding responses.
//
// NewTyped 创建一个新的泛型 patch tool calls middleware。
// 该 middleware 会在每次模型调用前扫描消息历史，并为任何没有对应响应的工具调用插入占位工具消息。
func NewTyped[M adk.MessageType](_ context.Context, cfg *Config) (adk.TypedChatModelAgentMiddleware[M], error) {
	if cfg == nil {
		cfg = &Config{}
	}
	return &typedMiddleware[M]{
		gen: cfg.PatchedContentGenerator,
	}, nil
}

// New creates a new patch tool calls middleware with the given configuration.
//
// The middleware scans the message history before each model invocation and inserts
// placeholder tool messages for any tool calls that don't have corresponding responses.
//
// New 使用给定配置创建一个新的 patch tool calls middleware。
// 该 middleware 会在每次模型调用前扫描消息历史，并为任何没有对应响应的工具调用插入占位工具消息。
func New(ctx context.Context, cfg *Config) (adk.ChatModelAgentMiddleware, error) {
	return NewTyped[*schema.Message](ctx, cfg)
}

type typedMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
	gen func(ctx context.Context, toolName, toolCallID string) (string, error)
}

func (m *typedMiddleware[M]) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[M],
	mc *adk.TypedModelContext[M],
) (context.Context, *adk.TypedChatModelAgentState[M], error) {
	if len(state.Messages) == 0 {
		return ctx, state, nil
	}

	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return patchToolCallsForMessage(ctx, m.gen, any(state).(*adk.TypedChatModelAgentState[*schema.Message]), mc)
	case *schema.AgenticMessage:
		return patchToolCallsForAgenticMessage(ctx, m.gen, any(state).(*adk.TypedChatModelAgentState[*schema.AgenticMessage]), mc)
	default:
		panic("unreachable: unknown MessageType")
	}
}

func patchToolCallsForMessage[M adk.MessageType](ctx context.Context,
	gen func(ctx context.Context, toolName, toolCallID string) (string, error),
	state *adk.TypedChatModelAgentState[*schema.Message],
	_ *adk.TypedModelContext[M],
) (context.Context, *adk.TypedChatModelAgentState[M], error) {
	patched := make([]*schema.Message, 0, len(state.Messages))

	for i, msg := range state.Messages {
		patched = append(patched, msg)

		if msg.Role != schema.Assistant || len(msg.ToolCalls) == 0 {
			continue
		}

		for _, tc := range msg.ToolCalls {
			if hasCorrespondingToolMessage(state.Messages[i+1:], tc.ID) {
				continue
			}

			toolMsg, err := createPatchedToolMessage(ctx, gen, tc)
			if err != nil {
				return ctx, nil, err
			}
			patched = append(patched, toolMsg)
		}
	}

	nState := *state
	nState.Messages = patched
	return ctx, any(&nState).(*adk.TypedChatModelAgentState[M]), nil
}

func patchToolCallsForAgenticMessage[M adk.MessageType](ctx context.Context,
	gen func(ctx context.Context, toolName, toolCallID string) (string, error),
	state *adk.TypedChatModelAgentState[*schema.AgenticMessage],
	_ *adk.TypedModelContext[M],
) (context.Context, *adk.TypedChatModelAgentState[M], error) {
	patched := make([]*schema.AgenticMessage, 0, len(state.Messages))

	for i, msg := range state.Messages {
		patched = append(patched, msg)

		if msg.Role != schema.AgenticRoleTypeAssistant {
			continue
		}

		// Collect tool call IDs from this assistant message.
		// 从此 assistant 消息中收集工具调用 ID。
		var toolCalls []struct {
			callID string
			name   string
		}
		for _, block := range msg.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall != nil {
				toolCalls = append(toolCalls, struct {
					callID string
					name   string
				}{callID: block.FunctionToolCall.CallID, name: block.FunctionToolCall.Name})
			}
		}
		if len(toolCalls) == 0 {
			continue
		}

		for _, tc := range toolCalls {
			if hasCorrespondingAgenticToolResult(state.Messages[i+1:], tc.callID) {
				continue
			}

			toolMsg, err := createPatchedAgenticToolMessage(ctx, gen, tc.name, tc.callID)
			if err != nil {
				return ctx, nil, err
			}
			patched = append(patched, toolMsg)
		}
	}

	nState := *state
	nState.Messages = patched
	return ctx, any(&nState).(*adk.TypedChatModelAgentState[M]), nil
}

func hasCorrespondingToolMessage(messages []*schema.Message, toolCallID string) bool {
	for _, msg := range messages {
		// Only consider successive tool messages after the tool call message
		// 仅考虑工具调用消息之后连续的工具消息
		if msg.Role != schema.Tool {
			return false
		}
		if msg.ToolCallID == toolCallID {
			return true
		}
	}
	return false
}

func hasCorrespondingAgenticToolResult(messages []*schema.AgenticMessage, toolCallID string) bool {
	for _, msg := range messages {
		// Only consider successive tool messages after the tool call message
		// 仅考虑工具调用消息之后连续的工具消息
		if msg.Role != schema.AgenticRoleTypeUser {
			return false
		}
		hasToolResult := false
		for _, block := range msg.ContentBlocks {
			if block == nil {
				continue
			}
			if block.Type == schema.ContentBlockTypeFunctionToolResult {
				hasToolResult = true
				if block.FunctionToolResult != nil && block.FunctionToolResult.CallID == toolCallID {
					return true
				}
			}
			if block.Type == schema.ContentBlockTypeToolSearchResult {
				hasToolResult = true
				if block.ToolSearchFunctionToolResult != nil && block.ToolSearchFunctionToolResult.CallID == toolCallID {
					return true
				}
			}
		}
		if !hasToolResult {
			return false
		}
	}
	return false
}

func createPatchedToolMessage(ctx context.Context, gen func(ctx context.Context, toolName, toolCallID string) (string, error), tc schema.ToolCall) (*schema.Message, error) {
	if gen != nil {
		content, err := gen(ctx, tc.Function.Name, tc.ID)
		if err != nil {
			return nil, err
		}
		return schema.ToolMessage(content, tc.ID, schema.WithToolName(tc.Function.Name)), nil
	}
	tpl := internal.SelectPrompt(internal.I18nPrompts{
		English: defaultPatchedToolMessageTemplate,
		Chinese: defaultPatchedToolMessageTemplateChinese,
	})

	return schema.ToolMessage(fmt.Sprintf(tpl, tc.Function.Name, tc.ID), tc.ID, schema.WithToolName(tc.Function.Name)), nil
}

func createPatchedAgenticToolMessage(ctx context.Context, gen func(ctx context.Context, toolName, toolCallID string) (string, error), toolName, callID string) (*schema.AgenticMessage, error) {
	var content string
	if gen != nil {
		var err error
		content, err = gen(ctx, toolName, callID)
		if err != nil {
			return nil, err
		}
	} else {
		tpl := internal.SelectPrompt(internal.I18nPrompts{
			English: defaultPatchedToolMessageTemplate,
			Chinese: defaultPatchedToolMessageTemplateChinese,
		})
		content = fmt.Sprintf(tpl, toolName, callID)
	}

	return &schema.AgenticMessage{
		Role: schema.AgenticRoleTypeUser,
		ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.FunctionToolResult{
				CallID: callID,
				Name:   toolName,
				Content: []*schema.FunctionToolResultContentBlock{
					{Type: schema.FunctionToolResultContentBlockTypeText, Text: &schema.UserInputText{Text: content}},
				},
			}),
		},
	}, nil
}

const (
	defaultPatchedToolMessageTemplate        = "Tool call %s with id %s was canceled - another message came in before it could be completed."
	defaultPatchedToolMessageTemplateChinese = "工具调用 %s（ID 为 %s）已被取消——在其完成之前收到了另一条消息。"
)
