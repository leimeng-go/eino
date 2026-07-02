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

package reduction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

// ---------------------------------------------------------------------------
// Generic message construction helpers
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 通用消息构造辅助函数
// ---------------------------------------------------------------------------

type testToolCall struct {
	ID        string
	Name      string
	Arguments string
}

func makeUserMsgG[M adk.MessageType](content string) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return any(schema.UserMessage(content)).(M)
	case *schema.AgenticMessage:
		return any(schema.UserAgenticMessage(content)).(M)
	}
	panic("unreachable")
}

func makeSystemMsgG[M adk.MessageType](content string) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return any(&schema.Message{Role: schema.System, Content: content}).(M)
	case *schema.AgenticMessage:
		return any(schema.SystemAgenticMessage(content)).(M)
	}
	panic("unreachable")
}

func makeAssistantMsgG[M adk.MessageType](content string) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return any(&schema.Message{Role: schema.Assistant, Content: content}).(M)
	case *schema.AgenticMessage:
		return any(&schema.AgenticMessage{
			Role:          schema.AgenticRoleTypeAssistant,
			ContentBlocks: []*schema.ContentBlock{schema.NewContentBlock(&schema.AssistantGenText{Text: content})},
		}).(M)
	}
	panic("unreachable")
}

func makeAssistantMsgWithToolCallsG[M adk.MessageType](toolCalls []testToolCall) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		tcs := make([]schema.ToolCall, len(toolCalls))
		for i, tc := range toolCalls {
			tcs[i] = schema.ToolCall{
				ID:       tc.ID,
				Type:     "function",
				Function: schema.FunctionCall{Name: tc.Name, Arguments: tc.Arguments},
			}
		}
		return any(schema.AssistantMessage("", tcs)).(M)
	case *schema.AgenticMessage:
		blocks := make([]*schema.ContentBlock, 0, len(toolCalls))
		for _, tc := range toolCalls {
			blocks = append(blocks, schema.NewContentBlock(&schema.FunctionToolCall{
				CallID:    tc.ID,
				Name:      tc.Name,
				Arguments: tc.Arguments,
			}))
		}
		return any(&schema.AgenticMessage{
			Role:          schema.AgenticRoleTypeAssistant,
			ContentBlocks: blocks,
		}).(M)
	}
	panic("unreachable")
}

func makeToolResultMsgG[M adk.MessageType](content string, callID string, toolName string) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		msg := schema.ToolMessage(content, callID)
		msg.ToolName = toolName
		return any(msg).(M)
	case *schema.AgenticMessage:
		return any(&schema.AgenticMessage{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.FunctionToolResult{
					CallID: callID,
					Name:   toolName,
					Content: []*schema.FunctionToolResultContentBlock{
						{Text: &schema.UserInputText{Text: content}},
					},
				}),
			},
		}).(M)
	}
	panic("unreachable")
}

func getMsgContentG[M adk.MessageType](msg M) string {
	switch v := any(msg).(type) {
	case *schema.Message:
		return v.Content
	case *schema.AgenticMessage:
		for _, block := range v.ContentBlocks {
			if block == nil {
				continue
			}
			if block.UserInputText != nil {
				return block.UserInputText.Text
			}
			if block.AssistantGenText != nil {
				return block.AssistantGenText.Text
			}
			if block.FunctionToolResult != nil {
				for _, b := range block.FunctionToolResult.Content {
					if b != nil && b.Text != nil {
						return b.Text.Text
					}
				}
			}
		}
		return ""
	}
	panic("unreachable")
}

// ---------------------------------------------------------------------------
// Part 1: Helper function tests
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 第 1 部分：辅助函数测试
// ---------------------------------------------------------------------------

func testHelperFunctions[M adk.MessageType](t *testing.T) {
	t.Run("isAssistantMsg", func(t *testing.T) {
		assistant := makeAssistantMsgG[M]("hello")
		user := makeUserMsgG[M]("hello")
		assert.True(t, isAssistantMsg(assistant))
		assert.False(t, isAssistantMsg(user))
	})

	t.Run("isSystemMsg", func(t *testing.T) {
		sys := makeSystemMsgG[M]("system prompt")
		user := makeUserMsgG[M]("hello")
		assert.True(t, isSystemMsg(sys))
		assert.False(t, isSystemMsg(user))
	})

	t.Run("isUserMsg", func(t *testing.T) {
		user := makeUserMsgG[M]("hello")
		assert.True(t, isUserMsg(user))

		// A user message that only has tool results should return false.
		// 仅包含工具结果的用户消息应返回 false。
		toolResultOnly := makeToolResultMsgG[M]("result", "call_1", "my_tool")
		assert.False(t, isUserMsg(toolResultOnly))
	})

	t.Run("hasToolCalls", func(t *testing.T) {
		withTC := makeAssistantMsgWithToolCallsG[M]([]testToolCall{
			{ID: "c1", Name: "tool1", Arguments: `{"a":1}`},
		})
		assert.True(t, hasToolCalls(withTC))

		noTC := makeAssistantMsgG[M]("plain response")
		assert.False(t, hasToolCalls(noTC))
	})

	t.Run("isToolResultMsg", func(t *testing.T) {
		tr := makeToolResultMsgG[M]("result content", "call_1", "my_tool")
		assert.True(t, isToolResultMsg(tr))

		user := makeUserMsgG[M]("not a tool result")
		assert.False(t, isToolResultMsg(user))
	})

	t.Run("isToolResultOnlyMsg", func(t *testing.T) {
		trOnly := makeToolResultMsgG[M]("result content", "call_1", "my_tool")
		assert.True(t, isToolResultOnlyMsg(trOnly))

		// A normal user message is not a tool-result-only message.
		// 普通用户消息不是仅含工具结果的消息。
		user := makeUserMsgG[M]("hello")
		assert.False(t, isToolResultOnlyMsg(user))

		// For AgenticMessage, a mixed message (user text + tool result) should return false.
		// 对于 AgenticMessage，混合消息（用户文本 + 工具结果）应返回 false。
		var zero M
		if _, ok := any(zero).(*schema.AgenticMessage); ok {
			mixed := any(&schema.AgenticMessage{
				Role: schema.AgenticRoleTypeUser,
				ContentBlocks: []*schema.ContentBlock{
					schema.NewContentBlock(&schema.UserInputText{Text: "hello"}),
					schema.NewContentBlock(&schema.FunctionToolResult{CallID: "c1", Name: "tool1", Content: []*schema.FunctionToolResultContentBlock{
						{Text: &schema.UserInputText{Text: "result"}},
					}}),
				},
			}).(M)
			assert.False(t, isToolResultOnlyMsg(mixed))
		}
	})

	t.Run("getMsgClearedFlagGeneric_setMsgClearedFlagGeneric", func(t *testing.T) {
		msg := makeAssistantMsgG[M]("test")
		assert.False(t, getMsgClearedFlagGeneric(msg))

		setMsgClearedFlagGeneric(msg)
		assert.True(t, getMsgClearedFlagGeneric(msg))
	})

	t.Run("getToolCallsGeneric", func(t *testing.T) {
		tcs := []testToolCall{
			{ID: "call_a", Name: "tool_alpha", Arguments: `{"x":1}`},
			{ID: "call_b", Name: "tool_beta", Arguments: `{"y":2}`},
		}
		msg := makeAssistantMsgWithToolCallsG[M](tcs)
		got := getToolCallsGeneric(msg)
		require.Len(t, got, 2)

		assert.Equal(t, "call_a", got[0].CallID)
		assert.Equal(t, "tool_alpha", got[0].Name)
		assert.Equal(t, `{"x":1}`, got[0].Arguments)
		assert.Equal(t, 0, got[0].BlockIndex)

		assert.Equal(t, "call_b", got[1].CallID)
		assert.Equal(t, "tool_beta", got[1].Name)
		assert.Equal(t, `{"y":2}`, got[1].Arguments)
		assert.Equal(t, 1, got[1].BlockIndex)

		// Empty assistant message returns nil.
		// 空 assistant 消息返回 nil。
		noTC := makeAssistantMsgG[M]("plain")
		assert.Nil(t, getToolCallsGeneric(noTC))
	})

	t.Run("setToolCallArguments", func(t *testing.T) {
		tcs := []testToolCall{
			{ID: "call_a", Name: "tool_alpha", Arguments: `{"old":"args"}`},
		}
		msg := makeAssistantMsgWithToolCallsG[M](tcs)
		setToolCallArguments(msg, 0, `{"new":"args"}`)

		got := getToolCallsGeneric(msg)
		require.Len(t, got, 1)
		assert.Equal(t, `{"new":"args"}`, got[0].Arguments)

		// Verify AgenticMessage path writes to the ContentBlock directly.
		// 验证 AgenticMessage 路径会直接写入 ContentBlock。
		if am, ok := any(msg).(*schema.AgenticMessage); ok {
			require.NotNil(t, am.ContentBlocks[0].FunctionToolCall)
			assert.Equal(t, `{"new":"args"}`, am.ContentBlocks[0].FunctionToolCall.Arguments)
		}
	})

	t.Run("copyMessagesGeneric", func(t *testing.T) {
		original := []M{
			makeAssistantMsgWithToolCallsG[M]([]testToolCall{
				{ID: "c1", Name: "t1", Arguments: `{"k":"v"}`},
			}),
			makeUserMsgG[M]("user text"),
		}
		copied := copyMessagesGeneric(original)
		require.Len(t, copied, 2)

		// Modify the copy's tool call arguments.
		// 修改副本的工具调用参数。
		setToolCallArguments(copied[0], 0, `{"modified":"true"}`)

		// Original must be unchanged.
		// 原始值必须保持不变。
		origTCs := getToolCallsGeneric(original[0])
		require.Len(t, origTCs, 1)
		assert.Equal(t, `{"k":"v"}`, origTCs[0].Arguments, "original must not be affected by copy mutation")

		copiedTCs := getToolCallsGeneric(copied[0])
		assert.Equal(t, `{"modified":"true"}`, copiedTCs[0].Arguments)
	})
}

// ---------------------------------------------------------------------------
// Part 2: Clear rewrite flow
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 第 2 部分：清除重写流程
// ---------------------------------------------------------------------------

func testClearFlowGeneric[M adk.MessageType](t *testing.T) {
	ctx := context.Background()

	// Token counter that always returns a high count to trigger clearing.
	// 始终返回高计数的 token 计数器，用于触发清除。
	highTokenCounter := func(_ context.Context, _ []M, _ []*schema.ToolInfo) (int64, error) {
		return 999999, nil
	}

	// ClearRetentionSuffixLimit defaults to 1 in copyAndFillDefaults when set to 0,
	// so we explicitly set it to 1. This means the last tool-call group (call_new)
	// is retained and only the older group (call_old) is cleared.
	//
	// ClearRetentionSuffixLimit 设为 0 时会在 copyAndFillDefaults 中默认为 1，
	// 因此这里显式设为 1。这意味着保留最后一个工具调用组（call_new），
	// 只清除较旧的组（call_old）。
	config := &TypedConfig[M]{
		SkipTruncation:            true,
		TokenCounter:              highTokenCounter,
		MaxTokensForClear:         100,
		ClearRetentionSuffixLimit: 1,
	}

	mw, err := NewTyped(ctx, config)
	require.NoError(t, err)

	// Messages: system, user, assistant+toolcalls(old), tool_result(old), user, assistant+toolcalls(new)
	// 消息：system、user、assistant+toolcalls(old)、tool_result(old)、user、assistant+toolcalls(new)
	msgs := []M{
		makeSystemMsgG[M]("you are helpful"),
		makeUserMsgG[M]("what's the weather?"),
		makeAssistantMsgWithToolCallsG[M]([]testToolCall{
			{ID: "call_old", Name: "get_weather", Arguments: `{"location":"London"}`},
		}),
		makeToolResultMsgG[M]("Sunny and warm", "call_old", "get_weather"),
		makeUserMsgG[M]("set thermostat"),
		makeAssistantMsgWithToolCallsG[M]([]testToolCall{
			{ID: "call_new", Name: "set_thermostat", Arguments: `{"temp":20}`},
		}),
	}

	state := &adk.TypedChatModelAgentState[M]{Messages: msgs}
	_, resultState, err := mw.BeforeModelRewriteState(ctx, state, &adk.TypedModelContext[M]{})
	require.NoError(t, err)
	require.Equal(t, 6, len(resultState.Messages))

	// The default ClearHandler preserves tool call arguments (sets them to the original).
	// Verify they are unchanged.
	//
	// 默认 ClearHandler 会保留工具调用参数（将其设为原始值）。
	// 验证它们未改变。
	oldTCs := getToolCallsGeneric(resultState.Messages[2])
	require.Len(t, oldTCs, 1)
	assert.Equal(t, `{"location":"London"}`, oldTCs[0].Arguments, "default handler preserves tool call arguments")

	// The old tool result (index 3) should have its content replaced with a placeholder.
	// The placeholder text is locale-dependent, so just verify it changed from the original.
	//
	// 旧工具结果（索引 3）的内容应替换为占位符。
	// 占位符文本依赖 locale，因此只验证它已不同于原始值。
	oldResultContent := getMsgContentG(resultState.Messages[3])
	assert.NotEqual(t, "Sunny and warm", oldResultContent, "old tool result content should be replaced with placeholder")

	// The cleared flag should be set on the old assistant message.
	// 应在旧 assistant 消息上设置 cleared 标记。
	assert.True(t, getMsgClearedFlagGeneric(resultState.Messages[2]), "cleared flag should be set on old assistant msg")

	// System message (index 0) should be untouched.
	// system 消息（索引 0）应保持不变。
	assert.Equal(t, "you are helpful", getMsgContentG(resultState.Messages[0]))

	// Recent messages (index 4, 5) should not be affected: the new tool-call group
	// is in the retention window.
	//
	// 最近的消息（索引 4、5）不应受影响：新的工具调用组
	// 位于保留窗口内。
	newTCs := getToolCallsGeneric(resultState.Messages[5])
	require.Len(t, newTCs, 1)
	assert.Equal(t, `{"temp":20}`, newTCs[0].Arguments, "recent tool calls must not be cleared")
}

// ---------------------------------------------------------------------------
// Part 3: Truncation flow
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 第 3 部分：截断流程
// ---------------------------------------------------------------------------

func testTruncationGeneric[M adk.MessageType](t *testing.T) {
	ctx := context.Background()

	callCount := 0
	// Token counter returns decreasing counts as messages shrink.
	// Token counter 会随着消息变少而返回递减的计数。
	tokenCounter := func(_ context.Context, msgs []M, _ []*schema.ToolInfo) (int64, error) {
		callCount++
		// First call: over limit. After truncation (fewer msgs), under limit.
		// 第一次调用：超过限制。截断后（消息更少），低于限制。
		return int64(len(msgs)) * 100, nil
	}

	config := &TypedConfig[M]{
		SkipTruncation:            true,
		SkipClear:                 true,
		TokenCounter:              tokenCounter,
		MaxTokensForClear:         250, // 5 messages * 100 = 500 > 250
		ClearRetentionSuffixLimit: 0,
	}

	mw, err := NewTyped(ctx, config)
	require.NoError(t, err)

	msgs := []M{
		makeSystemMsgG[M]("system prompt"),
		makeUserMsgG[M]("old user message"),
		makeAssistantMsgG[M]("old assistant response"),
		makeUserMsgG[M]("new user message"),
		makeAssistantMsgG[M]("new assistant response"),
	}

	state := &adk.TypedChatModelAgentState[M]{Messages: msgs}
	_, resultState, err := mw.BeforeModelRewriteState(ctx, state, &adk.TypedModelContext[M]{})
	require.NoError(t, err)

	// Since SkipClear is true, the clear path is entirely skipped.
	// The middleware should return the state unchanged because clear is skipped
	// (truncation in BeforeModelRewriteState is the clear phase, not the tool-output truncation).
	// The messages are returned as-is since the clearing loop is the only message-removal mechanism.
	//
	// 由于 SkipClear 为 true，clear 路径会被完全跳过。
	// 中间件应返回未改变的 state，因为 clear 被跳过了
	// （BeforeModelRewriteState 中的截断属于 clear 阶段，不是工具输出截断）。
	// 消息会原样返回，因为 clearing 循环是唯一的消息移除机制。
	assert.Equal(t, len(msgs), len(resultState.Messages))
}

// ---------------------------------------------------------------------------
// Part 4: ClearPostProcess callback
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 第 4 部分：ClearPostProcess 回调
// ---------------------------------------------------------------------------

func testClearPostProcessGeneric[M adk.MessageType](t *testing.T) {
	ctx := context.Background()

	postProcessCalled := false
	highTokenCounter := func(_ context.Context, _ []M, _ []*schema.ToolInfo) (int64, error) {
		return 999999, nil
	}

	// ClearRetentionSuffixLimit=0 defaults to 1 via copyAndFillDefaults.
	// We need at least 2 tool-call groups so that the first one gets cleared
	// while the second is retained by the suffix limit.
	//
	// ClearRetentionSuffixLimit=0 会通过 copyAndFillDefaults 默认设为 1。
	// 需要至少 2 个工具调用组，这样第一个会被清理，
	// 而第二个会因 suffix limit 被保留。
	config := &TypedConfig[M]{
		SkipTruncation:            true,
		TokenCounter:              highTokenCounter,
		MaxTokensForClear:         100,
		ClearRetentionSuffixLimit: 1,
		ClearPostProcess: func(ctx context.Context, state *adk.TypedChatModelAgentState[M]) context.Context {
			postProcessCalled = true
			return ctx
		},
	}

	mw, err := NewTyped(ctx, config)
	require.NoError(t, err)

	msgs := []M{
		makeSystemMsgG[M]("system"),
		makeUserMsgG[M]("user"),
		makeAssistantMsgWithToolCallsG[M]([]testToolCall{
			{ID: "call_1", Name: "tool1", Arguments: `{"a":"b"}`},
		}),
		makeToolResultMsgG[M]("result", "call_1", "tool1"),
		makeUserMsgG[M]("another request"),
		makeAssistantMsgWithToolCallsG[M]([]testToolCall{
			{ID: "call_2", Name: "tool2", Arguments: `{"c":"d"}`},
		}),
		makeToolResultMsgG[M]("result2", "call_2", "tool2"),
	}

	state := &adk.TypedChatModelAgentState[M]{Messages: msgs}
	_, _, err = mw.BeforeModelRewriteState(ctx, state, &adk.TypedModelContext[M]{})
	require.NoError(t, err)
	assert.True(t, postProcessCalled, "ClearPostProcess should have been called")
}

// ---------------------------------------------------------------------------
// Part 5: AgenticMessage-specific coverage
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 第 5 部分：AgenticMessage 专属覆盖
// ---------------------------------------------------------------------------

func TestGetDefaultTokenCounter_AgenticMessage(t *testing.T) {
	ctx := context.Background()
	counter := getDefaultTokenCounter[*schema.AgenticMessage]()

	msgs := []*schema.AgenticMessage{
		{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.UserInputText{Text: "Hello, world!"}),
			},
		},
		{
			Role: schema.AgenticRoleTypeAssistant,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.AssistantGenText{Text: "Hi there!"}),
			},
		},
		{
			Role: schema.AgenticRoleTypeAssistant,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.FunctionToolCall{CallID: "c1", Name: "my_tool", Arguments: `{"key":"value"}`}),
			},
		},
		{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.FunctionToolResult{
					CallID: "c1",
					Name:   "my_tool",
					Content: []*schema.FunctionToolResultContentBlock{
						{Text: &schema.UserInputText{Text: "tool output text"}},
					},
				}),
			},
		},
		nil, // nil message should be skipped
		// 应跳过 nil message
	}

	tokens, err := counter(ctx, msgs, nil)
	assert.NoError(t, err)
	assert.Greater(t, tokens, int64(0), "should count tokens from content blocks")

	// Also test with tools
	// 也测试 tools
	tools := []*schema.ToolInfo{
		{Name: "my_tool", Desc: "a test tool"},
	}
	tokensWithTools, err := counter(ctx, msgs, tools)
	assert.NoError(t, err)
	assert.Greater(t, tokensWithTools, tokens, "tokens should increase with tool info")
}

func TestCopyAgenticMessages_DeepCopy(t *testing.T) {
	original := []*schema.AgenticMessage{
		{
			Role: schema.AgenticRoleTypeAssistant,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.FunctionToolCall{
					CallID:    "call_1",
					Name:      "tool_a",
					Arguments: `{"x":1}`,
				}),
				{
					Type: schema.ContentBlockTypeFunctionToolResult,
					FunctionToolResult: &schema.FunctionToolResult{
						CallID: "call_1",
						Name:   "tool_a",
						Content: []*schema.FunctionToolResultContentBlock{
							{Text: &schema.UserInputText{Text: "original result"}},
						},
					},
					Extra: map[string]any{"meta": "data"},
				},
			},
			Extra: map[string]any{"msg_key": "msg_value"},
		},
	}

	copied := copyMessagesGeneric(original)
	require.Len(t, copied, 1)

	// Mutate the copy and verify original is unchanged.
	// 修改副本并验证原始值未改变。
	copied[0].ContentBlocks[0].FunctionToolCall.Arguments = `{"modified":true}`
	assert.Equal(t, `{"x":1}`, original[0].ContentBlocks[0].FunctionToolCall.Arguments,
		"original FunctionToolCall.Arguments must not be affected")

	copied[0].ContentBlocks[1].FunctionToolResult.Content[0].Text.Text = "modified result"
	assert.Equal(t, "original result", original[0].ContentBlocks[1].FunctionToolResult.Content[0].Text.Text,
		"original FunctionToolResult text must not be affected")

	copied[0].ContentBlocks[1].Extra["meta"] = "changed"
	assert.Equal(t, "data", original[0].ContentBlocks[1].Extra["meta"],
		"original ContentBlock.Extra must not be affected")

	copied[0].Extra["msg_key"] = "changed"
	assert.Equal(t, "msg_value", original[0].Extra["msg_key"],
		"original AgenticMessage.Extra must not be affected")
}

func TestToolResultFromMsgGeneric_AgenticMessage(t *testing.T) {
	t.Run("single text block returns fromContent=true", func(t *testing.T) {
		msg := &schema.AgenticMessage{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				{
					Type: schema.ContentBlockTypeFunctionToolResult,
					FunctionToolResult: &schema.FunctionToolResult{
						CallID: "c1",
						Name:   "tool1",
						Content: []*schema.FunctionToolResultContentBlock{
							{Text: &schema.UserInputText{Text: "hello result"}},
						},
					},
				},
			},
		}

		result, fromContent, err := toolResultFromMsgGeneric(msg)
		assert.NoError(t, err)
		assert.True(t, fromContent, "single text part should be fromContent=true")
		require.Len(t, result.Parts, 1)
		assert.Equal(t, schema.ToolPartTypeText, result.Parts[0].Type)
		assert.Equal(t, "hello result", result.Parts[0].Text)
	})

	t.Run("multiple blocks returns fromContent=false", func(t *testing.T) {
		msg := &schema.AgenticMessage{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				{
					Type: schema.ContentBlockTypeFunctionToolResult,
					FunctionToolResult: &schema.FunctionToolResult{
						CallID: "c1",
						Name:   "tool1",
						Content: []*schema.FunctionToolResultContentBlock{
							{Text: &schema.UserInputText{Text: "text part"}},
							{Text: &schema.UserInputText{Text: "another text part"}},
						},
					},
				},
			},
		}

		result, fromContent, err := toolResultFromMsgGeneric(msg)
		assert.NoError(t, err)
		assert.False(t, fromContent, "multiple parts should be fromContent=false")
		require.Len(t, result.Parts, 2)
		assert.Equal(t, "text part", result.Parts[0].Text)
		assert.Equal(t, "another text part", result.Parts[1].Text)
	})

	t.Run("empty blocks returns empty text", func(t *testing.T) {
		msg := &schema.AgenticMessage{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				{
					Type: schema.ContentBlockTypeFunctionToolResult,
					FunctionToolResult: &schema.FunctionToolResult{
						CallID:  "c1",
						Name:    "tool1",
						Content: nil,
					},
				},
			},
		}

		result, fromContent, err := toolResultFromMsgGeneric(msg)
		assert.NoError(t, err)
		assert.True(t, fromContent)
		require.Len(t, result.Parts, 1)
		assert.Equal(t, "", result.Parts[0].Text)
	})
}

func TestSetToolResultContent_AgenticMessage(t *testing.T) {
	t.Run("fromContent=true sets text", func(t *testing.T) {
		msg := &schema.AgenticMessage{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				{
					Type: schema.ContentBlockTypeFunctionToolResult,
					FunctionToolResult: &schema.FunctionToolResult{
						CallID: "c1",
						Name:   "tool1",
						Content: []*schema.FunctionToolResultContentBlock{
							{Text: &schema.UserInputText{Text: "old"}},
						},
					},
				},
			},
		}

		newResult := &schema.ToolResult{
			Parts: []schema.ToolOutputPart{
				{Type: schema.ToolPartTypeText, Text: "new content"},
			},
		}

		setToolResultContent(msg, newResult, true)

		// Verify the block was updated
		// 验证该 block 已更新
		blocks := msg.ContentBlocks[0].FunctionToolResult.Content
		require.Len(t, blocks, 1)
		assert.Equal(t, "new content", blocks[0].Text.Text)
	})

	t.Run("fromContent=false sets multi-part", func(t *testing.T) {
		msg := &schema.AgenticMessage{
			Role: schema.AgenticRoleTypeUser,
			ContentBlocks: []*schema.ContentBlock{
				{
					Type: schema.ContentBlockTypeFunctionToolResult,
					FunctionToolResult: &schema.FunctionToolResult{
						CallID: "c1",
						Name:   "tool1",
						Content: []*schema.FunctionToolResultContentBlock{
							{Text: &schema.UserInputText{Text: "old"}},
						},
					},
				},
			},
		}

		imgURL := "https://example.com/img.png"
		newResult := &schema.ToolResult{
			Parts: []schema.ToolOutputPart{
				{Type: schema.ToolPartTypeText, Text: "text part"},
				{Type: schema.ToolPartTypeImage, Image: &schema.ToolOutputImage{
					MessagePartCommon: schema.MessagePartCommon{URL: &imgURL, MIMEType: "image/png"},
				}},
			},
		}

		setToolResultContent(msg, newResult, false)

		blocks := msg.ContentBlocks[0].FunctionToolResult.Content
		require.Len(t, blocks, 2)
		assert.Equal(t, "text part", blocks[0].Text.Text)
		require.NotNil(t, blocks[1].Image)
		assert.Equal(t, "https://example.com/img.png", blocks[1].Image.URL)
		assert.Equal(t, "image/png", blocks[1].Image.MIMEType)
	})
}

func TestToolResultFromMsgGeneric_MediaBlocks(t *testing.T) {
	imgURL := "https://example.com/img.png"
	audioURL := "https://example.com/audio.wav"
	videoURL := "https://example.com/video.mp4"
	fileURL := "https://example.com/doc.pdf"

	msg := &schema.AgenticMessage{
		Role: schema.AgenticRoleTypeUser,
		ContentBlocks: []*schema.ContentBlock{
			{
				Type: schema.ContentBlockTypeFunctionToolResult,
				FunctionToolResult: &schema.FunctionToolResult{
					CallID: "c1",
					Name:   "media_tool",
					Content: []*schema.FunctionToolResultContentBlock{
						{Image: &schema.UserInputImage{URL: imgURL, MIMEType: "image/png"}},
						{Audio: &schema.UserInputAudio{URL: audioURL, MIMEType: "audio/wav"}},
						{Video: &schema.UserInputVideo{URL: videoURL, MIMEType: "video/mp4"}},
						{File: &schema.UserInputFile{URL: fileURL, MIMEType: "application/pdf"}},
					},
				},
			},
		},
	}

	result, fromContent, err := toolResultFromMsgGeneric(msg)
	assert.NoError(t, err)
	assert.False(t, fromContent, "multi-media should be fromContent=false")
	require.Len(t, result.Parts, 4)

	assert.Equal(t, schema.ToolPartTypeImage, result.Parts[0].Type)
	require.NotNil(t, result.Parts[0].Image)
	require.NotNil(t, result.Parts[0].Image.URL)
	assert.Equal(t, imgURL, *result.Parts[0].Image.URL)

	assert.Equal(t, schema.ToolPartTypeAudio, result.Parts[1].Type)
	require.NotNil(t, result.Parts[1].Audio)
	require.NotNil(t, result.Parts[1].Audio.URL)
	assert.Equal(t, audioURL, *result.Parts[1].Audio.URL)

	assert.Equal(t, schema.ToolPartTypeVideo, result.Parts[2].Type)
	require.NotNil(t, result.Parts[2].Video)
	require.NotNil(t, result.Parts[2].Video.URL)
	assert.Equal(t, videoURL, *result.Parts[2].Video.URL)

	assert.Equal(t, schema.ToolPartTypeFile, result.Parts[3].Type)
	require.NotNil(t, result.Parts[3].File)
	require.NotNil(t, result.Parts[3].File.URL)
	assert.Equal(t, fileURL, *result.Parts[3].File.URL)
}

func TestSetToolResultContent_MediaBlocks(t *testing.T) {
	audioURL := "https://example.com/speech.mp3"
	videoURL := "https://example.com/clip.mp4"
	fileURL := "https://example.com/report.pdf"

	msg := &schema.AgenticMessage{
		Role: schema.AgenticRoleTypeUser,
		ContentBlocks: []*schema.ContentBlock{
			{
				Type: schema.ContentBlockTypeFunctionToolResult,
				FunctionToolResult: &schema.FunctionToolResult{
					CallID: "c1",
					Name:   "tool1",
					Content: []*schema.FunctionToolResultContentBlock{
						{Text: &schema.UserInputText{Text: "old"}},
					},
				},
			},
		},
	}

	newResult := &schema.ToolResult{
		Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeAudio, Audio: &schema.ToolOutputAudio{
				MessagePartCommon: schema.MessagePartCommon{URL: &audioURL, MIMEType: "audio/mp3"},
			}},
			{Type: schema.ToolPartTypeVideo, Video: &schema.ToolOutputVideo{
				MessagePartCommon: schema.MessagePartCommon{URL: &videoURL, MIMEType: "video/mp4"},
			}},
			{Type: schema.ToolPartTypeFile, File: &schema.ToolOutputFile{
				MessagePartCommon: schema.MessagePartCommon{URL: &fileURL, MIMEType: "application/pdf"},
			}},
		},
	}

	setToolResultContent(msg, newResult, false)

	blocks := msg.ContentBlocks[0].FunctionToolResult.Content
	require.Len(t, blocks, 3)

	require.NotNil(t, blocks[0].Audio)
	assert.Equal(t, "https://example.com/speech.mp3", blocks[0].Audio.URL)
	assert.Equal(t, "audio/mp3", blocks[0].Audio.MIMEType)

	require.NotNil(t, blocks[1].Video)
	assert.Equal(t, "https://example.com/clip.mp4", blocks[1].Video.URL)
	assert.Equal(t, "video/mp4", blocks[1].Video.MIMEType)

	require.NotNil(t, blocks[2].File)
	assert.Equal(t, "https://example.com/report.pdf", blocks[2].File.URL)
	assert.Equal(t, "application/pdf", blocks[2].File.MIMEType)
}

func TestAgenticURLToMPC(t *testing.T) {
	t.Run("non-empty URL", func(t *testing.T) {
		ftr := &schema.FunctionToolResult{
			Content: []*schema.FunctionToolResultContentBlock{
				{Type: schema.FunctionToolResultContentBlockTypeFile, File: &schema.UserInputFile{URL: "https://example.com/file.pdf", MIMEType: "application/pdf"}},
			},
		}
		parts := toolResultToOutputParts(ftr)
		require.Len(t, parts, 1)
		require.NotNil(t, parts[0].File)
		require.NotNil(t, parts[0].File.URL)
		assert.Equal(t, "https://example.com/file.pdf", *parts[0].File.URL)
		assert.Equal(t, "application/pdf", parts[0].File.MIMEType)
	})

	t.Run("empty URL", func(t *testing.T) {
		ftr := &schema.FunctionToolResult{
			Content: []*schema.FunctionToolResultContentBlock{
				{Type: schema.FunctionToolResultContentBlockTypeFile, File: &schema.UserInputFile{URL: "", MIMEType: "text/plain"}},
			},
		}
		parts := toolResultToOutputParts(ftr)
		require.Len(t, parts, 1)
		require.NotNil(t, parts[0].File)
		assert.Nil(t, parts[0].File.URL)
		assert.Equal(t, "text/plain", parts[0].File.MIMEType)
	})
}

func TestMpcURLToString(t *testing.T) {
	t.Run("non-nil URL", func(t *testing.T) {
		urlStr := "https://example.com"
		tr := &schema.FunctionToolResult{}
		setToolResultFromOutputParts(tr, []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeFile, File: &schema.ToolOutputFile{MessagePartCommon: schema.MessagePartCommon{URL: &urlStr, MIMEType: "text/plain"}}},
		})
		require.Len(t, tr.Content, 1)
		require.NotNil(t, tr.Content[0].File)
		assert.Equal(t, "https://example.com", tr.Content[0].File.URL)
	})

	t.Run("nil URL", func(t *testing.T) {
		tr := &schema.FunctionToolResult{}
		setToolResultFromOutputParts(tr, []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeFile, File: &schema.ToolOutputFile{MessagePartCommon: schema.MessagePartCommon{URL: nil, MIMEType: "text/plain"}}},
		})
		require.Len(t, tr.Content, 1)
		require.NotNil(t, tr.Content[0].File)
		assert.Equal(t, "", tr.Content[0].File.URL)
	})
}

// ---------------------------------------------------------------------------
// Top-level test
// ---------------------------------------------------------------------------
//
// ---------------------------------------------------------------------------
// 顶层测试
// ---------------------------------------------------------------------------

func TestReductionGeneric(t *testing.T) {
	t.Run("Message", func(t *testing.T) {
		t.Run("Helpers", testHelperFunctions[*schema.Message])
		t.Run("ClearFlow", testClearFlowGeneric[*schema.Message])
		t.Run("Truncation", testTruncationGeneric[*schema.Message])
		t.Run("ClearPostProcess", testClearPostProcessGeneric[*schema.Message])
		t.Run("CopyNilMessage", testCopyNilMessage[*schema.Message])
	})
	t.Run("AgenticMessage", func(t *testing.T) {
		t.Run("Helpers", testHelperFunctions[*schema.AgenticMessage])
		t.Run("ClearFlow", testClearFlowGeneric[*schema.AgenticMessage])
		t.Run("Truncation", testTruncationGeneric[*schema.AgenticMessage])
		t.Run("ClearPostProcess", testClearPostProcessGeneric[*schema.AgenticMessage])
		t.Run("CopyNilMessage", testCopyNilMessage[*schema.AgenticMessage])
	})
}

// testCopyNilMessage verifies that copyMessagesGeneric does not panic when
// the input slice contains nil message elements (regression test).
//
// testCopyNilMessage 验证当输入切片包含 nil message 元素时，
// copyMessagesGeneric 不会 panic（回归测试）。
func testCopyNilMessage[M adk.MessageType](t *testing.T) {
	var zero M
	var msgs []M
	switch any(zero).(type) {
	case *schema.Message:
		msgs = any([]*schema.Message{
			schema.UserMessage("hello"),
			nil,
			schema.UserMessage("world"),
		}).([]M)
	case *schema.AgenticMessage:
		msgs = any([]*schema.AgenticMessage{
			schema.UserAgenticMessage("hello"),
			nil,
			schema.UserAgenticMessage("world"),
		}).([]M)
	}

	assert.NotPanics(t, func() {
		copied := copyMessagesGeneric(msgs)
		assert.Len(t, copied, 3)
		assert.Nil(t, copied[1], "nil element should be preserved as nil")
	})
}
