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

package adk

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	mockModel "github.com/cloudwego/eino/internal/mock/components/model"
	"github.com/cloudwego/eino/schema"
)

func isValidUUID(s string) bool {
	// UUID v4 format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (8-4-4-4-12 = 36 chars)
	// UUID v4 格式：xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx（8-4-4-4-12 = 36 个字符）
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// collectEvents drains all events from the iterator (non-streaming).
// collectEvents 从迭代器中取尽所有事件（非流式）。
func collectEvents(t *testing.T, iter *AsyncIterator[*AgentEvent]) []*AgentEvent {
	t.Helper()
	var events []*AgentEvent
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		events = append(events, event)
	}
	return events
}

// Scenario 1: AgentEvent messages have IDs (Generate mode)
// 场景 1：AgentEvent 消息带有 ID（Generate 模式）
func TestMessageID_EventHasID_Generate(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("hello world", nil), nil).
		Times(1)

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hi")},
	})

	events := collectEvents(t, iter)
	require.Len(t, events, 1)
	require.Nil(t, events[0].Err)
	require.NotNil(t, events[0].Output.MessageOutput)

	msg := events[0].Output.MessageOutput.Message
	require.NotNil(t, msg)
	msgID := GetMessageID(msg)
	assert.NotEmpty(t, msgID, "event message should have an ID")
	assert.True(t, isValidUUID(msgID), "message ID should be a valid UUID, got: %s", msgID)
}

// Scenario 2: Event and state messages share the same ID
// 场景 2：Event 和 state 消息共享同一个 ID
func TestMessageID_EventAndStateShareSameID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("response", nil), nil).
		Times(1)

	var stateMessagesAfterModel []*schema.Message

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
		Middlewares: []AgentMiddleware{
			{
				AfterChatModel: func(ctx context.Context, state *ChatModelAgentState) error {
					// Capture state messages after model call (including the model output)
					// 在 model 调用后捕获 state 消息（包括 model 输出）
					stateMessagesAfterModel = make([]*schema.Message, len(state.Messages))
					copy(stateMessagesAfterModel, state.Messages)
					return nil
				},
			},
		},
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hi")},
	})

	events := collectEvents(t, iter)
	require.Len(t, events, 1)
	require.Nil(t, events[0].Err)

	eventMsg := events[0].Output.MessageOutput.Message
	eventMsgID := GetMessageID(eventMsg)
	assert.NotEmpty(t, eventMsgID)

	// The last message in state should be the model output with the same ID
	// state 中最后一条消息应为 model 输出，并带有相同 ID
	require.NotEmpty(t, stateMessagesAfterModel)
	lastStateMsg := stateMessagesAfterModel[len(stateMessagesAfterModel)-1]
	stateMsgID := GetMessageID(lastStateMsg)

	assert.Equal(t, eventMsgID, stateMsgID,
		"event msg ID (%s) and state msg ID (%s) must match", eventMsgID, stateMsgID)
}

// Scenario 3: Stream — first chunk carries ID, concatenated message has correct ID
// 场景 3：Stream — 第一个 chunk 携带 ID，拼接后的消息有正确的 ID
func TestMessageID_Stream_FirstChunkOnly(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Stream(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.StreamReaderFromArray([]*schema.Message{
			schema.AssistantMessage("chunk1", nil),
			schema.AssistantMessage("chunk2", nil),
			schema.AssistantMessage("chunk3", nil),
		}), nil).
		Times(1)

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages:        []Message{schema.UserMessage("hi")},
		EnableStreaming: true,
	})

	event, ok := iter.Next()
	require.True(t, ok)
	require.Nil(t, event.Err)
	require.NotNil(t, event.Output.MessageOutput)
	require.True(t, event.Output.MessageOutput.IsStreaming)

	stream := event.Output.MessageOutput.MessageStream
	require.NotNil(t, stream)

	var chunks []*schema.Message
	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}
		chunks = append(chunks, msg)
	}
	require.GreaterOrEqual(t, len(chunks), 1)

	// First chunk should have the ID
	// 第一个 chunk 应带有该 ID
	firstChunkID := GetMessageID(chunks[0])
	assert.NotEmpty(t, firstChunkID, "first chunk should carry the message ID")
	assert.True(t, isValidUUID(firstChunkID))

	// Subsequent chunks should NOT have the ID in Extra (first-chunk-only injection)
	// 后续 chunk 的 Extra 中不应带有该 ID（仅注入第一个 chunk）
	for i := 1; i < len(chunks); i++ {
		chunkID := GetMessageID(chunks[i])
		assert.Empty(t, chunkID, "chunk %d should not have message ID (first-chunk-only)", i)
	}

	// No more events
	// 没有更多事件
	_, ok = iter.Next()
	assert.False(t, ok)
}

// Scenario 4: Tool messages have IDs
// 场景 4：Tool 消息带有 ID
func TestMessageID_ToolMessagesHaveID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	fakeTool := &fakeToolForTest{tarCount: 1}
	info, err := fakeTool.Info(ctx)
	require.NoError(t, err)

	generateCount := 0
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
			generateCount++
			if generateCount == 1 {
				return schema.AssistantMessage("calling tool",
					[]schema.ToolCall{{
						ID: "tc-1",
						Function: schema.FunctionCall{
							Name:      info.Name,
							Arguments: `{"name": "tester"}`,
						},
					}}), nil
			}
			return schema.AssistantMessage("done", nil), nil
		}).AnyTimes()
	cm.EXPECT().WithTools(gomock.Any()).Return(cm, nil).AnyTimes()

	// Capture tool result messages from state via BeforeChatModel on the 2nd model call.
	// 在第 2 次 model 调用时，通过 BeforeChatModel 从 state 捕获工具结果消息。
	var toolMsgIDInState string
	beforeModelCount := 0
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{fakeTool},
			},
		},
		Middlewares: []AgentMiddleware{
			{
				BeforeChatModel: func(ctx context.Context, state *ChatModelAgentState) error {
					beforeModelCount++
					if beforeModelCount == 2 {
						// 2nd model call: state.Messages contains tool result messages
						// 第 2 次 model 调用：state.Messages 包含工具结果消息
						for _, m := range state.Messages {
							if m.Role == schema.Tool && m.ToolCallID == "tc-1" {
								toolMsgIDInState = GetMessageID(m)
							}
						}
					}
					return nil
				},
			},
		},
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("use tool")},
	})

	events := collectEvents(t, iter)
	// Expect 3 events: model(tool_call) + tool(result) + model(final)
	// 预期 3 个事件：model(tool_call) + tool(result) + model(final)
	require.Len(t, events, 3)

	// Tool event (index 1)
	// 工具事件（index 1）
	toolEvent := events[1]
	require.Nil(t, toolEvent.Err)
	require.NotNil(t, toolEvent.Output.MessageOutput)
	assert.Equal(t, schema.Tool, toolEvent.Output.MessageOutput.Role)

	toolMsg := toolEvent.Output.MessageOutput.Message
	require.NotNil(t, toolMsg)
	toolMsgID := GetMessageID(toolMsg)
	assert.NotEmpty(t, toolMsgID, "tool message should have an ID")
	assert.True(t, isValidUUID(toolMsgID))

	// All events should have IDs
	// 所有事件都应有 ID
	for i, ev := range events {
		require.Nil(t, ev.Err)
		require.NotNil(t, ev.Output.MessageOutput)
		msg := ev.Output.MessageOutput.Message
		require.NotNil(t, msg)
		assert.NotEmpty(t, GetMessageID(msg), "event[%d] should have a message ID", i)
	}

	// The tool message in state should share the same ID as the event tool message.
	// state 中的工具消息应与事件中的工具消息共享同一 ID。
	assert.NotEmpty(t, toolMsgIDInState, "tool message in state should have an ID")
	assert.Equal(t, toolMsgID, toolMsgIDInState,
		"tool event msg ID (%s) and state msg ID (%s) must match", toolMsgID, toolMsgIDInState)
}

// Scenario 5: Retry — the final accepted result carries a message ID
// 场景 5：重试 — 最终接受的结果带有消息 ID
func TestMessageID_Retry_FinalResultHasID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	retryErr := errors.New("retryable error")

	var callCount int32
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
			count := atomic.AddInt32(&callCount, 1)
			if count < 3 {
				return nil, retryErr
			}
			return schema.AssistantMessage("Success after retry", nil), nil
		}).Times(3)

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
		ModelRetryConfig: &ModelRetryConfig{
			MaxRetries: 3,
			ShouldRetry: func(ctx context.Context, retryCtx *RetryContext) *RetryDecision {
				return &RetryDecision{Retry: errors.Is(retryCtx.Err, retryErr)}
			},
		},
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hello")},
	})

	events := collectEvents(t, iter)
	require.Len(t, events, 1)
	require.Nil(t, events[0].Err)

	msg := events[0].Output.MessageOutput.Message
	msgID := GetMessageID(msg)
	assert.NotEmpty(t, msgID, "surviving message should have an ID")
	assert.True(t, isValidUUID(msgID))
	assert.Equal(t, int32(3), atomic.LoadInt32(&callCount))
}

// Scenario 6: WrapModel handler sees model output with ID
// 场景 6：WrapModel 处理器看到带 ID 的模型输出
func TestMessageID_WrapModelSeesID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("model output", nil), nil).
		Times(1)

	var capturedMsgID string

	handler := &wrapModelIDCheckHandler{
		BaseChatModelAgentMiddleware: &BaseChatModelAgentMiddleware{},
		onGenerate: func(result *schema.Message) {
			capturedMsgID = GetMessageID(result)
		},
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
		Handlers:    []ChatModelAgentMiddleware{handler},
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hi")},
	})

	events := collectEvents(t, iter)
	require.Len(t, events, 1)
	require.Nil(t, events[0].Err)

	assert.NotEmpty(t, capturedMsgID, "WrapModel handler should see message ID on model output")
	assert.True(t, isValidUUID(capturedMsgID))

	// The event should carry the same ID
	// 事件应携带相同的 ID
	eventMsgID := GetMessageID(events[0].Output.MessageOutput.Message)
	assert.Equal(t, capturedMsgID, eventMsgID,
		"WrapModel-captured ID (%s) should match event ID (%s)", capturedMsgID, eventMsgID)
}

// wrapModelIDCheckHandler wraps the model to inspect the output for message ID.
// wrapModelIDCheckHandler 包装模型以检查输出中的消息 ID。
type wrapModelIDCheckHandler struct {
	*BaseChatModelAgentMiddleware
	onGenerate func(result *schema.Message)
}

func (h *wrapModelIDCheckHandler) WrapModel(_ context.Context, m model.BaseChatModel, _ *ModelContext) (model.BaseChatModel, error) {
	return &idCheckModelWrapper{inner: m, onGenerate: h.onGenerate}, nil
}

type idCheckModelWrapper struct {
	inner      model.BaseChatModel
	onGenerate func(result *schema.Message)
}

func (w *idCheckModelWrapper) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	result, err := w.inner.Generate(ctx, input, opts...)
	if err == nil && w.onGenerate != nil {
		w.onGenerate(result)
	}
	return result, err
}

func (w *idCheckModelWrapper) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return w.inner.Stream(ctx, input, opts...)
}

// Scenario 7: User input messages do NOT get automatic IDs (they are external, not framework-created).
// Only framework-created messages (model output, tool results, TypedSendEvent) get auto-assigned IDs.
//
// 场景 7：用户输入消息不会自动获得 ID（它们是外部的，不是框架创建的）。
// 只有框架创建的消息（模型输出、工具结果、TypedSendEvent）会自动分配 ID。
func TestMessageID_UserInputNoAutoID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)

	var stateMessagesBeforeModel []*schema.Message
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
			// Capture input messages
			// 捕获输入消息
			stateMessagesBeforeModel = make([]*schema.Message, len(input))
			copy(stateMessagesBeforeModel, input)
			return schema.AssistantMessage("response", nil), nil
		}).Times(1)

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hello")},
	})

	events := collectEvents(t, iter)
	require.Len(t, events, 1)
	require.Nil(t, events[0].Err)

	// User input messages should NOT have auto-assigned IDs.
	// Framework only assigns IDs to messages it creates (model output, tool results, SendEvent).
	//
	// 用户输入消息不应自动分配 ID。
	// 框架只为它创建的消息（模型输出、工具结果、SendEvent）分配 ID。
	require.NotEmpty(t, stateMessagesBeforeModel)

	for i, msg := range stateMessagesBeforeModel {
		msgID := GetMessageID(msg)
		assert.Empty(t, msgID, "input message[%d] (role=%s) should NOT have auto-assigned ID", i, msg.Role)
	}
}

// Scenario 8: Middleware must call EnsureMessageID before SendEvent; pointer identity ensures state consistency
// TestMessageID_SendEvent_MiddlewareMustEnsureID verifies that TypedSendEvent is a pure
// transport and does NOT auto-assign message IDs. Middleware authors must call
// EnsureMessageID themselves before sending.
//
// 场景 8：中间件必须在 SendEvent 前调用 EnsureMessageID；指针身份保证 state 一致性
// TestMessageID_SendEvent_MiddlewareMustEnsureID 验证 TypedSendEvent 是纯
// 传输层，不会自动分配消息 ID。中间件作者必须在发送前自行调用
// EnsureMessageID。
func TestMessageID_SendEvent_MiddlewareMustEnsureID(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("model response", nil), nil).
		Times(1)

	// Track the message pointer that the middleware creates and writes to both state and event
	// 跟踪中间件创建并写入 state 和 event 的消息指针
	var middlewareMsg *schema.Message
	var stateMsgIDAfterSendEvent string

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMsgID",
		Instruction: "test",
		Model:       cm,
		Middlewares: []AgentMiddleware{
			{
				AfterChatModel: func(ctx context.Context, state *ChatModelAgentState) error {
					// Middleware creates a new message and writes the SAME pointer to both state and event
					// 中间件创建一条新消息，并将同一个指针写入 state 和 event
					middlewareMsg = schema.AssistantMessage("middleware injected", nil)

					// Middleware is responsible for assigning the ID before sending
					// 中间件负责在发送前分配 ID
					EnsureMessageID(middlewareMsg)

					// Write to state
					// 写入 state
					state.Messages = append(state.Messages, middlewareMsg)

					// Send as event — TypedSendEvent does NOT auto-assign ID
					// 作为事件发送 — TypedSendEvent 不会自动分配 ID
					event := EventFromMessage(middlewareMsg, nil, schema.Assistant, "")
					err := SendEvent(ctx, event)
					if err != nil {
						return err
					}

					// Because we called EnsureMessageID on the shared pointer,
					// the state copy also has the ID (pointer identity)
					//
					// 因为我们在共享指针上调用了 EnsureMessageID，
					// state 副本也有该 ID（指针身份）
					stateMsgIDAfterSendEvent = internal.GetMessageID(middlewareMsg.Extra)

					return nil
				},
			},
		},
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hi")},
	})

	var allEvents []*AgentEvent
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		allEvents = append(allEvents, event)
	}

	// We expect at least 2 events: model response + middleware injected message
	// 预期至少有 2 个事件：模型响应 + 中间件注入的消息
	require.GreaterOrEqual(t, len(allEvents), 2)

	// The middleware message pointer should have an ID (assigned by middleware via EnsureMessageID)
	// 中间件消息指针应有 ID（由中间件通过 EnsureMessageID 分配）
	require.NotNil(t, middlewareMsg)
	middlewareMsgID := GetMessageID(middlewareMsg)
	assert.NotEmpty(t, middlewareMsgID, "middleware should have assigned an ID via EnsureMessageID")
	assert.True(t, isValidUUID(middlewareMsgID))

	// The ID captured right after SendEvent (via pointer identity) should be the same
	// 通过指针身份在 SendEvent 后立即捕获的 ID 应相同
	assert.Equal(t, middlewareMsgID, stateMsgIDAfterSendEvent,
		"pointer identity: ID read from state pointer (%s) should match message ID (%s)",
		stateMsgIDAfterSendEvent, middlewareMsgID)

	// Find the middleware event in the collected events
	// 在收集到的事件中查找 middleware 事件
	var middlewareEventMsgID string
	for _, ev := range allEvents {
		if ev.Err != nil || ev.Output == nil || ev.Output.MessageOutput == nil {
			continue
		}
		msg := ev.Output.MessageOutput.Message
		if msg != nil && msg.Content == "middleware injected" {
			middlewareEventMsgID = GetMessageID(msg)
			break
		}
	}
	assert.Equal(t, middlewareMsgID, middlewareEventMsgID,
		"event message ID (%s) should match the middleware message ID (%s)",
		middlewareEventMsgID, middlewareMsgID)
}

func TestAttack_ConcatCorruptsIDIfMultipleChunksCarryIt(t *testing.T) {
	id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	msgs := []*schema.Message{
		{Role: schema.Assistant, Content: "chunk1", Extra: map[string]any{internal.EinoMsgIDKey: id}},
		{Role: schema.Assistant, Content: "chunk2", Extra: map[string]any{internal.EinoMsgIDKey: id}},
		{Role: schema.Assistant, Content: "chunk3", Extra: map[string]any{internal.EinoMsgIDKey: id}},
	}
	concatenated, err := schema.ConcatMessages(msgs)
	require.NoError(t, err)

	resultID := internal.GetMessageID(concatenated.Extra)
	// ConcatMessages string-concatenates duplicate Extra keys, corrupting the ID
	// ConcatMessages 会对重复的 Extra 键做字符串拼接，导致 ID 损坏
	assert.NotEqual(t, id, resultID, "ConcatMessages should corrupt the ID when multiple chunks carry it")
	assert.NotEqual(t, 36, len(resultID), "corrupted ID should not be 36 chars")
	assert.Equal(t, "chunk1chunk2chunk3", concatenated.Content)
}

func TestAttack_ConcatPreservesIDIfOnlyFirstChunkHasIt(t *testing.T) {
	id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	msgs := []*schema.Message{
		{Role: schema.Assistant, Content: "chunk1", Extra: map[string]any{internal.EinoMsgIDKey: id}},
		{Role: schema.Assistant, Content: "chunk2"},
		{Role: schema.Assistant, Content: "chunk3"},
	}
	concatenated, err := schema.ConcatMessages(msgs)
	require.NoError(t, err)

	resultID := internal.GetMessageID(concatenated.Extra)
	assert.Equal(t, id, resultID, "ID should be preserved when only first chunk carries it")
	assert.Equal(t, "chunk1chunk2chunk3", concatenated.Content)
}

func TestAttack_ConcurrentGenerate_NoSharedExtraMutation(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Shared singleton message - same pointer returned every time
	// 共享的单例 message——每次都返回同一个指针
	sharedMsg := schema.AssistantMessage("shared response", nil)

	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(sharedMsg, nil).
		AnyTimes()

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAttack",
		Instruction: "test",
		Model:       cm,
	})
	require.NoError(t, err)

	const N = 10
	ids := make([]string, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			iter := agent.Run(ctx, &AgentInput{
				Messages: []Message{schema.UserMessage("hi")},
			})
			events := collectEvents(t, iter)
			require.Len(t, events, 1)
			require.Nil(t, events[0].Err)
			msg := events[0].Output.MessageOutput.Message
			require.NotNil(t, msg)
			ids[idx] = GetMessageID(msg)
		}(i)
	}
	wg.Wait()

	// All IDs should be unique and valid
	// 所有 ID 都应唯一且有效
	seen := make(map[string]bool)
	for i, id := range ids {
		assert.NotEmpty(t, id, "goroutine %d should have an ID", i)
		assert.True(t, isValidUUID(id), "goroutine %d ID should be valid UUID: %s", i, id)
		assert.False(t, seen[id], "goroutine %d has duplicate ID: %s", i, id)
		seen[id] = true
	}

	// The original shared message should NOT have been mutated (or if it was, it should still be valid)
	// The important thing is no panic and unique IDs
	//
	// 原始共享 message 不应被修改（如果被修改，也应仍然有效）
	// 关键是没有 panic 且 ID 唯一
}

func TestAttack_GenerateCopyDoesNotAffectOriginal(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	originalMsg := schema.AssistantMessage("original", nil)
	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(originalMsg, nil).
		Times(1)

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAttack",
		Instruction: "test",
		Model:       cm,
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("hi")},
	})

	events := collectEvents(t, iter)
	require.Len(t, events, 1)
	require.Nil(t, events[0].Err)

	eventMsg := events[0].Output.MessageOutput.Message
	eventMsgID := GetMessageID(eventMsg)
	assert.NotEmpty(t, eventMsgID)

	// The ORIGINAL message returned by the model should NOT have an ID
	// because wrapGenerateEndpoint copies before mutating
	//
	// 模型返回的 ORIGINAL message 不应带有 ID
	// 因为 wrapGenerateEndpoint 会先复制再修改
	originalID := GetMessageID(originalMsg)
	assert.Empty(t, originalID, "original model output should NOT be mutated by ID assignment")
}

// ============================================================
// AgenticMessage Integration Tests
// ============================================================
//
// ============================================================
// AgenticMessage 集成测试
// ============================================================

// TestMessageID_AgenticGenerate verifies that AgenticMessage-typed agents
// get message IDs assigned on Generate output, covering the *schema.AgenticMessage
// branches in EnsureMessageID, GetMessageID, and copyMessage.
//
// TestMessageID_AgenticGenerate 验证 AgenticMessage 类型的智能体
// 在 Generate 输出上会被分配 message ID，并覆盖 EnsureMessageID、GetMessageID 和 copyMessage 中的 *schema.AgenticMessage
// 分支。
func TestMessageID_AgenticGenerate(t *testing.T) {
	ctx := context.Background()

	agenticResponse := &schema.AgenticMessage{
		Role: schema.AgenticRoleTypeAssistant,
		ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.AssistantGenText{Text: "agentic response"}),
		},
	}

	m := &mockAgenticModel{
		generateFn: func(ctx context.Context, input []*schema.AgenticMessage, opts ...model.Option) (*schema.AgenticMessage, error) {
			return agenticResponse, nil
		},
	}

	agent, err := NewTypedChatModelAgent(ctx, &TypedChatModelAgentConfig[*schema.AgenticMessage]{
		Name:        "AgenticMsgID",
		Instruction: "test",
		Model:       m,
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &TypedAgentInput[*schema.AgenticMessage]{
		Messages: []*schema.AgenticMessage{schema.UserAgenticMessage("hi")},
	})

	event, ok := iter.Next()
	require.True(t, ok)
	require.Nil(t, event.Err)
	require.NotNil(t, event.Output)
	require.NotNil(t, event.Output.MessageOutput)

	msg := event.Output.MessageOutput.Message
	require.NotNil(t, msg)

	// Verify via the AgenticMessage-specific public API
	// 通过 AgenticMessage 专用的公开 API 验证
	msgID := GetMessageID(msg)
	assert.NotEmpty(t, msgID, "agentic model output should have message ID")
	assert.True(t, isValidUUID(msgID), "agentic message ID should be valid UUID: %s", msgID)

	// Original message should NOT be mutated (copyMessage for AgenticMessage branch)
	// 原始 message 不应被修改（copyMessage 的 AgenticMessage 分支）
	originalID := GetMessageID(agenticResponse)
	assert.Empty(t, originalID, "original agentic model output should NOT be mutated")

	// Drain iterator
	// 耗尽 iterator
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}
}

// TestMessageID_AgenticStream verifies first-chunk-only ID injection for AgenticMessage streams.
// TestMessageID_AgenticStream 验证 AgenticMessage 流只在首个 chunk 注入 ID。
func TestMessageID_AgenticStream(t *testing.T) {
	ctx := context.Background()

	m := &mockAgenticModel{
		generateFn: func(ctx context.Context, input []*schema.AgenticMessage, opts ...model.Option) (*schema.AgenticMessage, error) {
			return nil, errors.New("should not be called")
		},
		streamFn: func(ctx context.Context, input []*schema.AgenticMessage, opts ...model.Option) (*schema.StreamReader[*schema.AgenticMessage], error) {
			r, w := schema.Pipe[*schema.AgenticMessage](3)
			go func() {
				defer w.Close()
				for i := 0; i < 3; i++ {
					w.Send(&schema.AgenticMessage{
						Role: schema.AgenticRoleTypeAssistant,
						ContentBlocks: []*schema.ContentBlock{
							schema.NewContentBlock(&schema.AssistantGenText{Text: "chunk"}),
						},
					}, nil)
				}
			}()
			return r, nil
		},
	}

	agent, err := NewTypedChatModelAgent(ctx, &TypedChatModelAgentConfig[*schema.AgenticMessage]{
		Name:        "AgenticStreamMsgID",
		Instruction: "test",
		Model:       m,
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &TypedAgentInput[*schema.AgenticMessage]{
		Messages:        []*schema.AgenticMessage{schema.UserAgenticMessage("hi")},
		EnableStreaming: true,
	})

	event, ok := iter.Next()
	require.True(t, ok)
	require.Nil(t, event.Err)
	require.NotNil(t, event.Output)
	require.NotNil(t, event.Output.MessageOutput)
	require.True(t, event.Output.MessageOutput.IsStreaming)

	stream := event.Output.MessageOutput.MessageStream
	require.NotNil(t, stream)

	var streamMsgID string
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}
		chunkID := GetMessageID(chunk)
		if streamMsgID == "" && chunkID != "" {
			streamMsgID = chunkID
		} else if chunkID != "" {
			// Subsequent chunks should not have ID (first-chunk-only)
			// 后续 chunk 不应带 ID（仅首个 chunk）
			t.Errorf("expected only first chunk to have ID, got ID on later chunk: %s", chunkID)
		}
	}

	// Drain remaining events
	// 耗尽剩余事件
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}

	assert.NotEmpty(t, streamMsgID, "first stream chunk should have message ID")
	assert.True(t, isValidUUID(streamMsgID), "stream message ID should be valid UUID: %s", streamMsgID)
}

// TestMessageID_AgenticPublicAPIHelpers tests the batch helpers and ensures
// the AgenticMessage public API variants work correctly.
//
// TestMessageID_AgenticPublicAPIHelpers 测试批量 helper，并确保
// AgenticMessage 的公开 API 变体正常工作。
func TestMessageID_AgenticPublicAPIHelpers(t *testing.T) {
	t.Run("EnsureMessageID_idempotent", func(t *testing.T) {
		msg := &schema.AgenticMessage{
			Role: schema.AgenticRoleTypeAssistant,
			ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.AssistantGenText{Text: "test"}),
			},
		}
		assert.Empty(t, GetMessageID(msg))

		EnsureMessageID(msg)
		id1 := GetMessageID(msg)
		assert.NotEmpty(t, id1)
		assert.True(t, isValidUUID(id1))

		// Idempotent: second call should not change the ID
		// 幂等：第二次调用不应改变 ID
		EnsureMessageID(msg)
		id2 := GetMessageID(msg)
		assert.Equal(t, id1, id2)
	})

	t.Run("EnsureMessageIDs_batch", func(t *testing.T) {
		msgs := []*schema.AgenticMessage{
			{Role: schema.AgenticRoleTypeAssistant},
			{Role: schema.AgenticRoleTypeUser},
			{Role: schema.AgenticRoleTypeAssistant},
		}
		for _, msg := range msgs {
			EnsureMessageID(msg)
		}

		seen := make(map[string]bool)
		for i, msg := range msgs {
			id := GetMessageID(msg)
			assert.NotEmpty(t, id, "msg[%d] should have ID", i)
			assert.True(t, isValidUUID(id), "msg[%d] ID should be valid UUID: %s", i, id)
			assert.False(t, seen[id], "msg[%d] has duplicate ID: %s", i, id)
			seen[id] = true
		}
	})
}

// --- Adversarial attack tests for message ID system ---
// --- message ID 系统的对抗攻击测试 ---

// TestAttack_PopToolMsgID_DoublePop tests that calling popToolMsgID twice for the
// same key returns "" on second call.
//
// TestAttack_PopToolMsgID_DoublePop 测试对同一个 key 调用 popToolMsgID 两次时
// 第二次调用返回 ""。
func TestAttack_PopToolMsgID_DoublePop(t *testing.T) {
	st := &typedState[*schema.Message]{}
	st.setToolMsgID("myTool", "call-1", "uuid-abc")

	// First pop returns the ID
	// 第一次 pop 返回 ID
	id1 := st.popToolMsgID("myTool", "call-1")
	assert.Equal(t, "uuid-abc", id1)

	// Second pop returns empty
	// 第二次 pop 返回空
	id2 := st.popToolMsgID("myTool", "call-1")
	assert.Empty(t, id2, "double-pop should return empty")

	// Inner map should be cleaned up
	// 应清理内部 map
	assert.Nil(t, st.ToolMsgIDs["myTool"], "inner map should be removed when empty")
}

// namedFakeToolForTest is a variant of fakeToolForTest with a configurable name.
// namedFakeToolForTest 是 fakeToolForTest 的变体，名称可配置。
type namedFakeToolForTest struct {
	name string
}

func (t *namedFakeToolForTest) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: t.name,
		Desc: t.name + " tool for testing",
		ParamsOneOf: schema.NewParamsOneOfByParams(
			map[string]*schema.ParameterInfo{
				"name": {
					Desc:     "user name for testing",
					Required: true,
					Type:     schema.String,
				},
			}),
	}, nil
}

func (t *namedFakeToolForTest) InvokableRun(_ context.Context, _ string, _ ...tool.Option) (string, error) {
	return `{"say": "ok"}`, nil
}

// TestAttack_ToolMsgIDConsistency_MultipleTools is an integration test: when an agent
// has multiple tools called in one turn, verify that EACH tool's event message ID
// matches its corresponding state message ID.
//
// TestAttack_ToolMsgIDConsistency_MultipleTools 是集成测试：当一个智能体在一轮中调用多个工具时，验证每个工具的事件消息 ID 都与其对应的状态消息 ID 匹配。
func TestAttack_ToolMsgIDConsistency_MultipleTools(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tool1 := &namedFakeToolForTest{name: "greet"}
	tool2 := &namedFakeToolForTest{name: "farewell"}

	info1, err := tool1.Info(ctx)
	require.NoError(t, err)
	info2, err := tool2.Info(ctx)
	require.NoError(t, err)

	var generateCount int
	cm := mockModel.NewMockToolCallingChatModel(ctrl)
	cm.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
			generateCount++
			if generateCount == 1 {
				return schema.AssistantMessage("calling tools", []schema.ToolCall{
					{ID: "tc-1", Function: schema.FunctionCall{Name: info1.Name, Arguments: `{"name": "alice"}`}},
					{ID: "tc-2", Function: schema.FunctionCall{Name: info2.Name, Arguments: `{"name": "bob"}`}},
				}), nil
			}
			return schema.AssistantMessage("done", nil), nil
		}).AnyTimes()
	cm.EXPECT().WithTools(gomock.Any()).Return(cm, nil).AnyTimes()

	// Capture state message IDs
	// 捕获状态消息 ID
	var stateMsgIDs map[string]string // callID -> msgID
	// callID -> msgID
	beforeModelCount := 0
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestMultiTool",
		Instruction: "test",
		Model:       cm,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{tool1, tool2},
			},
		},
		Middlewares: []AgentMiddleware{
			{
				BeforeChatModel: func(ctx context.Context, state *ChatModelAgentState) error {
					beforeModelCount++
					if beforeModelCount == 2 {
						stateMsgIDs = make(map[string]string)
						for _, m := range state.Messages {
							if m.Role == schema.Tool {
								stateMsgIDs[m.ToolCallID] = GetMessageID(m)
							}
						}
					}
					return nil
				},
			},
		},
	})
	require.NoError(t, err)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("use tools")},
	})

	events := collectEvents(t, iter)
	// Expect: model(tool_calls) + tool1(result) + tool2(result) + model(final) = 4 events
	// 期望：model(tool_calls) + tool1(result) + tool2(result) + model(final) = 4 个事件
	require.GreaterOrEqual(t, len(events), 4)

	// Collect tool event IDs
	// 收集工具事件 ID
	eventMsgIDs := make(map[string]string) // callID -> msgID
	// callID -> msgID
	for _, ev := range events {
		if ev.Err != nil {
			continue
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			msg := ev.Output.MessageOutput.Message
			if msg != nil && msg.Role == schema.Tool {
				eventMsgIDs[msg.ToolCallID] = GetMessageID(msg)
			}
		}
	}

	// Each tool call should have an ID in both event and state, and they must match
	// 每个工具调用都应在事件和状态中都有 ID，且二者必须匹配
	require.NotEmpty(t, stateMsgIDs, "state should have tool message IDs")
	for callID, stateID := range stateMsgIDs {
		assert.NotEmpty(t, stateID, "state msg for %s should have ID", callID)
		assert.True(t, isValidUUID(stateID), "state msg ID should be UUID: %s", stateID)
		eventID, ok := eventMsgIDs[callID]
		assert.True(t, ok, "event should have msg for callID %s", callID)
		assert.Equal(t, stateID, eventID,
			"event and state msg IDs for callID %s must match: event=%s state=%s", callID, eventID, stateID)
	}
}

// TestAttack_ToolResultToBlocks_EdgeCases verifies toolResultToBlocks handles
// nil ToolResult, empty Parts, and Parts with nil media fields.
//
// TestAttack_ToolResultToBlocks_EdgeCases 验证 toolResultToBlocks 能处理 nil ToolResult、空 Parts，以及带 nil media 字段的 Parts。
func TestAttack_ToolResultToBlocks_EdgeCases(t *testing.T) {
	t.Run("nil ToolResult", func(t *testing.T) {
		blocks := toolResultToBlocks(nil)
		assert.Nil(t, blocks, "nil ToolResult should produce nil blocks")
	})

	t.Run("empty Parts", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{}}
		blocks := toolResultToBlocks(tr)
		assert.Nil(t, blocks, "empty Parts should produce nil blocks")
	})

	t.Run("text part with empty text", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeText, Text: ""},
		}}
		blocks := toolResultToBlocks(tr)
		require.Len(t, blocks, 1)
		assert.NotNil(t, blocks[0].Text)
		assert.Equal(t, "", blocks[0].Text.Text)
	})

	t.Run("image part with nil Image field", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeImage, Image: nil},
		}}
		blocks := toolResultToBlocks(tr)
		assert.Empty(t, blocks)
	})

	t.Run("audio part with nil Audio field", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeAudio, Audio: nil},
		}}
		blocks := toolResultToBlocks(tr)
		assert.Empty(t, blocks)
	})

	t.Run("video part with nil Video field", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeVideo, Video: nil},
		}}
		blocks := toolResultToBlocks(tr)
		assert.Empty(t, blocks)
	})

	t.Run("file part with nil File field", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeFile, File: nil},
		}}
		blocks := toolResultToBlocks(tr)
		assert.Empty(t, blocks)
	})

	t.Run("mixed: valid text + nil image + valid text", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeText, Text: "hello"},
			{Type: schema.ToolPartTypeImage, Image: nil},
			{Type: schema.ToolPartTypeText, Text: "world"},
		}}
		blocks := toolResultToBlocks(tr)
		require.Len(t, blocks, 2)
		assert.Equal(t, "hello", blocks[0].Text.Text)
		assert.Equal(t, "world", blocks[1].Text.Text)
	})

	t.Run("image part with nil URL pointers", func(t *testing.T) {
		tr := &schema.ToolResult{Parts: []schema.ToolOutputPart{
			{Type: schema.ToolPartTypeImage, Image: &schema.ToolOutputImage{
				MessagePartCommon: schema.MessagePartCommon{
					URL:        nil,
					Base64Data: nil,
					MIMEType:   "image/png",
				},
			}},
		}}
		blocks := toolResultToBlocks(tr)
		require.Len(t, blocks, 1)
		assert.NotNil(t, blocks[0].Image)
		assert.Equal(t, "", blocks[0].Image.URL, "nil URL pointer should deref to empty string")
		assert.Equal(t, "", blocks[0].Image.Base64Data)
		assert.Equal(t, "image/png", blocks[0].Image.MIMEType)
	})
}
