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

package adk

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/cloudwego/eino/callbacks"
	mockModel "github.com/cloudwego/eino/internal/mock/components/model"
	"github.com/cloudwego/eino/schema"
)

func strPtr(s string) *string { return &s }

func TestRewriteMessage(t *testing.T) {
	imageCommon := schema.MessagePartCommon{URL: strPtr("http://img.example.com")}
	audioCommon := schema.MessagePartCommon{URL: strPtr("http://audio.example.com")}
	videoCommon := schema.MessagePartCommon{URL: strPtr("http://video.example.com")}

	msg := &schema.Message{
		Role:    schema.Assistant,
		Content: "hello",
		MultiContent: []schema.ChatMessagePart{ //nolint:staticcheck // testing deprecated field backward compat
			{Type: schema.ChatMessagePartTypeText, Text: "legacy"},
		},
		UserInputMultiContent: []schema.MessageInputPart{
			{Type: schema.ChatMessagePartTypeText, Text: "pre-existing"},
		},
		AssistantGenMultiContent: []schema.MessageOutputPart{
			{Type: schema.ChatMessagePartTypeText, Text: "gen-text", Extra: map[string]any{"k": "v"}},
			{Type: schema.ChatMessagePartTypeImageURL, Image: &schema.MessageOutputImage{MessagePartCommon: imageCommon}},
			{Type: schema.ChatMessagePartTypeAudioURL, Audio: &schema.MessageOutputAudio{MessagePartCommon: audioCommon}},
			{Type: schema.ChatMessagePartTypeVideoURL, Video: &schema.MessageOutputVideo{MessagePartCommon: videoCommon}},
			{Type: schema.ChatMessagePartTypeReasoning, Reasoning: &schema.MessageOutputReasoning{Text: "secret thoughts"}},
		},
	}

	rewritten := rewriteMessage(msg, "OtherAgent")

	assert.Equal(t, schema.User, rewritten.Role)

	// MultiContent: copied, not shared
	// MultiContent：复制而非共享
	assert.Equal(t, msg.MultiContent, rewritten.MultiContent)
	rewritten.MultiContent[0].Text = "mutated"
	assert.Equal(t, "legacy", msg.MultiContent[0].Text)

	// UserInputMultiContent: pre-existing entry copied, AssistantGenMultiContent appended (reasoning dropped)
	// UserInputMultiContent：复制已有条目，追加 AssistantGenMultiContent（丢弃 reasoning）
	assert.Len(t, rewritten.UserInputMultiContent, 5) // 1 pre-existing + 4 converted (text/image/audio/video)
	// 1 个已有条目 + 4 个转换结果（text/image/audio/video）

	// pre-existing entry is not shared
	// 已有条目不会共享
	rewritten.UserInputMultiContent[0].Text = "mutated"
	assert.Equal(t, "pre-existing", msg.UserInputMultiContent[0].Text)

	// text conversion
	// text 转换
	assert.Equal(t, schema.ChatMessagePartTypeText, rewritten.UserInputMultiContent[1].Type)
	assert.Equal(t, "gen-text", rewritten.UserInputMultiContent[1].Text)
	assert.Equal(t, map[string]any{"k": "v"}, rewritten.UserInputMultiContent[1].Extra)

	// image conversion
	// image 转换
	assert.Equal(t, schema.ChatMessagePartTypeImageURL, rewritten.UserInputMultiContent[2].Type)
	assert.Equal(t, imageCommon, rewritten.UserInputMultiContent[2].Image.MessagePartCommon)

	// audio conversion
	// audio 转换
	assert.Equal(t, schema.ChatMessagePartTypeAudioURL, rewritten.UserInputMultiContent[3].Type)
	assert.Equal(t, audioCommon, rewritten.UserInputMultiContent[3].Audio.MessagePartCommon)

	// video conversion
	// video 转换
	assert.Equal(t, schema.ChatMessagePartTypeVideoURL, rewritten.UserInputMultiContent[4].Type)
	assert.Equal(t, videoCommon, rewritten.UserInputMultiContent[4].Video.MessagePartCommon)

	// reasoning is dropped; AssistantGenMultiContent is not set on rewritten message
	// reasoning 被丢弃；重写后的消息未设置 AssistantGenMultiContent
	assert.Empty(t, rewritten.AssistantGenMultiContent)
}

// TestTransferToAgent tests the TransferToAgent functionality
// TestTransferToAgent 测试 TransferToAgent 功能
func TestTransferToAgent(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock models for parent and child agents
	// 为父子智能体创建 mock models
	parentModel := mockModel.NewMockToolCallingChatModel(ctrl)
	childModel := mockModel.NewMockToolCallingChatModel(ctrl)

	// Set up expectations for the parent model
	// First call: parent model generates a message with TransferToAgent tool call
	//
	// 设置父模型的预期
	// 第一次调用：父模型生成一条带有 TransferToAgent 工具调用的消息
	parentModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("I'll transfer this to the child agent",
			[]schema.ToolCall{
				{
					ID: "tool-call-1",
					Function: schema.FunctionCall{
						Name:      TransferToAgentToolName,
						Arguments: `{"agent_name": "ChildAgent"}`,
					},
				},
			}), nil).
		Times(1)

	// Set up expectations for the child model
	// Second call: child model generates a response
	//
	// 设置子模型的预期
	// 第二次调用：子模型生成响应
	childModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("Hello from child agent", nil), nil).
		Times(1)

	// Both models should implement WithTools
	// 两个模型都应实现 WithTools
	parentModel.EXPECT().WithTools(gomock.Any()).Return(parentModel, nil).AnyTimes()
	childModel.EXPECT().WithTools(gomock.Any()).Return(childModel, nil).AnyTimes()

	// Create parent agent
	// 创建父智能体
	parentAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "ParentAgent",
		Description: "Parent agent that will transfer to child",
		Instruction: "You are a parent agent.",
		Model:       parentModel,
	})
	assert.NoError(t, err)
	assert.NotNil(t, parentAgent)

	// Create child agent
	// 创建子智能体
	childAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "ChildAgent",
		Description: "Child agent that handles specific tasks",
		Instruction: "You are a child agent.",
		Model:       childModel,
	})
	assert.NoError(t, err)
	assert.NotNil(t, childAgent)

	// Set up parent-child relationship
	// 设置父子关系
	flowAgent, err := SetSubAgents(ctx, parentAgent, []Agent{childAgent})
	assert.NoError(t, err)
	assert.NotNil(t, flowAgent)

	assert.NotNil(t, parentAgent.subAgents)
	assert.NotNil(t, childAgent.parentAgent)

	// Run the parent agent
	// 运行父智能体
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Please transfer this to the child agent"),
		},
	}
	ctx, _ = initRunCtx(ctx, flowAgent.Name(ctx), input)
	iterator := flowAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// First event: parent model output with tool call
	// 第一个事件：父模型输出，包含工具调用
	event1, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event1)
	assert.Nil(t, event1.Err)
	assert.NotNil(t, event1.Output)
	assert.NotNil(t, event1.Output.MessageOutput)
	assert.Equal(t, schema.Assistant, event1.Output.MessageOutput.Role)

	// Second event: tool output (TransferToAgent)
	// 第二个事件：工具输出（TransferToAgent）
	event2, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event2)
	assert.Nil(t, event2.Err)
	assert.NotNil(t, event2.Output)
	assert.NotNil(t, event2.Output.MessageOutput)
	assert.Equal(t, schema.Tool, event2.Output.MessageOutput.Role)

	// Verify the action is TransferToAgent
	// 验证 action 是 TransferToAgent
	assert.NotNil(t, event2.Action)
	assert.NotNil(t, event2.Action.TransferToAgent)
	assert.Equal(t, "ChildAgent", event2.Action.TransferToAgent.DestAgentName)

	// Third event: child model output
	// 第三个事件：子模型输出
	event3, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event3)
	assert.Nil(t, event3.Err)
	assert.NotNil(t, event3.Output)
	assert.NotNil(t, event3.Output.MessageOutput)
	assert.Equal(t, schema.Assistant, event3.Output.MessageOutput.Role)

	// Verify the message content from child agent
	// 验证来自子智能体的消息内容
	msg := event3.Output.MessageOutput.Message
	assert.NotNil(t, msg)
	assert.Equal(t, "Hello from child agent", msg.Content)

	// No more events
	// 没有更多事件
	_, ok = iterator.Next()
	assert.False(t, ok)
}

func TestResumeNonResumableWrapperError(t *testing.T) {
	ctx := context.Background()

	interruptingAgent := &nonResumableFlowTestAgent{
		name: "inner",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, gen := NewAsyncIteratorPair[*AgentEvent]()
			go func() {
				defer gen.Close()
				gen.Send(Interrupt(ctx, "please confirm"))
			}()
			return iter
		},
	}

	innerFlowAgent := toFlowAgent(ctx, interruptingAgent)

	wrapper := &nonResumableFlowTestAgent{
		name: "wrapper",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			return innerFlowAgent.Run(ctx, input, options...)
		},
	}

	store := &flowTestStore{m: map[string][]byte{}}
	runner := NewRunner(ctx, RunnerConfig{
		Agent:           wrapper,
		EnableStreaming: true,
		CheckPointStore: store,
	})

	iter := runner.Run(ctx, []Message{schema.UserMessage("test")}, WithCheckPointID("cp1"))

	var interruptID string
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Action != nil && ev.Action.Interrupted != nil {
			for _, intCtx := range ev.Action.Interrupted.InterruptContexts {
				if intCtx.IsRootCause {
					interruptID = intCtx.ID
				}
			}
		}
	}
	assert.NotEmpty(t, interruptID, "should have an interrupt ID")

	resumeIter, err := runner.ResumeWithParams(ctx, "cp1", &ResumeParams{
		Targets: map[string]any{interruptID: nil},
	})
	assert.NoError(t, err)

	var resumeErr error
	for {
		ev, ok := resumeIter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			resumeErr = ev.Err
		}
	}

	assert.Error(t, resumeErr)
	assert.Contains(t, resumeErr.Error(), "does not implement ResumableAgent interface")
	assert.Contains(t, resumeErr.Error(), "custom agent wrapper must implement the ResumableAgent interface")
}

type nonResumableFlowTestAgent struct {
	name  string
	runFn func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent]
}

func (a *nonResumableFlowTestAgent) Name(_ context.Context) string {
	return a.name
}

func (a *nonResumableFlowTestAgent) Description(_ context.Context) string {
	return a.name + " description"
}

func (a *nonResumableFlowTestAgent) Run(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	return a.runFn(ctx, input, options...)
}

type flowTestStore struct {
	m map[string][]byte
}

func (s *flowTestStore) Set(_ context.Context, key string, value []byte) error {
	s.m[key] = value
	return nil
}

func (s *flowTestStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := s.m[key]
	return v, ok, nil
}

func TestTransferToAgentWithDesignatedCallback(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parentModel := mockModel.NewMockToolCallingChatModel(ctrl)
	childModel := mockModel.NewMockToolCallingChatModel(ctrl)

	parentModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("I'll transfer this to the child agent",
			[]schema.ToolCall{
				{
					ID: "tool-call-1",
					Function: schema.FunctionCall{
						Name:      TransferToAgentToolName,
						Arguments: `{"agent_name": "ChildAgent"}`,
					},
				},
			}), nil).
		Times(1)

	childModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(schema.AssistantMessage("Hello from child agent", nil), nil).
		Times(1)

	parentModel.EXPECT().WithTools(gomock.Any()).Return(parentModel, nil).AnyTimes()
	childModel.EXPECT().WithTools(gomock.Any()).Return(childModel, nil).AnyTimes()

	parentAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "ParentAgent",
		Description: "Parent agent that will transfer to child",
		Instruction: "You are a parent agent.",
		Model:       parentModel,
	})
	assert.NoError(t, err)

	childAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "ChildAgent",
		Description: "Child agent that handles specific tasks",
		Instruction: "You are a child agent.",
		Model:       childModel,
	})
	assert.NoError(t, err)

	flowAgent, err := SetSubAgents(ctx, parentAgent, []Agent{childAgent})
	assert.NoError(t, err)

	var childCallbackCount int
	var mu sync.Mutex

	handler := callbacks.NewHandlerBuilder().OnStartFn(
		func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
			if info.Component == ComponentOfAgent && info.Name == "ChildAgent" {
				mu.Lock()
				childCallbackCount++
				mu.Unlock()
			}
			return ctx
		}).Build()

	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Please transfer this to the child agent"),
		},
	}
	ctx, _ = initRunCtx(ctx, flowAgent.Name(ctx), input)
	iterator := flowAgent.Run(ctx, input, WithCallbacks(handler).DesignateAgent("ChildAgent"))

	for {
		_, ok := iterator.Next()
		if !ok {
			break
		}
	}

	assert.Equal(t, 1, childCallbackCount, "designated callback for ChildAgent should fire exactly once during transfer")
}
