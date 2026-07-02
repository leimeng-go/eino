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

package planexecute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	mockAdk "github.com/cloudwego/eino/internal/mock/adk"
	mockModel "github.com/cloudwego/eino/internal/mock/components/model"
	"github.com/cloudwego/eino/schema"
)

// TestNewPlanner tests the NewPlanner function with ChatModelWithFormattedOutput
// TestNewPlanner 测试使用 ChatModelWithFormattedOutput 的 NewPlanner 函数
func TestNewPlannerWithFormattedOutput(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock chat model
	// 创建 mock chat model
	mockChatModel := mockModel.NewMockBaseChatModel(ctrl)

	// Create the PlannerConfig
	// 创建 PlannerConfig
	conf := &PlannerConfig{
		ChatModelWithFormattedOutput: mockChatModel,
	}

	// Create the planner
	// 创建 planner
	p, err := NewPlanner(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// Verify the planner's name and description
	// 验证 planner 的名称和描述
	assert.Equal(t, "planner", p.Name(ctx))
	assert.Equal(t, "a planner agent", p.Description(ctx))
}

// TestNewPlannerWithToolCalling tests the NewPlanner function with ToolCallingChatModel
// TestNewPlannerWithToolCalling 测试使用 ToolCallingChatModel 的 NewPlanner 函数
func TestNewPlannerWithToolCalling(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建 mock tool calling chat model
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)
	mockToolCallingModel.EXPECT().WithTools(gomock.Any()).Return(mockToolCallingModel, nil).Times(1)

	// Create the PlannerConfig
	// 创建 PlannerConfig
	conf := &PlannerConfig{
		ToolCallingChatModel: mockToolCallingModel,
		// Use default instruction and tool info
		// 使用默认 instruction 和 tool info
	}

	// Create the planner
	// 创建 planner
	p, err := NewPlanner(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// Verify the planner's name and description
	// 验证 planner 的名称和描述
	assert.Equal(t, "planner", p.Name(ctx))
	assert.Equal(t, "a planner agent", p.Description(ctx))
}

// TestPlannerRunWithFormattedOutput tests the Run method of a planner created with ChatModelWithFormattedOutput
// TestPlannerRunWithFormattedOutput 测试使用 ChatModelWithFormattedOutput 创建的 planner 的 Run 方法
func TestPlannerRunWithFormattedOutput(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock chat model
	// 创建 mock chat model
	mockChatModel := mockModel.NewMockBaseChatModel(ctrl)

	// Create a plan response
	// 创建 plan response
	planJSON := `{"steps":["Step 1", "Step 2", "Step 3"]}`
	planMsg := schema.AssistantMessage(planJSON, nil)
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(planMsg, nil)
	sw.Close()

	// Mock the Generate method
	// Mock Generate 方法
	mockChatModel.EXPECT().Stream(gomock.Any(), gomock.Any(), gomock.Any()).Return(sr, nil).Times(1)

	// Create the PlannerConfig
	// 创建 PlannerConfig
	conf := &PlannerConfig{
		ChatModelWithFormattedOutput: mockChatModel,
	}

	// Create the planner
	// 创建 planner
	p, err := NewPlanner(ctx, conf)
	assert.NoError(t, err)

	// Run the planner
	// 运行 planner
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: p})
	iterator := runner.Run(ctx, []adk.Message{schema.UserMessage("Plan this task")})

	// Get the event from the iterator
	// 从 iterator 获取 event
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.Nil(t, event.Err)
	msg, _, err := adk.GetMessage(event)
	assert.NoError(t, err)
	assert.Equal(t, planMsg.Content, msg.Content)

	_, ok = iterator.Next()
	assert.False(t, ok)

	plan := defaultNewPlan(ctx)
	err = plan.UnmarshalJSON([]byte(msg.Content))
	assert.NoError(t, err)
	plan_ := plan.(*defaultPlan)
	assert.Equal(t, 3, len(plan_.Steps))
	assert.Equal(t, "Step 1", plan_.Steps[0])
	assert.Equal(t, "Step 2", plan_.Steps[1])
	assert.Equal(t, "Step 3", plan_.Steps[2])
}

// TestPlannerRunWithToolCalling tests the Run method of a planner created with ToolCallingChatModel
// TestPlannerRunWithToolCalling 测试使用 ToolCallingChatModel 创建的 planner 的 Run 方法
func TestPlannerRunWithToolCalling(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建 mock tool calling chat model
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)

	// Create a tool call response with a plan
	// 创建带 plan 的 tool call response
	planArgs := `{"steps":["Step 1", "Step 2", "Step 3"]}`
	toolCall := schema.ToolCall{
		ID:   "tool_call_id",
		Type: "function",
		Function: schema.FunctionCall{
			Name: "plan", // This should match PlanToolInfo.Name
			// 这应与 PlanToolInfo.Name 匹配
			Arguments: planArgs,
		},
	}

	toolCallMsg := schema.AssistantMessage("", nil)
	toolCallMsg.ToolCalls = []schema.ToolCall{toolCall}
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(toolCallMsg, nil)
	sw.Close()

	// Mock the WithTools method to return a model that will be used for Generate
	// Mock WithTools 方法，使其返回将用于 Generate 的 model
	mockToolCallingModel.EXPECT().WithTools(gomock.Any()).Return(mockToolCallingModel, nil).Times(1)

	// Mock the Generate method to return the tool call message
	// Mock Generate 方法，使其返回 tool call message
	mockToolCallingModel.EXPECT().Stream(gomock.Any(), gomock.Any(), gomock.Any()).Return(sr, nil).Times(1)

	// Create the PlannerConfig with ToolCallingChatModel
	// 使用 ToolCallingChatModel 创建 PlannerConfig
	conf := &PlannerConfig{
		ToolCallingChatModel: mockToolCallingModel,
		// Use default instruction and tool info
		// 使用默认 instruction 和 tool info
	}

	// Create the planner
	// 创建 planner
	p, err := NewPlanner(ctx, conf)
	assert.NoError(t, err)

	// Run the planner
	// 运行 planner
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: p})
	iterator := runner.Run(ctx, []adk.Message{schema.UserMessage("no input")})

	// Get the event from the iterator
	// 从 iterator 获取 event
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.Nil(t, event.Err)

	msg, _, err := adk.GetMessage(event)
	assert.NoError(t, err)
	assert.Equal(t, planArgs, msg.Content)

	_, ok = iterator.Next()
	assert.False(t, ok)

	plan := defaultNewPlan(ctx)
	err = plan.UnmarshalJSON([]byte(msg.Content))
	assert.NoError(t, err)
	plan_ := plan.(*defaultPlan)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(plan_.Steps))
	assert.Equal(t, "Step 1", plan_.Steps[0])
	assert.Equal(t, "Step 2", plan_.Steps[1])
	assert.Equal(t, "Step 3", plan_.Steps[2])
}

// TestNewExecutor tests the NewExecutor function
// TestNewExecutor 测试 NewExecutor 函数
func TestNewExecutor(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建 mock 工具调用 chat model
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)

	// Create the ExecutorConfig
	// 创建 ExecutorConfig
	conf := &ExecutorConfig{
		Model:         mockToolCallingModel,
		MaxIterations: 3,
	}

	// Create the executor
	// 创建 executor
	executor, err := NewExecutor(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, executor)

	// Verify the executor's name and description
	// 验证 executor 的名称和描述
	assert.Equal(t, "executor", executor.Name(ctx))
	assert.Equal(t, "an executor agent", executor.Description(ctx))
}

// TestExecutorRun tests the Run method of the executor
// TestExecutorRun 测试 executor 的 Run 方法
func TestExecutorRun(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建 mock 工具调用 chat model
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)

	// Store a plan in the session
	// 在 session 中存储 plan
	plan := &defaultPlan{Steps: []string{"Step 1", "Step 2", "Step 3"}}
	adk.AddSessionValue(ctx, PlanSessionKey, plan)

	// Set up expectations for the mock model
	// The model should return the last user message as its response
	//
	// 设置 mock model 的预期
	// model 应返回最后一条 user message 作为响应
	mockToolCallingModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
			// Find the last user message
			// 查找最后一条 user message
			var lastUserMessage string
			for _, msg := range messages {
				if msg.Role == schema.User {
					lastUserMessage = msg.Content
				}
			}
			// Return the last user message as the model's response
			// 将最后一条 user message 作为 model 的响应返回
			return schema.AssistantMessage(lastUserMessage, nil), nil
		}).Times(1)

	// Create the ExecutorConfig
	// 创建 ExecutorConfig
	conf := &ExecutorConfig{
		Model:         mockToolCallingModel,
		MaxIterations: 3,
	}

	// Create the executor
	// 创建 executor
	executor, err := NewExecutor(ctx, conf)
	assert.NoError(t, err)

	// Run the executor
	// 运行 executor
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: executor})
	iterator := runner.Run(ctx, []adk.Message{schema.UserMessage("no input")},
		adk.WithSessionValues(map[string]any{
			PlanSessionKey:      plan,
			UserInputSessionKey: []adk.Message{schema.UserMessage("no input")},
		}),
	)

	// Get the event from the iterator
	// 从 iterator 获取 event
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.Nil(t, event.Err)
	assert.NotNil(t, event.Output)
	assert.NotNil(t, event.Output.MessageOutput)
	msg, _, err := adk.GetMessage(event)
	assert.NoError(t, err)
	t.Logf("executor model input msg:\n %s\n", msg.Content)

	_, ok = iterator.Next()
	assert.False(t, ok)
}

// TestNewReplanner tests the NewReplanner function
// TestNewReplanner 测试 NewReplanner 函数
func TestNewReplanner(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建 mock 工具调用 chat model
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)
	// Mock the WithTools method
	// Mock WithTools 方法
	mockToolCallingModel.EXPECT().WithTools(gomock.Any()).Return(mockToolCallingModel, nil).Times(1)

	// Create plan and respond tools
	// 创建 plan 和 respond 工具
	planTool := &schema.ToolInfo{
		Name: "Plan",
		Desc: "Plan tool",
	}

	respondTool := &schema.ToolInfo{
		Name: "Respond",
		Desc: "Respond tool",
	}

	// Create the ReplannerConfig
	// 创建 ReplannerConfig
	conf := &ReplannerConfig{
		ChatModel:   mockToolCallingModel,
		PlanTool:    planTool,
		RespondTool: respondTool,
	}

	// Create the replanner
	// 创建 replanner
	rp, err := NewReplanner(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, rp)

	// Verify the replanner's name and description
	// 验证 replanner 的名称和描述
	assert.Equal(t, "replanner", rp.Name(ctx))
	assert.Equal(t, "a replanner agent", rp.Description(ctx))
}

// TestReplannerRunWithPlan tests the Replanner's ability to use the plan_tool
// TestReplannerRunWithPlan 测试 Replanner 使用 plan_tool 的能力
func TestReplannerRunWithPlan(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建 mock 工具调用聊天模型
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)

	// Create plan and respond tools
	// 创建 plan 和 respond 工具
	planTool := &schema.ToolInfo{
		Name: "Plan",
		Desc: "Plan tool",
	}

	respondTool := &schema.ToolInfo{
		Name: "Respond",
		Desc: "Respond tool",
	}

	// Create a tool call response for the Plan tool
	// 为 Plan 工具创建工具调用响应
	planArgs := `{"steps":["Updated Step 1", "Updated Step 2"]}`
	toolCall := schema.ToolCall{
		ID:   "tool_call_id",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      planTool.Name,
			Arguments: planArgs,
		},
	}

	toolCallMsg := schema.AssistantMessage("", nil)
	toolCallMsg.ToolCalls = []schema.ToolCall{toolCall}
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(toolCallMsg, nil)
	sw.Close()

	// Mock the Generate method
	// Mock Generate 方法
	mockToolCallingModel.EXPECT().WithTools(gomock.Any()).Return(mockToolCallingModel, nil).Times(1)
	mockToolCallingModel.EXPECT().Stream(gomock.Any(), gomock.Any(), gomock.Any()).Return(sr, nil).Times(1)

	// Create the ReplannerConfig
	// 创建 ReplannerConfig
	conf := &ReplannerConfig{
		ChatModel:   mockToolCallingModel,
		PlanTool:    planTool,
		RespondTool: respondTool,
	}

	// Create the replanner
	// 创建 replanner
	rp, err := NewReplanner(ctx, conf)
	assert.NoError(t, err)

	// Store necessary values in the session
	// 在 session 中存储必要的值
	plan := &defaultPlan{Steps: []string{"Step 1", "Step 2", "Step 3"}}

	rp, err = agentOutputSessionKVs(ctx, rp)
	assert.NoError(t, err)

	// Run the replanner
	// 运行 replanner
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: rp})
	iterator := runner.Run(ctx, []adk.Message{schema.UserMessage("no input")},
		adk.WithSessionValues(map[string]any{
			PlanSessionKey:         plan,
			ExecutedStepSessionKey: "Execution result",
			UserInputSessionKey:    []adk.Message{schema.UserMessage("User input")},
		}),
	)

	// Get the event from the iterator
	// 从迭代器获取 event
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.Nil(t, event.Err)

	event, ok = iterator.Next()
	assert.True(t, ok)
	kvs := event.Output.CustomizedOutput.(map[string]any)
	assert.Greater(t, len(kvs), 0)

	// Verify the updated plan was stored in the session
	// 验证更新后的 plan 已存储在 session 中
	planValue, ok := kvs[PlanSessionKey]
	assert.True(t, ok)
	updatedPlan, ok := planValue.(*defaultPlan)
	assert.True(t, ok)
	assert.Equal(t, 2, len(updatedPlan.Steps))
	assert.Equal(t, "Updated Step 1", updatedPlan.Steps[0])
	assert.Equal(t, "Updated Step 2", updatedPlan.Steps[1])

	// Verify the execute results were updated
	// 验证 execute results 已更新
	executeResultsValue, ok := kvs[ExecutedStepsSessionKey]
	assert.True(t, ok)
	executeResults, ok := executeResultsValue.([]ExecutedStep)
	assert.True(t, ok)
	assert.Equal(t, 1, len(executeResults))
	assert.Equal(t, "Step 1", executeResults[0].Step)
	assert.Equal(t, "Execution result", executeResults[0].Result)

	_, ok = iterator.Next()
	assert.False(t, ok)
}

// TestReplannerRunWithRespond tests the Replanner's ability to use the respond_tool
// TestReplannerRunWithRespond 测试 Replanner 使用 respond_tool 的能力
func TestReplannerRunWithRespond(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建 mock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock tool calling chat model
	// 创建一个模拟工具调用聊天模型
	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)

	// Create plan and respond tools
	// 创建 plan 和 respond 工具
	planTool := &schema.ToolInfo{
		Name: "Plan",
		Desc: "Plan tool",
	}

	respondTool := &schema.ToolInfo{
		Name: "Respond",
		Desc: "Respond tool",
	}

	// Create a tool call response for the Respond tool
	// 为 Respond 工具创建工具调用响应
	responseArgs := `{"response":"This is the final response to the user"}`
	toolCall := schema.ToolCall{
		ID:   "tool_call_id",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      respondTool.Name,
			Arguments: responseArgs,
		},
	}

	toolCallMsg := schema.AssistantMessage("", nil)
	toolCallMsg.ToolCalls = []schema.ToolCall{toolCall}
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(toolCallMsg, nil)
	sw.Close()

	// Mock the Generate method
	// 模拟 Generate 方法
	mockToolCallingModel.EXPECT().WithTools(gomock.Any()).Return(mockToolCallingModel, nil).Times(1)
	mockToolCallingModel.EXPECT().Stream(gomock.Any(), gomock.Any(), gomock.Any()).Return(sr, nil).Times(1)

	// Create the ReplannerConfig
	// 创建 ReplannerConfig
	conf := &ReplannerConfig{
		ChatModel:   mockToolCallingModel,
		PlanTool:    planTool,
		RespondTool: respondTool,
	}

	// Create the replanner
	// 创建 replanner
	rp, err := NewReplanner(ctx, conf)
	assert.NoError(t, err)

	// Store necessary values in the session
	// 在 session 中存储必要的值
	plan := &defaultPlan{Steps: []string{"Step 1", "Step 2", "Step 3"}}

	// Run the replanner
	// 运行 replanner
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: rp})
	iterator := runner.Run(ctx, []adk.Message{schema.UserMessage("no input")},
		adk.WithSessionValues(map[string]any{
			PlanSessionKey:         plan,
			ExecutedStepSessionKey: "Execution result",
			UserInputSessionKey:    []adk.Message{schema.UserMessage("User input")},
		}),
	)

	// Get the event from the iterator
	// 从 iterator 获取 event
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.Nil(t, event.Err)
	msg, _, err := adk.GetMessage(event)
	assert.NoError(t, err)
	assert.Equal(t, responseArgs, msg.Content)

	// Verify that an exit action was generated
	// 验证已生成 exit action
	event, ok = iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event.Action)
	assert.NotNil(t, event.Action.BreakLoop)
	assert.False(t, event.Action.BreakLoop.Done)

	_, ok = iterator.Next()
	assert.False(t, ok)
}

// TestNewPlanExecuteAgent tests the New function
// TestNewPlanExecuteAgent 测试 New 函数
func TestNewPlanExecuteAgent(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建模拟 controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock agents
	// 创建模拟智能体
	mockPlanner := mockAdk.NewMockAgent(ctrl)
	mockExecutor := mockAdk.NewMockAgent(ctrl)
	mockReplanner := mockAdk.NewMockAgent(ctrl)

	// Set up expectations for the mock agents
	// 为模拟智能体设置预期
	mockPlanner.EXPECT().Name(gomock.Any()).Return("planner").AnyTimes()
	mockPlanner.EXPECT().Description(gomock.Any()).Return("a planner agent").AnyTimes()

	mockExecutor.EXPECT().Name(gomock.Any()).Return("executor").AnyTimes()
	mockExecutor.EXPECT().Description(gomock.Any()).Return("an executor agent").AnyTimes()

	mockReplanner.EXPECT().Name(gomock.Any()).Return("replanner").AnyTimes()
	mockReplanner.EXPECT().Description(gomock.Any()).Return("a replanner agent").AnyTimes()

	conf := &Config{
		Planner:   mockPlanner,
		Executor:  mockExecutor,
		Replanner: mockReplanner,
	}

	// Create the plan execute agent
	// 创建 plan execute 智能体
	agent, err := New(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, agent)
}

func TestPlanExecuteAgentWithReplan(t *testing.T) {
	ctx := context.Background()

	// Create a mock controller
	// 创建模拟 controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock agents
	// 创建模拟智能体
	mockPlanner := mockAdk.NewMockAgent(ctrl)
	mockExecutor := mockAdk.NewMockAgent(ctrl)
	mockReplanner := mockAdk.NewMockAgent(ctrl)

	// Set up expectations for the mock agents
	// 为模拟智能体设置预期
	mockPlanner.EXPECT().Name(gomock.Any()).Return("planner").AnyTimes()
	mockPlanner.EXPECT().Description(gomock.Any()).Return("a planner agent").AnyTimes()

	mockExecutor.EXPECT().Name(gomock.Any()).Return("executor").AnyTimes()
	mockExecutor.EXPECT().Description(gomock.Any()).Return("an executor agent").AnyTimes()

	mockReplanner.EXPECT().Name(gomock.Any()).Return("replanner").AnyTimes()
	mockReplanner.EXPECT().Description(gomock.Any()).Return("a replanner agent").AnyTimes()

	// Create a plan
	// 创建 plan
	originalPlan := &defaultPlan{Steps: []string{"Step 1", "Step 2", "Step 3"}}
	// Create an updated plan with fewer steps (after replanning)
	// 创建步骤更少的更新后 plan（重新规划后）
	updatedPlan := &defaultPlan{Steps: []string{"Updated Step 2", "Updated Step 3"}}
	// Create execute result
	// 创建执行结果
	originalExecuteResult := "Execution result for Step 1"
	updatedExecuteResult := "Execution result for Updated Step 2"

	// Create user input
	// 创建用户输入
	userInput := []adk.Message{schema.UserMessage("User task input")}

	finalResponse := &Response{Response: "Final response to user after executing all steps"}

	// Mock the planner Run method to set the original plan
	// Mock planner 的 Run 方法以设置原始计划
	mockPlanner.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()

			// Set the plan in the session
			// 在 session 中设置计划
			adk.AddSessionValue(ctx, PlanSessionKey, originalPlan)
			adk.AddSessionValue(ctx, UserInputSessionKey, userInput)

			// Send a message event
			// 发送消息事件
			planJSON, _ := sonic.MarshalString(originalPlan)
			msg := schema.AssistantMessage(planJSON, nil)
			event := adk.EventFromMessage(msg, nil, schema.Assistant, "")
			generator.Send(event)
			generator.Close()

			return iterator
		},
	).Times(1)

	// Mock the executor Run method to set the execute result
	// Mock executor 的 Run 方法以设置执行结果
	mockExecutor.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()

			plan, _ := adk.GetSessionValue(ctx, PlanSessionKey)
			currentPlan := plan.(*defaultPlan)
			var msg adk.Message
			// Check if this is the first replanning (original plan has 3 steps)
			// 检查这是否是第一次重新规划（原始计划有 3 个步骤）
			if len(currentPlan.Steps) == 3 {
				msg = schema.AssistantMessage(originalExecuteResult, nil)
				adk.AddSessionValue(ctx, ExecutedStepSessionKey, originalExecuteResult)
			} else {
				msg = schema.AssistantMessage(updatedExecuteResult, nil)
				adk.AddSessionValue(ctx, ExecutedStepSessionKey, updatedExecuteResult)
			}
			event := adk.EventFromMessage(msg, nil, schema.Assistant, "")
			generator.Send(event)
			generator.Close()

			return iterator
		},
	).Times(2)

	// Mock the replanner Run method to first update the plan, then respond to user
	// Mock replanner 的 Run 方法，先更新计划，再回复用户
	mockReplanner.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()

			// First call: Update the plan
			// Get the current plan from the session
			//
			// 第一次调用：更新计划
			// 从 session 获取当前计划
			plan, _ := adk.GetSessionValue(ctx, PlanSessionKey)
			currentPlan := plan.(*defaultPlan)

			// Check if this is the first replanning (original plan has 3 steps)
			// 检查这是否是第一次重新规划（原始计划有 3 个步骤）
			if len(currentPlan.Steps) == 3 {
				// Send a message event with the updated plan
				// 发送带有更新后计划的消息事件
				planJSON, _ := sonic.MarshalString(updatedPlan)
				msg := schema.AssistantMessage(planJSON, nil)
				event := adk.EventFromMessage(msg, nil, schema.Assistant, "")
				generator.Send(event)

				// Set the updated plan & execute result in the session
				// 在 session 中设置更新后的计划和执行结果
				adk.AddSessionValue(ctx, PlanSessionKey, updatedPlan)
				adk.AddSessionValue(ctx, ExecutedStepsSessionKey, []ExecutedStep{{
					Step:   currentPlan.Steps[0],
					Result: originalExecuteResult,
				}})
			} else {
				// Second call: Respond to user
				// 第二次调用：回复用户
				responseJSON, err := sonic.MarshalString(finalResponse)
				assert.NoError(t, err)
				msg := schema.AssistantMessage(responseJSON, nil)
				event := adk.EventFromMessage(msg, nil, schema.Assistant, "")
				generator.Send(event)

				// Send exit action
				// 发送退出动作
				action := adk.NewExitAction()
				generator.Send(&adk.AgentEvent{Action: action})
			}

			generator.Close()
			return iterator
		},
	).Times(2)

	conf := &Config{
		Planner:   mockPlanner,
		Executor:  mockExecutor,
		Replanner: mockReplanner,
	}

	// Create the plan execute agent
	// 创建 plan execute agent
	agent, err := New(ctx, conf)
	assert.NoError(t, err)
	assert.NotNil(t, agent)

	// Run the agent
	// 运行智能体
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent})
	iterator := runner.Run(ctx, userInput)

	// Collect all events
	// 收集所有事件
	var events []*adk.AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		events = append(events, event)
	}

	// Verify the events
	// 验证事件
	assert.Greater(t, len(events), 0)

	for i, event := range events {
		eventJSON, e := sonic.MarshalString(event)
		assert.NoError(t, e)
		t.Logf("event %d:\n%s", i, eventJSON)
	}
}

type interruptibleTool struct {
	name string
	t    *testing.T
}

func (m *interruptibleTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: m.name,
		Desc: "A tool that requires human approval before execution",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"action": {
				Type:     schema.String,
				Desc:     "The action to perform",
				Required: true,
			},
		}),
	}, nil
}

func (m *interruptibleTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	wasInterrupted, _, _ := tool.GetInterruptState[any](ctx)
	if !wasInterrupted {
		return "", tool.Interrupt(ctx, fmt.Sprintf("Tool '%s' requires human approval", m.name))
	}

	isResumeTarget, hasData, data := tool.GetResumeContext[string](ctx)
	if !isResumeTarget {
		return "", tool.Interrupt(ctx, fmt.Sprintf("Tool '%s' requires human approval", m.name))
	}

	if hasData {
		return fmt.Sprintf("Approved action executed with data: %s", data), nil
	}
	return "Approved action executed", nil
}

type checkpointStore struct {
	data map[string][]byte
}

func newCheckpointStore() *checkpointStore {
	return &checkpointStore{data: make(map[string][]byte)}
}

func (s *checkpointStore) Set(_ context.Context, key string, value []byte) error {
	s.data[key] = value
	return nil
}

func (s *checkpointStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := s.data[key]
	return v, ok, nil
}

func TestPlanExecuteAgentInterruptResume(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockToolCallingModel := mockModel.NewMockToolCallingChatModel(ctrl)

	approvalTool := &interruptibleTool{name: "approve_action", t: t}

	plan := &defaultPlan{Steps: []string{"Execute action requiring approval", "Complete task"}}
	userInput := []adk.Message{schema.UserMessage("Please execute the action")}

	mockPlanner := mockAdk.NewMockAgent(ctrl)
	mockPlanner.EXPECT().Name(gomock.Any()).Return("planner").AnyTimes()
	mockPlanner.EXPECT().Description(gomock.Any()).Return("a planner agent").AnyTimes()

	mockPlanner.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()

			adk.AddSessionValue(ctx, PlanSessionKey, plan)
			adk.AddSessionValue(ctx, UserInputSessionKey, userInput)

			planJSON, _ := sonic.MarshalString(plan)
			msg := schema.AssistantMessage(planJSON, nil)
			event := adk.EventFromMessage(msg, nil, schema.Assistant, "")
			generator.Send(event)
			generator.Close()

			return iterator
		},
	).Times(1)

	toolCallMsg := schema.AssistantMessage("", []schema.ToolCall{
		{
			ID:   "call_1",
			Type: "function",
			Function: schema.FunctionCall{
				Name:      "approve_action",
				Arguments: `{"action": "execute"}`,
			},
		},
	})

	completionMsg := schema.AssistantMessage("Action approved and executed successfully", nil)

	mockToolCallingModel.EXPECT().WithTools(gomock.Any()).Return(mockToolCallingModel, nil).AnyTimes()
	mockToolCallingModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(toolCallMsg, nil).Times(1)
	mockToolCallingModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(completionMsg, nil).AnyTimes()

	executor, err := NewExecutor(ctx, &ExecutorConfig{
		Model: mockToolCallingModel,
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{approvalTool},
			},
		},
		MaxIterations: 5,
	})
	assert.NoError(t, err)

	mockReplanner := mockAdk.NewMockAgent(ctrl)
	mockReplanner.EXPECT().Name(gomock.Any()).Return("replanner").AnyTimes()
	mockReplanner.EXPECT().Description(gomock.Any()).Return("a replanner agent").AnyTimes()

	mockReplanner.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()

			responseJSON := `{"response":"Task completed successfully"}`
			msg := schema.AssistantMessage(responseJSON, nil)
			event := adk.EventFromMessage(msg, nil, schema.Assistant, "")
			generator.Send(event)

			action := adk.NewBreakLoopAction("replanner")
			generator.Send(&adk.AgentEvent{Action: action})

			generator.Close()
			return iterator
		},
	).AnyTimes()

	agent, err := New(ctx, &Config{
		Planner:       mockPlanner,
		Executor:      executor,
		Replanner:     mockReplanner,
		MaxIterations: 5,
	})
	assert.NoError(t, err)

	store := newCheckpointStore()
	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		CheckPointStore: store,
	})

	iter := runner.Run(ctx, userInput, adk.WithCheckPointID("test-interrupt-1"))

	var events []*adk.AgentEvent
	var interruptEvent *adk.AgentEvent
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			interruptEvent = event
		}
		events = append(events, event)
	}

	t.Logf("Total events received: %d", len(events))
	for i, event := range events {
		eventJSON, _ := sonic.MarshalString(event)
		t.Logf("Event %d: %s", i, eventJSON)
	}

	if interruptEvent == nil {
		t.Fatal("Expected an interrupt event from the tool, but none was received")
	}

	assert.NotNil(t, interruptEvent.Action.Interrupted, "Should have interrupt info")
	assert.NotEmpty(t, interruptEvent.Action.Interrupted.InterruptContexts, "Should have interrupt contexts")

	t.Logf("Interrupt event received with %d contexts", len(interruptEvent.Action.Interrupted.InterruptContexts))
	for i, ctx := range interruptEvent.Action.Interrupted.InterruptContexts {
		t.Logf("Interrupt context %d: ID=%s, Info=%v, Address=%v", i, ctx.ID, ctx.Info, ctx.Address)
	}

	var toolInterruptID string
	for _, intCtx := range interruptEvent.Action.Interrupted.InterruptContexts {
		if intCtx.IsRootCause {
			toolInterruptID = intCtx.ID
			break
		}
	}
	assert.NotEmpty(t, toolInterruptID, "Should have a root cause interrupt ID")

	t.Logf("Attempting to resume with interrupt ID: %s", toolInterruptID)

	resumeIter, err := runner.ResumeWithParams(ctx, "test-interrupt-1", &adk.ResumeParams{
		Targets: map[string]any{
			toolInterruptID: "approved",
		},
	})
	assert.NoError(t, err, "Resume should not error")
	assert.NotNil(t, resumeIter, "Resume iterator should not be nil")

	var resumeEvents []*adk.AgentEvent
	for {
		event, ok := resumeIter.Next()
		if !ok {
			break
		}
		resumeEvents = append(resumeEvents, event)
	}

	assert.NotEmpty(t, resumeEvents, "Should have resume events")

	for _, event := range resumeEvents {
		assert.NoError(t, event.Err, "Resume event should not have error")
	}

	var hasToolResponse, hasAssistantCompletion, hasBreakLoop bool
	for _, event := range resumeEvents {
		if event.Output != nil && event.Output.MessageOutput != nil {
			msg := event.Output.MessageOutput.Message
			if msg != nil {
				if msg.Role == "tool" && strings.Contains(msg.Content, "Approved action executed") {
					hasToolResponse = true
				}
				if msg.Role == "assistant" && strings.Contains(msg.Content, "approved") {
					hasAssistantCompletion = true
				}
			}
		}
		if event.Action != nil && event.Action.BreakLoop != nil && event.Action.BreakLoop.Done {
			hasBreakLoop = true
		}
	}

	assert.True(t, hasToolResponse, "Should have tool response with approved action")
	assert.True(t, hasAssistantCompletion, "Should have assistant completion message")
	assert.True(t, hasBreakLoop, "Should have break loop action indicating completion")
}

// slowChatModel is a ChatModel that blocks for a configurable duration.
// slowChatModel 是一个会阻塞可配置时长的 ChatModel。
type slowChatModel struct {
	delay       time.Duration
	response    *schema.Message
	startedChan chan struct{}
	startedOnce sync.Once
}

func (m *slowChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	m.startedOnce.Do(func() {
		close(m.startedChan)
	})

	select {
	case <-time.After(m.delay):
		return m.response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *slowChatModel) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	sr, sw := schema.Pipe[*schema.Message](1)
	sw.Send(msg, nil)
	sw.Close()
	return sr, nil
}

func (m *slowChatModel) BindTools(tools []*schema.ToolInfo) error { return nil }
func (m *slowChatModel) WithTools(tools []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return m, nil
}

// TestWithCancel_PlanExecute_DuringExecution verifies that cancel works
// during the executor (ChatModelAgent) phase of the PlanExecute agent.
//
// TestWithCancel_PlanExecute_DuringExecution 验证取消在 PlanExecute agent 的 executor（ChatModelAgent）阶段有效。
func TestWithCancel_PlanExecute_DuringExecution(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Planner: returns a plan quickly
	// Planner：快速返回一个计划
	mockPlanner := mockAdk.NewMockAgent(ctrl)
	mockPlanner.EXPECT().Name(gomock.Any()).Return("planner").AnyTimes()
	mockPlanner.EXPECT().Description(gomock.Any()).Return("a planner agent").AnyTimes()

	plan := &defaultPlan{Steps: []string{"Step 1", "Step 2"}}
	userInput := []adk.Message{schema.UserMessage("test task")}

	mockPlanner.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()
			adk.AddSessionValue(ctx, PlanSessionKey, plan)
			adk.AddSessionValue(ctx, UserInputSessionKey, userInput)
			planJSON, _ := sonic.MarshalString(plan)
			msg := schema.AssistantMessage(planJSON, nil)
			generator.Send(adk.EventFromMessage(msg, nil, schema.Assistant, ""))
			generator.Close()
			return iterator
		},
	).Times(1)

	// Executor: uses a slow model that we can cancel
	// Executor：使用一个可取消的慢模型
	executorStarted := make(chan struct{})
	slowModel := &slowChatModel{
		delay:       5 * time.Second,
		response:    schema.AssistantMessage("step result", nil),
		startedChan: executorStarted,
	}

	executor, err := NewExecutor(ctx, &ExecutorConfig{
		Model:         slowModel,
		MaxIterations: 5,
	})
	assert.NoError(t, err)

	// Replanner: should not be reached since we cancel during executor
	// Replanner：由于在 executor 期间取消，不应执行到这里
	mockReplanner := mockAdk.NewMockAgent(ctrl)
	mockReplanner.EXPECT().Name(gomock.Any()).Return("replanner").AnyTimes()
	mockReplanner.EXPECT().Description(gomock.Any()).Return("a replanner agent").AnyTimes()

	agent, err := New(ctx, &Config{
		Planner:       mockPlanner,
		Executor:      executor,
		Replanner:     mockReplanner,
		MaxIterations: 5,
	})
	assert.NoError(t, err)

	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent})

	cancelOpt, cancelFn := adk.WithCancel()
	iter := runner.Run(ctx, userInput, cancelOpt)

	// Wait for the executor's model to start
	// 等待 executor 的模型启动
	select {
	case <-executorStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("Executor model did not start")
	}

	time.Sleep(50 * time.Millisecond)

	// Cancel should NOT return ErrExecutionEnded
	// Cancel 不应返回 ErrExecutionEnded
	handle, _ := cancelFn()
	err = handle.Wait()
	assert.NoError(t, err, "Cancel during executor should succeed")

	hasCancelError := false
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		var ce *adk.CancelError
		if event.Err != nil && errors.As(event.Err, &ce) {
			hasCancelError = true
		}
	}

	assert.True(t, hasCancelError, "Should have CancelError event")
}

// TestWithCancel_PlanExecute_BetweenTransitions verifies that cancel works
// when fired between agent transitions (e.g., after planner, before executor starts).
//
// TestWithCancel_PlanExecute_BetweenTransitions 验证在智能体转换之间触发取消时可正常工作
// （例如 planner 之后、executor 启动之前）。
func TestWithCancel_PlanExecute_BetweenTransitions(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	plannerDone := make(chan struct{})

	// Planner: signals when done
	// Planner：完成时发出信号
	mockPlanner := mockAdk.NewMockAgent(ctrl)
	mockPlanner.EXPECT().Name(gomock.Any()).Return("planner").AnyTimes()
	mockPlanner.EXPECT().Description(gomock.Any()).Return("a planner agent").AnyTimes()

	plan := &defaultPlan{Steps: []string{"Step 1"}}
	userInput := []adk.Message{schema.UserMessage("test task")}

	mockPlanner.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
			iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()
			go func() {
				defer generator.Close()
				adk.AddSessionValue(ctx, PlanSessionKey, plan)
				adk.AddSessionValue(ctx, UserInputSessionKey, userInput)
				planJSON, _ := sonic.MarshalString(plan)
				msg := schema.AssistantMessage(planJSON, nil)
				generator.Send(adk.EventFromMessage(msg, nil, schema.Assistant, ""))
				close(plannerDone)
			}()
			return iterator
		},
	).Times(1)

	// Executor: slow model to give time to observe cancel
	// Executor：使用慢模型，以便有时间观察取消
	executorModelStarted := make(chan struct{})
	slowExecModel := &slowChatModel{
		delay:       5 * time.Second,
		response:    schema.AssistantMessage("step result", nil),
		startedChan: executorModelStarted,
	}

	executor, err := NewExecutor(ctx, &ExecutorConfig{
		Model:         slowExecModel,
		MaxIterations: 5,
	})
	assert.NoError(t, err)

	mockReplanner := mockAdk.NewMockAgent(ctrl)
	mockReplanner.EXPECT().Name(gomock.Any()).Return("replanner").AnyTimes()
	mockReplanner.EXPECT().Description(gomock.Any()).Return("a replanner agent").AnyTimes()

	agent, err := New(ctx, &Config{
		Planner:       mockPlanner,
		Executor:      executor,
		Replanner:     mockReplanner,
		MaxIterations: 5,
	})
	assert.NoError(t, err)

	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent})

	cancelOpt, cancelFn := adk.WithCancel()
	iter := runner.Run(ctx, userInput, cancelOpt)

	// Wait for planner to finish, then cancel before executor has a chance to produce output
	// 等待 planner 完成，然后在 executor 有机会产生输出前取消
	select {
	case <-plannerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Planner did not finish")
	}

	// Cancel after planner, during executor phase
	// The executor is a ChatModelAgent which will handle the cancel
	//
	// planner 之后、executor 阶段期间取消
	// executor 是 ChatModelAgent，会处理该取消
	select {
	case <-executorModelStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("Executor model did not start")
	}

	start := time.Now()
	handle, _ := cancelFn()
	err = handle.Wait()
	assert.NoError(t, err, "Cancel between transitions should succeed")

	hasCancelError := false
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		var ce *adk.CancelError
		if event.Err != nil && errors.As(event.Err, &ce) {
			hasCancelError = true
		}
	}
	elapsed := time.Since(start)

	assert.True(t, hasCancelError, "Should have CancelError event")
	assert.True(t, elapsed < 3*time.Second, "Should complete quickly after cancel, elapsed: %v", elapsed)
}
