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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/schema"
)

// mockAgent is a simple implementation of the Agent interface for testing
// mockAgent 是用于测试的 Agent 接口简单实现
type mockAgent struct {
	name        string
	description string
	responses   []*AgentEvent
}

func (a *mockAgent) Name(_ context.Context) string {
	return a.name
}

func (a *mockAgent) Description(_ context.Context) string {
	return a.description
}

func (a *mockAgent) Run(_ context.Context, _ *AgentInput, _ ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iterator, generator := NewAsyncIteratorPair[*AgentEvent]()

	go func() {
		defer generator.Close()

		for _, event := range a.responses {
			generator.Send(event)

			// If the event has an Exit action, stop sending events
			// 如果事件包含 Exit action，则停止发送事件
			if event.Action != nil && event.Action.Exit {
				break
			}
		}
	}()

	return iterator
}

// newMockAgent creates a new mock agent with the given name, description, and responses
// newMockAgent 使用给定的 name、description 和 responses 创建新的 mock agent
func newMockAgent(name, description string, responses []*AgentEvent) *mockAgent {
	return &mockAgent{
		name:        name,
		description: description,
		responses:   responses,
	}
}

// TestSequentialAgent tests the sequential workflow agent
// TestSequentialAgent 测试顺序工作流智能体
func TestSequentialAgent(t *testing.T) {
	ctx := context.Background()

	// Create mock agents with predefined responses
	// 创建带预定义 responses 的 mock agents
	agent1 := newMockAgent("Agent1", "First agent", []*AgentEvent{
		{
			AgentName: "Agent1",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Response from Agent1", nil),
					Role:        schema.Assistant,
				},
			},
		},
	})

	agent2 := newMockAgent("Agent2", "Second agent", []*AgentEvent{
		{
			AgentName: "Agent2",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Response from Agent2", nil),
					Role:        schema.Assistant,
				},
			}},
	})

	// Create a sequential agent with the mock agents
	// 使用 mock agents 创建顺序智能体
	config := &SequentialAgentConfig{
		Name:        "SequentialTestAgent",
		Description: "Test sequential agent",
		SubAgents:   []Agent{agent1, agent2},
	}

	sequentialAgent, err := NewSequentialAgent(ctx, config)
	assert.NoError(t, err)
	assert.NotNil(t, sequentialAgent)

	assert.Equal(t, "Test sequential agent", sequentialAgent.Description(ctx))

	// Run the sequential agent
	// 运行顺序智能体
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}

	// Initialize the run context
	// 初始化运行上下文
	ctx, _ = initRunCtx(ctx, sequentialAgent.Name(ctx), input)

	iterator := sequentialAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// First event should be from agent1
	// 第一个事件应来自 agent1
	event1, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event1)
	assert.Nil(t, event1.Err)
	assert.NotNil(t, event1.Output)
	assert.NotNil(t, event1.Output.MessageOutput)

	// Get the message content from agent1
	// 获取 agent1 的消息内容
	msg1 := event1.Output.MessageOutput.Message
	assert.NotNil(t, msg1)
	assert.Equal(t, "Response from Agent1", msg1.Content)

	// Second event should be from agent2
	// 第二个事件应来自 agent2
	event2, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event2)
	assert.Nil(t, event2.Err)
	assert.NotNil(t, event2.Output)
	assert.NotNil(t, event2.Output.MessageOutput)

	// Get the message content from agent2
	// 获取 agent2 的消息内容
	msg2 := event2.Output.MessageOutput.Message
	assert.NotNil(t, msg2)
	assert.Equal(t, "Response from Agent2", msg2.Content)

	// No more events
	// 没有更多事件
	_, ok = iterator.Next()
	assert.False(t, ok)
}

// TestSequentialAgentWithExit tests the sequential workflow agent with an exit action
// TestSequentialAgentWithExit 测试带 exit action 的顺序工作流智能体
func TestSequentialAgentWithExit(t *testing.T) {
	ctx := context.Background()

	// Create mock agents with predefined responses
	// 创建带有预定义响应的 mock 智能体
	agent1 := newMockAgent("Agent1", "First agent", []*AgentEvent{
		{
			AgentName: "Agent1",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Response from Agent1", nil),
					Role:        schema.Assistant,
				},
			},
			Action: &AgentAction{
				Exit: true,
			},
		},
	})

	agent2 := newMockAgent("Agent2", "Second agent", []*AgentEvent{
		{
			AgentName: "Agent2",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Response from Agent2", nil),
					Role:        schema.Assistant,
				},
			},
		},
	})

	// Create a sequential agent with the mock agents
	// 使用这些 mock 智能体创建一个顺序智能体
	config := &SequentialAgentConfig{
		Name:        "SequentialTestAgent",
		Description: "Test sequential agent",
		SubAgents:   []Agent{agent1, agent2},
	}

	sequentialAgent, err := NewSequentialAgent(ctx, config)
	assert.NoError(t, err)
	assert.NotNil(t, sequentialAgent)

	// Run the sequential agent
	// 运行顺序智能体
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}

	ctx, _ = initRunCtx(ctx, sequentialAgent.Name(ctx), input)

	iterator := sequentialAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// First event should be from agent1 with exit action
	// 第一个事件应来自 agent1，并带有退出动作
	event1, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event1)
	assert.Nil(t, event1.Err)
	assert.NotNil(t, event1.Output)
	assert.NotNil(t, event1.Output.MessageOutput)
	assert.NotNil(t, event1.Action)
	assert.True(t, event1.Action.Exit)

	// No more events due to exit action
	// 由于退出动作，不再有事件
	_, ok = iterator.Next()
	assert.False(t, ok)
}

// TestParallelAgent tests the parallel workflow agent
// TestParallelAgent 测试并行工作流智能体
func TestParallelAgent(t *testing.T) {
	ctx := context.Background()

	// Create mock agents with predefined responses
	// 创建带有预定义响应的 mock 智能体
	agent1 := newMockAgent("Agent1", "First agent", []*AgentEvent{
		{
			AgentName: "Agent1",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Response from Agent1", nil),
					Role:        schema.Assistant,
				},
			},
		},
	})

	agent2 := newMockAgent("Agent2", "Second agent", []*AgentEvent{
		{
			AgentName: "Agent2",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Response from Agent2", nil),
					Role:        schema.Assistant,
				},
			},
		},
	})

	// Create a parallel agent with the mock agents
	// 使用这些 mock 智能体创建一个并行智能体
	config := &ParallelAgentConfig{
		Name:        "ParallelTestAgent",
		Description: "Test parallel agent",
		SubAgents:   []Agent{agent1, agent2},
	}

	parallelAgent, err := NewParallelAgent(ctx, config)
	assert.NoError(t, err)
	assert.NotNil(t, parallelAgent)

	// Run the parallel agent
	// 运行并行智能体
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}

	ctx, _ = initRunCtx(ctx, parallelAgent.Name(ctx), input)

	iterator := parallelAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// Collect all events
	// 收集所有事件
	var events []*AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		events = append(events, event)
	}

	// Should have two events, one from each agent
	// 应有两个事件，每个智能体各一个
	assert.Equal(t, 2, len(events))

	// Verify the events
	// 验证事件
	for _, event := range events {
		assert.Nil(t, event.Err)
		assert.NotNil(t, event.Output)
		assert.NotNil(t, event.Output.MessageOutput)

		msg := event.Output.MessageOutput.Message
		assert.NotNil(t, msg)
		assert.NoError(t, err)

		// Check the source agent name and message content
		// 检查源智能体名称和消息内容
		switch event.AgentName {
		case "Agent1":
			assert.Equal(t, "Response from Agent1", msg.Content)
		case "Agent2":
			assert.Equal(t, "Response from Agent2", msg.Content)
		default:
			t.Fatalf("Unexpected source agent name: %s", event.AgentName)
		}
	}
}

// TestLoopAgent tests the loop workflow agent
// TestLoopAgent 测试循环工作流智能体
func TestLoopAgent(t *testing.T) {
	ctx := context.Background()

	// Create a mock agent that will be called multiple times
	// 创建一个会被多次调用的 mock 智能体
	agent := newMockAgent("LoopAgent", "Loop agent", []*AgentEvent{
		{
			AgentName: "LoopAgent",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Loop iteration", nil),
					Role:        schema.Assistant,
				},
			},
		},
	})

	// Create a loop agent with the mock agent and max iterations set to 3
	// 创建一个包含该 mock 智能体且最大迭代次数设为 3 的循环智能体
	config := &LoopAgentConfig{
		Name:        "LoopTestAgent",
		Description: "Test loop agent",
		SubAgents:   []Agent{agent},

		MaxIterations: 3,
	}

	loopAgent, err := NewLoopAgent(ctx, config)
	assert.NoError(t, err)
	assert.NotNil(t, loopAgent)

	// Run the loop agent
	// 运行循环智能体
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}

	ctx, _ = initRunCtx(ctx, loopAgent.Name(ctx), input)

	iterator := loopAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// Collect all events
	// 收集所有事件
	var events []*AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		events = append(events, event)
	}

	// Should have 3 events (one for each iteration)
	// 应有 3 个事件（每次迭代一个）
	assert.Equal(t, 3, len(events))

	// Verify all events
	// 验证所有事件
	for _, event := range events {
		assert.Nil(t, event.Err)
		assert.NotNil(t, event.Output)
		assert.NotNil(t, event.Output.MessageOutput)

		msg := event.Output.MessageOutput.Message
		assert.NotNil(t, msg)
		assert.Equal(t, "Loop iteration", msg.Content)
	}
}

// TestLoopAgentWithBreakLoop tests the loop workflow agent with an break loop action
// TestLoopAgentWithBreakLoop 测试带 break loop 动作的循环工作流智能体
func TestLoopAgentWithBreakLoop(t *testing.T) {
	ctx := context.Background()

	// Create a mock agent that will break the loop after the first iteration
	// 创建一个 mock agent，在第一次迭代后跳出循环
	agent := newMockAgent("LoopAgent", "Loop agent", []*AgentEvent{
		{
			AgentName: "LoopAgent",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Loop iteration with break loop", nil),
					Role:        schema.Assistant,
				},
			},
			Action: NewBreakLoopAction("LoopAgent"),
		},
	})

	// Create a loop agent with the mock agent and max iterations set to 3
	// 创建一个包含 mock agent 且 max iterations 设为 3 的 loop agent
	config := &LoopAgentConfig{
		Name:          "LoopTestAgent",
		Description:   "Test loop agent",
		SubAgents:     []Agent{agent},
		MaxIterations: 3,
	}

	loopAgent, err := NewLoopAgent(ctx, config)
	assert.NoError(t, err)
	assert.NotNil(t, loopAgent)

	// Run the loop agent
	// 运行 loop agent
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}
	ctx, _ = initRunCtx(ctx, loopAgent.Name(ctx), input)

	iterator := loopAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// Collect all events
	// 收集所有事件
	var events []*AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		events = append(events, event)
	}

	// Should have only 1 event due to break loop action
	// 由于 break loop 动作，应只有 1 个事件
	assert.Equal(t, 1, len(events))

	// Verify the event
	// 验证事件
	event := events[0]
	assert.Nil(t, event.Err)
	assert.NotNil(t, event.Output)
	assert.NotNil(t, event.Output.MessageOutput)
	assert.NotNil(t, event.Action)
	assert.NotNil(t, event.Action.BreakLoop)
	assert.True(t, event.Action.BreakLoop.Done)
	assert.Equal(t, "LoopAgent", event.Action.BreakLoop.From)
	assert.Equal(t, 0, event.Action.BreakLoop.CurrentIterations)

	msg := event.Output.MessageOutput.Message
	assert.NotNil(t, msg)
	assert.Equal(t, "Loop iteration with break loop", msg.Content)
}

// Add these test functions to the existing workflow_test.go file
// 将这些测试函数添加到现有 workflow_test.go 文件中

// Replace the existing TestWorkflowAgentPanicRecovery function
// 替换现有的 TestWorkflowAgentPanicRecovery 函数
func TestWorkflowAgentPanicRecovery(t *testing.T) {
	ctx := context.Background()

	// Create a panic agent that panics in Run method
	// 创建一个在 Run 方法中 panic 的 panic agent
	panicAgent := &panicMockAgent{
		mockAgent: mockAgent{
			name:        "PanicAgent",
			description: "Agent that panics",
			responses:   []*AgentEvent{},
		},
	}

	// Create a sequential agent with the panic agent
	// 创建一个包含 panic agent 的 sequential agent
	config := &SequentialAgentConfig{
		Name:        "PanicTestAgent",
		Description: "Test agent with panic",
		SubAgents:   []Agent{panicAgent},
	}

	sequentialAgent, err := NewSequentialAgent(ctx, config)
	assert.NoError(t, err)

	// Run the agent and expect panic recovery
	// 运行 agent，并期望 panic recovery
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}

	ctx, _ = initRunCtx(ctx, sequentialAgent.Name(ctx), input)
	iterator := sequentialAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// Should receive an error event due to panic recovery
	// 由于 panic recovery，应收到一个错误事件
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event)
	assert.NotNil(t, event.Err)
	assert.Contains(t, event.Err.Error(), "panic")

	// No more events
	// 没有更多事件
	_, ok = iterator.Next()
	assert.False(t, ok)
}

// Add these new mock agent types that properly panic
// 添加这些会正确 panic 的新 mock agent 类型
type panicMockAgent struct {
	mockAgent
}

func (a *panicMockAgent) Run(_ context.Context, _ *AgentInput, _ ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	panic("test panic in agent")
}

func TestParallelWorkflowResumeWithEvents(t *testing.T) {
	ctx := context.Background()

	// Create interruptible agents
	// 创建可中断的智能体
	sa1 := &myAgent{
		name: "sa1",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			// Send a normal message event first, called event1
			// 先发送一个普通消息事件，称为 event1
			generator.Send(&AgentEvent{
				AgentName: "sa1",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("sa1 normal message"),
					},
				},
			})
			intEvent := Interrupt(ctx, "sa1 interrupt data")
			generator.Send(intEvent)
			generator.Close()
			return iter
		},
		resumeFn: func(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			assert.True(t, info.WasInterrupted)
			assert.Nil(t, info.InterruptState)
			assert.True(t, info.IsResumeTarget)
			assert.Equal(t, "resume sa1", info.ResumeData)

			// Get the events from session and verify visibility
			// 从 session 获取事件并验证可见性
			runCtx := getRunCtx(ctx)
			assert.NotNil(t, runCtx.Session, "sa1 resumer should have session")
			allEvents := runCtx.Session.getEvents()

			// Assert that allEvents only have 1 event, that is event1
			// 断言 allEvents 只有 1 个事件，即 event1
			assert.Equal(t, 1, len(allEvents), "sa1 should only see its own event in session")
			assert.Equal(t, "sa1", allEvents[0].AgentEvent.AgentName, "sa1 should see its own event")
			assert.Equal(t, "sa1 normal message", allEvents[0].AgentEvent.Output.MessageOutput.Message.Content, "sa1 should see its own message content")

			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Close()
			return iter
		},
	}

	sa2 := &myAgent{
		name: "sa2",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			// Send a normal message event first, called event2
			// 先发送一个普通消息事件，称为 event2
			generator.Send(&AgentEvent{
				AgentName: "sa2",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("sa2 normal message"),
					},
				},
			})
			intEvent := StatefulInterrupt(ctx, "sa2 interrupt data", "sa2 interrupt")
			generator.Send(intEvent)
			generator.Close()
			return iter
		},
		resumeFn: func(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			assert.True(t, info.WasInterrupted)
			assert.NotNil(t, info.InterruptState)
			assert.Equal(t, "sa2 interrupt", info.InterruptState)
			assert.True(t, info.IsResumeTarget)
			assert.Equal(t, "resume sa2", info.ResumeData)

			// Get the events from session and verify visibility
			// 从 session 获取事件并验证可见性
			runCtx := getRunCtx(ctx)
			assert.NotNil(t, runCtx.Session, "sa2 resumer should have session")
			allEvents := runCtx.Session.getEvents()

			// Assert that allEvents only have 1 event, that is event2
			// 断言 allEvents 只有 1 个事件，即 event2
			assert.Equal(t, 1, len(allEvents), "sa2 should only see its own event in session")
			assert.Equal(t, "sa2", allEvents[0].AgentEvent.AgentName, "sa2 should see its own event")
			assert.Equal(t, "sa2 normal message", allEvents[0].AgentEvent.Output.MessageOutput.Message.Content, "sa2 should see its own message content")

			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Close()
			return iter
		},
	}

	sa3 := &myAgent{
		name: "sa3",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Send(&AgentEvent{
				AgentName: "sa3",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("sa3 completed"),
					},
				},
			})
			generator.Close()
			return iter
		},
	}

	sa4 := &myAgent{
		name: "sa4",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Send(&AgentEvent{
				AgentName: "sa4",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("sa4 completed"),
					},
				},
			})
			generator.Close()
			return iter
		},
	}

	t.Run("test parallel workflow agent", func(t *testing.T) {
		// parallel
		a, err := NewParallelAgent(ctx, &ParallelAgentConfig{
			Name:      "parallel agent",
			SubAgents: []Agent{sa1, sa2, sa3, sa4},
		})
		assert.NoError(t, err)
		runner := NewRunner(ctx, RunnerConfig{
			Agent:           a,
			CheckPointStore: newMyStore(),
		})
		iter := runner.Query(ctx, "hello world", WithCheckPointID("1"))
		var (
			events         []*AgentEvent
			interruptEvent *AgentEvent
		)

		for {
			event, ok := iter.Next()
			if !ok {
				break
			}
			if event.Action != nil && event.Action.Interrupted != nil {
				interruptEvent = event
				continue
			}
			events = append(events, event)
		}
		assert.Equal(t, 4, len(events), "should have 4 events (2 normal messages + 2 completed agents)")

		// Verify specific properties of each event
		// 验证每个事件的特定属性
		var sa3Event, sa4Event *AgentEvent
		for _, event := range events {
			switch event.AgentName {
			case "sa3":
				sa3Event = event
			case "sa4":
				sa4Event = event
			}
		}

		// Verify sa3 event properties
		// 验证 sa3 事件属性
		assert.NotNil(t, sa3Event, "should have event from sa3")
		assert.Equal(t, "sa3", sa3Event.AgentName, "sa3 event should have correct agent name")
		assert.Equal(t, []RunStep{{"parallel agent"}, {"sa3"}}, sa3Event.RunPath, "sa3 event should have correct run path")
		assert.NotNil(t, sa3Event.Output, "sa3 event should have output")
		assert.NotNil(t, sa3Event.Output.MessageOutput, "sa3 event should have message output")
		assert.Equal(t, "sa3 completed", sa3Event.Output.MessageOutput.Message.Content, "sa3 event should have correct message content")

		// Verify sa4 event properties
		// 验证 sa4 事件属性
		assert.NotNil(t, sa4Event, "should have event from sa4")
		assert.Equal(t, "sa4", sa4Event.AgentName, "sa4 event should have correct agent name")
		assert.Equal(t, []RunStep{{"parallel agent"}, {"sa4"}}, sa4Event.RunPath, "sa4 event should have correct run path")
		assert.NotNil(t, sa4Event.Output, "sa4 event should have output")
		assert.NotNil(t, sa4Event.Output.MessageOutput, "sa4 event should have message output")
		assert.Equal(t, "sa4 completed", sa4Event.Output.MessageOutput.Message.Content, "sa4 event should have correct message content")

		assert.NotNil(t, interruptEvent)
		assert.Equal(t, "parallel agent", interruptEvent.AgentName)
		assert.Equal(t, []RunStep{{"parallel agent"}}, interruptEvent.RunPath)
		assert.NotNil(t, interruptEvent.Action.Interrupted)

		var sa1InfoFound, sa2InfoFound bool
		for _, ctx := range interruptEvent.Action.Interrupted.InterruptContexts {
			switch ctx.Info {
			case "sa1 interrupt data":
				sa1InfoFound = true
			case "sa2 interrupt data":
				sa2InfoFound = true
			}
		}

		assert.Equal(t, 2, len(interruptEvent.Action.Interrupted.InterruptContexts))
		assert.True(t, sa1InfoFound)
		assert.True(t, sa2InfoFound)

		var parallelInterruptID1, parallelInterruptID2 string
		for _, ctx := range interruptEvent.Action.Interrupted.InterruptContexts {
			switch ctx.Info {
			case "sa1 interrupt data":
				parallelInterruptID1 = ctx.ID
			case "sa2 interrupt data":
				parallelInterruptID2 = ctx.ID
			}
		}
		assert.NotEmpty(t, parallelInterruptID1)
		assert.NotEmpty(t, parallelInterruptID2)

		iter, err = runner.ResumeWithParams(ctx, "1", &ResumeParams{
			Targets: map[string]any{
				parallelInterruptID1: "resume sa1",
				parallelInterruptID2: "resume sa2",
			},
		})
		assert.NoError(t, err)
		_, ok := iter.Next()
		assert.False(t, ok)
	})
}

func TestNestedParallelWorkflow(t *testing.T) {
	ctx := context.Background()

	// Create predecessor agent that runs before the parallel structure
	// 创建在并行结构之前运行的前置智能体
	predecessorAgent := &myAgent{
		name: "predecessor",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Send(&AgentEvent{
				AgentName: "predecessor",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("predecessor completed"),
					},
				},
			})
			generator.Close()
			return iter
		},
	}

	// Create interruptible inner agents
	// 创建可中断的内部智能体
	innerAgent1 := &myAgent{
		name: "inner1",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()

			// Verify inner1 can see predecessor's event
			// 验证 inner1 可以看到 predecessor 的事件
			runCtx := getRunCtx(ctx)
			allEvents := runCtx.Session.getEvents()
			assert.Equal(t, 1, len(allEvents), "inner1 should see exactly 1 event (predecessor)")

			assert.Equal(t, "predecessor", allEvents[0].AgentEvent.AgentName, "inner1 should see predecessor event")
			assert.Equal(t, "predecessor completed", allEvents[0].AgentEvent.Output.MessageOutput.Message.Content, "inner1 should see predecessor message content")

			generator.Send(&AgentEvent{
				AgentName: "inner1",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("inner1 normal"),
					},
				},
			})
			intEvent := Interrupt(ctx, "inner1 interrupt")
			generator.Send(intEvent)
			generator.Close()
			return iter
		},
		resumeFn: func(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			assert.True(t, info.WasInterrupted)
			assert.Equal(t, "resume inner1", info.ResumeData)

			// Verify inner1 can see predecessor's event during resume
			// 验证恢复期间 inner1 可以看到 predecessor 的事件
			runCtx := getRunCtx(ctx)
			allEvents := runCtx.Session.getEvents()
			assert.Equal(t, 2, len(allEvents), "inner1 should see exactly 2 events (predecessor + own normal message) during resume")

			// Find and verify predecessor event
			// 查找并验证 predecessor 事件
			var foundPredecessor bool
			for _, event := range allEvents {
				if event.AgentEvent != nil && event.AgentEvent.AgentName == "predecessor" {
					foundPredecessor = true
					assert.Equal(t, "predecessor completed", event.AgentEvent.Output.MessageOutput.Message.Content)
				}
			}
			assert.True(t, foundPredecessor, "inner1 should see predecessor event during resume")

			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Close()
			return iter
		},
	}

	innerAgent2 := &myAgent{
		name: "inner2",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()

			// Verify inner2 can see predecessor's event
			// 验证 inner2 可以看到 predecessor 的事件
			runCtx := getRunCtx(ctx)
			allEvents := runCtx.Session.getEvents()
			assert.Equal(t, 1, len(allEvents), "inner2 should see exactly 1 event (predecessor)")

			assert.Equal(t, "predecessor", allEvents[0].AgentEvent.AgentName, "inner2 should see predecessor event")
			assert.Equal(t, "predecessor completed", allEvents[0].AgentEvent.Output.MessageOutput.Message.Content, "inner2 should see predecessor message content")

			generator.Send(&AgentEvent{
				AgentName: "inner2",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("inner2 normal"),
					},
				},
			})
			intEvent := StatefulInterrupt(ctx, "inner2 interrupt", "inner2 state")
			generator.Send(intEvent)
			generator.Close()
			return iter
		},
		resumeFn: func(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			assert.True(t, info.WasInterrupted)
			assert.Equal(t, "inner2 state", info.InterruptState)
			assert.Equal(t, "resume inner2", info.ResumeData)

			// Verify inner2 can see predecessor's event during resume
			// 验证恢复期间 inner2 可以看到 predecessor 的事件
			runCtx := getRunCtx(ctx)
			allEvents := runCtx.Session.getEvents()
			assert.Equal(t, 2, len(allEvents), "inner2 should see exactly 2 events (predecessor + own normal message) during resume")

			// Find and verify predecessor event
			// 查找并验证 predecessor 事件
			var foundPredecessor bool
			for _, event := range allEvents {
				if event.AgentEvent != nil && event.AgentEvent.AgentName == "predecessor" {
					foundPredecessor = true
					assert.Equal(t, "predecessor completed", event.AgentEvent.Output.MessageOutput.Message.Content)
				}
			}
			assert.True(t, foundPredecessor, "inner2 should see predecessor event during resume")

			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Close()
			return iter
		},
	}

	// Create inner parallel workflow
	// 创建内部并行 workflow
	innerParallel, err := NewParallelAgent(ctx, &ParallelAgentConfig{
		Name:      "inner parallel",
		SubAgents: []Agent{innerAgent1, innerAgent2},
	})
	assert.NoError(t, err)

	// Create simple outer agents
	// 创建简单的外部智能体
	outerAgent1 := &myAgent{
		name: "outer1",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Send(&AgentEvent{
				AgentName: "outer1",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("outer1 completed"),
					},
				},
			})
			generator.Close()
			return iter
		},
	}

	outerAgent2 := &myAgent{
		name: "outer2",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			generator.Send(&AgentEvent{
				AgentName: "outer2",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("outer2 completed"),
					},
				},
			})
			generator.Close()
			return iter
		},
	}

	// Create outer parallel workflow with nested parallel agent
	// 创建带嵌套并行智能体的外部并行 workflow
	outerParallel, err := NewParallelAgent(ctx, &ParallelAgentConfig{
		Name:      "outer parallel",
		SubAgents: []Agent{outerAgent1, innerParallel, outerAgent2},
	})
	assert.NoError(t, err)

	// Create successor agent that runs after the parallel structure
	// 创建在并行结构之后运行的后续智能体
	successorAgent := &myAgent{
		name: "successor",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()

			// Verify successor can see all events from predecessor and parallel agents
			// 验证 successor 可以看到 predecessor 和并行智能体的所有事件
			runCtx := getRunCtx(ctx)
			allEvents := runCtx.Session.getEvents()
			assert.GreaterOrEqual(t, len(allEvents), 5, "successor should see all events")

			var foundPredecessor, foundOuter1, foundOuter2, foundInner1, foundInner2 bool
			for _, event := range allEvents {
				if event.AgentEvent != nil {
					switch event.AgentEvent.AgentName {
					case "predecessor":
						foundPredecessor = true
						assert.Equal(t, "predecessor completed", event.AgentEvent.Output.MessageOutput.Message.Content)
					case "outer1":
						foundOuter1 = true
					case "outer2":
						foundOuter2 = true
					case "inner1":
						foundInner1 = true
					case "inner2":
						foundInner2 = true
					}
				}
			}

			assert.True(t, foundPredecessor, "successor should see predecessor event")
			assert.True(t, foundOuter1, "successor should see outer1 event")
			assert.True(t, foundOuter2, "successor should see outer2 event")
			assert.True(t, foundInner1, "successor should see inner1 event")
			assert.True(t, foundInner2, "successor should see inner2 event")

			generator.Send(&AgentEvent{
				AgentName: "successor",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						Message: schema.UserMessage("successor completed"),
					},
				},
			})
			generator.Close()
			return iter
		},
	}

	// Create sequential workflow: predecessor -> parallel -> successor
	// 创建顺序 workflow：predecessor -> parallel -> successor
	sequentialWorkflow, err := NewSequentialAgent(ctx, &SequentialAgentConfig{
		Name:      "sequential workflow",
		SubAgents: []Agent{predecessorAgent, outerParallel, successorAgent},
	})
	assert.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent:           sequentialWorkflow,
		CheckPointStore: newMyStore(),
	})

	iter := runner.Query(ctx, "test nested parallel with predecessor and successor", WithCheckPointID("nested-parallel-test"))

	var events []*AgentEvent
	var interruptEvent *AgentEvent
	for event, ok := iter.Next(); ok; event, ok = iter.Next() {
		if event.Action != nil && event.Action.Interrupted != nil {
			interruptEvent = event
			continue
		}
		events = append(events, event)
	}

	// Should get events from predecessor, outer agents, and inner normal messages (successor doesn't run due to interruption)
	// 应获取 predecessor、外部智能体和内部普通消息的事件（successor 因中断未运行）
	assert.Equal(t, 5, len(events), "should have 5 events (predecessor + 2 outer + 2 inner)")
	if interruptEvent == nil {
		t.Fatal("should have interrupt event")
	}

	// Resume the inner parallel workflow
	// 恢复内部 parallel 工作流
	var innerInterruptID1, innerInterruptID2 string
	for _, ctx := range interruptEvent.Action.Interrupted.InterruptContexts {
		switch ctx.Info {
		case "inner1 interrupt":
			innerInterruptID1 = ctx.ID
		case "inner2 interrupt":
			innerInterruptID2 = ctx.ID
		}
	}

	iter, err = runner.ResumeWithParams(ctx, "nested-parallel-test", &ResumeParams{
		Targets: map[string]any{
			innerInterruptID1: "resume inner1",
			innerInterruptID2: "resume inner2",
		},
	})
	assert.NoError(t, err)

	// Verify resume completes successfully and successor runs
	// 验证恢复成功完成且后继节点运行
	var resumeEvents []*AgentEvent
	for event, ok := iter.Next(); ok; event, ok = iter.Next() {
		resumeEvents = append(resumeEvents, event)
	}

	// Should get successor event after resume
	// 恢复后应收到后继事件
	assert.Equal(t, 1, len(resumeEvents), "should have successor event after resume")
	assert.Equal(t, "successor", resumeEvents[0].AgentName)
}

// TestWorkflowAgentUnsupportedMode tests unsupported workflow mode error (lines 65-71)
// TestWorkflowAgentUnsupportedMode 测试不支持的工作流模式错误（第 65-71 行）
func TestWorkflowAgentUnsupportedMode(t *testing.T) {
	ctx := context.Background()

	// Create a workflow agent with unsupported mode
	// 创建使用不支持模式的工作流智能体
	agent := &workflowAgent{
		name:        "UnsupportedModeAgent",
		description: "Agent with unsupported mode",
		subAgents:   []*flowAgent{},
		mode:        workflowAgentMode(999),
	}

	// Run the agent and expect error
	// 运行智能体并期望出错
	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}

	ctx, _ = initRunCtx(ctx, agent.Name(ctx), input)
	iterator := agent.Run(ctx, input)
	assert.NotNil(t, iterator)

	// Should receive an error event due to unsupported mode
	// 应收到因模式不支持导致的错误事件
	event, ok := iterator.Next()
	assert.True(t, ok)
	assert.NotNil(t, event)
	assert.NotNil(t, event.Err)
	assert.Contains(t, event.Err.Error(), "unsupported workflow agent mode")

	// No more events
	// 没有更多事件
	_, ok = iterator.Next()
	assert.False(t, ok)
}

func TestFilterOptions(t *testing.T) {
	a1 := &myAgent{
		name: "Agent1",
		runFn: func(ctx context.Context, input *AgentInput, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			o := GetImplSpecificOptions[myAgentOptions](nil, opts...)
			assert.Equal(t, "Agent1", o.value)
			iter, gen := NewAsyncIteratorPair[*AgentEvent]()
			gen.Close()
			return iter
		},
	}
	a2 := &myAgent{
		name: "Agent2",
		runFn: func(ctx context.Context, input *AgentInput, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			o := GetImplSpecificOptions[myAgentOptions](nil, opts...)
			assert.Equal(t, "Agent2", o.value)
			iter, gen := NewAsyncIteratorPair[*AgentEvent]()
			gen.Close()
			return iter
		},
	}
	ctx := context.Background()
	// sequential
	seqAgent, err := NewSequentialAgent(ctx, &SequentialAgentConfig{
		SubAgents: []Agent{a1, a2},
	})
	assert.NoError(t, err)
	iter := seqAgent.Run(ctx, &AgentInput{}, withValue("Agent1").DesignateAgent("Agent1"), withValue("Agent2").DesignateAgent("Agent2"))
	_, ok := iter.Next()
	assert.False(t, ok)

	// parallel
	parAgent, err := NewParallelAgent(ctx, &ParallelAgentConfig{
		SubAgents: []Agent{a1, a2},
	})
	assert.NoError(t, err)
	iter = parAgent.Run(ctx, &AgentInput{}, withValue("Agent1").DesignateAgent("Agent1"), withValue("Agent2").DesignateAgent("Agent2"))
	_, ok = iter.Next()
	assert.False(t, ok)
}

func TestLoopAgentWithError(t *testing.T) {
	ctx := context.Background()

	iterationCount := 0
	agent := &myAgent{
		name: "ErrorAgent",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			go func() {
				defer generator.Close()
				iterationCount++
				if iterationCount == 3 {
					generator.Send(&AgentEvent{Err: fmt.Errorf("error on iteration %d", iterationCount)})
					return
				}
				generator.Send(&AgentEvent{
					Output: &AgentOutput{
						MessageOutput: &MessageVariant{
							Message: schema.AssistantMessage(fmt.Sprintf("iteration %d", iterationCount), nil),
							Role:    schema.Assistant,
						},
					},
				})
			}()
			return iter
		},
	}

	loopAgent, err := NewLoopAgent(ctx, &LoopAgentConfig{
		Name:          "LoopErrorTestAgent",
		SubAgents:     []Agent{agent},
		MaxIterations: 10,
	})
	assert.NoError(t, err)

	input := &AgentInput{Messages: []Message{schema.UserMessage("test")}}
	ctx, _ = initRunCtx(ctx, loopAgent.Name(ctx), input)
	iterator := loopAgent.Run(ctx, input)

	var events []*AgentEvent
	var errorEvent *AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			errorEvent = event
		} else {
			events = append(events, event)
		}
	}

	assert.Equal(t, 2, len(events), "should have 2 successful iterations before error")
	assert.NotNil(t, errorEvent, "should have received error event")
	assert.Contains(t, errorEvent.Err.Error(), "error on iteration 3")
	assert.Equal(t, 3, iterationCount, "loop should stop at iteration 3")
}

func TestWorkflowCallbackHandlerNotDoubled(t *testing.T) {
	ctx := context.Background()
	store := newMyStore()

	var globalCallbackCount int
	var designatedCallbackCount int
	var mu sync.Mutex

	globalHandler := callbacks.NewHandlerBuilder().OnStartFn(
		func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
			if info.Component == ComponentOfAgent && info.Name == "SubSubAgent" {
				mu.Lock()
				globalCallbackCount++
				mu.Unlock()
			}
			return ctx
		}).Build()

	designatedHandler := callbacks.NewHandlerBuilder().OnStartFn(
		func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
			if info.Component == ComponentOfAgent && info.Name == "SubSubAgent" {
				mu.Lock()
				designatedCallbackCount++
				mu.Unlock()
			}
			return ctx
		}).Build()

	iterationCount := 0
	shouldInterrupt := true
	subSubAgent := &myAgent{
		name: "SubSubAgent",
		runFn: func(ctx context.Context, input *AgentInput, options ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			go func() {
				defer generator.Close()
				iterationCount++
				if shouldInterrupt && iterationCount == 2 {
					generator.Send(Interrupt(ctx, "test_interrupt"))
					return
				}
				generator.Send(&AgentEvent{
					Output: &AgentOutput{
						MessageOutput: &MessageVariant{
							Message: schema.AssistantMessage(fmt.Sprintf("iteration %d", iterationCount), nil),
							Role:    schema.Assistant,
						},
					},
				})
			}()
			return iter
		},
		resumeFn: func(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
			iter, generator := NewAsyncIteratorPair[*AgentEvent]()
			go func() {
				defer generator.Close()
				iterationCount++
				generator.Send(&AgentEvent{
					Output: &AgentOutput{
						MessageOutput: &MessageVariant{
							Message: schema.AssistantMessage(fmt.Sprintf("resumed iteration %d", iterationCount), nil),
							Role:    schema.Assistant,
						},
					},
				})
			}()
			return iter
		},
	}

	subWorkflow, err := NewLoopAgent(ctx, &LoopAgentConfig{
		Name:          "SubWorkflow",
		SubAgents:     []Agent{subSubAgent},
		MaxIterations: 2,
	})
	assert.NoError(t, err)

	parentWorkflow, err := NewLoopAgent(ctx, &LoopAgentConfig{
		Name:          "ParentWorkflow",
		SubAgents:     []Agent{subWorkflow},
		MaxIterations: 2,
	})
	assert.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent:           parentWorkflow,
		CheckPointStore: store,
	})

	opts := []AgentRunOption{
		WithCallbacks(globalHandler),
		WithCallbacks(designatedHandler).DesignateAgent("ParentWorkflow", "SubSubAgent"),
		WithCheckPointID("cp1"),
	}

	iterator := runner.Run(ctx, []Message{schema.UserMessage("test")}, opts...)

	var interruptEvent *AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		if event.Action != nil && event.Action.Interrupted != nil {
			interruptEvent = event
		}
	}

	assert.NotNil(t, interruptEvent)
	assert.Equal(t, 2, iterationCount)
	assert.Equal(t, 2, globalCallbackCount)
	assert.Equal(t, 2, designatedCallbackCount)

	shouldInterrupt = false
	var rootCauseID string
	for _, intCtx := range interruptEvent.Action.Interrupted.InterruptContexts {
		if intCtx.IsRootCause {
			rootCauseID = intCtx.ID
			break
		}
	}

	resumeIter, err := runner.ResumeWithParams(ctx, "cp1", &ResumeParams{
		Targets: map[string]any{rootCauseID: nil},
	}, opts...)
	assert.NoError(t, err)

	for {
		_, ok := resumeIter.Next()
		if !ok {
			break
		}
	}

	assert.Equal(t, 5, iterationCount)
	assert.Equal(t, 5, globalCallbackCount)
	assert.Equal(t, 5, designatedCallbackCount)
}

func TestLoopAgentWithBreakLoopFollowedByMoreEvents(t *testing.T) {
	ctx := context.Background()

	agent := newMockAgent("SubAgent", "Sub agent", []*AgentEvent{
		{
			AgentName: "SubAgent",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.ToolMessage("tool result", "call_123"),
					Role:        schema.Tool,
				},
			},
			Action: NewBreakLoopAction("SubAgent"),
		},
		{
			AgentName: "SubAgent",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming: false,
					Message:     schema.AssistantMessage("Final response after tool", nil),
					Role:        schema.Assistant,
				},
			},
		},
	})

	loopAgent, err := NewLoopAgent(ctx, &LoopAgentConfig{
		Name:          "LoopTestAgent",
		Description:   "Test loop agent",
		SubAgents:     []Agent{agent},
		MaxIterations: 3,
	})
	assert.NoError(t, err)
	assert.NotNil(t, loopAgent)

	input := &AgentInput{
		Messages: []Message{
			schema.UserMessage("Test input"),
		},
	}
	ctx, _ = initRunCtx(ctx, loopAgent.Name(ctx), input)

	iterator := loopAgent.Run(ctx, input)
	assert.NotNil(t, iterator)

	var events []*AgentEvent
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		events = append(events, event)
	}

	assert.Equal(t, 2, len(events), "should have 2 events (tool event with BreakLoop + final response) and loop should break")

	assert.NotNil(t, events[0].Action, "first event should have an action")
	assert.NotNil(t, events[0].Action.BreakLoop, "first event should have BreakLoop action")
	assert.True(t, events[0].Action.BreakLoop.Done, "BreakLoop should be marked as Done")
	assert.Equal(t, "SubAgent", events[0].Action.BreakLoop.From)
	assert.Equal(t, 0, events[0].Action.BreakLoop.CurrentIterations)
	assert.Equal(t, schema.Tool, events[0].Output.MessageOutput.Role, "first event should be tool message")

	assert.Nil(t, events[1].Action, "second event should not have an action")
	assert.Equal(t, schema.Assistant, events[1].Output.MessageOutput.Role, "second event should be assistant message")
	assert.Equal(t, "Final response after tool", events[1].Output.MessageOutput.Message.Content)
}
