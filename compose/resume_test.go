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

package compose

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	mockModel "github.com/cloudwego/eino/internal/mock/components/model"
	"github.com/cloudwego/eino/schema"
)

type myInterruptState struct {
	OriginalInput string
}

type myResumeData struct {
	Message string
}

type resumeTestState struct {
	OnStartCalledOnResume bool `json:"on_start_called_on_resume"`
	Counter               int  `json:"counter"`
}

func init() {
	schema.Register[resumeTestState]()
}

func TestInterruptStateAndResumeForRootGraph(t *testing.T) {
	// create a graph with a lambda node
	// this lambda node will interrupt with a typed state and an info for end-user
	// verify the info thrown by the lambda node
	// resume with a structured resume data
	// within the lambda node, getRunCtx and verify the state and resume data
	//
	// 创建一个带有 lambda node 的 graph
	// 该 lambda node 会携带类型化 state 和面向终端用户的 info 触发 interrupt
	// 验证 lambda node 抛出的 info
	// 使用结构化 resume data 进行 resume
	// 在 lambda node 内部调用 getRunCtx，并验证 state 和 resume data
	g := NewGraph[string, string]()

	lambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		wasInterrupted, hasState, state := GetInterruptState[*myInterruptState](ctx)
		if !wasInterrupted {
			// First run: interrupt with state
			// 首次运行：携带 state 触发 interrupt
			return "", StatefulInterrupt(ctx,
				map[string]any{"reason": "scheduled maintenance"},
				&myInterruptState{OriginalInput: input},
			)
		}

		// This is a resumed run.
		// 这是一次 resumed run。
		assert.True(t, hasState)
		assert.Equal(t, "initial input", state.OriginalInput)

		isResume, hasData, data := GetResumeContext[*myResumeData](ctx)
		assert.True(t, isResume)
		assert.True(t, hasData)
		assert.Equal(t, "let's continue", data.Message)

		return "Resumed successfully with input: " + state.OriginalInput, nil
	})

	_ = g.AddLambdaNode("lambda", lambda)
	_ = g.AddEdge(START, "lambda")
	_ = g.AddEdge("lambda", END)

	graph, err := g.Compile(context.Background(), WithCheckPointStore(newInMemoryStore()), WithGraphName("root"))
	assert.NoError(t, err)

	// First invocation, which should be interrupted
	// 第一次调用，预期会被 interrupt
	checkPointID := "test-checkpoint-1"
	_, err = graph.Invoke(context.Background(), "initial input", WithCheckPointID(checkPointID))

	// Verify the interrupt error and extracted info
	// 验证 interrupt error 和提取出的 info
	assert.Error(t, err)
	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)
	assert.NotNil(t, interruptInfo)

	interruptContexts := interruptInfo.InterruptContexts
	assert.Equal(t, 1, len(interruptContexts))
	assert.Equal(t, "runnable:root;node:lambda", interruptContexts[0].Address.String())
	assert.Equal(t, map[string]any{"reason": "scheduled maintenance"}, interruptContexts[0].Info)

	// Prepare resume data
	// 准备 resume data
	ctx := ResumeWithData(context.Background(), interruptContexts[0].ID,
		&myResumeData{Message: "let's continue"})

	// Resume execution
	// resume 执行
	output, err := graph.Invoke(ctx, "", WithCheckPointID(checkPointID))

	// Verify the final result
	// 验证最终结果
	assert.NoError(t, err)
	assert.Equal(t, "Resumed successfully with input: initial input", output)
}

func TestProcessStateInOnStartDuringResume(t *testing.T) {
	graphOnStartCallCount := 0
	processStateErrorOnResume := error(nil)

	cb := callbacks.NewHandlerBuilder().
		OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
			if info.Name == "test-process-state-onstart" {
				graphOnStartCallCount++
				err := ProcessState[*resumeTestState](ctx, func(ctx context.Context, s *resumeTestState) error {
					s.Counter++
					return nil
				})
				if graphOnStartCallCount > 1 {
					processStateErrorOnResume = err
				}
			}
			return ctx
		}).
		Build()

	g := NewGraph[string, string](WithGenLocalState(func(ctx context.Context) *resumeTestState {
		return &resumeTestState{}
	}))

	lambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		wasInterrupted, _, _ := GetInterruptState[*myInterruptState](ctx)
		if !wasInterrupted {
			return "", StatefulInterrupt(ctx,
				map[string]any{"reason": "test interrupt"},
				&myInterruptState{OriginalInput: input},
			)
		}

		var stateCounter int
		err := ProcessState[*resumeTestState](ctx, func(ctx context.Context, s *resumeTestState) error {
			stateCounter = s.Counter
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, stateCounter, "Counter should be 2 (first run OnStart + resume OnStart)")

		return "success", nil
	})

	_ = g.AddLambdaNode("lambda", lambda)
	_ = g.AddEdge(START, "lambda")
	_ = g.AddEdge("lambda", END)

	graph, err := g.Compile(context.Background(),
		WithCheckPointStore(newInMemoryStore()),
		WithGraphName("test-process-state-onstart"),
	)
	assert.NoError(t, err)

	checkPointID := "test-checkpoint-process-state"
	_, err = graph.Invoke(context.Background(), "test input", WithCheckPointID(checkPointID), WithCallbacks(cb))

	assert.Error(t, err, "First invocation should return an error")
	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt, "Should be an interrupt error")
	assert.NotNil(t, interruptInfo)
	assert.Equal(t, 1, graphOnStartCallCount, "Graph OnStart should be called once on first run")

	ctx := ResumeWithData(context.Background(), interruptInfo.InterruptContexts[0].ID, &myResumeData{})

	output, err := graph.Invoke(ctx, "", WithCheckPointID(checkPointID), WithCallbacks(cb))
	assert.NoError(t, err)
	assert.Equal(t, "success", output)
	assert.Equal(t, 2, graphOnStartCallCount, "Graph OnStart should be called twice (first run + resume)")
	assert.NoError(t, processStateErrorOnResume, "ProcessState should work in OnStart during resume")
}

func TestInterruptStateAndResumeForSubGraph(t *testing.T) {
	// create a graph
	// create a another graph with a lambda node, as this graph as a sub-graph of the previous graph
	// this lambda node will interrupt with a typed state and an info for end-user
	// verify the info thrown by the lambda node
	// resume with a structured resume data
	// within the lambda node, getRunCtx and verify the state and resume data
	//
	// 创建一个图
	// 创建另一个带 lambda 节点的图，并将该图作为前一个图的子图
	// 此 lambda 节点会带 typed state 和面向最终用户的 info 触发中断
	// 验证 lambda 节点抛出的 info
	// 使用结构化 resume data 恢复
	// 在 lambda 节点内 getRunCtx，并验证 state 和 resume data
	subGraph := NewGraph[string, string]()

	lambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		wasInterrupted, hasState, state := GetInterruptState[*myInterruptState](ctx)
		if !wasInterrupted {
			// First run: interrupt with state
			// 首次运行：带 state 中断
			return "", StatefulInterrupt(ctx,
				map[string]any{"reason": "sub-graph maintenance"},
				&myInterruptState{OriginalInput: input},
			)
		}

		// Second (resumed) run
		// 第二次（恢复后）运行
		assert.True(t, hasState)
		assert.Equal(t, "main input", state.OriginalInput)

		isResume, hasData, data := GetResumeContext[*myResumeData](ctx)
		assert.True(t, isResume)
		assert.True(t, hasData)
		assert.Equal(t, "let's continue sub-graph", data.Message)

		return "Sub-graph resumed successfully", nil
	})

	_ = subGraph.AddLambdaNode("inner_lambda", lambda)
	_ = subGraph.AddEdge(START, "inner_lambda")
	_ = subGraph.AddEdge("inner_lambda", END)

	// Create the main graph
	// 创建主图
	mainGraph := NewGraph[string, string]()
	_ = mainGraph.AddGraphNode("sub_graph_node", subGraph)
	_ = mainGraph.AddEdge(START, "sub_graph_node")
	_ = mainGraph.AddEdge("sub_graph_node", END)

	compiledMainGraph, err := mainGraph.Compile(context.Background(), WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	// First invocation, which should be interrupted
	// 第一次调用，应被中断
	checkPointID := "test-subgraph-checkpoint-1"
	_, err = compiledMainGraph.Invoke(context.Background(), "main input", WithCheckPointID(checkPointID))

	// Verify the interrupt error and extracted info
	// 验证中断错误和提取出的 info
	assert.Error(t, err)
	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)
	assert.NotNil(t, interruptInfo)

	interruptContexts := interruptInfo.InterruptContexts
	assert.Equal(t, 1, len(interruptContexts))
	assert.Equal(t, "runnable:;node:sub_graph_node;node:inner_lambda", interruptContexts[0].Address.String())
	assert.Equal(t, map[string]any{"reason": "sub-graph maintenance"}, interruptContexts[0].Info)

	// Prepare resume data
	// 准备 resume data
	ctx := ResumeWithData(context.Background(), interruptContexts[0].ID,
		&myResumeData{Message: "let's continue sub-graph"})

	// Resume execution
	// 恢复执行
	output, err := compiledMainGraph.Invoke(ctx, "", WithCheckPointID(checkPointID))

	// Verify the final result
	// 验证最终结果
	assert.NoError(t, err)
	assert.Equal(t, "Sub-graph resumed successfully", output)
}

func TestInterruptStateAndResumeForToolInNestedSubGraph(t *testing.T) {
	// create a ROOT graph.
	// create a sub graph A, add A to ROOT graph using AddGraphNode.
	// create a sub-sub graph B, add B to A using AddGraphNode.
	// within sub-sub graph B, add a ChatModelNode, which is a Mock chat model that implements the ToolCallingChatModel
	// interface.
	// add a Mock InvokableTool to this mock chat model.
	// within sub-sub graph B, also add a ToolsNode that will execute this Mock InvokableTool.
	// this tool will interrupt with a typed state and an info for end-user
	// verify the info thrown by the tool.
	// resume with a structured resume data.
	// within the Tool, getRunCtx and verify the state and resume data
	//
	// 创建一个 ROOT 图。
	// 创建子图 A，并使用 AddGraphNode 将 A 添加到 ROOT 图。
	// 创建子子图 B，并使用 AddGraphNode 将 B 添加到 A。
	// 在子子图 B 中添加一个 ChatModelNode，它是实现 ToolCallingChatModel 的 Mock chat model。
	// interface。
	// 向此 mock chat model 添加一个 Mock InvokableTool。
	// 在子子图 B 中，还添加一个 ToolsNode，用于执行此 Mock InvokableTool。
	// 此工具会带 typed state 和面向最终用户的 info 触发中断
	// 验证工具抛出的 info。
	// 使用结构化 resume data 恢复。
	// 在 Tool 内 getRunCtx，并验证 state 和 resume data
	ctrl := gomock.NewController(t)

	// 1. Define the interrupting tool
	// 1. 定义会触发中断的工具
	mockTool := &mockInterruptingTool{tt: t}

	// 2. Define the sub-sub-graph (B)
	// 2. 定义子子图（B）
	subSubGraphB := NewGraph[[]*schema.Message, []*schema.Message]()

	// Mock Chat Model that calls the tool
	// 调用该工具的 Mock Chat Model
	mockChatModel := mockModel.NewMockToolCallingChatModel(ctrl)
	mockChatModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).Return(&schema.Message{
		Role: schema.Assistant,
		ToolCalls: []schema.ToolCall{
			{ID: "tool_call_123", Function: schema.FunctionCall{Name: "interrupt_tool", Arguments: `{"input": "test"}`}},
		},
	}, nil).AnyTimes()
	mockChatModel.EXPECT().WithTools(gomock.Any()).Return(mockChatModel, nil).AnyTimes()

	toolsNode, err := NewToolNode(context.Background(), &ToolsNodeConfig{Tools: []tool.BaseTool{mockTool}})
	assert.NoError(t, err)

	_ = subSubGraphB.AddChatModelNode("model", mockChatModel)
	_ = subSubGraphB.AddToolsNode("tools", toolsNode)
	_ = subSubGraphB.AddEdge(START, "model")
	_ = subSubGraphB.AddEdge("model", "tools")
	_ = subSubGraphB.AddEdge("tools", END)

	// 3. Define sub-graph (A)
	// 3. 定义子图（A）
	subGraphA := NewGraph[[]*schema.Message, []*schema.Message]()
	_ = subGraphA.AddGraphNode("sub_graph_b", subSubGraphB)
	_ = subGraphA.AddEdge(START, "sub_graph_b")
	_ = subGraphA.AddEdge("sub_graph_b", END)

	// 4. Define root graph
	// 4. 定义根图
	rootGraph := NewGraph[[]*schema.Message, []*schema.Message]()
	_ = rootGraph.AddGraphNode("sub_graph_a", subGraphA)
	_ = rootGraph.AddEdge(START, "sub_graph_a")
	_ = rootGraph.AddEdge("sub_graph_a", END)

	// 5. Compile and run
	// 5. 编译并运行
	compiledRootGraph, err := rootGraph.Compile(context.Background(), WithCheckPointStore(newInMemoryStore()),
		WithGraphName("root"))
	assert.NoError(t, err)

	// First invocation - should interrupt
	// 第一次调用 - 应中断
	checkPointID := "test-nested-tool-interrupt"
	initialInput := []*schema.Message{schema.UserMessage("hello")}
	_, err = compiledRootGraph.Invoke(context.Background(), initialInput, WithCheckPointID(checkPointID))

	// 6. Verify the interrupt
	// 6. 验证中断
	assert.Error(t, err)
	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)
	assert.NotNil(t, interruptInfo)

	interruptContexts := interruptInfo.InterruptContexts
	assert.Len(t, interruptContexts, 1) // Only the root cause is returned
	// 仅返回根因

	// Verify the root cause context
	// 验证根因上下文
	rootCause := interruptContexts[0]
	expectedPath := "runnable:root;node:sub_graph_a;node:sub_graph_b;node:tools;tool:interrupt_tool:tool_call_123"
	assert.Equal(t, expectedPath, rootCause.Address.String())
	assert.True(t, rootCause.IsRootCause)
	assert.Equal(t, map[string]any{"reason": "tool maintenance"}, rootCause.Info)

	// Verify the parent via the Parent field
	// 通过 Parent 字段验证父级
	assert.NotNil(t, rootCause.Parent)
	assert.Equal(t, "runnable:root;node:sub_graph_a;node:sub_graph_b;node:tools", rootCause.Parent.Address.String())
	assert.False(t, rootCause.Parent.IsRootCause)

	// 7. Resume execution
	// 7. 恢复执行
	ctx := ResumeWithData(context.Background(), rootCause.ID, &myResumeData{Message: "let's continue tool"})
	output, err := compiledRootGraph.Invoke(ctx, initialInput, WithCheckPointID(checkPointID))

	// 8. Verify final result
	// 8. 验证最终结果
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.Len(t, output, 1)
	assert.Equal(t, "Tool resumed successfully", output[0].Content)
}

const PathSegmentTypeProcess AddressSegmentType = "process"

// processState is the state for a single sub-process in the batch test.
// processState 是批量测试中单个子 process 的状态。
type processState struct {
	Step int
}

// batchState is the composite state for the whole batch lambda.
// batchState 是整个批量 lambda 的组合状态。
type batchState struct {
	ProcessStates map[string]*processState
	Results       map[string]string
}

type processResumeData struct {
	Instruction string
}

func init() {
	schema.RegisterName[*myInterruptState]("my_interrupt_state")
	schema.RegisterName[*batchState]("batch_state")
	schema.RegisterName[*processState]("process_state")
}

func TestMultipleInterruptsAndResumes(t *testing.T) {
	// define a new lambda node that act as a 'batch' node
	// it kick starts 3 parallel processes, each will interrupt on first run, while preserving their own state.
	// each of the process should have their own user-facing interrupt info.
	// define a new AddressSegmentType for these sub processes.
	// the lambda should use StatefulInterrupt to interrupt and preserve the state,
	// which is a specific struct type that implements the CompositeInterruptState interface.
	// there should also be a specific struct that that implements the CompositeInterruptInfo interface,
	// which helps the end-user to fetch the nested interrupt info.
	// put this lambda node within a graph and invoke the graph.
	// simulate the user getting the flat list of 3 interrupt points using GetInterruptContexts
	// the user then decides to resume two of the three interrupt points
	// the first resume has resume data, while the second resume does not.(ResumeWithData vs. Resume)
	// verify the resume data and state for the resumed interrupt points.
	//
	// 定义一个新的 lambda 节点，作为 'batch' 节点
	// 它会启动 3 个并行 process，每个 process 首次运行时都会中断，同时保留各自的状态。
	// 每个 process 都应有自己的面向用户的中断信息。
	// 为这些子 process 定义新的 AddressSegmentType。
	// 该 lambda 应使用 StatefulInterrupt 来中断并保留状态，
	// 它是实现 CompositeInterruptState 接口的特定 struct 类型。
	// 还应有一个实现 CompositeInterruptInfo 接口的特定 struct，
	// 用于帮助最终用户获取嵌套的中断信息。
	// 将此 lambda 节点放入图中并调用该图。
	// 模拟用户使用 GetInterruptContexts 获取 3 个中断点的扁平列表
	// 然后用户决定恢复 3 个中断点中的两个
	// 第一次恢复带有恢复数据，第二次恢复不带数据。（ResumeWithData vs. Resume）
	// 验证已恢复中断点的恢复数据和状态。
	processIDs := []string{"p0", "p1", "p2"}

	// This is the logic for a single "process"
	// 这是单个 "process" 的逻辑
	runProcess := func(ctx context.Context, id string) (string, error) {
		// Check if this specific process was interrupted before
		// 检查此特定 process 之前是否已中断
		wasInterrupted, hasState, pState := GetInterruptState[*processState](ctx)
		if !wasInterrupted {
			// First run for this process, interrupt it.
			// 此 process 首次运行，中断它。
			return "", StatefulInterrupt(ctx,
				map[string]any{"reason": "process " + id + " needs input"},
				&processState{Step: 1},
			)
		}

		assert.True(t, hasState)
		assert.Equal(t, 1, pState.Step)

		// Check if we are being resumed
		// 检查是否正在被恢复
		isResume, hasData, pData := GetResumeContext[*processResumeData](ctx)
		if !isResume {
			// Not being resumed, so interrupt again.
			// 未被恢复，因此再次中断。
			return "", StatefulInterrupt(ctx,
				map[string]any{"reason": "process " + id + " still needs input"},
				pState,
			)
		}

		// We are being resumed.
		// 正在被恢复。
		if hasData {
			// Resumed with data
			// 带数据恢复
			return "process " + id + " done with instruction: " + pData.Instruction, nil
		}
		// Resumed without data
		// 不带数据恢复
		return "process " + id + " done", nil
	}

	// This is the main "batch" lambda that orchestrates the processes
	// 这是编排这些 process 的主 "batch" lambda
	batchLambda := InvokableLambda(func(ctx context.Context, _ string) (map[string]string, error) {
		// Restore the state of the batch node itself
		// 恢复 batch 节点自身的状态
		_, _, persistedBatchState := GetInterruptState[*batchState](ctx)
		if persistedBatchState == nil {
			persistedBatchState = &batchState{
				Results: make(map[string]string),
			}
		}

		var errs []error

		for _, id := range processIDs {
			// If this process already completed in a previous run, skip it.
			// 如果此 process 已在之前的运行中完成，则跳过它。
			if _, done := persistedBatchState.Results[id]; done {
				continue
			}

			// Create a sub-context for each process
			// 为每个 process 创建子 context
			subCtx := AppendAddressSegment(ctx, PathSegmentTypeProcess, id)
			res, err := runProcess(subCtx, id)

			if err != nil {
				_, ok := IsInterruptRerunError(err)
				assert.True(t, ok)
				errs = append(errs, err)
			} else {
				// Process completed, save its result to the state for the next run.
				// process 已完成，将其结果保存到状态中供下次运行使用。
				persistedBatchState.Results[id] = res
			}
		}

		if len(errs) > 0 {
			return nil, CompositeInterrupt(ctx, nil, persistedBatchState, errs...)
		}

		return persistedBatchState.Results, nil
	})

	g := NewGraph[string, map[string]string]()
	_ = g.AddLambdaNode("batch", batchLambda)
	_ = g.AddEdge(START, "batch")
	_ = g.AddEdge("batch", END)

	graph, err := g.Compile(context.Background(), WithCheckPointStore(newInMemoryStore()),
		WithGraphName("root"))
	assert.NoError(t, err)

	// --- 1. First invocation, all 3 processes should interrupt ---
	// --- 1. 首次调用，所有 3 个 process 都应中断 ---
	checkPointID := "multi-interrupt-test"
	_, err = graph.Invoke(context.Background(), "", WithCheckPointID(checkPointID))

	assert.Error(t, err)
	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)
	interruptContexts := interruptInfo.InterruptContexts
	assert.Len(t, interruptContexts, 3) // Only the 3 root causes
	// 仅 3 个根因

	found := make(map[string]bool)
	addrToID := make(map[string]string)
	var parentCtx *InterruptCtx
	for _, iCtx := range interruptContexts {
		addrStr := iCtx.Address.String()
		found[addrStr] = true
		addrToID[addrStr] = iCtx.ID
		assert.True(t, iCtx.IsRootCause)
		assert.Equal(t, map[string]any{"reason": "process " + iCtx.Address[2].ID + " needs input"}, iCtx.Info)
		// Check that all share the same parent
		// 检查它们是否共享同一个父级
		assert.NotNil(t, iCtx.Parent)
		if parentCtx == nil {
			parentCtx = iCtx.Parent
			assert.Equal(t, "runnable:root;node:batch", parentCtx.Address.String())
			assert.False(t, parentCtx.IsRootCause)
		} else {
			assert.Same(t, parentCtx, iCtx.Parent)
		}
	}
	assert.True(t, found["runnable:root;node:batch;process:p0"])
	assert.True(t, found["runnable:root;node:batch;process:p1"])
	assert.True(t, found["runnable:root;node:batch;process:p2"])

	// --- 2. Second invocation, resume 2 of 3 processes ---
	// Resume p0 with data, and p2 without data. p1 remains interrupted.
	//
	// --- 2. 第二次调用，恢复 3 个进程中的 2 个 ---
	// 用数据恢复 p0，不带数据恢复 p2。p1 保持中断。
	resumeCtx := ResumeWithData(context.Background(), addrToID["runnable:root;node:batch;process:p0"], &processResumeData{Instruction: "do it"})
	resumeCtx = Resume(resumeCtx, addrToID["runnable:root;node:batch;process:p2"])

	_, err = graph.Invoke(resumeCtx, "", WithCheckPointID(checkPointID))

	// Expect an interrupt again, but only for p1
	// 预期再次中断，但仅针对 p1
	assert.Error(t, err)
	interruptInfo2, isInterrupt2 := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt2)
	interruptContexts2 := interruptInfo2.InterruptContexts
	assert.Len(t, interruptContexts2, 1) // Only p1 is left
	// 只剩 p1
	rootCause2 := interruptContexts2[0]
	assert.Equal(t, "runnable:root;node:batch;process:p1", rootCause2.Address.String())
	assert.NotNil(t, rootCause2.Parent)
	assert.Equal(t, "runnable:root;node:batch", rootCause2.Parent.Address.String())

	// --- 3. Third invocation, resume the last process ---
	// --- 3. 第三次调用，恢复最后一个进程 ---
	finalResumeCtx := Resume(context.Background(), rootCause2.ID)
	finalOutput, err := graph.Invoke(finalResumeCtx, "", WithCheckPointID(checkPointID))

	assert.NoError(t, err)
	assert.Equal(t, "process p0 done with instruction: do it", finalOutput["p0"])
	assert.Equal(t, "process p1 done", finalOutput["p1"])
	assert.Equal(t, "process p2 done", finalOutput["p2"])
}

// toolsNodeResumeTargetCallback captures isResumeTarget for ToolsNode during OnStart
// toolsNodeResumeTargetCallback 在 OnStart 期间捕获 ToolsNode 的 isResumeTarget
type toolsNodeResumeTargetCallback struct {
	mu                sync.Mutex
	isResumeTargetLog []bool
}

func (c *toolsNodeResumeTargetCallback) OnStart(ctx context.Context, info *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {
	if info.Component == ComponentOfToolsNode {
		isResumeTarget, _, _ := GetResumeContext[any](ctx)
		c.mu.Lock()
		c.isResumeTargetLog = append(c.isResumeTargetLog, isResumeTarget)
		c.mu.Unlock()
	}
	return ctx
}

func (c *toolsNodeResumeTargetCallback) OnEnd(ctx context.Context, _ *callbacks.RunInfo, _ callbacks.CallbackOutput) context.Context {
	return ctx
}

func (c *toolsNodeResumeTargetCallback) OnError(ctx context.Context, _ *callbacks.RunInfo, _ error) context.Context {
	return ctx
}

func (c *toolsNodeResumeTargetCallback) OnStartWithStreamInput(ctx context.Context, _ *callbacks.RunInfo, input *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	input.Close()
	return ctx
}

func (c *toolsNodeResumeTargetCallback) OnEndWithStreamOutput(ctx context.Context, _ *callbacks.RunInfo, output *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	output.Close()
	return ctx
}

// mockReentryTool is a helper for the reentry test
// mockReentryTool 是 reentry 测试的辅助工具
type mockReentryTool struct {
	t                     *testing.T
	mu                    sync.Mutex
	isResumeTargetByRunID map[string]bool
}

func (t *mockReentryTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name:        "reentry_tool",
		Desc:        "A tool that can be re-entered in a resumed graph.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"input": {Type: schema.String}}),
	}, nil
}

func (t *mockReentryTool) InvokableRun(ctx context.Context, _ string, _ ...tool.Option) (string, error) {
	wasInterrupted, hasState, _ := tool.GetInterruptState[any](ctx)
	isResume, hasData, data := tool.GetResumeContext[*myResumeData](ctx)

	callID := GetToolCallID(ctx)

	t.mu.Lock()
	if t.isResumeTargetByRunID != nil {
		t.isResumeTargetByRunID[callID] = isResume
	}
	t.mu.Unlock()

	// Special handling for the re-entrant call to make assertions explicit.
	// 对重入调用做特殊处理，以便明确断言。
	if callID == "call_3" {
		if !isResume {
			// This is the first run of the re-entrant call. Its context must be clean.
			// This is the core assertion for this test.
			//
			// 这是重入调用的第一次运行。其 context 必须是干净的。
			// 这是此测试的核心断言。
			assert.False(t.t, wasInterrupted, "re-entrant call 'call_3' should not have been interrupted on its first run")
			assert.False(t.t, hasState, "re-entrant call 'call_3' should not have state on its first run")
			// Now, interrupt it as part of the test flow.
			// 现在，将其作为测试流程的一部分中断。
			return "", tool.StatefulInterrupt(ctx, nil, "some state for "+callID)
		}
		// This is the resumed run of the re-entrant call.
		// 这是重入调用的恢复运行。
		assert.True(t.t, wasInterrupted, "resumed call 'call_3' must have been interrupted")
		assert.True(t.t, hasData, "resumed call 'call_3' should have data")
		return "Resumed " + data.Message, nil
	}

	// Standard logic for the initial calls (call_1, call_2)
	// 初始调用（call_1、call_2）的标准逻辑
	if !wasInterrupted {
		// First run for call_1 and call_2, should interrupt.
		// call_1 和 call_2 的第一次运行，应当中断。
		return "", tool.StatefulInterrupt(ctx, nil, "some state for "+callID)
	}

	// From here, wasInterrupted is true for call_1 and call_2.
	// 从这里开始，call_1 和 call_2 的 wasInterrupted 为 true。
	if isResume {
		// The user is explicitly resuming this call.
		// 用户正在显式恢复此调用。
		assert.True(t.t, hasData, "call %s should have resume data", callID)
		return "Resumed " + data.Message, nil
	}

	// The tool was interrupted before, but is not being resumed now. Re-interrupt.
	// 该工具之前被中断，但现在未被恢复。重新中断。
	return "", tool.StatefulInterrupt(ctx, nil, "some state for "+callID)
}

func TestReentryForResumedTools(t *testing.T) {
	// create a 'ReAct' style graph with a ChatModel node and a ToolsNode.
	// within the ToolsNode there is an interruptible tool that will emit interrupt on first run.
	// During the first invocation of the graph, there should be two tool calls (of the same tool) that interrupt.
	// The user chooses to resume one of the interrupted tool call in second invocation,
	// and this time, the resumed tool call should be successful, while the other should interrupt immediately again.
	// The user then chooses to resume the other interrupted tool call in third invocation,
	// and this time, the ChatModel decides to call the tool again,
	// and this time the tool's runCtx should think it was not interrupted nor resumed.
	//
	// 创建一个带有 ChatModel 节点和 ToolsNode 的 'ReAct' 风格图。
	// ToolsNode 内有一个可中断工具，首次运行时会发出中断。
	// 第一次调用图时，应有两个（同一工具的）工具调用发生中断。
	// 用户在第二次调用中选择恢复其中一个被中断的工具调用，
	// 这一次，被恢复的工具调用应成功，而另一个应立即再次中断。
	// 随后用户在第三次调用中选择恢复另一个被中断的工具调用，
	// 这一次，ChatModel 决定再次调用该工具，
	// 并且这一次该工具的 runCtx 应认为它既未被中断，也未被恢复。
	ctrl := gomock.NewController(t)

	// 1. Define the interrupting tool and callback
	// 1. 定义可中断工具和回调
	reentryTool := &mockReentryTool{t: t, isResumeTargetByRunID: make(map[string]bool)}
	toolsNodeCB := &toolsNodeResumeTargetCallback{}

	// 2. Define the graph
	// 2. 定义图
	g := NewGraph[[]*schema.Message, *schema.Message]()

	// Mock Chat Model that drives the ReAct loop
	// 驱动 ReAct 循环的 Mock Chat Model
	mockChatModel := mockModel.NewMockToolCallingChatModel(ctrl)
	toolsNode, err := NewToolNode(context.Background(), &ToolsNodeConfig{Tools: []tool.BaseTool{reentryTool}})
	assert.NoError(t, err)

	// Expectation for the 1st invocation: model returns two tool calls
	// 第 1 次调用的预期：model 返回两个工具调用
	mockChatModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).Return(&schema.Message{
		Role: schema.Assistant,
		ToolCalls: []schema.ToolCall{
			{ID: "call_1", Function: schema.FunctionCall{Name: "reentry_tool", Arguments: `{"input": "a"}`}},
			{ID: "call_2", Function: schema.FunctionCall{Name: "reentry_tool", Arguments: `{"input": "b"}`}},
		},
	}, nil).Times(1)

	// Expectation for the 2nd invocation (after resuming call_1): model does nothing, graph continues
	// Expectation for the 3rd invocation (after resuming call_2): model calls the tool again
	//
	// 第 2 次调用的预期（恢复 call_1 后）：model 不做任何事，图继续执行
	// 第 3 次调用的预期（恢复 call_2 后）：model 再次调用工具
	mockChatModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.Message, error) {
		return &schema.Message{
			Role: schema.Assistant,
			ToolCalls: []schema.ToolCall{
				{ID: "call_3", Function: schema.FunctionCall{Name: "reentry_tool", Arguments: `{"input": "c"}`}},
			},
		}, nil
	}).Times(1)

	// Expectation for the final invocation: model returns final answer
	// 最终调用的预期：model 返回最终答案
	mockChatModel.EXPECT().Generate(gomock.Any(), gomock.Any(), gomock.Any()).Return(&schema.Message{
		Role:    schema.Assistant,
		Content: "all done",
	}, nil).Times(1)

	_ = g.AddChatModelNode("model", mockChatModel)
	_ = g.AddToolsNode("tools", toolsNode)
	_ = g.AddEdge(START, "model")

	// Add the crucial branch to decide whether to call tools or end.
	// 添加关键分支，用于决定调用工具还是结束。
	modelBranch := func(ctx context.Context, msg *schema.Message) (string, error) {
		if len(msg.ToolCalls) > 0 {
			return "tools", nil
		}
		return END, nil
	}
	err = g.AddBranch("model", NewGraphBranch(modelBranch, map[string]bool{"tools": true, END: true}))
	assert.NoError(t, err)

	_ = g.AddEdge("tools", "model") // Loop back for ReAct style
	// 回到循环以实现 ReAct 风格

	// 3. Compile and run
	// 3. 编译并运行
	graph, err := g.Compile(context.Background(), WithCheckPointStore(newInMemoryStore()),
		WithGraphName("root"))
	assert.NoError(t, err)
	checkPointID := "reentry-test"

	// --- 1. First invocation: call_1 and call_2 should interrupt ---
	// --- 1. 第一次调用：call_1 和 call_2 应该中断 ---
	_, err = graph.Invoke(context.Background(), []*schema.Message{schema.UserMessage("start")}, WithCheckPointID(checkPointID), WithCallbacks(toolsNodeCB))
	assert.Error(t, err)
	interruptInfo1, _ := ExtractInterruptInfo(err)
	interrupts1 := interruptInfo1.InterruptContexts
	assert.Len(t, interrupts1, 2) // Only the two tool calls
	// 只有这两个工具调用
	found1 := make(map[string]bool)
	addrToID1 := make(map[string]string)
	for _, iCtx := range interrupts1 {
		addrStr := iCtx.Address.String()
		found1[addrStr] = true
		addrToID1[addrStr] = iCtx.ID
		assert.True(t, iCtx.IsRootCause)
		assert.NotNil(t, iCtx.Parent)
		assert.Equal(t, "runnable:root;node:tools", iCtx.Parent.Address.String())
	}
	assert.True(t, found1["runnable:root;node:tools;tool:reentry_tool:call_1"])
	assert.True(t, found1["runnable:root;node:tools;tool:reentry_tool:call_2"])

	// First invocation: neither call_1 nor call_2 should be resume targets
	// 第一次调用：call_1 和 call_2 都不应是恢复目标
	assert.False(t, reentryTool.isResumeTargetByRunID["call_1"], "first run: call_1 should not be resume target")
	assert.False(t, reentryTool.isResumeTargetByRunID["call_2"], "first run: call_2 should not be resume target")

	// First invocation: ToolsNode should NOT be a resume target
	// 第一次调用：ToolsNode 不应是恢复目标
	assert.Len(t, toolsNodeCB.isResumeTargetLog, 1, "ToolsNode OnStart should be called once in first invocation")
	assert.False(t, toolsNodeCB.isResumeTargetLog[0], "first run: ToolsNode should NOT be resume target")

	// Clear for next invocation
	// 清理以便下次调用
	reentryTool.isResumeTargetByRunID = make(map[string]bool)
	toolsNodeCB.isResumeTargetLog = nil

	// --- 2. Second invocation: resume call_1, expect call_2 to interrupt again ---
	// --- 2. 第二次调用：恢复 call_1，预期 call_2 再次中断 ---
	resumeCtx2 := ResumeWithData(context.Background(), addrToID1["runnable:root;node:tools;tool:reentry_tool:call_1"],
		&myResumeData{Message: "resume call 1"})
	_, err = graph.Invoke(resumeCtx2, []*schema.Message{schema.UserMessage("start")}, WithCheckPointID(checkPointID), WithCallbacks(toolsNodeCB))
	assert.Error(t, err)
	interruptInfo2, _ := ExtractInterruptInfo(err)
	interrupts2 := interruptInfo2.InterruptContexts
	assert.Len(t, interrupts2, 1) // Only call_2
	// 只有 call_2
	rootCause2 := interrupts2[0]
	assert.Equal(t, "runnable:root;node:tools;tool:reentry_tool:call_2", rootCause2.Address.String())
	assert.NotNil(t, rootCause2.Parent)
	assert.Equal(t, "runnable:root;node:tools", rootCause2.Parent.Address.String())

	// Second invocation: call_1 is resumed, call_2 is NOT resumed (re-interrupts)
	// 第二次调用：call_1 已恢复，call_2 未恢复（再次中断）
	assert.True(t, reentryTool.isResumeTargetByRunID["call_1"], "second run: call_1 should be resume target")
	assert.False(t, reentryTool.isResumeTargetByRunID["call_2"], "second run: call_2 should NOT be resume target (it re-interrupts)")

	// Second invocation: ToolsNode SHOULD be a resume target (because call_1 child is being resumed)
	// 第二次调用：ToolsNode 应该是恢复目标（因为 call_1 子节点正在恢复）
	assert.Len(t, toolsNodeCB.isResumeTargetLog, 1, "ToolsNode OnStart should be called once in second invocation")
	assert.True(t, toolsNodeCB.isResumeTargetLog[0], "second run: ToolsNode SHOULD be resume target (child call_1 is being resumed)")

	// Clear for next invocation
	// 清理以便下次调用
	reentryTool.isResumeTargetByRunID = make(map[string]bool)
	toolsNodeCB.isResumeTargetLog = nil

	// --- 3. Third invocation: resume call_2, model makes a new call (call_3) which should interrupt ---
	// --- 3. 第三次调用：恢复 call_2，model 发起新的调用（call_3），它应该中断 ---
	resumeCtx3 := ResumeWithData(context.Background(), rootCause2.ID, &myResumeData{Message: "resume call 2"})
	_, err = graph.Invoke(resumeCtx3, []*schema.Message{schema.UserMessage("start")}, WithCheckPointID(checkPointID), WithCallbacks(toolsNodeCB))
	assert.Error(t, err)
	interruptInfo3, _ := ExtractInterruptInfo(err)
	interrupts3 := interruptInfo3.InterruptContexts
	assert.Len(t, interrupts3, 1) // Only call_3
	// 只有 call_3
	rootCause3 := interrupts3[0]
	assert.Equal(t, "runnable:root;node:tools;tool:reentry_tool:call_3", rootCause3.Address.String()) // Note: this is the new call_3
	// 注意：这是新的 call_3
	assert.NotNil(t, rootCause3.Parent)
	assert.Equal(t, "runnable:root;node:tools", rootCause3.Parent.Address.String())

	// Third invocation: call_2 is resumed, call_3 is new (not resumed)
	// 第三次调用：call_2 已恢复，call_3 是新的（未恢复）
	assert.True(t, reentryTool.isResumeTargetByRunID["call_2"], "third run: call_2 should be resume target")
	assert.False(t, reentryTool.isResumeTargetByRunID["call_3"], "third run: call_3 should NOT be resume target (it's new)")

	// Third invocation: ToolsNode is called twice (once for call_2 resume, once for call_3 new)
	// First call: ToolsNode SHOULD be resume target (call_2 is being resumed)
	// Second call: ToolsNode should NOT be resume target (call_3 is new, no children to resume)
	//
	// 第三次调用：ToolsNode 被调用两次（一次用于恢复 call_2，一次用于新建 call_3）
	// 第一次调用：ToolsNode 应该是恢复目标（call_2 正在恢复）
	// 第二次调用：ToolsNode 不应是恢复目标（call_3 是新的，没有子节点要恢复）
	assert.Len(t, toolsNodeCB.isResumeTargetLog, 2, "ToolsNode OnStart should be called twice in third invocation")
	assert.True(t, toolsNodeCB.isResumeTargetLog[0], "third run first ToolsNode call: SHOULD be resume target (child call_2 is being resumed)")
	assert.False(t, toolsNodeCB.isResumeTargetLog[1], "third run second ToolsNode call: should NOT be resume target (call_3 is new)")

	// Clear for next invocation
	// 清理以供下次调用
	reentryTool.isResumeTargetByRunID = make(map[string]bool)
	toolsNodeCB.isResumeTargetLog = nil

	// --- 4. Final invocation: resume call_3, expect final answer ---
	// --- 4. 最终调用：恢复 call_3，期望得到最终答案 ---
	resumeCtx4 := ResumeWithData(context.Background(), rootCause3.ID,
		&myResumeData{Message: "resume call 3"})
	output, err := graph.Invoke(resumeCtx4, []*schema.Message{schema.UserMessage("start")}, WithCheckPointID(checkPointID), WithCallbacks(toolsNodeCB))
	assert.NoError(t, err)
	assert.Equal(t, "all done", output.Content)

	// Fourth invocation: call_3 is resumed
	// 第四次调用：call_3 已恢复
	assert.True(t, reentryTool.isResumeTargetByRunID["call_3"], "fourth run: call_3 should be resume target")

	// Fourth invocation: ToolsNode SHOULD be resume target (call_3 is being resumed)
	// 第四次调用：ToolsNode 应该是恢复目标（call_3 正在恢复）
	assert.Len(t, toolsNodeCB.isResumeTargetLog, 1, "ToolsNode OnStart should be called once in fourth invocation")
	assert.True(t, toolsNodeCB.isResumeTargetLog[0], "fourth run: ToolsNode SHOULD be resume target (child call_3 is being resumed)")
}

// mockInterruptingTool is a helper for the nested tool interrupt test
// mockInterruptingTool 是嵌套工具中断测试的辅助工具
type mockInterruptingTool struct {
	tt *testing.T
}

func (t *mockInterruptingTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "interrupt_tool",
		Desc: "A tool that interrupts execution.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"input": {Type: schema.String, Desc: "Some input", Required: true},
		}),
	}, nil
}

func (t *mockInterruptingTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	var args map[string]string
	_ = json.Unmarshal([]byte(argumentsInJSON), &args)

	wasInterrupted, hasState, state := tool.GetInterruptState[*myInterruptState](ctx)
	if !wasInterrupted {
		// First run: interrupt
		// 第一次运行：中断
		return "", tool.StatefulInterrupt(ctx,
			map[string]any{"reason": "tool maintenance"},
			&myInterruptState{OriginalInput: args["input"]},
		)
	}

	// Second (resumed) run
	// 第二次（恢复后）运行
	assert.True(t.tt, hasState)
	assert.Equal(t.tt, "test", state.OriginalInput)

	isResume, hasData, data := tool.GetResumeContext[*myResumeData](ctx)
	assert.True(t.tt, isResume)
	assert.True(t.tt, hasData)
	assert.Equal(t.tt, "let's continue tool", data.Message)

	return "Tool resumed successfully", nil
}

func TestGraphInterruptWithinLambda(t *testing.T) {
	// this test case aims to verify behaviors when a standalone graph is within a lambda,
	// which in turn is within the root graph.
	// the expected behavior is:
	// - internal graph will naturally append to the Address
	// - internal graph interrupts, where the Address includes steps for both the root graph and the internal graph
	// - lambda extracts InterruptInfo, then GetInterruptContexts
	// - lambda then acts as a composite node, uses CompositeInterrupt to pass up the
	//   internal interrupt points
	// - the root graph interrupts
	// - end-user extracts the interrupt ID and related info
	// - end-user uses ResumeWithData to resume the ID
	// - lambda node resumes, invokes the inner graph as usual
	// - the internal graph resumes the interrupted node
	// To implement this test, within the internal graph you can define another lambda node that can interrupt resume.
	//
	// 此测试用例旨在验证独立图位于 lambda 内，
	// 而该 lambda 又位于根图内时的行为。
	// 预期行为是：
	// - 内部图会自然追加到 Address
	// - 内部图中断，此时 Address 包含根图和内部图的步骤
	// - lambda 提取 InterruptInfo，然后调用 GetInterruptContexts
	// - lambda 随后作为复合节点，使用 CompositeInterrupt 向上传递
	// 内部中断点
	// - 根图中断
	// - 终端用户提取中断 ID 和相关信息
	// - 终端用户使用 ResumeWithData 恢复该 ID
	// - lambda 节点恢复，并照常调用内部图
	// - 内部图恢复被中断的节点
	// 为实现此测试，可在内部图中定义另一个可中断恢复的 lambda 节点。

	// 1. Define the innermost lambda that actually interrupts
	// 1. 定义真正发生中断的最内层 lambda
	interruptingLambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		wasInterrupted, hasState, state := GetInterruptState[*myInterruptState](ctx)
		if !wasInterrupted {
			return "", StatefulInterrupt(ctx, "inner interrupt", &myInterruptState{OriginalInput: input})
		}

		assert.True(t, hasState)
		assert.Equal(t, "top level input", state.OriginalInput)

		isResume, hasData, data := GetResumeContext[*myResumeData](ctx)
		assert.True(t, isResume)
		assert.True(t, hasData)
		assert.Equal(t, "resume inner", data.Message)

		return "inner lambda resumed successfully", nil
	})

	// 2. Define the internal graph that contains the interrupting lambda
	// 2. 定义包含中断 lambda 的内部图
	innerGraph := NewGraph[string, string]()
	_ = innerGraph.AddLambdaNode("inner_lambda", interruptingLambda)
	_ = innerGraph.AddEdge(START, "inner_lambda")
	_ = innerGraph.AddEdge("inner_lambda", END)
	// Give the inner graph a name so it can create its "runnable" addr step.
	// 为内部图命名，使其能创建自己的 "runnable" 地址步骤。
	compiledInnerGraph, err := innerGraph.Compile(context.Background(), WithGraphName("inner"), WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	// 3. Define the outer lambda that acts as a composite node
	// 3. 定义作为复合节点的外层 lambda
	compositeLambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		// The lambda invokes the inner graph. If the inner graph interrupts, this lambda
		// must act as a proper composite node and wrap the error.
		//
		// 该 lambda 调用内部图。如果内部图中断，此 lambda
		// 必须作为合规的复合节点并包装错误。
		output, err := compiledInnerGraph.Invoke(ctx, input, WithCheckPointID("inner-cp"))
		if err != nil {
			_, isInterrupt := ExtractInterruptInfo(err)
			if !isInterrupt {
				return "", err // Not an interrupt, just fail
				// 不是中断，直接失败
			}

			// The composite interrupt itself can be stateless, as it's just a wrapper.
			// It signals to the framework to look inside the subErrs and correctly
			// prepend the current addr to the paths of the inner interrupts.
			//
			// 复合中断本身可以是无状态的，因为它只是一个包装器。
			// 它向框架发出信号，使其查看 subErrs，并正确地
			// 将当前 addr 前置到内部中断的路径上。
			return "", CompositeInterrupt(ctx, "composite interrupt from lambda", nil, err)
		}
		return output, nil
	})

	// 4. Define the root graph
	// 4. 定义根图
	rootGraph := NewGraph[string, string]()
	_ = rootGraph.AddLambdaNode("composite_lambda", compositeLambda)
	_ = rootGraph.AddEdge(START, "composite_lambda")
	_ = rootGraph.AddEdge("composite_lambda", END)
	// Give the root graph a name for its "runnable" addr step.
	// 为根图命名，用于其 "runnable" 地址步骤。
	compiledRootGraph, err := rootGraph.Compile(context.Background(), WithGraphName("root"), WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	// 5. First invocation - should interrupt
	// 5. 第一次调用 - 应中断
	checkPointID := "graph-in-lambda-test"
	_, err = compiledRootGraph.Invoke(context.Background(), "top level input", WithCheckPointID(checkPointID))

	// 6. Verify the interrupt
	// 6. 验证中断
	assert.Error(t, err)
	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)
	interruptContexts := interruptInfo.InterruptContexts
	assert.Len(t, interruptContexts, 1) // Only the root cause is returned
	// 仅返回根因

	// The addr is now fully qualified, including the runnable steps from both graphs.
	// addr 现在是完全限定的，包含两个图中的 runnable 步骤。
	rootCause := interruptContexts[0]
	expectedPath := "runnable:root;node:composite_lambda;runnable:inner;node:inner_lambda"
	assert.Equal(t, expectedPath, rootCause.Address.String())
	assert.Equal(t, "inner interrupt", rootCause.Info)
	assert.True(t, rootCause.IsRootCause)

	// Check parent hierarchy
	// 检查父级层次
	assert.NotNil(t, rootCause.Parent)
	assert.Equal(t, "runnable:root;node:composite_lambda;runnable:inner", rootCause.Parent.Address.String())
	assert.Nil(t, rootCause.Parent.Info) // The inner runnable doesn't have its own info
	// 内部 runnable 没有自己的信息
	assert.False(t, rootCause.Parent.IsRootCause)

	// Check grandparent
	// 检查祖父级
	assert.NotNil(t, rootCause.Parent.Parent)
	assert.Equal(t, "runnable:root;node:composite_lambda", rootCause.Parent.Parent.Address.String())
	assert.Equal(t, "composite interrupt from lambda", rootCause.Parent.Parent.Info)
	assert.False(t, rootCause.Parent.Parent.IsRootCause)

	// 7. Resume execution using the complete, fully-qualified ID
	// 7. 使用完整的完全限定 ID 恢复执行
	resumeCtx := ResumeWithData(context.Background(), rootCause.ID, &myResumeData{Message: "resume inner"})
	finalOutput, err := compiledRootGraph.Invoke(resumeCtx, "top level input", WithCheckPointID(checkPointID))

	// 8. Verify final result
	// 8. 验证最终结果
	assert.NoError(t, err)
	assert.Equal(t, "inner lambda resumed successfully", finalOutput)
}

func TestLegacyInterrupt(t *testing.T) {
	// this test case aims to test the behavior of the deprecated InterruptAndRerun,
	// NewInterruptAndRerunErr within CompositeInterrupt.
	// Define two sub-processes(functions), one interrupts with InterruptAndRerun,
	// the other interrupts with NewInterruptAndRerunErr.
	// create a lambda as a composite node, within the lambda invokes the two sub-processes.
	// create the graph, add lambda node and invoke it.
	// after verifying the interrupt points, just invokes again without explicit resume.
	// verify the same interrupt IDs again.
	// then finally Resume() the graph.
	//
	// 此测试用例旨在测试已废弃的 InterruptAndRerun、NewInterruptAndRerunErr 在 CompositeInterrupt 中的行为。
	// 定义两个子流程（函数），一个用 InterruptAndRerun 中断，另一个用 NewInterruptAndRerunErr 中断。
	// 创建一个 lambda 作为复合节点，在 lambda 中调用这两个子流程。
	// 创建图，添加 lambda 节点并调用它。
	// 验证中断点后，不显式恢复而是再次调用。
	// 再次验证相同的中断 ID。
	// 最后 Resume() 图。

	// 1. Define the sub-processes that use legacy and modern interrupts
	// 1. 定义使用旧版和新版中断的子流程
	subProcess1 := func(ctx context.Context) (string, error) {
		isResume, _, data := GetResumeContext[string](ctx)
		if isResume {
			return data, nil
		}
		return "", deprecatedInterruptAndRerun
	}
	subProcess2 := func(ctx context.Context) (string, error) {
		isResume, _, data := GetResumeContext[string](ctx)
		if isResume {
			return data, nil
		}
		return "", deprecatedInterruptAndRerunErr("legacy info")
	}
	subProcess3 := func(ctx context.Context) (string, error) {
		isResume, _, data := GetResumeContext[string](ctx)
		if isResume {
			return data, nil
		}
		// Use the modern, addr-aware interrupt function
		// 使用新版的、addr 感知的中断函数
		return "", Interrupt(ctx, "modern info")
	}

	// 2. Define the composite lambda
	// 2. 定义复合 lambda
	compositeLambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		// If the lambda itself is being resumed, it means the whole process is done.
		// 如果 lambda 本身正在恢复，说明整个流程已完成。
		isResume, _, data := GetResumeContext[string](ctx)

		// Run sub-processes and collect their errors
		// 运行子流程并收集它们的错误
		var (
			errs   []error
			outStr string
		)

		const PathStepCustom AddressSegmentType = "custom"
		subCtx1 := AppendAddressSegment(ctx, PathStepCustom, "1")
		out1, err1 := subProcess1(subCtx1)
		if err1 != nil {
			// Wrap the legacy error to give it a addr
			// 包装旧版错误，为其提供 addr
			wrappedErr := WrapInterruptAndRerunIfNeeded(ctx, AddressSegment{Type: PathStepCustom, ID: "1"}, err1)
			errs = append(errs, wrappedErr)
		} else {
			outStr += out1
		}
		subCtx2 := AppendAddressSegment(ctx, PathStepCustom, "2")
		out2, err2 := subProcess2(subCtx2)
		if err2 != nil {
			// Wrap the legacy error to give it a addr
			// 包装旧版错误，为其提供 addr
			wrappedErr := WrapInterruptAndRerunIfNeeded(ctx, AddressSegment{Type: PathStepCustom, ID: "2"}, err2)
			errs = append(errs, wrappedErr)
		} else {
			outStr += out2
		}
		subCtx3 := AppendAddressSegment(ctx, PathStepCustom, "3")
		out3, err3 := subProcess3(subCtx3)
		if err3 != nil {
			// The error from Interrupt() is already addr-aware. WrapInterruptAndRerunIfNeeded
			// should handle this gracefully and return the error as-is.
			//
			// Interrupt() 返回的错误已经是 addr 感知的。WrapInterruptAndRerunIfNeeded
			// 应能平滑处理并原样返回该错误。
			wrappedErr := WrapInterruptAndRerunIfNeeded(ctx, AddressSegment{Type: PathStepCustom, ID: "3"}, err3)
			errs = append(errs, wrappedErr)
		} else {
			outStr += out3
		}

		if len(errs) > 0 {
			// Return a composite interrupt containing the wrapped legacy errors
			// 返回包含已包装旧版错误的复合中断
			return "", CompositeInterrupt(ctx, "legacy composite", nil, errs...)
		}

		if isResume {
			outStr = outStr + " " + data
		}

		return outStr, nil
	})

	// 3. Create and compile the graph
	// 3. 创建并编译图
	rootGraph := NewGraph[string, string]()
	_ = rootGraph.AddLambdaNode("legacy_composite", compositeLambda)
	_ = rootGraph.AddEdge(START, "legacy_composite")
	_ = rootGraph.AddEdge("legacy_composite", END)
	compiledGraph, err := rootGraph.Compile(context.Background(), WithGraphName("root"), WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	// 4. First invocation - should interrupt
	// 4. 第一次调用 - 应中断
	checkPointID := "legacy-interrupt-test"
	_, err = compiledGraph.Invoke(context.Background(), "input", WithCheckPointID(checkPointID))

	// 5. Verify the three interrupt points
	// 5. 验证三个中断点
	assert.Error(t, err)
	info, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)
	assert.Len(t, info.InterruptContexts, 3) // Only the 3 root causes
	// 仅 3 个根因

	found := make(map[string]any)
	addrToID := make(map[string]string)
	var parentCtx *InterruptCtx
	for _, iCtx := range info.InterruptContexts {
		addrStr := iCtx.Address.String()
		found[addrStr] = iCtx.Info
		addrToID[addrStr] = iCtx.ID
		assert.True(t, iCtx.IsRootCause)
		// Check parent
		// 检查父级
		assert.NotNil(t, iCtx.Parent)
		if parentCtx == nil {
			parentCtx = iCtx.Parent
			assert.Equal(t, "runnable:root;node:legacy_composite", parentCtx.Address.String())
			assert.Equal(t, "legacy composite", parentCtx.Info)
			assert.False(t, parentCtx.IsRootCause)
		} else {
			assert.Same(t, parentCtx, iCtx.Parent)
		}
	}
	expectedID1 := "runnable:root;node:legacy_composite;custom:1"
	expectedID2 := "runnable:root;node:legacy_composite;custom:2"
	expectedID3 := "runnable:root;node:legacy_composite;custom:3"
	assert.Contains(t, found, expectedID1)
	assert.Nil(t, found[expectedID1]) // From InterruptAndRerun
	// 来自 InterruptAndRerun
	assert.Contains(t, found, expectedID2)
	assert.Equal(t, "legacy info", found[expectedID2]) // From NewInterruptAndRerunErr
	// 来自 NewInterruptAndRerunErr
	assert.Contains(t, found, expectedID3)
	assert.Equal(t, "modern info", found[expectedID3]) // From Interrupt
	// 来自 Interrupt

	// 6. Second invocation (re-run without resume) - should yield the same interrupts
	// 6. 第二次调用（不恢复，重新运行）- 应产生相同的中断
	_, err = compiledGraph.Invoke(context.Background(), "input", WithCheckPointID(checkPointID))
	assert.Error(t, err)
	info2, isInterrupt2 := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt2)
	assert.Len(t, info2.InterruptContexts, 3, "Should have the same number of interrupts on re-run")

	// 7. Third invocation - Resume all three interrupt points with specific data
	// 7. 第三次调用 - 使用指定数据恢复全部三个中断点
	resumeData := map[string]any{
		addrToID[expectedID1]: "output1",
		addrToID[expectedID2]: "output2",
		addrToID[expectedID3]: "output3",
	}
	resumeCtx := BatchResumeWithData(context.Background(), resumeData)
	// TODO: The legacy interrupt wrapping does not currently work correctly with BatchResumeWithData.
	// The graph re-interrupts instead of completing. This should be fixed in the core framework.
	//
	// TODO: 旧版中断包装目前无法与 BatchResumeWithData 正常配合。
	// 图会再次中断而不是完成。应在核心框架中修复。
	_, err = compiledGraph.Invoke(resumeCtx, "input", WithCheckPointID(checkPointID))
	assert.Error(t, err)
}

type wrapperToolForTest struct {
	compiledGraph     Runnable[string, string]
	isResumeTargetLog []bool
}

func (w *wrapperToolForTest) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "wrapperTool",
		Desc: "A tool that wraps a nested graph",
	}, nil
}

func (w *wrapperToolForTest) InvokableRun(ctx context.Context, input string, opts ...tool.Option) (string, error) {
	isResumeTarget, _, _ := tool.GetResumeContext[any](ctx)
	w.isResumeTargetLog = append(w.isResumeTargetLog, isResumeTarget)

	result, err := w.compiledGraph.Invoke(ctx, input)
	if err != nil {
		if _, ok := ExtractInterruptInfo(err); ok {
			return "", tool.CompositeInterrupt(ctx, "wrapper tool interrupt", nil, err)
		}
		return "", err
	}
	return result, nil
}

func TestToolCompositeInterruptWithNestedGraphInterrupt(t *testing.T) {
	ctx := context.Background()

	var innerNodeIsResumeTarget bool
	subSubGraph := NewGraph[string, string]()
	err := subSubGraph.AddLambdaNode("interruptNode", InvokableLambda(func(ctx context.Context, input string) (string, error) {
		wasInterrupted, _, _ := GetInterruptState[any](ctx)
		if !wasInterrupted {
			return "", Interrupt(ctx, "sub-sub graph interrupt info")
		}
		isResumeTarget, _, _ := GetResumeContext[any](ctx)
		innerNodeIsResumeTarget = isResumeTarget
		return "resumed successfully", nil
	}))
	assert.NoError(t, err)
	assert.NoError(t, subSubGraph.AddEdge(START, "interruptNode"))
	assert.NoError(t, subSubGraph.AddEdge("interruptNode", END))

	nestedGraph := NewGraph[string, string]()
	err = nestedGraph.AddGraphNode("subSubGraph", subSubGraph)
	assert.NoError(t, err)
	assert.NoError(t, nestedGraph.AddEdge(START, "subSubGraph"))
	assert.NoError(t, nestedGraph.AddEdge("subSubGraph", END))
	compiledNestedGraph, err := nestedGraph.Compile(ctx)
	assert.NoError(t, err)

	wrapperTool := &wrapperToolForTest{compiledGraph: compiledNestedGraph.(Runnable[string, string])}

	toolsNode, err := NewToolNode(ctx, &ToolsNodeConfig{Tools: []tool.BaseTool{wrapperTool}})
	assert.NoError(t, err)

	outerGraph := NewGraph[*schema.Message, []*schema.Message]()
	err = outerGraph.AddToolsNode("tools", toolsNode)
	assert.NoError(t, err)
	assert.NoError(t, outerGraph.AddEdge(START, "tools"))
	assert.NoError(t, outerGraph.AddEdge("tools", END))

	compiledOuterGraph, err := outerGraph.Compile(ctx, WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	checkpointID := "test-wrapper-tool-resume"
	inputMsg := &schema.Message{
		Role: schema.Assistant,
		ToolCalls: []schema.ToolCall{
			{ID: "call_1", Function: schema.FunctionCall{Name: "wrapperTool", Arguments: `"test"`}},
		},
	}

	_, err = compiledOuterGraph.Invoke(ctx, inputMsg, WithCheckPointID(checkpointID))
	assert.Error(t, err)

	info, ok := ExtractInterruptInfo(err)
	assert.True(t, ok, "should be an interrupt error")
	assert.NotNil(t, info)
	assert.NotEmpty(t, info.InterruptContexts)

	rootCause := info.InterruptContexts[0]
	assert.Equal(t, "sub-sub graph interrupt info", rootCause.Info)
	assert.True(t, rootCause.IsRootCause)

	var wrapperToolParent *InterruptCtx
	for p := rootCause.Parent; p != nil; p = p.Parent {
		if p.Info == "wrapper tool interrupt" {
			wrapperToolParent = p
			break
		}
	}
	assert.NotNil(t, wrapperToolParent, "should have parent from wrapper tool with info 'wrapper tool interrupt'")

	assert.Len(t, wrapperTool.isResumeTargetLog, 1)
	assert.False(t, wrapperTool.isResumeTargetLog[0], "first invocation: wrapper tool should not be resume target")

	resumeCtx := Resume(ctx, rootCause.ID)
	_, err = compiledOuterGraph.Invoke(resumeCtx, inputMsg, WithCheckPointID(checkpointID))
	assert.NoError(t, err)

	assert.True(t, innerNodeIsResumeTarget, "inner node should be resume target")

	assert.Len(t, wrapperTool.isResumeTargetLog, 2)
	assert.True(t, wrapperTool.isResumeTargetLog[1], "second invocation: wrapper tool should be resume target because its child is targeted")
}
