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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// --- helpers shared across edge-case tests ---
// --- 边界场景测试共享的辅助项 ---

// blockingChatModel blocks until unblockCh is closed, then returns a fixed response.
// blockingChatModel 会阻塞直到 unblockCh 被关闭，然后返回固定响应。
type blockingChatModel struct {
	unblockCh chan struct{}
	response  *schema.Message
	started   chan struct{}
	callCount int32
}

func newBlockingChatModel(response *schema.Message) *blockingChatModel {
	return &blockingChatModel{
		unblockCh: make(chan struct{}),
		response:  response,
		started:   make(chan struct{}, 1),
	}
}

func (m *blockingChatModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	atomic.AddInt32(&m.callCount, 1)
	select {
	case m.started <- struct{}{}:
	default:
	}
	<-m.unblockCh
	return m.response, nil
}

func (m *blockingChatModel) Stream(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	atomic.AddInt32(&m.callCount, 1)
	select {
	case m.started <- struct{}{}:
	default:
	}
	<-m.unblockCh
	return schema.StreamReaderFromArray([]*schema.Message{m.response}), nil
}

func (m *blockingChatModel) BindTools(_ []*schema.ToolInfo) error { return nil }

// errorChatModel returns an error from Generate/Stream.
// errorChatModel 从 Generate/Stream 返回错误。
type errorChatModel struct {
	err     error
	started chan struct{}
}

func (m *errorChatModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if m.started != nil {
		select {
		case m.started <- struct{}{}:
		default:
		}
	}
	return nil, m.err
}

func (m *errorChatModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, m.err
}

func (m *errorChatModel) BindTools(_ []*schema.ToolInfo) error { return nil }

// plainResponseModel returns immediately with a fixed text response (no tool calls).
// plainResponseModel 立即返回固定文本响应（无工具调用）。
type plainResponseModel struct {
	text string
}

func (m *plainResponseModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return schema.AssistantMessage(m.text, nil), nil
}

func (m *plainResponseModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{schema.AssistantMessage(m.text, nil)}), nil
}

func (m *plainResponseModel) BindTools(_ []*schema.ToolInfo) error { return nil }

// blockingTool blocks until unblockCh is closed.
// blockingTool 会阻塞直到 unblockCh 被关闭。
type blockingTool struct {
	name      string
	unblockCh chan struct{}
	started   chan struct{}
	callCount int32
}

func newBlockingTool(name string) *blockingTool {
	return &blockingTool{
		name:      name,
		unblockCh: make(chan struct{}),
		started:   make(chan struct{}, 4),
	}
}

func (t *blockingTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: t.name,
		Desc: "blocking tool",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"input": {Type: "string"},
		}),
	}, nil
}

func (t *blockingTool) InvokableRun(_ context.Context, _ string, _ ...tool.Option) (string, error) {
	atomic.AddInt32(&t.callCount, 1)
	select {
	case t.started <- struct{}{}:
	default:
	}
	<-t.unblockCh
	return "result", nil
}

func toolCallMsg(calls ...schema.ToolCall) *schema.Message {
	return &schema.Message{Role: schema.Assistant, ToolCalls: calls}
}

func toolCall(id, name, args string) schema.ToolCall {
	return schema.ToolCall{ID: id, Type: "function", Function: schema.FunctionCall{Name: name, Arguments: args}}
}

func drainEvents(iter *AsyncIterator[*AgentEvent]) ([]*AgentEvent, bool) {
	var events []*AgentEvent
	hasCancelError := false
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		events = append(events, e)
		var ce *CancelError
		if e.Err != nil && errors.As(e.Err, &ce) {
			hasCancelError = true
		}
	}
	return events, hasCancelError
}

// --- tests ---
// --- 测试 ---

// TestWithCancel_BeforeExecutionStarts verifies that a cancel issued before
// the graph begins executing still produces a CancelError without invoking
// the model or tools.
//
// TestWithCancel_BeforeExecutionStarts 验证在图开始执行前发出的取消，仍会产生 CancelError，且不会调用模型或工具。
func TestWithCancel_BeforeExecutionStarts(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(toolCallMsg(toolCall("c1", "bt", `{"input":"x"}`)))
	bt := newBlockingTool("bt")

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{bt}},
		},
	})
	assert.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()

	// Extract the cancelContext so we can wait for cancelChan to close,
	// ensuring the cancel is fully registered before Run starts.
	//
	// 提取 cancelContext，以便等待 cancelChan 关闭，确保在 Run 启动前取消已完全注册。
	cc := getCommonOptions(nil, cancelOpt).cancelCtx

	// Call cancel BEFORE calling agent.Run.
	// The cancelFunc must succeed (not hang) even though execution hasn't started.
	//
	// 在调用 agent.Run 之前调用 cancel。
	// 即使执行尚未开始，cancelFunc 也必须成功（不挂起）。
	cancelDone := make(chan error, 1)
	go func() {
		handle, _ := cancelFn()
		cancelDone <- handle.Wait()
	}()

	// Wait for cancelChan to close so the pre-execution check in runFunc
	// deterministically sees shouldCancel()=true (eliminates goroutine scheduling race).
	//
	// 等待 cancelChan 关闭，使 runFunc 中的执行前检查确定性地看到 shouldCancel()=true（消除 goroutine 调度竞争）。
	<-cc.cancelChan

	// Now start the run — it should see shouldCancel()=true and emit CancelError immediately.
	// 现在启动运行——它应看到 shouldCancel()=true 并立即发出 CancelError。
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, cancelOpt)

	_, hasCancelError := drainEvents(iter)
	assert.True(t, hasCancelError, "expected CancelError when cancel precedes execution")

	// cancelFn must have already returned (or return quickly now that doneChan is closed).
	// cancelFn 必须已经返回（或在 doneChan 关闭后很快返回）。
	select {
	case cancelErr := <-cancelDone:
		// Either nil (cancel handled) or ErrExecutionEnded is acceptable
		// depending on exact timing; what matters is it didn't hang.
		//
		// nil（已处理取消）或 ErrExecutionEnded 都可以，
		// 取决于具体时序；关键是不能挂起。
		_ = cancelErr
	case <-time.After(3 * time.Second):
		t.Fatal("cancelFn blocked indefinitely after pre-start cancel")
	}

	// Model and tool must not have been invoked.
	// Model 和 tool 不应被调用。
	assert.Equal(t, int32(0), atomic.LoadInt32(&bt.callCount), "tool must not be called")
}

// TestWithCancel_AfterCompletion verifies cancelFn returns ErrExecutionEnded
// when called after a normal run finishes.
//
// TestWithCancel_AfterCompletion 验证正常运行结束后调用 cancelFn 时会返回 ErrExecutionEnded。
func TestWithCancel_AfterCompletion(t *testing.T) {
	ctx := context.Background()

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       &plainResponseModel{text: "done"},
	})
	require.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, cancelOpt)

	// Drain all events so the run completes.
	// 消费所有事件，让运行完成。
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}

	handle, _ := cancelFn()
	cancelErr := handle.Wait()
	assert.ErrorIs(t, cancelErr, ErrExecutionEnded)
}

// TestWithCancel_DerivedAgentToolCancelContextMarkedDoneAfterRun verifies that
// an explicitly derived AgentTool child cancel context is owned by the child run,
// even when the Go context also carries the parent cancel context.
//
// TestWithCancel_DerivedAgentToolCancelContextMarkedDoneAfterRun 验证显式派生的 AgentTool 子取消 context 由子运行拥有，
// 即使 Go context 也携带父取消 context。
func TestWithCancel_DerivedAgentToolCancelContextMarkedDoneAfterRun(t *testing.T) {
	ctx := context.Background()

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "ChildAgent",
		Description: "test child agent",
		Model:       &plainResponseModel{text: "done"},
	})
	require.NoError(t, err)

	parent := newCancelContext()
	parentCtx := withCancelContext(ctx, parent)
	child := parent.deriveAgentToolCancelContext(parentCtx)

	childOpt := WrapImplSpecificOptFn(func(o *options) {
		o.cancelCtx = child
	})
	iter := agent.Run(parentCtx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, childOpt)
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}

	select {
	case <-child.doneChan:
	case <-time.After(time.Second):
		t.Fatal("derived AgentTool cancel context was not marked done after child run completion")
	}
}

// TestWithCancel_AfterBusinessInterrupt verifies cancelFn returns ErrExecutionEnded
// when called after the agent has been interrupted by business logic.
//
// TestWithCancel_AfterBusinessInterrupt 验证智能体被业务逻辑中断后调用 cancelFn 时会返回 ErrExecutionEnded。
func TestWithCancel_AfterBusinessInterrupt(t *testing.T) {
	ctx := context.Background()

	// Use a model that triggers a compose.Interrupt so the agent stops with an interrupt.
	// 使用会触发 compose.Interrupt 的 model，让智能体因中断而停止。
	interruptModel := &interruptingChatModel{}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       interruptModel,
	})
	require.NoError(t, err)

	store := newCancelTestStore()
	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agent,
		CheckPointStore: store,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("hi")}, cancelOpt, WithCheckPointID("biz-interrupt-1"))

	// Drain — expect an interrupt action event, no cancel error.
	// 消费事件——预期有 interrupt action event，没有 cancel error。
	var gotInterrupt bool
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		if e.Action != nil && e.Action.Interrupted != nil {
			gotInterrupt = true
		}
	}
	assert.True(t, gotInterrupt, "expected business interrupt event")

	handle, _ := cancelFn()
	cancelErr := handle.Wait()
	assert.ErrorIs(t, cancelErr, ErrExecutionEnded)
}

// TestWithCancel_AfterError verifies cancelFn returns ErrExecutionEnded
// when called after the agent errors out.
//
// TestWithCancel_AfterError 验证智能体出错后调用 cancelFn 时会返回 ErrExecutionEnded。
func TestWithCancel_AfterError(t *testing.T) {
	ctx := context.Background()

	modelErr := errors.New("model exploded")
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       &errorChatModel{err: modelErr},
	})
	require.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, cancelOpt)

	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}

	handle, _ := cancelFn()
	cancelErr := handle.Wait()
	assert.ErrorIs(t, cancelErr, ErrExecutionEnded)
}

// TestWithCancel_TimeoutEscalation tests that WithAgentCancelTimeout causes the
// cancel to escalate to immediate when the safe-point hasn't fired yet, and
// that the resulting CancelError has Escalated=true.
//
// Strategy: use CancelAfterChatModel mode. The model blocks (never completes),
// so the safe-point can't fire naturally. After the timeout, escalateToImmediate
// closes immediateChan which aborts the model stream via cancelMonitoredModel
// and causes a CancelError — no compose graph-interrupt races involved.
//
// TestWithCancel_TimeoutEscalation 测试 WithAgentCancelTimeout 会在 safe-point 尚未触发时将取消升级为 immediate，
// 并且生成的 CancelError 的 Escalated=true。
// 策略：使用 CancelAfterChatModel 模式。model 会阻塞（永不完成），
// 因此 safe-point 无法自然触发。超时后，escalateToImmediate
// 会关闭 immediateChan，通过 cancelMonitoredModel 中止 model stream，
// 并导致 CancelError——不涉及 compose graph-interrupt 竞争。
func TestWithCancel_TimeoutEscalation(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(schema.AssistantMessage("hello", nil))

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
	})
	require.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agent,
		EnableStreaming: true, // use streaming so cancelMonitoredModel.Stream is exercised
		// 使用流式，以覆盖 cancelMonitoredModel.Stream
	})

	timeout := 300 * time.Millisecond
	// CancelAfterChatModel + timeout: safe-point can't fire (model never finishes),
	// so after 300ms the timeout goroutine escalates to immediate.
	//
	// CancelAfterChatModel + timeout：safe-point 无法触发（model 永不结束），
	// 因此 300ms 后 timeout goroutine 会升级为 immediate。
	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("go")}, cancelOpt)

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}

	// Fire cancelFn; it will wait for escalation to complete.
	// 触发 cancelFn；它会等待升级完成。
	start := time.Now()
	handle, _ := cancelFn(WithAgentCancelMode(CancelAfterChatModel), WithAgentCancelTimeout(timeout))
	cancelErr := handle.Wait()
	elapsed := time.Since(start)

	assert.ErrorIs(t, cancelErr, ErrCancelTimeout, "cancel should return ErrCancelTimeout after timeout escalation")
	assert.True(t, elapsed >= timeout, "should wait at least the timeout duration, elapsed=%v", elapsed)
	assert.True(t, elapsed < 3*time.Second, "should complete shortly after timeout, elapsed=%v", elapsed)

	var cancelError *CancelError
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		var ce *CancelError
		if e.Err != nil && errors.As(e.Err, &ce) {
			cancelError = ce
		}
	}
	if assert.NotNil(t, cancelError, "expected CancelError after timeout escalation") {
		assert.True(t, cancelError.Info.Escalated, "CancelError should report Escalated=true")
		assert.True(t, cancelError.Info.Timeout, "CancelError should report Timeout=true")
	}
}

// TestWithCancel_AfterChatModel_WithTools verifies CancelAfterChatModel fires
// when the model returns tool calls (the safe-point is on the tool-calls path).
//
// TestWithCancel_AfterChatModel_WithTools 验证当 model 返回 tool calls 时 CancelAfterChatModel 会触发（safe-point 在 tool-calls 路径上）。
func TestWithCancel_AfterChatModel_WithTools(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(toolCallMsg(toolCall("c1", "bt", `{"input":"x"}`)))
	bt := newBlockingTool("bt")

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{bt}},
		},
	})
	require.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, cancelOpt)

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}

	cancelDone := make(chan error, 1)
	go func() {
		handle, _ := cancelFn(WithAgentCancelMode(CancelAfterChatModel))
		cancelDone <- handle.Wait()
	}()

	time.Sleep(20 * time.Millisecond)

	close(blk.unblockCh)

	cancelErr := <-cancelDone
	assert.NoError(t, cancelErr)

	_, hasCancelError := drainEvents(iter)
	assert.True(t, hasCancelError, "CancelError expected after model returns tool calls")
}

// TestWithCancel_CancelImmediate_StreamAborted verifies that CancelImmediate
// during model execution surfaces CancelError and completes quickly.
// Uses blockingChatModel which blocks in Stream(), keeping the agent's run
// function alive so the cancel context stays in stateRunning.
//
// TestWithCancel_CancelImmediate_StreamAborted 验证在 model 执行期间 CancelImmediate 会暴露 CancelError 并快速完成。
// 使用 blockingChatModel，它会在 Stream() 中阻塞，使智能体的 run function 保持存活，从而让 cancel context 维持 stateRunning。
func TestWithCancel_CancelImmediate_StreamAborted(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(schema.AssistantMessage("hello", nil))

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
	})
	require.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("hi")}, cancelOpt)

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	handle, _ := cancelFn()
	cancelErr := handle.Wait()
	assert.NoError(t, cancelErr)
	elapsed := time.Since(start)
	assert.True(t, elapsed < 2*time.Second, "cancel should complete quickly, elapsed=%v", elapsed)

	var foundCancelError bool
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		if e.Action != nil && e.Action.Interrupted != nil {
			foundCancelError = true
		}
		var ce *CancelError
		if e.Err != nil && errors.As(e.Err, &ce) {
			foundCancelError = true
		}
	}
	assert.True(t, foundCancelError, "expected CancelError in event stream")
}

// TestWithCancel_MultipleToolsConcurrent verifies that CancelAfterToolCalls
// waits for ALL concurrent tool calls to complete before cancelling.
//
// TestWithCancel_MultipleToolsConcurrent 验证 CancelAfterToolCalls 会等待所有并发 tool calls 完成后再取消。
func TestWithCancel_MultipleToolsConcurrent(t *testing.T) {
	ctx := context.Background()

	bt1 := newBlockingTool("tool1")
	bt2 := newBlockingTool("tool2")

	// Model calls both tools in one response.
	// Model 在一个响应中调用两个 tools。
	modelResp := toolCallMsg(
		toolCall("c1", "tool1", `{"input":"a"}`),
		toolCall("c2", "tool2", `{"input":"b"}`),
	)
	modelWithTools := &simpleChatModel{response: modelResp}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       modelWithTools,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{bt1, bt2}},
		},
	})
	assert.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("go")}}, cancelOpt)

	// Wait for both tools to start.
	// 等待两个 tools 都启动。
	for i := 0; i < 2; i++ {
		select {
		case <-bt1.started:
		case <-bt2.started:
		case <-time.After(5 * time.Second):
			t.Fatal("tools did not start")
		}
	}

	// Request cancel after tool calls while both are still blocking.
	// 在两个 tools 仍阻塞时，请求在 tool calls 后取消。
	cancelDone := make(chan error, 1)
	go func() {
		handle, _ := cancelFn(WithAgentCancelMode(CancelAfterToolCalls))
		cancelDone <- handle.Wait()
	}()

	// Unblock both tools — cancel should fire only after both complete.
	// 解除两个工具的阻塞——只有两者都完成后才应触发 cancel。
	time.Sleep(50 * time.Millisecond)
	close(bt1.unblockCh)
	time.Sleep(50 * time.Millisecond)
	close(bt2.unblockCh)

	cancelErr := <-cancelDone
	assert.NoError(t, cancelErr)

	assert.Equal(t, int32(1), atomic.LoadInt32(&bt1.callCount), "tool1 should complete")
	assert.Equal(t, int32(1), atomic.LoadInt32(&bt2.callCount), "tool2 should complete")

	_, hasCancelError := drainEvents(iter)
	assert.True(t, hasCancelError, "expected CancelError after concurrent tools completed")
}

// TestWithCancel_GraphInterruptRaceBeforeSet verifies that a CancelImmediate
// issued before setGraphInterruptFunc is called still results in cancellation.
// This exercises the retroactive-fire path in setGraphInterruptFunc.
//
// TestWithCancel_GraphInterruptRaceBeforeSet 验证在调用 setGraphInterruptFunc 之前发出的 CancelImmediate 仍会导致取消。
// 这会覆盖 setGraphInterruptFunc 中的回溯触发路径。
func TestWithCancel_GraphInterruptRaceBeforeSet(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(schema.AssistantMessage("hi", nil))

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
	})
	require.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()

	// Cancel immediately before run starts.
	// 在运行开始前立即取消。
	go func() {
		handle, _ := cancelFn()
		_ = handle.Wait()
	}()

	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, cancelOpt)

	done := make(chan struct{})
	go func() {
		defer close(done)
		drainEvents(iter)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("iteration did not complete after pre-start CancelImmediate")
	}
}

// TestWithCancel_NoCheckpointStore verifies cancel completes and does not panic
// when no checkpoint store is configured.
//
// TestWithCancel_NoCheckpointStore 验证未配置 checkpoint store 时，取消会完成且不会 panic。
func TestWithCancel_NoCheckpointStore(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(schema.AssistantMessage("hi", nil))

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
	})
	require.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent: agent,
		// No CheckPointStore set.
		// 未设置 CheckPointStore。
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("hi")}, cancelOpt)

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}
	time.Sleep(30 * time.Millisecond)

	handle, _ := cancelFn()
	cancelErr := handle.Wait()
	assert.NoError(t, cancelErr)

	var ce *CancelError
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		if e.Err != nil && errors.As(e.Err, &ce) {
			break
		}
	}
	assert.NotNil(t, ce, "expected CancelError even without checkpoint store")
}

// TestWithCancel_ModelError verifies that a model error marks the cancelCtx as
// done so that a subsequent cancelFn call returns ErrExecutionEnded.
//
// TestWithCancel_ModelError 验证模型错误会将 cancelCtx 标记为 done，使后续 cancelFn 调用返回 ErrExecutionEnded。
func TestWithCancel_ModelError(t *testing.T) {
	ctx := context.Background()

	modelErr := errors.New("model failed")
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       &errorChatModel{err: modelErr},
	})
	require.NoError(t, err)

	cancelOpt, cancelFn := WithCancel()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, cancelOpt)

	var gotModelErr bool
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		if e.Err != nil && !errors.As(e.Err, new(*CancelError)) {
			gotModelErr = true
		}
	}
	assert.True(t, gotModelErr, "expected non-cancel error event from model failure")

	handle, _ := cancelFn()
	cancelErr := handle.Wait()
	assert.ErrorIs(t, cancelErr, ErrExecutionEnded, "cancelFn should return ErrExecutionEnded after model error")
}

// TestWithCancel_Resume_SafePoint covers CancelAfterChatModel and
// CancelAfterToolCalls on a Resume path.
//
// TestWithCancel_Resume_SafePoint 覆盖 Resume 路径上的 CancelAfterChatModel 和 CancelAfterToolCalls。
func TestWithCancel_Resume_SafePoint(t *testing.T) {
	ctx := context.Background()

	// --- phase 1: run to get a checkpoint via CancelImmediate ---
	// --- 阶段 1：通过 CancelImmediate 运行以获取检查点 ---
	blk := newBlockingChatModel(toolCallMsg(toolCall("c1", "bt", `{"input":"x"}`)))
	bt := newSlowTool("bt", 50*time.Millisecond, "result")

	agent1, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{bt}},
		},
	})
	assert.NoError(t, err)

	store := newCancelTestStore()
	runner1 := NewRunner(ctx, RunnerConfig{
		Agent:           agent1,
		CheckPointStore: store,
	})

	cancelOpt1, cancelFn1 := WithCancel()
	iter1 := runner1.Run(ctx, []Message{schema.UserMessage("hi")}, cancelOpt1, WithCheckPointID("resume-sp-1"))

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start in phase 1")
	}
	_, _ = cancelFn1()
	drainEvents(iter1)

	// --- phase 2: resume, cancel after chat model ---
	// --- 阶段 2：恢复，在 chat model 后取消 ---
	resumeModel := newBlockingChatModel(toolCallMsg(toolCall("c1", "bt", `{"input":"x"}`)))

	bt2 := newSlowTool("bt", 50*time.Millisecond, "result")
	agent2, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       resumeModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{bt2}},
		},
	})
	assert.NoError(t, err)

	runner2 := NewRunner(ctx, RunnerConfig{
		Agent:           agent2,
		CheckPointStore: store,
	})

	cancelOpt2, cancelFn2 := WithCancel()
	resumeIter, err := runner2.Resume(ctx, "resume-sp-1", cancelOpt2)
	require.NoError(t, err)

	select {
	case <-resumeModel.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start in phase 2")
	}

	cancelDone := make(chan error, 1)
	go func() {
		handle, _ := cancelFn2(WithAgentCancelMode(CancelAfterChatModel))
		cancelDone <- handle.Wait()
	}()

	time.Sleep(50 * time.Millisecond)

	close(resumeModel.unblockCh)

	cancelErr := <-cancelDone
	assert.NoError(t, cancelErr)

	_, hasCancelError := drainEvents(resumeIter)
	assert.True(t, hasCancelError, "CancelError expected after resumed model returns tool calls")
}

// callbackTool is a tool that calls onCall when invoked.
// callbackTool 是一个在调用时会调用 onCall 的工具。
type callbackTool struct {
	name   string
	onCall func()
}

func (t *callbackTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: t.name,
		Desc: "callback tool",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"input": {Type: "string"},
		}),
	}, nil
}

func (t *callbackTool) InvokableRun(_ context.Context, _ string, _ ...tool.Option) (string, error) {
	if t.onCall != nil {
		t.onCall()
	}
	return "ok", nil
}

// interruptingChatModel returns a compose.Interrupt error to simulate a
// business interrupt during execution.
//
// interruptingChatModel 返回 compose.Interrupt 错误，用于模拟执行期间的业务中断。
type interruptingChatModel struct{}

func (m *interruptingChatModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return nil, compose.Interrupt(ctx, "test interrupt")
}

func (m *interruptingChatModel) Stream(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, compose.Interrupt(ctx, "test interrupt")
}

func (m *interruptingChatModel) BindTools(_ []*schema.ToolInfo) error { return nil }

// TestWithCancel_TargetedResume_CancelImmediate cancels an agent via CancelImmediate,
// extracts InterruptContexts from the resulting CancelError, and uses them
// for targeted resumption via Runner.ResumeWithParams.
//
// TestWithCancel_TargetedResume_CancelImmediate 通过 CancelImmediate 取消智能体，从生成的 CancelError 中提取 InterruptContexts，并用它们通过 Runner.ResumeWithParams 进行定向恢复。
func TestWithCancel_TargetedResume_CancelImmediate(t *testing.T) {
	ctx := context.Background()

	blk := newBlockingChatModel(toolCallMsg(toolCall("c1", "st", `{"input":"x"}`)))
	st := newSlowTool("st", 50*time.Millisecond, "result")

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
		},
	})
	require.NoError(t, err)

	store := newCancelTestStore()
	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agent,
		CheckPointStore: store,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("go")}, cancelOpt, WithCheckPointID("targeted-imm-1"))

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}

	handle, _ := cancelFn() // CancelImmediate (default)
	// CancelImmediate（默认）
	cancelErr := handle.Wait()
	assert.NoError(t, cancelErr)

	var cancelError *CancelError
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		var ce *CancelError
		if e.Err != nil && errors.As(e.Err, &ce) {
			cancelError = ce
		}
	}

	require.NotNil(t, cancelError, "expected CancelError")
	require.NotEmpty(t, cancelError.InterruptContexts, "CancelError should have InterruptContexts for targeted resume")

	// --- resume with targeted params ---
	// --- 使用定向参数恢复 ---
	targets := make(map[string]any)
	for _, ic := range cancelError.InterruptContexts {
		targets[ic.ID] = nil
	}

	resumeModel := &plainResponseModel{text: "resumed"}
	agent2, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       resumeModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
		},
	})
	require.NoError(t, err)

	runner2 := NewRunner(ctx, RunnerConfig{
		Agent:           agent2,
		CheckPointStore: store,
	})

	resumeIter, err := runner2.ResumeWithParams(ctx, "targeted-imm-1", &ResumeParams{Targets: targets})
	require.NoError(t, err)

	var gotOutput bool
	for {
		e, ok := resumeIter.Next()
		if !ok {
			break
		}
		if e.Err != nil {
			t.Fatalf("unexpected error during targeted resume: %v", e.Err)
		}
		if e.Output != nil && e.Output.MessageOutput != nil {
			gotOutput = true
		}
	}
	assert.True(t, gotOutput, "targeted resume should produce output")
}

// TestWithCancel_TargetedResume_SafePoint cancels an agent via CancelAfterChatModel
// (safe-point) and verifies that InterruptContexts are populated on the CancelError
// and that targeted resume via ResumeWithParams succeeds.
// Since safe-point cancels now use compose.Interrupt, compose saves checkpoint data,
// making the cancel fully resumable.
//
// TestWithCancel_TargetedResume_SafePoint 通过 CancelAfterChatModel（safe-point）取消智能体，并验证 CancelError 上填充了 InterruptContexts，且通过 ResumeWithParams 定向恢复成功。
// 由于 safe-point 取消现在使用 compose.Interrupt，compose 会保存检查点数据，使取消完全可恢复。
func TestWithCancel_TargetedResume_SafePoint(t *testing.T) {
	ctx := context.Background()

	// The model returns a tool call so the react graph routes to toolPreHandle,
	// which detects CancelAfterChatModel and fires compose.Interrupt.
	//
	// 模型返回一个工具调用，因此 react 图会路由到 toolPreHandle，后者检测到 CancelAfterChatModel 并触发 compose.Interrupt。
	blk := newBlockingChatModel(toolCallMsg(toolCall("c1", "st", `{"input":"x"}`)))
	st := newSlowTool("st", 0, "result")

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       blk,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
		},
	})
	require.NoError(t, err)

	store := newCancelTestStore()
	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agent,
		CheckPointStore: store,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("go")}, cancelOpt, WithCheckPointID("targeted-sp-1"))

	select {
	case <-blk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}

	// Start cancelFn in background so the CAS happens before the model unblocks.
	// 在后台启动 cancelFn，使 CAS 在模型解除阻塞前发生。
	cancelDone := make(chan error, 1)
	go func() {
		handle, _ := cancelFn(WithAgentCancelMode(CancelAfterChatModel))
		cancelDone <- handle.Wait()
	}()
	time.Sleep(50 * time.Millisecond)
	close(blk.unblockCh)

	cancelErr := <-cancelDone
	assert.NoError(t, cancelErr)

	var cancelError *CancelError
	for {
		e, ok := iter.Next()
		if !ok {
			break
		}
		var ce *CancelError
		if e.Err != nil && errors.As(e.Err, &ce) {
			cancelError = ce
		}
	}

	require.NotNil(t, cancelError, "expected CancelError")
	require.NotEmpty(t, cancelError.InterruptContexts, "CancelError should have InterruptContexts for targeted resume")

	// --- resume with targeted params ---
	// --- 使用定向参数恢复 ---
	targets := make(map[string]any)
	for _, ic := range cancelError.InterruptContexts {
		targets[ic.ID] = nil
	}

	resumeModel := &plainResponseModel{text: "resumed"}
	agent2, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       resumeModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
		},
	})
	require.NoError(t, err)

	runner2 := NewRunner(ctx, RunnerConfig{
		Agent:           agent2,
		CheckPointStore: store,
	})

	resumeIter, err := runner2.ResumeWithParams(ctx, "targeted-sp-1", &ResumeParams{Targets: targets})
	require.NoError(t, err)

	var gotOutput bool
	for {
		e, ok := resumeIter.Next()
		if !ok {
			break
		}
		if e.Err != nil {
			t.Fatalf("unexpected error during targeted resume: %v", e.Err)
		}
		if e.Output != nil && e.Output.MessageOutput != nil {
			gotOutput = true
		}
	}
	assert.True(t, gotOutput, "targeted resume should produce output")
}

// TestWithCancel_Resume_CancelAfterChatModel_MessagePreserved tests both the
// ReAct (with-tools) and noTools paths to ensure that when a
// CancelAfterChatModel safe-point fires and the run is later resumed, the
// original Message returned by the chat model is preserved through the
// StatefulInterrupt checkpoint.
//
// For the ReAct path: the model returns a tool-call message. On resume the
// cancelCheck node must return that same message so the branch routes to the
// ToolNode and the tool actually executes.
//
// For the noTools path: the model returns a plain text message. On resume the
// cancel-check lambda must return that same message as the chain output.
//
// TestWithCancel_Resume_CancelAfterChatModel_MessagePreserved 测试 ReAct（带工具）和 noTools 两条路径，以确保触发 CancelAfterChatModel safe-point 且稍后恢复运行时，chat model 返回的原始 Message 会通过 StatefulInterrupt 检查点保留下来。
// 对于 ReAct 路径：模型返回工具调用消息。恢复时，cancelCheck 节点必须返回同一条消息，以便分支路由到 ToolNode 并实际执行工具。
// 对于 noTools 路径：模型返回纯文本消息。恢复时，cancel-check lambda 必须返回同一条消息作为链输出。
func TestWithCancel_Resume_CancelAfterChatModel_MessagePreserved(t *testing.T) {
	t.Run("react_path_tool_call_preserved", func(t *testing.T) {
		ctx := context.Background()

		// Phase-2 model returns no tool calls so the graph ends.
		// We track whether the tool actually executes on resume.
		//
		// 阶段 2 的模型不返回工具调用，因此图会结束。
		// 我们跟踪工具是否确实在恢复时执行。
		toolExecuted := make(chan struct{}, 1)
		st := &callbackTool{
			name: "my_tool",
			onCall: func() {
				select {
				case toolExecuted <- struct{}{}:
				default:
				}
			},
		}

		// Phase-1 model returns a tool call.
		// Phase-1 模型返回一个工具调用。
		blk := newBlockingChatModel(toolCallMsg(toolCall("c1", "my_tool", `{"input":"x"}`)))

		agent1, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
			Name:        "TestAgent",
			Description: "test",
			Model:       blk,
			ToolsConfig: ToolsConfig{
				ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
			},
		})
		require.NoError(t, err)

		store := newCancelTestStore()
		runner1 := NewRunner(ctx, RunnerConfig{
			Agent:           agent1,
			CheckPointStore: store,
		})

		cancelOpt1, cancelFn1 := WithCancel()
		iter1 := runner1.Run(ctx, []Message{schema.UserMessage("hi")},
			cancelOpt1, WithCheckPointID("react-msg-preserved-1"))

		select {
		case <-blk.started:
		case <-time.After(5 * time.Second):
			t.Fatal("model did not start in phase 1")
		}

		cancelDone := make(chan error, 1)
		go func() {
			handle, _ := cancelFn1(WithAgentCancelMode(CancelAfterChatModel))
			cancelDone <- handle.Wait()
		}()
		time.Sleep(50 * time.Millisecond)
		close(blk.unblockCh)

		cancelErr := <-cancelDone
		assert.NoError(t, cancelErr)

		_, hasCancelError := drainEvents(iter1)
		assert.True(t, hasCancelError, "expected CancelError from phase 1")

		// Phase 2: resume. The model for phase-2 returns plain text (no tool
		// calls) so the react graph ends after one iteration. But first the
		// tool from the checkpoint must execute.
		//
		// Phase 2: 恢复。phase-2 的模型返回纯文本（无工具调用），因此 react graph 一次迭代后结束。但必须先执行检查点中的工具。
		resumeModel := &plainResponseModel{text: "done"}
		agent2, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
			Name:        "TestAgent",
			Description: "test",
			Model:       resumeModel,
			ToolsConfig: ToolsConfig{
				ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
			},
		})
		require.NoError(t, err)

		runner2 := NewRunner(ctx, RunnerConfig{
			Agent:           agent2,
			CheckPointStore: store,
		})

		resumeIter, err := runner2.Resume(ctx, "react-msg-preserved-1")
		require.NoError(t, err)

		for {
			e, ok := resumeIter.Next()
			if !ok {
				break
			}
			if e.Err != nil {
				t.Fatalf("unexpected error during resume: %v", e.Err)
			}
		}

		// The key assertion: the tool must have been called during resume,
		// which can only happen if the tool-call message was preserved.
		//
		// 关键断言：恢复期间必须调用过该工具，这只有在工具调用消息被保留时才可能发生。
		select {
		case <-toolExecuted:
			// success
		default:
			t.Fatal("tool was not executed on resume — the tool-call message was lost")
		}
	})

}

// TestHandleRunFuncError_AlreadyHandled_NoDuplicate verifies that when
// markCancelHandled() was already claimed by a sub-agent's handleRunFuncError,
// the sequential workflow's checkCancel does not emit a second CancelError.
//
// Setup: sequential[cma1, cma2] with CancelAfterToolCalls. cma1 has tools,
// cancel fires while tool is running. After tool completes, the safe-point
// fires in cma1's handleRunFuncError (claiming markCancelHandled). The
// sequential workflow's checkCancel at the transition point should find
// markCancelHandled returns false and skip — producing exactly 1 CancelError.
//
// TestHandleRunFuncError_AlreadyHandled_NoDuplicate 验证：当 markCancelHandled() 已被子智能体的 handleRunFuncError 认领时，顺序工作流的 checkCancel 不会再发出第二个 CancelError。
// 设置：带 CancelAfterToolCalls 的 sequential[cma1, cma2]。cma1 有工具，取消在工具运行时触发。工具完成后，安全点在 cma1 的 handleRunFuncError 中触发（认领 markCancelHandled）。顺序工作流在转换点的 checkCancel 应发现 markCancelHandled 返回 false 并跳过——最终只产生 1 个 CancelError。
func TestHandleRunFuncError_AlreadyHandled_NoDuplicate(t *testing.T) {
	ctx := context.Background()

	bt := newBlockingTool("bt")

	// cma1: model returns a tool call immediately, tool blocks until unblocked
	// cma1：模型立即返回一个工具调用，工具会阻塞直到解除阻塞
	cma1Model := newBlockingChatModel(toolCallMsg(toolCall("c1", "bt", `{"input":"x"}`)))
	close(cma1Model.unblockCh) // model returns immediately
	// 模型立即返回

	agent1, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name: "agent1", Description: "first", Instruction: "test",
		Model: cma1Model,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{bt}},
		},
	})
	require.NoError(t, err)

	agent2Model := &plainResponseModel{text: "agent2-response"}
	agent2, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name: "agent2", Description: "second", Instruction: "test",
		Model: agent2Model,
	})
	require.NoError(t, err)

	seqAgent, err := NewSequentialAgent(ctx, &SequentialAgentConfig{
		Name: "seq", Description: "sequential", SubAgents: []Agent{agent1, agent2},
	})
	require.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent: seqAgent, EnableStreaming: false,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("test")}, cancelOpt)

	// Wait for tool to start
	// 等待工具启动
	select {
	case <-bt.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Tool did not start")
	}

	// Cancel while tool is still running (in goroutine because cancelFn blocks
	// until execution finishes), then unblock tool so safe-point fires
	//
	// 在工具仍在运行时取消（放在 goroutine 中，因为 cancelFn 会阻塞直到执行完成），然后解除工具阻塞以触发安全点
	go func() {
		handle, _ := cancelFn(WithAgentCancelMode(CancelAfterToolCalls))
		_ = handle.Wait()
	}()

	// Give cancel time to register, then unblock tool
	// 给取消一点时间完成登记，然后解除工具阻塞
	time.Sleep(50 * time.Millisecond)
	close(bt.unblockCh)

	cancelCount := 0
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		var ce *CancelError
		if event.Err != nil && errors.As(event.Err, &ce) {
			cancelCount++
		}
	}

	assert.Equal(t, 1, cancelCount, "Should have exactly one CancelError, no duplicate from handleRunFuncError + checkCancel")
}

func TestWithCancel_CancelAfterChatModel_NestedAgentTool(t *testing.T) {
	ctx := context.Background()

	subAgentModel := newBlockingChatModel(toolCallMsg(toolCall("c1", "sub_tool", `{"input":"x"}`)))
	subAgentModelStarted := subAgentModel.started
	subTool := newBlockingTool("sub_tool")

	subAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "sub_agent",
		Description: "test sub agent",
		Instruction: "you are a sub agent",
		Model:       subAgentModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{subTool}},
		},
	})
	require.NoError(t, err)

	supervisorModel := &simpleChatModel{
		response: &schema.Message{
			Role: schema.Assistant,
			ToolCalls: []schema.ToolCall{{
				ID: "call_1", Type: "function",
				Function: schema.FunctionCall{
					Name:      TransferToAgentToolName,
					Arguments: `{"agent_name": "sub_agent"}`,
				},
			}},
		},
	}

	supervisorAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "supervisor",
		Description: "supervisor agent (equivalent to DeepAgent)",
		Instruction: "you are a supervisor",
		Model:       supervisorModel,
	})
	require.NoError(t, err)

	agentWithSubAgents, err := SetSubAgents(ctx, supervisorAgent, []Agent{subAgent})
	require.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agentWithSubAgents,
		EnableStreaming: false,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("test")}, cancelOpt)

	select {
	case <-subAgentModelStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("Sub-agent model did not start")
	}

	time.Sleep(50 * time.Millisecond)

	cancelDone := make(chan error, 1)
	go func() {
		handle, _ := cancelFn(WithAgentCancelMode(CancelAfterChatModel), WithRecursive())
		cancelDone <- handle.Wait()
	}()

	time.Sleep(100 * time.Millisecond)
	close(subAgentModel.unblockCh)

	cancelErr := <-cancelDone
	assert.NoError(t, cancelErr)

	hasCancelError := false
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		var ce *CancelError
		if event.Err != nil && errors.As(event.Err, &ce) {
			hasCancelError = true
		}
	}

	assert.True(t, hasCancelError, "CancelError expected from nested agent tool with tools")
}

// slowStreamingTool implements StreamableTool (but NOT InvokableTool), streaming
// chunks slowly so CancelImmediate can fire mid-stream.
//
// slowStreamingTool 实现 StreamableTool（但不实现 InvokableTool），缓慢流式输出分块，使 CancelImmediate 能在流中途触发。
type slowStreamingTool struct {
	name          string
	chunkInterval time.Duration
	chunks        []string
	started       chan struct{}
	gate          chan struct{} // if non-nil, blocks after first chunk until closed
	// 若非 nil，则在第一个分块后阻塞直到关闭
}

func (t *slowStreamingTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: t.name,
		Desc: "slow streaming tool",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"input": {Type: "string"},
		}),
	}, nil
}

func (t *slowStreamingTool) StreamableRun(_ context.Context, _ string, _ ...tool.Option) (*schema.StreamReader[string], error) {
	r, w := schema.Pipe[string](1)
	go func() {
		defer w.Close()
		select {
		case t.started <- struct{}{}:
		default:
		}
		for i, chunk := range t.chunks {
			time.Sleep(t.chunkInterval)
			if closed := w.Send(chunk, nil); closed {
				return
			}
			// After the second chunk, block on gate so the caller can
			// issue a cancel while the tool is deterministically still streaming.
			// We wait until chunk index 1 (second chunk) so that the framework
			// has time to receive the first chunk and forward the streaming
			// event to the iterator, ensuring ErrStreamCanceled is observable.
			//
			// 第二个分块之后，在 gate 上阻塞，使调用方可以在工具确定仍在流式输出时发起取消。我们等到分块索引 1（第二个分块），让框架有时间接收第一个分块并把流式事件转发给迭代器，确保 ErrStreamCanceled 可被观测到。
			if i == 1 && t.gate != nil {
				<-t.gate
			}
		}
	}()
	return r, nil
}

// toolCallStreamModel returns a tool-call message on the first Stream call,
// then a plain text response on subsequent calls.
//
// toolCallStreamModel 在第一次 Stream 调用时返回工具调用消息，后续调用返回纯文本响应。
type toolCallStreamModel struct {
	callCount int32
}

func (m *toolCallStreamModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	if atomic.AddInt32(&m.callCount, 1) == 1 {
		return toolCallMsg(toolCall("c1", "slow_tool", `{"input":"x"}`)), nil
	}
	return schema.AssistantMessage("done", nil), nil
}

func (m *toolCallStreamModel) Stream(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *toolCallStreamModel) BindTools(_ []*schema.ToolInfo) error { return nil }

// TestWithCancel_CancelImmediate_StreamableToolAborted verifies that CancelImmediate
// during StreamableTool streaming surfaces ErrStreamCanceled on the tool's
// MessageStream.Recv(), just like it does for ChatModel streaming.
//
// TestWithCancel_CancelImmediate_StreamableToolAborted 验证：在 StreamableTool 流式输出期间触发 CancelImmediate，会在工具的 MessageStream.Recv() 上暴露 ErrStreamCanceled，和 ChatModel 流式输出时一样。
func TestWithCancel_CancelImmediate_StreamableToolAborted(t *testing.T) {
	ctx := context.Background()

	tcm := &toolCallStreamModel{}
	gate := make(chan struct{})
	st := &slowStreamingTool{
		name:          "slow_tool",
		chunkInterval: 100 * time.Millisecond,
		chunks:        []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
		started:       make(chan struct{}, 1),
		gate:          gate,
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "test",
		Model:       tcm,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{st}},
		},
	})
	require.NoError(t, err)

	runner := NewRunner(ctx, RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	})

	cancelOpt, cancelFn := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("hi")}, cancelOpt)

	// Wait for the tool to start streaming and send its first chunk.
	// The tool then blocks on the gate, guaranteeing the execution is
	// still in progress when we issue the cancel.
	//
	// 等待工具开始流式输出并发送第一个分块。随后工具会在 gate 上阻塞，保证我们发起取消时执行仍在进行。
	select {
	case <-st.started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool did not start streaming")
	}

	// Drain events in a separate goroutine so we can issue the cancel
	// from the main goroutine after confirming the tool stream event
	// has been received.
	//
	// 在单独的 goroutine 中耗尽事件，这样主 goroutine 可在确认收到工具流事件后发起取消。
	type result struct {
		foundStreamCanceled bool
		foundCancelError    bool
	}
	resultCh := make(chan result, 1)
	toolStreamReady := make(chan struct{})
	go func() {
		var r result
		for {
			e, ok := iter.Next()
			if !ok {
				break
			}

			// ErrStreamCanceled appears on the tool's MessageStream.Recv()
			// ErrStreamCanceled 出现在工具的 MessageStream.Recv() 上
			if e.Output != nil && e.Output.MessageOutput != nil && e.Output.MessageOutput.IsStreaming &&
				e.Output.MessageOutput.Role == schema.Tool {
				// Signal that the tool stream event has been received.
				// 标记已收到工具流事件。
				close(toolStreamReady)
				stream := e.Output.MessageOutput.MessageStream
				for {
					_, recvErr := stream.Recv()
					if recvErr != nil {
						if errors.Is(recvErr, ErrStreamCanceled) {
							r.foundStreamCanceled = true
						}
						break
					}
				}
			}

			if e.Action != nil && e.Action.Interrupted != nil {
				r.foundCancelError = true
			}
			var ce *CancelError
			if e.Err != nil && errors.As(e.Err, &ce) {
				r.foundCancelError = true
			}
		}
		resultCh <- r
	}()

	// Wait for the iterator goroutine to receive the tool streaming event.
	// At this point the tool goroutine is blocked on the gate, and the
	// iterator goroutine is blocked on stream.Recv(), so the execution is
	// guaranteed to still be in progress.
	//
	// 等待迭代器 goroutine 收到工具流事件。此时工具 goroutine 阻塞在 gate 上，迭代器 goroutine 阻塞在 stream.Recv() 上，因此可保证执行仍在进行。
	select {
	case <-toolStreamReady:
	case <-time.After(5 * time.Second):
		t.Fatal("tool stream event was not received by the iterator")
	}

	// Issue cancel while the tool goroutine is blocked on gate.
	// wrapStreamWithCancelMonitoring detects immediateChan and sends
	// ErrStreamCanceled to the consumer side. We do NOT close gate here —
	// keeping the tool goroutine blocked ensures the graph interrupt (timeout=0)
	// wins the race against normal completion. Close gate in defer for cleanup.
	//
	// 在工具 goroutine 阻塞于 gate 时发起取消。wrapStreamWithCancelMonitoring 检测到 immediateChan，并向消费者侧发送 ErrStreamCanceled。这里不要关闭 gate——保持工具 goroutine 阻塞可确保图中断（timeout=0）在与正常完成的竞争中胜出。清理时在 defer 中关闭 gate。
	defer close(gate)
	handle, _ := cancelFn()
	cancelErr := handle.Wait()

	r := <-resultCh

	if errors.Is(cancelErr, ErrExecutionEnded) {
		// On slower runtimes (e.g. Go 1.19 CI), the execution can complete
		// before the cancel signal is delivered — this is a valid race outcome.
		//
		// 在较慢的运行时（例如 Go 1.19 CI）上，执行可能会在 cancel 信号送达前完成——这是有效的竞态结果。
		t.Log("cancel raced with completion (ErrExecutionEnded) — skipping cancel assertions")
		return
	}
	assert.NoError(t, cancelErr)
	assert.True(t, r.foundStreamCanceled, "expected ErrStreamCanceled on tool's MessageStream.Recv()")
	assert.True(t, r.foundCancelError, "expected CancelError in event stream")
}

// TestWithCancel_CancelImmediate_NestedAgentTool_ResumeFromToolsNode verifies that
// when a nested ChatModelAgent (wrapped as an AgentTool inside an outer ChatModelAgent)
// is canceled via CancelImmediate and then resumed with Runner.Resume (no params),
// the outer agent resumes from the ToolsNode rather than restarting from the beginning.
//
// Regression test: previously, the outer ChatModelAgent would restart from its Init/ChatModel
// node instead of resuming from the ToolsNode, causing the outer model to be called again
// with the original user message before the AgentTool and inner ChatModelAgent were resumed.
//
// TestWithCancel_CancelImmediate_NestedAgentTool_ResumeFromToolsNode 验证：
// 当嵌套的 ChatModelAgent（作为外层 ChatModelAgent 内的 AgentTool 包装）通过 CancelImmediate 取消，然后用 Runner.Resume（无参数）恢复时，
// 外层智能体会从 ToolsNode 恢复，而不是从头重新开始。
// 回归测试：以前外层 ChatModelAgent 会从其 Init/ChatModel 节点重新开始，而不是从 ToolsNode 恢复，
// 导致在 AgentTool 和内层 ChatModelAgent 恢复前，外层模型再次用原始用户消息被调用。
func TestWithCancel_CancelImmediate_NestedAgentTool_ResumeFromToolsNode(t *testing.T) {
	for _, tc := range []struct {
		name            string
		enableStreaming bool
		innerHasTools   bool
		recursive       bool
	}{
		{"Invoke_InnerNoTools_NonRecursive", false, false, false},
		{"Stream_InnerNoTools_NonRecursive", true, false, false},
		{"Invoke_InnerWithTools_NonRecursive", false, true, false},
		{"Stream_InnerWithTools_NonRecursive", true, true, false},
		{"Invoke_InnerNoTools_Recursive", false, false, true},
		{"Stream_InnerNoTools_Recursive", true, false, true},
		{"Invoke_InnerWithTools_Recursive", false, true, true},
		{"Stream_InnerWithTools_Recursive", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// --- inner agent: its model blocks so we can cancel mid-execution ---
			// --- 内层智能体：其模型会阻塞，以便我们在执行中途取消 ---
			var innerTools []tool.BaseTool
			var innerModelResp *schema.Message
			if tc.innerHasTools {
				innerModelResp = toolCallMsg(toolCall("ic1", "inner_tool", `{"input":"x"}`))
				innerTools = []tool.BaseTool{newBlockingTool("inner_tool")}
			} else {
				innerModelResp = &schema.Message{Role: schema.Assistant, Content: "inner agent done"}
			}
			innerModel := newBlockingChatModel(innerModelResp)
			t.Cleanup(func() {
				close(innerModel.unblockCh)
			})

			innerCfg := &ChatModelAgentConfig{
				Name:        "InnerAgent",
				Description: "inner agent that blocks",
				Instruction: "you are an inner agent",
				Model:       innerModel,
			}
			if len(innerTools) > 0 {
				innerCfg.ToolsConfig = ToolsConfig{
					ToolsNodeConfig: compose.ToolsNodeConfig{Tools: innerTools},
				}
			}
			innerAgent, err := NewChatModelAgent(ctx, innerCfg)
			require.NoError(t, err)

			// --- outer agent: counting model ---
			// Call 1: returns a tool call that invokes InnerAgent.
			// Call 2 (only needed on resume): returns a plain final answer.
			//
			// --- 外层智能体：计数模型 ---
			// 第 1 次调用：返回调用 InnerAgent 的工具调用。
			// 第 2 次调用（仅恢复时需要）：返回普通的最终答案。
			outerModelCallCount := int32(0)
			outerModel := &countingChatModel{
				callCount: &outerModelCallCount,
				responses: []*schema.Message{
					toolCallMsg(toolCall("c1", "InnerAgent", `{"request":"do something"}`)),
					schema.AssistantMessage("outer completed", nil),
				},
			}

			outerAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
				Name:        "OuterAgent",
				Description: "outer agent with nested agent tool",
				Instruction: "you are an outer agent",
				Model:       outerModel,
				ToolsConfig: ToolsConfig{
					ToolsNodeConfig: compose.ToolsNodeConfig{
						Tools: []tool.BaseTool{NewAgentTool(ctx, innerAgent)},
					},
				},
			})
			require.NoError(t, err)

			store := newCancelTestStore()
			checkpointID := "cancel-nested-resume-" + tc.name

			runner1 := NewRunner(ctx, RunnerConfig{
				Agent:           outerAgent,
				EnableStreaming: tc.enableStreaming,
				CheckPointStore: store,
			})

			// --- phase 1: run and cancel while inner agent model is blocked ---
			// --- 阶段 1：运行并在内层智能体模型阻塞时取消 ---
			cancelOpt, cancelFn := WithCancel()
			iter := runner1.Run(ctx, []Message{schema.UserMessage("go")}, cancelOpt, WithCheckPointID(checkpointID))

			// Wait for inner model to start (meaning outer model already returned tool call).
			// 等待内层模型启动（表示外层模型已经返回工具调用）。
			select {
			case <-innerModel.started:
			case <-time.After(5 * time.Second):
				t.Fatal("inner model did not start")
			}

			// At this point outerModel should have been called exactly once.
			// 此时 outerModel 应该只被调用一次。
			assert.Equal(t, int32(1), atomic.LoadInt32(&outerModelCallCount),
				"outer model should have been called once before cancel")

			// Cancel immediately. Recursive cases additionally propagate the cancel
			// request into the AgentTool's internal ChatModelAgent.
			//
			// 立即取消。递归场景还会把取消请求传播到 AgentTool 内部的 ChatModelAgent。
			var handle *CancelHandle
			if tc.recursive {
				handle, _ = cancelFn(WithRecursive())
			} else {
				handle, _ = cancelFn()
			}
			cancelErr := handle.Wait()
			assert.NoError(t, cancelErr)

			_, hasCancelError := drainEvents(iter)
			assert.True(t, hasCancelError, "expected CancelError from canceled nested agent tool")

			// --- phase 2: resume with Runner.Resume (no ResumeWithParams, no interrupt ID) ---
			// Build fresh agents for resume. Recursive cancel should resume the
			// inner ChatModelAgent inside AgentTool before the top-level
			// ChatModelAgent produces the final answer.
			//
			// --- 阶段 2：使用 Runner.Resume 恢复（无 ResumeWithParams，无 interrupt ID） ---
			// 为恢复构建新的智能体。递归取消应先恢复 AgentTool 内部的内层 ChatModelAgent，
			// 然后顶层 ChatModelAgent 再生成最终答案。
			resumeFirstModelCall := make(chan string, 5)
			resumeOuterModelCallCount := int32(0)
			resumeOuterModel := &countingChatModel{
				callCount: &resumeOuterModelCallCount,
				callCh:    resumeFirstModelCall,
				callLabel: "outer",
				responses: []*schema.Message{
					schema.AssistantMessage("outer completed after resume", nil),
				},
			}

			resumeInnerModelCallCount := int32(0)
			resumeInnerResponses := []*schema.Message{schema.AssistantMessage("inner agent done after resume", nil)}
			if len(innerTools) > 0 {
				resumeInnerResponses = []*schema.Message{
					toolCallMsg(toolCall("ic1", "inner_tool", `{"input":"x"}`)),
					schema.AssistantMessage("inner agent done after resume", nil),
				}
			}
			resumeInnerModel := &countingChatModel{
				callCount: &resumeInnerModelCallCount,
				callCh:    resumeFirstModelCall,
				callLabel: "inner",
				responses: resumeInnerResponses,
			}
			resumeInnerCfg := &ChatModelAgentConfig{
				Name:        "InnerAgent",
				Description: "inner agent that returns immediately on resume",
				Instruction: "you are an inner agent",
				Model:       resumeInnerModel,
			}
			if len(innerTools) > 0 {
				resumeInnerCfg.ToolsConfig = ToolsConfig{
					ToolsNodeConfig: compose.ToolsNodeConfig{
						Tools: []tool.BaseTool{newSlowTool("inner_tool", 0, "inner tool result")},
					},
				}
			}
			resumeInnerAgent, err := NewChatModelAgent(ctx, resumeInnerCfg)
			require.NoError(t, err)

			resumeOuterAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
				Name:        "OuterAgent",
				Description: "outer agent with nested agent tool",
				Instruction: "you are an outer agent",
				Model:       resumeOuterModel,
				ToolsConfig: ToolsConfig{
					ToolsNodeConfig: compose.ToolsNodeConfig{
						Tools: []tool.BaseTool{NewAgentTool(ctx, resumeInnerAgent)},
					},
				},
			})
			require.NoError(t, err)

			runner2 := NewRunner(ctx, RunnerConfig{
				Agent:           resumeOuterAgent,
				EnableStreaming: tc.enableStreaming,
				CheckPointStore: store,
			})

			resumeIter, err := runner2.Resume(ctx, checkpointID)
			require.NoError(t, err)

			select {
			case firstModel := <-resumeFirstModelCall:
				if tc.recursive {
					assert.Equal(t, "inner", firstModel,
						"recursive cancel should resume the AgentTool/internal ChatModelAgent first")
				} else {
					assert.Contains(t, []string{"outer", "inner"}, firstModel,
						"non-recursive cancel does not define whether a root or already-persisted inner checkpoint resumes first")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no model call observed during resume")
			}

			var resumeEvents []*AgentEvent
			for {
				event, ok := resumeIter.Next()
				if !ok {
					break
				}
				if event.Err != nil {
					t.Fatalf("unexpected error during resume: %v", event.Err)
				}
				resumeEvents = append(resumeEvents, event)
			}

			// The outer model should have been called exactly once during resume
			// (to produce the final answer after receiving tool results).
			// If it was called with the original user message (restarting from scratch),
			// the counting model would either exceed its response list or the call count
			// would be wrong.
			//
			// 恢复期间外层模型应该只被调用一次（在收到工具结果后生成最终答案）。
			// 如果它以原始用户消息被调用（从头重启），计数模型要么会超出响应列表，要么调用次数会不正确。
			assert.Equal(t, int32(1), atomic.LoadInt32(&resumeOuterModelCallCount),
				"outer model should be called exactly once during resume (for final answer after tool results), "+
					"not restarted from the beginning")

			// Verify we got the completion output.
			// 验证我们获得了完成输出。
			var gotOutput bool
			for _, event := range resumeEvents {
				content, err := messageOutputContent(event)
				require.NoError(t, err)
				if content == "outer completed after resume" {
					gotOutput = true
				}
			}
			assert.True(t, gotOutput, "should get final output from resumed outer agent")
		})
	}
}

func TestWithCancel_CancelImmediate_RecursiveAgentTool_ResumeDeepestAgentTool(t *testing.T) {
	ctx := context.Background()

	leafModel := newBlockingChatModel(schema.AssistantMessage("leaf done", nil))
	t.Cleanup(func() {
		close(leafModel.unblockCh)
	})
	leafAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "LeafAgent",
		Description: "leaf agent that blocks",
		Instruction: "you are a leaf agent",
		Model:       leafModel,
	})
	require.NoError(t, err)

	middleModelCallCount := int32(0)
	middleModel := &countingChatModel{
		callCount: &middleModelCallCount,
		responses: []*schema.Message{
			toolCallMsg(toolCall("middle-leaf", "LeafAgent", `{"request":"leaf work"}`)),
			schema.AssistantMessage("middle done", nil),
		},
	}
	middleAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "MiddleAgent",
		Description: "middle agent with an agent tool",
		Instruction: "you are a middle agent",
		Model:       middleModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{NewAgentTool(ctx, leafAgent)},
			},
		},
	})
	require.NoError(t, err)

	outerModelCallCount := int32(0)
	outerModel := &countingChatModel{
		callCount: &outerModelCallCount,
		responses: []*schema.Message{
			toolCallMsg(toolCall("outer-middle", "MiddleAgent", `{"request":"middle work"}`)),
			schema.AssistantMessage("outer done", nil),
		},
	}
	outerAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "OuterAgent",
		Description: "outer agent with recursive agent tool nesting",
		Instruction: "you are an outer agent",
		Model:       outerModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{NewAgentTool(ctx, middleAgent)},
			},
		},
	})
	require.NoError(t, err)

	store := newCancelTestStore()
	checkpointID := "cancel-recursive-agent-tool-resume"
	runner1 := NewRunner(ctx, RunnerConfig{Agent: outerAgent, CheckPointStore: store})

	cancelOpt, cancelFn := WithCancel()
	iter := runner1.Run(ctx, []Message{schema.UserMessage("go")}, cancelOpt, WithCheckPointID(checkpointID))

	select {
	case <-leafModel.started:
	case <-time.After(5 * time.Second):
		t.Fatal("leaf model did not start")
	}

	handle, _ := cancelFn(WithRecursive())
	require.NoError(t, handle.Wait())
	_, hasCancelError := drainEvents(iter)
	assert.True(t, hasCancelError, "expected CancelError from recursive nested agent tool")

	firstModelCall := make(chan string, 8)
	resumeLeafModelCallCount := int32(0)
	resumeLeafModel := &countingChatModel{
		callCount: &resumeLeafModelCallCount,
		callCh:    firstModelCall,
		callLabel: "leaf",
		responses: []*schema.Message{schema.AssistantMessage("leaf done after resume", nil)},
	}
	resumeLeafAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "LeafAgent",
		Description: "leaf agent that returns on resume",
		Instruction: "you are a leaf agent",
		Model:       resumeLeafModel,
	})
	require.NoError(t, err)

	resumeMiddleModelCallCount := int32(0)
	resumeMiddleModel := &countingChatModel{
		callCount: &resumeMiddleModelCallCount,
		callCh:    firstModelCall,
		callLabel: "middle",
		responses: []*schema.Message{schema.AssistantMessage("middle done after resume", nil)},
	}
	resumeMiddleAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "MiddleAgent",
		Description: "middle agent with an agent tool",
		Instruction: "you are a middle agent",
		Model:       resumeMiddleModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{NewAgentTool(ctx, resumeLeafAgent)},
			},
		},
	})
	require.NoError(t, err)

	resumeOuterModelCallCount := int32(0)
	resumeOuterModel := &countingChatModel{
		callCount: &resumeOuterModelCallCount,
		callCh:    firstModelCall,
		callLabel: "outer",
		responses: []*schema.Message{schema.AssistantMessage("outer done after resume", nil)},
	}
	resumeOuterAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "OuterAgent",
		Description: "outer agent with recursive agent tool nesting",
		Instruction: "you are an outer agent",
		Model:       resumeOuterModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{NewAgentTool(ctx, resumeMiddleAgent)},
			},
		},
	})
	require.NoError(t, err)

	runner2 := NewRunner(ctx, RunnerConfig{Agent: resumeOuterAgent, CheckPointStore: store})
	resumeIter, err := runner2.Resume(ctx, checkpointID)
	require.NoError(t, err)

	select {
	case first := <-firstModelCall:
		assert.Equal(t, "leaf", first, "recursive AgentTool nesting should resume the deepest internal agent first")
	case <-time.After(5 * time.Second):
		t.Fatal("no model call observed during resume")
	}

	resumeEvents, hasResumeCancelError := drainEvents(resumeIter)
	require.False(t, hasResumeCancelError, "resume should complete without another CancelError")
	assert.NotEmpty(t, resumeEvents)
	assert.Equal(t, int32(1), atomic.LoadInt32(&resumeLeafModelCallCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&resumeMiddleModelCallCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&resumeOuterModelCallCount))
}

func TestWithCancel_CancelImmediate_ConcurrentAgentTools_ResumeWithoutRestart(t *testing.T) {
	ctx := context.Background()

	innerAModel := newBlockingChatModel(schema.AssistantMessage("inner A done", nil))
	innerBModel := newBlockingChatModel(schema.AssistantMessage("inner B done", nil))
	t.Cleanup(func() {
		close(innerAModel.unblockCh)
		close(innerBModel.unblockCh)
	})

	innerAAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "InnerAgentA",
		Description: "inner agent A",
		Instruction: "you are inner agent A",
		Model:       innerAModel,
	})
	require.NoError(t, err)
	innerBAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "InnerAgentB",
		Description: "inner agent B",
		Instruction: "you are inner agent B",
		Model:       innerBModel,
	})
	require.NoError(t, err)

	outerModelCallCount := int32(0)
	outerModel := &countingChatModel{
		callCount: &outerModelCallCount,
		responses: []*schema.Message{
			toolCallMsg(
				toolCall("outer-a", "InnerAgentA", `{"request":"work A"}`),
				toolCall("outer-b", "InnerAgentB", `{"request":"work B"}`),
			),
			schema.AssistantMessage("outer done", nil),
		},
	}
	outerAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "OuterAgent",
		Description: "outer agent with concurrent agent tools",
		Instruction: "you are an outer agent",
		Model:       outerModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{
					NewAgentTool(ctx, innerAAgent),
					NewAgentTool(ctx, innerBAgent),
				},
			},
		},
	})
	require.NoError(t, err)

	store := newCancelTestStore()
	checkpointID := "cancel-concurrent-agent-tools-resume"
	runner1 := NewRunner(ctx, RunnerConfig{Agent: outerAgent, CheckPointStore: store})

	cancelOpt, cancelFn := WithCancel()
	iter := runner1.Run(ctx, []Message{schema.UserMessage("go")}, cancelOpt, WithCheckPointID(checkpointID))

	for _, started := range []chan struct{}{innerAModel.started, innerBModel.started} {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("both concurrent inner models should start before cancel")
		}
	}

	handle, _ := cancelFn(WithRecursive())
	require.NoError(t, handle.Wait())
	_, hasCancelError := drainEvents(iter)
	assert.True(t, hasCancelError, "expected CancelError from concurrent agent tools")

	firstModelCall := make(chan string, 8)
	resumeInnerAModelCallCount := int32(0)
	resumeInnerAModel := &countingChatModel{
		callCount: &resumeInnerAModelCallCount,
		callCh:    firstModelCall,
		callLabel: "innerA",
		responses: []*schema.Message{schema.AssistantMessage("inner A done after resume", nil)},
	}
	resumeInnerBModelCallCount := int32(0)
	resumeInnerBModel := &countingChatModel{
		callCount: &resumeInnerBModelCallCount,
		callCh:    firstModelCall,
		callLabel: "innerB",
		responses: []*schema.Message{schema.AssistantMessage("inner B done after resume", nil)},
	}

	resumeInnerAAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "InnerAgentA",
		Description: "inner agent A",
		Instruction: "you are inner agent A",
		Model:       resumeInnerAModel,
	})
	require.NoError(t, err)
	resumeInnerBAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "InnerAgentB",
		Description: "inner agent B",
		Instruction: "you are inner agent B",
		Model:       resumeInnerBModel,
	})
	require.NoError(t, err)

	resumeOuterModelCallCount := int32(0)
	resumeOuterModel := &countingChatModel{
		callCount: &resumeOuterModelCallCount,
		callCh:    firstModelCall,
		callLabel: "outer",
		responses: []*schema.Message{schema.AssistantMessage("outer done after resume", nil)},
	}
	resumeOuterAgent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "OuterAgent",
		Description: "outer agent with concurrent agent tools",
		Instruction: "you are an outer agent",
		Model:       resumeOuterModel,
		ToolsConfig: ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{
					NewAgentTool(ctx, resumeInnerAAgent),
					NewAgentTool(ctx, resumeInnerBAgent),
				},
			},
		},
	})
	require.NoError(t, err)

	runner2 := NewRunner(ctx, RunnerConfig{Agent: resumeOuterAgent, CheckPointStore: store})
	resumeIter, err := runner2.Resume(ctx, checkpointID)
	require.NoError(t, err)

	select {
	case first := <-firstModelCall:
		assert.Contains(t, []string{"innerA", "innerB"}, first,
			"concurrent AgentTools should resume an internal agent before the outer model")
	case <-time.After(5 * time.Second):
		t.Fatal("no model call observed during resume")
	}

	resumeEvents, hasResumeCancelError := drainEvents(resumeIter)
	require.False(t, hasResumeCancelError, "resume should complete without another CancelError")
	assert.NotEmpty(t, resumeEvents)
	assert.Equal(t, int32(1), atomic.LoadInt32(&resumeInnerAModelCallCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&resumeInnerBModelCallCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&resumeOuterModelCallCount))
}

// countingChatModel is a chat model that counts calls and records inputs.
// It returns responses from a fixed slice, indexed by call count.
//
// countingChatModel 是一个会统计调用次数并记录输入的聊天模型。
// 它按调用次数作为索引，从固定切片返回响应。
type countingChatModel struct {
	callCount *int32
	inputsCh  chan []*schema.Message // optional: receives a copy of each input
	// 可选：接收每次输入的副本
	callCh chan string // optional: receives callLabel when Generate is called
	// 可选：在调用 Generate 时接收 callLabel
	callLabel string
	responses []*schema.Message
}

func (m *countingChatModel) Generate(_ context.Context, input []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	idx := int(atomic.AddInt32(m.callCount, 1)) - 1
	if m.callCh != nil {
		select {
		case m.callCh <- m.callLabel:
		default:
		}
	}
	if m.inputsCh != nil {
		cp := make([]*schema.Message, len(input))
		copy(cp, input)
		select {
		case m.inputsCh <- cp:
		default:
		}
	}
	if idx >= len(m.responses) {
		return nil, fmt.Errorf("countingChatModel: call %d exceeds %d responses (outer model was called too many times - possible restart from beginning)", idx+1, len(m.responses))
	}
	return m.responses[idx], nil
}

func (m *countingChatModel) Stream(_ context.Context, input []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(context.Background(), input, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *countingChatModel) BindTools(_ []*schema.ToolInfo) error { return nil }

func messageOutputContent(event *AgentEvent) (string, error) {
	if event.Output == nil || event.Output.MessageOutput == nil {
		return "", nil
	}
	mo := event.Output.MessageOutput
	if mo.IsStreaming {
		msg, err := schema.ConcatMessageStream(mo.MessageStream)
		if err != nil {
			return "", err
		}
		if msg == nil {
			return "", nil
		}
		return msg.Content, nil
	}
	if mo.Message == nil {
		return "", nil
	}
	return mo.Message.Content, nil
}
