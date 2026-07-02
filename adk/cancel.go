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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

func init() {
	schema.RegisterName[*CancelError]("_eino_adk_cancel_error")
	schema.RegisterName[*AgentCancelInfo]("_eino_adk_agent_cancel_info")
	schema.RegisterName[*StreamCanceledError]("_eino_adk_stream_cancelled_error")
}

// CancelMode specifies when an agent should be canceled.
// Modes can be combined with bitwise OR to cancel at multiple safe-points.
// For example, CancelAfterChatModel | CancelAfterToolCalls cancels the agent
// after whichever safe-point is reached first.
//
// CancelMode 指定何时取消智能体。
// 可用按位 OR 组合多个模式，在多个安全点取消。
// 例如，CancelAfterChatModel | CancelAfterToolCalls 会在最先到达的安全点后取消智能体。
type CancelMode int

const (
	// CancelImmediate cancels the agent as soon as the signal is received,
	// without waiting for a ChatModel or ToolCalls safe-point.
	// By default, only the root agent is interrupted; descendant agents inside
	// AgentTools are torn down via context cancellation as a side effect.
	// Use WithRecursive to propagate explicit immediate-cancel signals to
	// descendants for clean teardown with grace period.
	//
	// CancelImmediate 在收到信号后立即取消智能体，
	// 不等待 ChatModel 或 ToolCalls 安全点。
	// 默认只中断根智能体；AgentTools 内的后代智能体会作为 context 取消的副作用被终止。
	// 使用 WithRecursive 可将显式的立即取消信号传播给后代，以便在宽限期内干净地收尾。
	CancelImmediate CancelMode = 0
	// CancelAfterChatModel cancels after the root agent's next chat model call
	// completes. By default, only the root agent checks this safe-point;
	// nested sub-agents inside AgentTools are unaware of the cancel.
	// Use WithRecursive to propagate the cancel to all descendants — whichever
	// ChatModel finishes first triggers the cancel.
	//
	// CancelAfterChatModel 会在根智能体下一次 chat model 调用完成后取消。
	// 默认只有根智能体检查此安全点；AgentTools 内的嵌套子智能体不会感知取消。
	// 使用 WithRecursive 可将取消传播给所有后代——最先完成的 ChatModel 会触发取消。
	CancelAfterChatModel CancelMode = 1 << iota
	// CancelAfterToolCalls cancels after the root agent's next set of tool calls
	// completes. By default, only the root agent checks this safe-point.
	// Use WithRecursive to propagate to all descendants.
	//
	// CancelAfterToolCalls 会在根智能体下一组工具调用完成后取消。
	// 默认只有根智能体检查此安全点。
	// 使用 WithRecursive 可传播给所有后代。
	CancelAfterToolCalls
)

// CancelHandle represents a cancel operation that can be waited on.
// CancelHandle 表示可等待的取消操作。
type CancelHandle struct {
	wait func() error
}

// Wait blocks until the cancel request reaches a terminal outcome.
//
// It reports the result of the cancel operation itself, not the agent's final
// business error:
//   - nil: cancellation succeeded, including the case where a business interrupt
//     was absorbed into CancelError while cancellation was active
//   - ErrCancelTimeout: the requested safe-point cancellation timed out and was
//     escalated to immediate cancellation
//   - ErrExecutionEnded: the execution ended before cancellation took effect,
//     meaning the stream drained to completion without any interrupt
//
// Wait 会阻塞，直到取消请求达到终态。
// 它报告的是取消操作本身的结果，而不是智能体最终的业务错误：
// - nil：取消成功，包括取消处于激活状态时业务中断被吸收到 CancelError 的情况
// - ErrCancelTimeout：请求的安全点取消超时，并升级为立即取消
// - ErrExecutionEnded：执行在取消生效前已结束，表示流已完整耗尽且没有任何中断
func (h *CancelHandle) Wait() error {
	return h.wait()
}

// AgentCancelFunc is called to request cancellation of a running agent.
// It returns after the cancel request is committed; use the returned handle's
// Wait to block for completion and outcome.
//
// The returned bool reports whether this call contributed to the CancelError
// for the current execution. "Contributed" means this call's cancel options
// were included before cancellation was finalized. It is false when cancellation
// was already finalized (handled or execution completed).
//
// AgentCancelFunc 用于请求取消正在运行的智能体。
// 它会在取消请求提交后返回；使用返回的 handle 的 Wait 来阻塞等待完成及结果。
// 返回的 bool 表示本次调用是否参与了当前执行的 CancelError。
// “参与”表示本次调用的取消选项在取消最终确定前已被纳入。
// 如果取消已最终确定（已处理或执行已完成），则为 false。
type AgentCancelFunc func(...AgentCancelOption) (*CancelHandle, bool)

type agentCancelConfig struct {
	Mode      CancelMode
	Recursive bool
	Timeout   *time.Duration
}

// AgentCancelOption configures cancel behavior.
// AgentCancelOption 配置取消行为。
type AgentCancelOption func(*agentCancelConfig)

// WithAgentCancelMode sets the cancel mode for the agent cancel operation.
// WithAgentCancelMode 设置智能体取消操作的取消模式。
func WithAgentCancelMode(mode CancelMode) AgentCancelOption {
	return func(config *agentCancelConfig) {
		config.Mode = mode
	}
}

// WithAgentCancelTimeout sets a timeout for the cancel operation.
// This only applies to safe-point modes (CancelAfterChatModel, CancelAfterToolCalls):
// if the safe-point hasn't fired within this duration, the cancel escalates to
// CancelImmediate. The escalated cancel still saves a checkpoint, so the execution
// can be resumed via Runner.Resume or Runner.ResumeWithParams.
// For CancelImmediate this timeout is ignored — the cancel fires immediately.
//
// WithAgentCancelTimeout 为取消操作设置超时。
// 这只适用于安全点模式（CancelAfterChatModel、CancelAfterToolCalls）：
// 如果安全点在该时长内未触发，取消会升级为 CancelImmediate。
// 升级后的取消仍会保存检查点，因此执行可通过 Runner.Resume 或 Runner.ResumeWithParams 恢复。
// 对于 CancelImmediate，此超时会被忽略——取消会立即触发。
func WithAgentCancelTimeout(timeout time.Duration) AgentCancelOption {
	return func(config *agentCancelConfig) {
		config.Timeout = &timeout
	}
}

// WithRecursive opts into recursive cancel propagation. By default, cancel
// modes only affect the root agent; descendant agents inside AgentTools are
// not notified. WithRecursive makes the cancel propagate to all descendants:
//   - CancelAfterChatModel / CancelAfterToolCalls: descendants check their own safe-points.
//   - CancelImmediate: descendants receive explicit immediate-cancel signals for
//     clean teardown; the root uses a grace period to collect child interrupts.
//
// With recursive cancellation, each descendant agent also triggers cancellation
// and cascades its interrupt information upward. The root agent ultimately
// produces a complete checkpoint that includes descendant checkpoints, enabling
// resumption from the exact point where each descendant was interrupted.
//
// Once any cancel call includes WithRecursive, the flag stays set for the
// entire cancel lifecycle (monotonic escalation).
//
// WithRecursive 启用递归取消传播。默认情况下，取消模式只影响根智能体；AgentTools 内的后代智能体不会收到通知。
// WithRecursive 会将取消传播给所有后代：
// - CancelAfterChatModel / CancelAfterToolCalls：后代检查自己的安全点。
// - CancelImmediate：后代接收显式的立即取消信号以干净收尾；根智能体使用宽限期收集子级中断。
// 使用递归取消时，每个后代智能体也会触发取消，并向上传播其中断信息。
// 根智能体最终会生成包含后代检查点的完整检查点，从而能从每个后代被中断的准确位置恢复。
// 一旦任何取消调用包含 WithRecursive，该标志会在整个取消生命周期内保持设置（单调升级）。
func WithRecursive() AgentCancelOption {
	return func(config *agentCancelConfig) {
		config.Recursive = true
	}
}

// AgentCancelInfo contains information about a cancel operation.
// AgentCancelInfo 包含取消操作的信息。
type AgentCancelInfo struct {
	Mode      CancelMode
	Escalated bool
	Timeout   bool
}

// CancelError is sent via AgentEvent.Err when an agent is canceled.
// Use errors.As to match and extract *CancelError from event errors.
//
// Interrupt absorption: when a cancel is active (shouldCancel() == true), ANY
// interrupt — whether from a cancel safe-point node or from business logic
// (e.g. tool.Interrupt in a tool) — is converted to a CancelError. The
// cancel "absorbs" the business interrupt. This is intentional:
//
//   - In concurrent execution (parallel workflows, concurrent tool calls),
//     cancel-induced and business interrupts can arrive as a single composite
//     signal that cannot be split apart.
//   - Even in sequential execution, treating business interrupts as CancelError
//     during active cancel gives consistent semantics.
//   - The business interrupt is NOT lost — the checkpoint preserves the full
//     interrupt hierarchy. On resume (Runner.Resume or Runner.ResumeWithParams),
//     the agent re-executes the interrupting code path and the business
//     interrupt re-fires naturally.
//
// CancelError 会在智能体被取消时通过 AgentEvent.Err 发送。
// 使用 errors.As 匹配并从事件错误中提取 *CancelError。
// 中断吸收：当取消处于激活状态（shouldCancel() == true）时，任何中断——无论来自取消安全点节点，还是来自业务逻辑（例如工具中的 tool.Interrupt）——都会转换为 CancelError。
// 取消会“吸收”业务中断。这是有意设计的：
// - 在并发执行（并行工作流、并发工具调用）中，取消引发的中断和业务中断可能作为一个无法拆分的复合信号到达。
// - 即使在顺序执行中，活跃取消期间将业务中断视为 CancelError 也能提供一致语义。
// - 业务中断不会丢失——检查点会保留完整的中断层级。恢复时（Runner.Resume 或 Runner.ResumeWithParams），智能体会重新执行触发中断的代码路径，业务中断会自然再次触发。
type CancelError struct {
	Info *AgentCancelInfo

	// InterruptContexts provides the interrupt contexts needed for targeted
	// resumption via Runner.ResumeWithParams. Each context represents a step
	// in the agent hierarchy that was interrupted. This is a slice because
	// composite agents (e.g. parallel workflows) may interrupt at multiple
	// points simultaneously, matching the shape of AgentAction.Interrupted.InterruptContexts.
	// Use each InterruptCtx.ID as a key in ResumeParams.Targets.
	//
	// InterruptContexts 提供通过 Runner.ResumeWithParams 进行定向恢复所需的中断上下文。
	// 每个上下文表示智能体层级中被中断的一个步骤。
	// 这里使用 slice，是因为复合智能体（例如并行工作流）可能同时在多个点中断，与 AgentAction.Interrupted.InterruptContexts 的结构一致。
	// 使用每个 InterruptCtx.ID 作为 ResumeParams.Targets 中的键。
	InterruptContexts []*InterruptCtx

	interruptSignal *InterruptSignal // unexported — only Runner needs it for checkpoint
	// 未导出——只有 Runner 需要它用于检查点
}

func (e *CancelError) Error() string {
	return fmt.Sprintf("agent canceled: mode=%v, escalated=%v", e.Info.Mode, e.Info.Escalated)
}

// Sentinel errors for cancel outcomes.
// 取消结果的哨兵错误。
var (
	// ErrCancelTimeout is returned by CancelHandle.Wait when the cancel operation timed out.
	// ErrCancelTimeout 是 CancelHandle.Wait 在取消操作超时时返回的错误。
	ErrCancelTimeout = errors.New("cancel timed out")

	// ErrExecutionEnded is returned by CancelHandle.Wait when the agent ended
	// before the cancel took effect. "Ended" means the event stream was fully
	// drained without any interrupt — normal completion or a fatal error.
	//
	// Note: business interrupts that occur while cancel is active are absorbed
	// into CancelError (see CancelError doc), so they result in nil (cancel
	// succeeded), NOT ErrExecutionEnded. Only execution that completes with
	// no interrupt at all produces this error.
	//
	// ErrExecutionEnded 是 CancelHandle.Wait 在智能体结束且取消尚未生效时返回的错误。
	// “Ended”表示事件流已完全耗尽且没有任何中断——正常完成或发生致命错误。
	// 注意：取消处于活动状态时发生的业务中断会被吸收到 CancelError 中（见 CancelError 文档），因此结果为 nil（取消成功），而不是 ErrExecutionEnded。只有完全没有中断就结束的执行才会产生此错误。
	ErrExecutionEnded = errors.New("execution already ended")

	// ErrStreamCanceled is the error sent through the stream when CancelImmediate aborts it.
	// It is a *StreamCanceledError so it can be gob-serialized during checkpoint save
	// (when stored as agentEventWrapper.StreamErr).
	//
	// ErrStreamCanceled 是 CancelImmediate 中止流时通过流发送的错误。
	// 它是 *StreamCanceledError，因此在保存检查点时可以被 gob 序列化（当存储为 agentEventWrapper.StreamErr 时）。
	ErrStreamCanceled error = &StreamCanceledError{}
)

// StreamCanceledError is the concrete error type for ErrStreamCanceled.
// It is exported so that gob can serialize it during checkpoint save when the error
// is stored in agentEventWrapper.StreamErr.
//
// StreamCanceledError 是 ErrStreamCanceled 的具体错误类型。
// 它被导出，以便在保存检查点且错误存储在 agentEventWrapper.StreamErr 中时，gob 可以序列化它。
type StreamCanceledError struct{}

func (e *StreamCanceledError) Error() string {
	return "stream canceled"
}

// WithCancel creates an AgentRunOption that enables cancellation for an agent run.
// It returns the option to pass to Run/Resume and a cancel function.
// Cancel options (mode, timeout) are passed to the returned AgentCancelFunc at call time.
//
// WithCancel 创建一个 AgentRunOption，用于为智能体运行启用取消。
// 它返回传给 Run/Resume 的 option，以及一个取消函数。
// 取消选项（mode、timeout）在调用时传给返回的 AgentCancelFunc。
func WithCancel() (AgentRunOption, AgentCancelFunc) {
	cc := newCancelContext()
	opt := WrapImplSpecificOptFn(func(o *options) {
		o.cancelCtx = cc
	})
	cancelFn := cc.buildCancelFunc()
	return opt, cancelFn
}

// cancelContext state constants (for int32 CAS).
//
// State transition rules:
//
//	stateRunning -> stateCancelling     (cancel requested by AgentCancelFunc)
//	stateRunning -> stateDone           (execution finished without interrupt)
//	stateCancelling -> stateCancelHandled (ANY interrupt absorbed as CancelError)
//	stateCancelling -> stateDone        (execution finished without interrupt while cancel pending)
//
// Terminal states: stateDone, stateCancelHandled.
//
// Note: We intentionally do NOT distinguish between "completed" and "errored"
// terminal states. End-users get the actual outcome from AgentEvent.
// This simplification keeps the state machine minimal — only the cancel/non-cancel
// distinction matters for the AgentCancelFunc return value.
//
// Business interrupt handling: when cancel is active (stateCancelling) and any
// interrupt arrives — cancel-induced OR business — wrapIterWithCancelCtx absorbs
// it as a CancelError and transitions to stateCancelHandled. The business interrupt
// data is preserved in the checkpoint for re-emission on resume.
//
// cancelContext 状态常量（用于 int32 CAS）。
// 状态转换规则：
// stateRunning -> stateCancelling     （AgentCancelFunc 请求取消）
// stateRunning -> stateDone           （执行完成且无中断）
// stateCancelling -> stateCancelHandled（任何中断都作为 CancelError 被吸收）
// stateCancelling -> stateDone        （取消挂起时执行完成且无中断）
// 终态：stateDone、stateCancelHandled。
// 注意：我们有意不区分“completed”和“errored”终态。最终用户可从 AgentEvent 获取实际结果。
// 这种简化让状态机保持最小化——AgentCancelFunc 返回值只关心取消/非取消的区别。
// 业务中断处理：当取消处于活动状态（stateCancelling）且任何中断到达时——无论是取消引发还是业务引发——wrapIterWithCancelCtx 都会将其作为 CancelError 吸收，并转换到 stateCancelHandled。业务中断数据会保留在检查点中，以便恢复时重新发出。
const (
	// stateRunning is the initial state: agent is executing normally.
	// stateRunning 是初始状态：智能体正在正常执行。
	stateRunning int32 = 0
	// stateCancelling means AgentCancelFunc has been called and cancelChan is
	// closed, but the cancel has not yet been handled by the runFunc.
	//
	// stateCancelling 表示 AgentCancelFunc 已被调用且 cancelChan 已关闭，但 runFunc 尚未处理该取消。
	stateCancelling int32 = 1
	// stateDone means execution has finished through any non-cancel path:
	// normal completion, business interrupt, or error. The specific outcome
	// is conveyed through AgentEvent, not through the cancel state machine.
	//
	// stateDone 表示执行已通过任意非取消路径结束：正常完成、业务中断或错误。具体结果通过 AgentEvent 传递，而不是通过取消状态机。
	stateDone int32 = 2
	// stateCancelHandled means the cancel was processed by the runFunc and a
	// CancelError was emitted through the event stream. This is the success
	// terminal state for cancellation.
	//
	// stateCancelHandled 表示取消已由 runFunc 处理，并且 CancelError 已通过事件流发出。这是取消成功的终态。
	stateCancelHandled int32 = 5
)

// interruptSent constants (for int32 CAS).
//
// Transition rules:
//
//	interruptNotSent -> interruptImmediate (CancelImmediate or escalation)
//
// interruptSent 常量（用于 int32 CAS）。
// 转换规则：
// interruptNotSent -> interruptImmediate（CancelImmediate 或升级）
const (
	// interruptNotSent means no compose graph interrupt has been sent.
	// interruptNotSent 表示尚未发送 compose 图中断。
	interruptNotSent int32 = 0
	// interruptImmediate means an immediate graph interrupt was sent with
	// timeout=0, forcing the graph to stop as soon as possible.
	//
	// interruptImmediate 表示已发送 timeout=0 的立即图中断，强制图尽快停止。
	interruptImmediate int32 = 1
)

// defaultCancelImmediateGracePeriod is the bounded time a recursive
// AgentTool cancel waits before forcing the current level's graph interrupt.
// This gives deeper AgentTool/internal-agent interrupts a chance to bubble up
// as CompositeInterrupts. If this proves insufficient for deeply nested
// structures or too slow for latency-sensitive use cases, consider making it
// configurable via an AgentCancelOption.
//
// defaultCancelImmediateGracePeriod 是递归 AgentTool 取消在强制当前层级的图中断前等待的有界时间。
// 这让更深层的 AgentTool/internal-agent 中断有机会作为 CompositeInterrupts 向上冒泡。
// 如果这对深度嵌套结构不足，或对延迟敏感场景太慢，可考虑通过 AgentCancelOption 使其可配置。
const defaultCancelImmediateGracePeriod = 1 * time.Second

type cancelContextKey struct{}

// withCancelContext stores a cancelContext in the Go context.
// withCancelContext 将 cancelContext 存储到 Go context 中。
func withCancelContext(ctx context.Context, cc *cancelContext) context.Context {
	if cc == nil {
		return ctx
	}
	return context.WithValue(ctx, cancelContextKey{}, cc)
}

// getCancelContext retrieves the cancelContext from the Go context, or nil.
// getCancelContext 从 Go context 中取回 cancelContext；没有则返回 nil。
func getCancelContext(ctx context.Context) *cancelContext {
	if v := ctx.Value(cancelContextKey{}); v != nil {
		return v.(*cancelContext)
	}
	return nil
}

type cancelContext struct {
	mode int32 // atomic, CancelMode
	// atomic、CancelMode

	cancelChan chan struct{} // closed when cancel is requested (all modes, not just safe-point)
	// 请求取消时关闭（所有模式，不只是 safe-point）
	immediateChan chan struct{} // closed when an immediate graph interrupt fires
	// 立即图中断触发时关闭
	doneChan chan struct{} // closed when execution completes (by any mark* method)
	// 执行完成时关闭（由任意 mark* 方法触发）
	doneOnce sync.Once // ensures doneChan is closed exactly once
	// 确保 doneChan 只关闭一次

	state int32 // stateRunning, stateCancelling, stateDone, stateCancelHandled
	// stateRunning、stateCancelling、stateDone、stateCancelHandled
	interruptSent int32 // interruptNotSent, interruptImmediate
	// interruptNotSent、interruptImmediate
	escalated int32 // 1 if escalated from safe-point to immediate
	// 从安全点升级为立即取消时为 1
	timeoutEscalated int32 // 1 if escalation was triggered by timeout
	// 由超时触发升级时为 1
	startedMode int32 // atomic, mode when state transitioned to cancelling
	// atomic，状态转换为 cancelling 时的模式
	deadlineUnixNano int64 // atomic, 0 means no deadline
	// atomic，0 表示无截止时间

	recursive int32 // atomic; 1 if cancel should propagate into AgentTool internal agents
	// atomic；如果取消应传播到 AgentTool 内部智能体，则为 1
	recursiveChan chan struct{} // closed when recursive transitions from 0 to 1
	// recursive 从 0 变为 1 时关闭

	root bool // true for the original cancelContext created by WithCancel(); false for AgentTool internal agents
	// WithCancel() 创建的原始 cancelContext 为 true；AgentTool 内部智能体为 false
	parent *cancelContext // non-nil for AgentTool internal agents; used to propagate AgentTool boundary markers upward
	// AgentTool 内部智能体中为非 nil；用于向上传播 AgentTool 边界标记

	agentToolDescendant int32 // atomic; 1 once an AgentTool runs under this cancel context
	// atomic；此取消 context 下运行过 AgentTool 后为 1

	cancelMu      sync.Mutex
	timeoutOnce   sync.Once
	timeoutNotify chan struct{}

	mu                  sync.Mutex
	graphInterruptFuncs []func(...compose.GraphInterruptOption)
}

func newCancelContext() *cancelContext {
	return &cancelContext{
		cancelChan:    make(chan struct{}),
		immediateChan: make(chan struct{}),
		doneChan:      make(chan struct{}),
		timeoutNotify: make(chan struct{}, 1),
		recursiveChan: make(chan struct{}),
		root:          true,
	}
}

func (cc *cancelContext) isRoot() bool {
	return cc != nil && cc.root
}

func (cc *cancelContext) isRecursive() bool {
	return cc != nil && atomic.LoadInt32(&cc.recursive) == 1
}

// setRecursive(false) is a no-op; recursive is monotonically escalating:
// once set to true, it cannot be reverted.
//
// setRecursive(false) 是 no-op；recursive 只能单调升级：
// 一旦设为 true，就不能恢复。
func (cc *cancelContext) setRecursive(v bool) {
	if v && atomic.CompareAndSwapInt32(&cc.recursive, 0, 1) {
		close(cc.recursiveChan)
	}
}

// deriveAgentToolCancelContext creates the cancelContext used by an AgentTool's
// internal agent. It receives recursive cancel propagation from the parent
// AgentTool call. The caller MUST ensure the child's markDone() is eventually
// called (e.g., via wrapIterWithCancelCtx's defer) or that ctx is canceled;
// otherwise the two propagation goroutines will leak.
//
// deriveAgentToolCancelContext 创建供 AgentTool 内部智能体使用的 cancelContext。
// 它接收来自父 AgentTool 调用的递归取消传播。调用方必须确保最终调用子级的 markDone()
// （例如通过 wrapIterWithCancelCtx 的 defer），或确保 ctx 被取消；
// 否则两个传播 goroutine 会泄漏。
func (cc *cancelContext) deriveAgentToolCancelContext(ctx context.Context) *cancelContext {
	if cc == nil {
		return nil
	}
	child := newCancelContext()
	child.root = false
	child.parent = cc

	// Each goroutine below propagates one signal class (cancel / immediate) to
	// the child. The pattern is a two-phase select:
	//   Phase 1: wait for the parent signal (or child/ctx completion).
	//   Phase 2: if the signal fired but recursive mode is not active yet,
	//            enter a second select waiting for either recursive escalation
	//            (recursiveChan) or child/ctx completion. This ensures
	//            non-recursive cancels leave children unaware, while a late
	//            escalation to recursive still propagates.
	//
	// 下面每个 goroutine 向子级传播一种信号类别（cancel / immediate）。
	// 该模式是两阶段 select：
	// 阶段 1：等待父级信号（或子级/ctx 完成）。
	// 阶段 2：如果信号已触发但递归模式尚未激活，
	// 进入第二个 select，等待递归升级
	// (recursiveChan) 或子级/ctx 完成。这样可确保
	// 非递归取消不会让子级感知，同时后续升级为
	// recursive 时仍会传播。
	go func() {
		select {
		case <-cc.cancelChan:
			if cc.isRecursive() {
				child.setRecursive(true)
				child.triggerCancel(cc.getMode())
				return
			}
			select {
			case <-cc.recursiveChan:
				child.setRecursive(true)
				child.triggerCancel(cc.getMode())
			case <-child.doneChan:
			case <-ctx.Done():
			}
		case <-child.doneChan:
		case <-ctx.Done():
		}
	}()

	go func() {
		select {
		case <-cc.immediateChan:
			if cc.isRecursive() {
				child.setRecursive(true)
				child.triggerImmediateCancel()
				return
			}
			select {
			case <-cc.recursiveChan:
				child.setRecursive(true)
				child.triggerImmediateCancel()
			case <-child.doneChan:
			case <-ctx.Done():
			}
		case <-child.doneChan:
		case <-ctx.Done():
		}
	}()

	return child
}

func (cc *cancelContext) triggerCancel(mode CancelMode) {
	cc.setMode(mode)
	if atomic.CompareAndSwapInt32(&cc.state, stateRunning, stateCancelling) {
		close(cc.cancelChan)
	}
}

func (cc *cancelContext) triggerImmediateCancel() {
	atomic.StoreInt32(&cc.escalated, 1)
	cc.setMode(CancelImmediate)
	if atomic.CompareAndSwapInt32(&cc.state, stateRunning, stateCancelling) {
		close(cc.cancelChan)
	}
	cc.sendImmediateInterrupt()
}

func (cc *cancelContext) getMode() CancelMode {
	if cc == nil {
		return CancelImmediate
	}
	return CancelMode(atomic.LoadInt32(&cc.mode))
}

func (cc *cancelContext) setMode(mode CancelMode) {
	atomic.StoreInt32(&cc.mode, int32(mode))
}

func (cc *cancelContext) getDeadlineUnixNano() int64 {
	return atomic.LoadInt64(&cc.deadlineUnixNano)
}

func (cc *cancelContext) setDeadlineUnixNano(v int64) {
	atomic.StoreInt64(&cc.deadlineUnixNano, v)
}

func (cc *cancelContext) wakeTimeoutController() {
	select {
	case cc.timeoutNotify <- struct{}{}:
	default:
	}
}

// shouldCancel returns true if a cancel has been requested (cancelChan is closed).
// shouldCancel 在已请求取消（cancelChan 已关闭）时返回 true。
func (cc *cancelContext) shouldCancel() bool {
	if cc == nil {
		return false
	}
	select {
	case <-cc.cancelChan:
		return true
	default:
		return false
	}
}

// isImmediateCancelled returns true if an immediate graph interrupt has been
// fired (CancelImmediate or timeout escalation). This is stronger than
// shouldCancel: it means the compose graph is being torn down right now and
// orphaned goroutines should not attempt to send events.
//
// isImmediateCancelled 在已触发立即图中断时返回 true
// （CancelImmediate 或超时升级）。这比 shouldCancel 更强：
// 它表示 compose 图正在立即拆除，
// 孤立的 goroutine 不应尝试发送事件。
func (cc *cancelContext) isImmediateCancelled() bool {
	if cc == nil {
		return false
	}
	select {
	case <-cc.immediateChan:
		return true
	default:
		return false
	}
}

// sendImmediateInterrupt sends the compose graph interrupt signal via graphInterruptFuncs.
// Also closes immediateChan (used by cancelMonitoredModel to abort an in-progress stream).
// Returns false if an interrupt was already sent or if no graphInterruptFuncs have been
// registered yet (the deferred fire in setGraphInterruptFunc will handle that case).
//
// sendImmediateInterrupt 通过 graphInterruptFuncs 发送 compose 图中断信号。
// 还会关闭 immediateChan（cancelMonitoredModel 用它中止进行中的流）。
// 如果中断已发送，或尚未注册 graphInterruptFuncs，则返回 false
// （setGraphInterruptFunc 中的延迟触发会处理这种情况）。
func (cc *cancelContext) sendImmediateInterrupt() bool {
	cc.mu.Lock()

	if !atomic.CompareAndSwapInt32(&cc.interruptSent, interruptNotSent, interruptImmediate) {
		cc.mu.Unlock()
		return false
	}

	close(cc.immediateChan)

	fns := make([]func(...compose.GraphInterruptOption), len(cc.graphInterruptFuncs))
	copy(fns, cc.graphInterruptFuncs)

	if cc.isRecursive() && cc.hasAgentToolDescendant() {
		select {
		case <-cc.doneChan:
			cc.mu.Unlock()
			return true
		case <-time.After(defaultCancelImmediateGracePeriod):
		}
	}

	if len(fns) == 0 {
		cc.mu.Unlock()
		return false
	}

	for _, fn := range fns {
		fn(compose.WithGraphInterruptTimeout(0))
	}
	cc.mu.Unlock()
	return true
}

// setGraphInterruptFunc appends a graph interrupt function to the list.
// If an immediate cancel was already requested, fires it retroactively.
// Multiple functions can be registered (e.g. one per parallel sub-agent).
//
// Both this method and sendImmediateInterrupt hold cc.mu across the entire
// check-and-fire sequence, ensuring each interrupt function is called exactly
// once (compose.WithGraphInterrupt returns a non-idempotent closure that panics
// on double-call).
//
// setGraphInterruptFunc 将一个图中断函数追加到列表中。
// 如果已请求立即取消，则会回溯触发。
// 可以注册多个函数（例如每个并行子智能体一个）。
// 此方法和 sendImmediateInterrupt 都会在整个检查并触发流程中持有 cc.mu，
// 确保每个中断函数只被调用一次（compose.WithGraphInterrupt 返回的是非幂等闭包，
// 重复调用会 panic）。
func (cc *cancelContext) setGraphInterruptFunc(interrupt func(...compose.GraphInterruptOption)) {
	cc.mu.Lock()
	cc.graphInterruptFuncs = append(cc.graphInterruptFuncs, interrupt)

	shouldFire := atomic.LoadInt32(&cc.interruptSent) == interruptImmediate
	if shouldFire {
		interrupt(compose.WithGraphInterruptTimeout(0))
	}
	cc.mu.Unlock()
}

// markDone marks the execution as finished through any non-cancel path
// (normal completion, business interrupt, or error).
// This is safe to call even if a cancel is in progress — it allows the
// cancel func to detect that execution finished before cancel took effect.
//
// markDone 将执行标记为通过任何非取消路径完成
// （正常完成、业务中断或错误）。
// 即使正在取消，也可以安全调用它——这允许 cancel func
// 检测到执行在取消生效前已经完成。
func (cc *cancelContext) markDone() {
	if cc == nil {
		return
	}
	if atomic.CompareAndSwapInt32(&cc.state, stateRunning, stateDone) ||
		atomic.CompareAndSwapInt32(&cc.state, stateCancelling, stateDone) {
		cc.doneOnce.Do(func() { close(cc.doneChan) })
	}
}

func (cc *cancelContext) hasAgentToolDescendant() bool {
	return cc != nil && atomic.LoadInt32(&cc.agentToolDescendant) == 1
}

func (cc *cancelContext) markAgentToolDescendant() {
	for cur := cc; cur != nil; cur = cur.parent {
		atomic.StoreInt32(&cur.agentToolDescendant, 1)
	}
}

// markCancelHandled signals that the cancel path in the runFunc has created
// and sent a CancelError. Transitions state to stateCancelHandled so that:
// 1. The deferred markDone() becomes a no-op (CAS from cancelling fails).
// 2. buildCancelFunc sees stateCancelHandled and returns nil (cancel succeeded).
// Returns true if the transition succeeded, false if cancel was already handled
// (e.g., by a sub-agent). This prevents duplicate CancelError emission.
//
// markCancelHandled 表示 runFunc 中的取消路径已创建并发送 CancelError。
// 将状态转换为 stateCancelHandled，从而：
// 1. 延迟执行的 markDone() 变为 no-op（从 cancelling 发起的 CAS 失败）。
// 2. buildCancelFunc 看到 stateCancelHandled 并返回 nil（取消成功）。
// 转换成功返回 true；如果取消已被处理（例如由 sub-agent 处理）则返回 false。
// 这可避免重复发出 CancelError。
func (cc *cancelContext) markCancelHandled() bool {
	if cc == nil {
		return false
	}
	if atomic.CompareAndSwapInt32(&cc.state, stateCancelling, stateCancelHandled) {
		cc.doneOnce.Do(func() { close(cc.doneChan) })
		return true
	}
	return false
}

// createCancelError creates a CancelError based on the current cancel state.
// createCancelError 基于当前取消状态创建 CancelError。
func (cc *cancelContext) createCancelError() *CancelError {
	info := &AgentCancelInfo{}
	info.Mode = cc.getMode()
	if atomic.LoadInt32(&cc.escalated) == 1 {
		info.Escalated = true
		info.Timeout = atomic.LoadInt32(&cc.timeoutEscalated) == 1
	}
	return &CancelError{
		Info: info,
	}
}

func (cc *cancelContext) createAndMarkCancelHandled() (*CancelError, bool) {
	cc.cancelMu.Lock()
	defer cc.cancelMu.Unlock()
	cancelErr := cc.createCancelError()
	ok := cc.markCancelHandled()
	return cancelErr, ok
}

// buildCancelFunc builds the AgentCancelFunc for external use.
// buildCancelFunc 构建供外部使用的 AgentCancelFunc。
func (cc *cancelContext) buildCancelFunc() AgentCancelFunc {
	joinMode := func(a, b CancelMode) CancelMode {
		if a == CancelImmediate || b == CancelImmediate {
			return CancelImmediate
		}
		return a | b
	}

	parseReq := func(callOpts ...AgentCancelOption) *agentCancelConfig {
		cfg := &agentCancelConfig{Mode: CancelImmediate}
		for _, opt := range callOpts {
			opt(cfg)
		}
		return cfg
	}

	startTimeoutController := func() {
		cc.timeoutOnce.Do(func() {
			go func() {
				for {
					select {
					case <-cc.doneChan:
						return
					default:
					}

					mode := cc.getMode()
					if mode == CancelImmediate {
						return
					}

					deadline := cc.getDeadlineUnixNano()
					if deadline == 0 {
						select {
						case <-cc.timeoutNotify:
							continue
						case <-cc.doneChan:
							return
						}
					}

					now := time.Now().UnixNano()
					wait := time.Duration(deadline - now)
					if wait <= 0 {
						atomic.StoreInt32(&cc.escalated, 1)
						atomic.StoreInt32(&cc.timeoutEscalated, 1)
						cc.sendImmediateInterrupt()
						return
					}

					timer := time.NewTimer(wait)
					select {
					case <-timer.C:
						timer.Stop()
						atomic.StoreInt32(&cc.escalated, 1)
						atomic.StoreInt32(&cc.timeoutEscalated, 1)
						cc.sendImmediateInterrupt()
						return
					case <-cc.timeoutNotify:
						timer.Stop()
						continue
					case <-cc.doneChan:
						timer.Stop()
						return
					}
				}
			}()
		})
	}

	newHandle := func(wait func() error) *CancelHandle {
		return &CancelHandle{wait: wait}
	}

	waitForCompletion := func() error {
		<-cc.doneChan

		st := atomic.LoadInt32(&cc.state)
		switch st {
		case stateDone:
			return ErrExecutionEnded
		default:
			if atomic.LoadInt32(&cc.timeoutEscalated) == 1 {
				return ErrCancelTimeout
			}
			return nil
		}
	}

	return func(callOpts ...AgentCancelOption) (*CancelHandle, bool) {
		req := parseReq(callOpts...)

		st := atomic.LoadInt32(&cc.state)
		switch st {
		case stateCancelHandled:
			return newHandle(func() error { return nil }), false
		case stateDone:
			return newHandle(func() error { return ErrExecutionEnded }), false
		}

		var needImmediate, needTimeoutCtl bool

		cc.cancelMu.Lock()

		st = atomic.LoadInt32(&cc.state)
		switch st {
		case stateCancelHandled:
			cc.cancelMu.Unlock()
			return newHandle(func() error { return nil }), false
		case stateDone:
			cc.cancelMu.Unlock()
			return newHandle(func() error { return ErrExecutionEnded }), false
		}

		curMode := cc.getMode()
		if st == stateRunning {
			if !atomic.CompareAndSwapInt32(&cc.state, stateRunning, stateCancelling) {
				st = atomic.LoadInt32(&cc.state)
				cc.cancelMu.Unlock()
				if st == stateDone {
					return newHandle(func() error { return ErrExecutionEnded }), false
				}
				return newHandle(waitForCompletion), true
			}

			curMode = req.Mode
			cc.setMode(curMode)
			atomic.StoreInt32(&cc.startedMode, int32(curMode))
			cc.setRecursive(req.Recursive)
			close(cc.cancelChan)
		} else {
			// Recursive is monotonic: once set, cannot be unset. The first
			// cancel call uses the bool directly; subsequent calls only
			// escalate (false → true) — setRecursive(false) is a no-op.
			//
			// Recursive 是单调的：一旦设置就不能取消。
			// 第一次 cancel 调用直接使用该 bool；后续调用只会升级（false → true）—— setRecursive(false) 是 no-op。
			curMode = joinMode(curMode, req.Mode)
			cc.setMode(curMode)
			if req.Recursive {
				cc.setRecursive(true)
			}
		}

		if curMode == CancelImmediate {
			cc.setDeadlineUnixNano(0)
			needImmediate = true
		} else if req.Timeout != nil && *req.Timeout > 0 {
			proposed := time.Now().Add(*req.Timeout).UnixNano()
			existing := cc.getDeadlineUnixNano()
			if existing == 0 || proposed < existing {
				cc.setDeadlineUnixNano(proposed)
				cc.wakeTimeoutController()
			}
			needTimeoutCtl = cc.getDeadlineUnixNano() != 0
		}

		cc.cancelMu.Unlock()

		if needImmediate {
			if atomic.LoadInt32(&cc.startedMode) != int32(CancelImmediate) {
				atomic.StoreInt32(&cc.escalated, 1)
			}
			cc.sendImmediateInterrupt()
		}
		if needTimeoutCtl {
			startTimeoutController()
		}

		return newHandle(waitForCompletion), true
	}
}

// wrapIterWithCancelCtx wraps an iterator with cancel lifecycle management.
// It calls markDone when the inner iterator is fully drained, ensuring the
// cancelContext's doneChan is closed and propagation goroutines can exit.
//
// For root cancelContexts (created by WithCancel, not deriveAgentToolCancelContext), it also
// converts interrupt ACTION events to CancelError when cancel is active.
// This is the single point of interrupt-to-CancelError conversion in the
// system — Runner.handleIter only enriches the resulting CancelError with
// checkpoint metadata.
//
// Interrupt absorption: ALL interrupts are converted when cancel is active,
// including business interrupts (compose.Interrupt from user code). Cancel and
// business interrupts cannot be reliably distinguished in concurrent execution
// (parallel workflows, concurrent tool calls) where they merge into a single
// composite signal. The business interrupt data is preserved in the checkpoint
// and re-fires naturally on resume.
//
// This conversion MUST happen in this wrapper (not deferred to Runner.handleIter)
// because markDone runs as a defer in this goroutine — if the interrupt event
// were passed through unconverted, markDone would transition stateCancelling→stateDone
// before the Runner goroutine could call createAndMarkCancelHandled, causing it
// to fail the CAS.
//
// wrapIterWithCancelCtx 用取消生命周期管理包装迭代器。
// 它在内部迭代器完全耗尽时调用 markDone，确保 cancelContext 的 doneChan 被关闭，传播 goroutine 可以退出。
// 对于根 cancelContext（由 WithCancel 创建，而不是 deriveAgentToolCancelContext），它还会在取消处于活动状态时将 interrupt ACTION 事件转换为 CancelError。
// 这是系统中唯一的 interrupt 到 CancelError 转换点——Runner.handleIter 只会为生成的 CancelError 补充 checkpoint 元数据。
// 中断吸收：取消处于活动状态时，所有 interrupt 都会被转换，包括业务 interrupt（来自用户代码的 compose.Interrupt）。
// 在并发执行（并行工作流、并发工具调用）中，取消和业务 interrupt 会合并成单个复合信号，无法可靠区分。
// 业务 interrupt 数据会保存在 checkpoint 中，并在 resume 时自然重新触发。
// 此转换必须在该 wrapper 中完成（不能延迟到 Runner.handleIter），因为 markDone 作为 defer 在此 goroutine 中运行——如果 interrupt 事件未经转换直接传递，markDone 会在 Runner goroutine 调用 createAndMarkCancelHandled 之前将 stateCancelling→stateDone，导致 CAS 失败。
func wrapIterWithCancelCtx[M MessageType](iter *AsyncIterator[*TypedAgentEvent[M]], cancelCtx *cancelContext) *AsyncIterator[*TypedAgentEvent[M]] {
	if cancelCtx == nil {
		return iter
	}
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go func() {
		defer cancelCtx.markDone()
		defer gen.Close()
		for {
			event, ok := iter.Next()
			if !ok {
				break
			}

			if cancelCtx.isRoot() && event.Action != nil && event.Action.internalInterrupted != nil {
				if cancelCtx.shouldCancel() {
					cancelErr, ok := cancelCtx.createAndMarkCancelHandled()
					if ok {
						cancelErr.interruptSignal = event.Action.internalInterrupted
						gen.Send(&TypedAgentEvent[M]{Err: cancelErr})
					}
					return
				}
			}

			gen.Send(event)
		}
	}()
	return it
}

// typedCancelMonitoredModel wraps a model with cancel monitoring.
// Generate: pure delegate to the inner model (CancelAfterChatModel is handled
// by a dedicated node after the ChatModel in the compose graph).
// Stream: pipes chunks through a goroutine that selects on immediateChan for
// CancelImmediate abort.
//
// typedCancelMonitoredModel 用取消监控包装模型。
// Generate：纯粹委托给内部模型（CancelAfterChatModel 由 compose graph 中 ChatModel 之后的专用节点处理）。
// Stream：通过一个 goroutine 传递 chunk，并在 immediateChan 上 select 以处理 CancelImmediate 中止。
type typedCancelMonitoredModel[M MessageType] struct {
	inner         model.BaseModel[M]
	cancelContext *cancelContext
}

type recvResult[T any] struct {
	data T
	err  error
}

func (m *typedCancelMonitoredModel[M]) Generate(ctx context.Context, input []M, opts ...model.Option) (M, error) {
	return m.inner.Generate(ctx, input, opts...)
}

func (m *typedCancelMonitoredModel[M]) Stream(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
	stream, err := m.inner.Stream(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	wrapped := wrapStreamWithCancelMonitoring(stream, m.cancelContext)
	return wrapped, nil
}

// wrapStreamWithCancelMonitoring wraps a stream with cancel monitoring.
// When immediateChan fires (CancelImmediate or timeout escalation), the output
// stream is terminated with ErrStreamCanceled.
//
// wrapStreamWithCancelMonitoring 用取消监控包装流。
// 当 immediateChan 触发（CancelImmediate 或超时升级）时，输出流会以 ErrStreamCanceled 终止。
func wrapStreamWithCancelMonitoring[T any](stream *schema.StreamReader[T], cc *cancelContext) *schema.StreamReader[T] {
	if cc == nil {
		return stream
	}

	// Already canceled — terminate immediately
	// 已取消——立即终止
	select {
	case <-cc.immediateChan:
		stream.Close()
		r, w := schema.Pipe[T](1)
		var zero T
		w.Send(zero, ErrStreamCanceled)
		w.Close()
		return r
	default:
	}

	reader, writer := schema.Pipe[T](1)

	go func() {
		done := make(chan struct{})
		defer close(done)
		defer writer.Close()
		defer stream.Close()

		ch := make(chan recvResult[T])
		go func() {
			defer close(ch)
			for {
				chunk, recvErr := stream.Recv()
				select {
				case ch <- recvResult[T]{chunk, recvErr}:
				case <-done:
					return
				}
				if recvErr != nil {
					return
				}
			}
		}()

		for {
			select {
			case <-cc.immediateChan:
				var zero T
				writer.Send(zero, ErrStreamCanceled)
				return

			case r, ok := <-ch:
				if !ok {
					return
				}
				if r.err != nil {
					if r.err == io.EOF {
						return
					}
					var zero T
					writer.Send(zero, r.err)
					return
				}
				if closed := writer.Send(r.data, nil); closed {
					return
				}
			}
		}
	}()

	return reader
}

// cancelMonitoredToolHandler wraps streamable tool calls with cancel monitoring.
// When CancelImmediate fires, the tool output stream is terminated with ErrStreamCanceled.
// This handler reads the cancelContext from the Go context via getCancelContext.
//
// cancelMonitoredToolHandler 用取消监控包装可流式工具调用。
// 当 CancelImmediate 触发时，工具输出流会以 ErrStreamCanceled 终止。
// 该处理器通过 getCancelContext 从 Go context 读取 cancelContext。
type cancelMonitoredToolHandler struct{}

func (h *cancelMonitoredToolHandler) WrapStreamableToolCall(next compose.StreamableToolEndpoint) compose.StreamableToolEndpoint {
	return func(ctx context.Context, input *compose.ToolInput) (*compose.StreamToolOutput, error) {
		output, err := next(ctx, input)
		if err != nil {
			return nil, err
		}

		cc := getCancelContext(ctx)
		if cc == nil {
			return output, nil
		}

		wrapped := wrapStreamWithCancelMonitoring(output.Result, cc)
		return &compose.StreamToolOutput{Result: wrapped}, nil
	}
}

func (h *cancelMonitoredToolHandler) WrapEnhancedStreamableToolCall(next compose.EnhancedStreamableToolEndpoint) compose.EnhancedStreamableToolEndpoint {
	return func(ctx context.Context, input *compose.ToolInput) (*compose.EnhancedStreamableToolOutput, error) {
		output, err := next(ctx, input)
		if err != nil {
			return nil, err
		}

		cc := getCancelContext(ctx)
		if cc == nil {
			return output, nil
		}

		wrapped := wrapStreamWithCancelMonitoring(output.Result, cc)
		return &compose.EnhancedStreamableToolOutput{Result: wrapped}, nil
	}
}
