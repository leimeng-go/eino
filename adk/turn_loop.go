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
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/eino/internal/safe"
)

type stopPhase uint8

const (
	stopOpen stopPhase = iota
	stopIdleWaiting
	stopCommitted
)

type preemptTurnPhase uint8

const (
	preemptTurnIdle preemptTurnPhase = iota
	preemptTurnPlanning
	preemptTurnActive
)

func (p preemptTurnPhase) String() string {
	switch p {
	case preemptTurnIdle:
		return "idle"
	case preemptTurnPlanning:
		return "planning"
	case preemptTurnActive:
		return "active"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

type preemptTurnSnapshot struct {
	hasTargetTurn bool
	turnID        uint64
	ctx           context.Context
	tc            any
}

type cancelRequestState struct {
	cfg             agentCancelConfig
	timeoutDeadline *time.Time
}

type preemptRequest struct {
	cancel   cancelRequestState
	ackChans []chan struct{}
}

func parseAgentCancelOptions(opts ...AgentCancelOption) agentCancelConfig {
	cfg := agentCancelConfig{Mode: CancelImmediate}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func newCancelRequestState(opts []AgentCancelOption, now time.Time) cancelRequestState {
	cfg := parseAgentCancelOptions(opts...)
	var deadline *time.Time
	if cfg.Timeout != nil && *cfg.Timeout > 0 && cfg.Mode != CancelImmediate {
		d := now.Add(*cfg.Timeout)
		deadline = &d
	}
	cfg.Timeout = nil

	return cancelRequestState{
		cfg:             cfg,
		timeoutDeadline: deadline,
	}
}

func (s *cancelRequestState) merge(opts []AgentCancelOption, now time.Time) {
	if opts == nil {
		return
	}

	next := newCancelRequestState(opts, now)
	if s.cfg.Mode == CancelImmediate || next.cfg.Mode == CancelImmediate {
		s.cfg.Mode = CancelImmediate
		s.timeoutDeadline = nil
	} else {
		s.cfg.Mode |= next.cfg.Mode
		if next.timeoutDeadline != nil {
			if s.timeoutDeadline == nil || next.timeoutDeadline.Before(*s.timeoutDeadline) {
				deadline := *next.timeoutDeadline
				s.timeoutDeadline = &deadline
			}
		}
	}
	if next.cfg.Recursive {
		s.cfg.Recursive = true
	}
}

func (s cancelRequestState) cancelOptions(now time.Time) []AgentCancelOption {
	cfg := s.cfg
	if cfg.Mode != CancelImmediate && s.timeoutDeadline != nil {
		remaining := s.timeoutDeadline.Sub(now)
		if remaining <= 0 {
			cfg.Mode = CancelImmediate
			cfg.Timeout = nil
		} else {
			cfg.Timeout = &remaining
		}
	}

	opts := []AgentCancelOption{WithAgentCancelMode(cfg.Mode)}
	if cfg.Recursive {
		opts = append(opts, WithRecursive())
	}
	if cfg.Timeout != nil {
		opts = append(opts, WithAgentCancelTimeout(*cfg.Timeout))
	}
	return opts
}

func newPreemptRequest(ack chan struct{}, opts []AgentCancelOption, now time.Time) *preemptRequest {
	req := &preemptRequest{cancel: newCancelRequestState(opts, now)}
	if ack != nil {
		req.ackChans = append(req.ackChans, ack)
	}
	return req
}

func (r *preemptRequest) ack() {
	if r == nil {
		return
	}
	for _, ack := range r.ackChans {
		close(ack)
	}
	r.ackChans = nil
}

func (r *preemptRequest) merge(ack chan struct{}, opts []AgentCancelOption, now time.Time) {
	if ack != nil {
		r.ackChans = append(r.ackChans, ack)
	}
	r.cancel.merge(opts, now)
}

func (r *preemptRequest) cancelOptions(now time.Time) []AgentCancelOption {
	if r == nil {
		return nil
	}
	return r.cancel.cancelOptions(now)
}

// preemptController owns turn-targeted preempt requests and Push critical sections.
//
// Turn lifecycle:
//
//	idle ──beginPlanningTurn──▶ planning ──beginActiveTurn──▶ active ──endActiveTurn──▶ idle
//	                              │                                                      ▲
//	                              └────────abortPlanningTurn─────────────────────────────┘
//
// Push critical section (beginPush/endPush) overlaps with the turn lifecycle. The
// run loop calls waitForPushes before beginPlanningTurn to ensure no in-flight Push
// can observe stale turn state.
//
// Preempt request flow:
//   - Push captures a snapshot (turnID + hasTargetTurn) via beginPush.
//   - requestPreempt binds to the captured turnID; if the turn has moved on, the
//     request is resolved as a no-op.
//   - During active phase, receivePreempt transfers the pending request to the
//     watcher, which submits cancel and then acks.
//
// preemptController 负责面向特定轮次的 preempt 请求和 Push 临界区。
// 轮次生命周期：
// idle ──beginPlanningTurn──▶ planning ──beginActiveTurn──▶ active ──endActiveTurn──▶ idle
// │                                                      ▲
// └────────abortPlanningTurn─────────────────────────────┘
// Push 临界区（beginPush/endPush）与轮次生命周期重叠。run loop 会在 beginPlanningTurn 前调用 waitForPushes，以确保没有进行中的 Push 能观察到过期的轮次状态。
// Preempt 请求流程：
// - Push 通过 beginPush 捕获快照（turnID + hasTargetTurn）。
// - requestPreempt 绑定到捕获的 turnID；如果轮次已经推进，该请求会作为 no-op 完成。
// - 在 active 阶段，receivePreempt 将待处理请求转交给 watcher，watcher 提交 cancel 后再 ack。
type preemptController struct {
	mu   sync.Mutex
	cond *sync.Cond

	turnPhase     preemptTurnPhase
	turnID        uint64
	currentTC     any
	currentRunCtx context.Context

	pushInFlight int
	pending      *preemptRequest
	notify       chan struct{}
	closed       bool
}

func newPreemptController() *preemptController {
	c := &preemptController{notify: make(chan struct{}, 1)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *preemptController) beginPlanningTurn() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnIdle, "beginPlanningTurn")
	c.requireNoPendingLocked("beginPlanningTurn")
	c.turnID++
	c.turnPhase = preemptTurnPlanning
	c.currentRunCtx = nil
	c.currentTC = nil
}

func (c *preemptController) abortPlanningTurn() *preemptRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnPlanning, "abortPlanningTurn")
	c.turnPhase = preemptTurnIdle
	c.currentRunCtx = nil
	c.currentTC = nil
	req := c.pending
	c.pending = nil
	c.cond.Broadcast()
	return req
}

func (c *preemptController) beginActiveTurn(ctx context.Context, tc any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnPlanning, "beginActiveTurn")
	c.turnPhase = preemptTurnActive
	c.currentRunCtx = ctx
	c.currentTC = tc
	if c.pending != nil {
		c.notifyWatcherLocked()
	}
}

func (c *preemptController) endActiveTurn() *preemptRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnActive, "endActiveTurn")
	c.turnPhase = preemptTurnIdle
	c.currentRunCtx = nil
	c.currentTC = nil
	req := c.pending
	c.pending = nil
	c.cond.Broadcast()
	return req
}

func (c *preemptController) requirePhaseLocked(expected preemptTurnPhase, op string) {
	if c.turnPhase != expected {
		panic(fmt.Sprintf("adk: preemptController.%s called while turn phase is %s; expected %s", op, c.turnPhase, expected))
	}
}

func (c *preemptController) requireNoPendingLocked(op string) {
	if c.pending != nil {
		panic(fmt.Sprintf("adk: preemptController.%s called with stale pending preempt request", op))
	}
}

func (c *preemptController) beginPush() preemptTurnSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pushInFlight++
	return preemptTurnSnapshot{
		hasTargetTurn: c.turnPhase == preemptTurnPlanning || c.turnPhase == preemptTurnActive,
		turnID:        c.turnID,
		ctx:           c.currentRunCtx,
		tc:            c.currentTC,
	}
}

func (c *preemptController) endPush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pushInFlight--
	if c.pushInFlight < 0 {
		panic("adk: preemptController.endPush called without matching beginPush")
	}
	c.cond.Broadcast()
}

func (c *preemptController) waitForPushes() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.pushInFlight > 0 {
		c.cond.Wait()
	}
}

func (c *preemptController) requestPreempt(target preemptTurnSnapshot, ack chan struct{}, opts ...AgentCancelOption) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || !target.hasTargetTurn || c.turnPhase == preemptTurnIdle || c.turnID != target.turnID {
		if ack != nil {
			close(ack)
		}
		return
	}

	now := time.Now()
	if c.pending == nil {
		c.pending = newPreemptRequest(ack, opts, now)
	} else {
		c.pending.merge(ack, opts, now)
	}
	if c.turnPhase == preemptTurnActive {
		c.notifyWatcherLocked()
	}
}

func (c *preemptController) receivePreempt() (*preemptRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.turnPhase != preemptTurnActive || c.pending == nil {
		return nil, false
	}
	req := c.pending
	c.pending = nil
	return req, true
}

func (c *preemptController) closeForLoopExit() {
	c.mu.Lock()
	c.closed = true
	c.turnPhase = preemptTurnIdle
	c.currentRunCtx = nil
	c.currentTC = nil
	req := c.pending
	c.pending = nil
	select {
	case <-c.notify:
	default:
	}
	c.cond.Broadcast()
	c.mu.Unlock()

	req.ack()
}

func (c *preemptController) notifyWatcherLocked() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

type stopDecision struct {
	commit   bool
	wakeIdle bool
}

type stopCancelRequest struct {
	cancel cancelRequestState
}

func newStopCancelRequest(opts []AgentCancelOption, now time.Time) *stopCancelRequest {
	return &stopCancelRequest{cancel: newCancelRequestState(opts, now)}
}

func (r *stopCancelRequest) merge(opts []AgentCancelOption, now time.Time) {
	if r == nil {
		return
	}
	r.cancel.merge(opts, now)
}

func (r *stopCancelRequest) cancelOptions(now time.Time) []AgentCancelOption {
	if r == nil {
		return nil
	}
	return r.cancel.cancelOptions(now)
}

// stopController owns global Stop state and optional active-turn cancel requests.
//
// Stop has two independent layers:
//   - terminal loop intent: committed Stop prevents future turns and closes the buffer;
//   - optional active-turn cancel: cancel-capable Stop calls create a pending request
//     consumed by the watcher if the current turn is still active.
//
// Unlike preempt, Stop is not bound to a turnID. It is global and terminal.
// A pending cancel request is consumed by the active turn or dropped when that
// turn ends before consumption.
//
// stopController 负责全局 Stop 状态和可选的 active-turn cancel 请求。
// Stop 有两个独立层次：
// - 终止性循环意图：已提交的 Stop 会阻止后续轮次并关闭 buffer；
// - 可选的 active-turn cancel：支持 cancel 的 Stop 调用会创建一个待处理请求，若当前轮次仍处于 active，则由 watcher 消费。
// 与 preempt 不同，Stop 不绑定到 turnID。它是全局且终止性的。
// 待处理的 cancel 请求会被 active 轮次消费，或在该轮次消费前结束时被丢弃。
type stopController struct {
	mu sync.Mutex

	phase stopPhase

	hasActiveCancelTarget bool
	pending               *stopCancelRequest
	notify                chan struct{}

	idleFor        time.Duration
	skipCheckpoint bool
	stopCause      string

	closed bool
}

func newStopController() *stopController {
	return &stopController{notify: make(chan struct{}, 1)}
}

func (c *stopController) requestStop(cfg *stopConfig) stopDecision {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return stopDecision{}
	}
	if cfg.skipCheckpoint {
		c.skipCheckpoint = true
	}
	if cfg.stopCause != "" && c.stopCause == "" {
		c.stopCause = cfg.stopCause
	}
	if cfg.idleFor > 0 {
		if c.phase != stopCommitted && c.idleFor == 0 {
			c.phase = stopIdleWaiting
			c.idleFor = cfg.idleFor
		}
		return stopDecision{wakeIdle: c.phase == stopIdleWaiting}
	}

	committed := c.commitLocked()
	if cfg.agentCancelOpts != nil {
		now := time.Now()
		if c.pending == nil {
			c.pending = newStopCancelRequest(cfg.agentCancelOpts, now)
		} else {
			c.pending.merge(cfg.agentCancelOpts, now)
		}
		if c.hasActiveCancelTarget {
			c.notifyWatcherLocked()
		}
	}
	return stopDecision{commit: committed}
}

func (c *stopController) commit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitLocked()
}

func (c *stopController) commitLocked() bool {
	if c.closed || c.phase == stopCommitted {
		return false
	}
	c.phase = stopCommitted
	c.idleFor = 0
	return true
}

func (c *stopController) isCommitted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == stopCommitted
}

func (c *stopController) idleDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != stopIdleWaiting {
		return 0
	}
	return c.idleFor
}

func (c *stopController) skipCheckpointEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.skipCheckpoint
}

func (c *stopController) cause() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCause
}

func (c *stopController) beginActiveTurn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.hasActiveCancelTarget = true
	if c.pending != nil {
		c.notifyWatcherLocked()
	}
}

func (c *stopController) endActiveTurn() *stopCancelRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hasActiveCancelTarget = false
	req := c.pending
	c.pending = nil
	return req
}

func (c *stopController) receiveCancel() (*stopCancelRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasActiveCancelTarget || c.pending == nil {
		return nil, false
	}
	req := c.pending
	c.pending = nil
	return req, true
}

func (c *stopController) closeForLoopExit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.hasActiveCancelTarget = false
	c.pending = nil
	select {
	case <-c.notify:
	default:
	}
}

func (c *stopController) notifyWatcherLocked() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// TurnLoopConfig is the configuration for creating a TurnLoop.
// TurnLoopConfig 是创建 TurnLoop 的配置。
type TurnLoopConfig[T any, M MessageType] struct {
	// GenInput receives the TurnLoop instance and all buffered items, and decides what to process.
	// It returns which items to consume now vs keep for later turns.
	// The loop parameter allows calling Push() or Stop() directly from within the callback.
	// Required.
	//
	// GenInput 接收 TurnLoop 实例和所有已缓冲项，并决定要处理哪些内容。
	// 它返回哪些项现在消费、哪些项保留到后续轮次。
	// loop 参数允许在回调内直接调用 Push() 或 Stop()。
	// 必需。
	GenInput func(ctx context.Context, loop *TurnLoop[T, M], items []T) (*GenInputResult[T, M], error)

	// GenResume is called at most once during Run(). When CheckpointID is
	// configured, Run() queries Store for the checkpoint:
	//   - If the checkpoint contains runner state (i.e. an agent was interrupted
	//     or canceled mid-turn), Run() calls GenResume to plan a resume turn.
	//   - Otherwise (no checkpoint, or between-turns checkpoint), GenResume is
	//     never called and the loop proceeds via GenInput.
	//
	// It receives:
	//   - interruptedItems: the items being processed when the prior run was interrupted / canceled
	//   - unhandledItems: items buffered but not processed when the prior run exited
	//   - newItems: items that were Push()-ed before Run() was called
	//
	// It returns a GenResumeResult describing how to resume the interrupted agent
	// turn (optional ResumeParams) and how to manipulate the buffer
	// (Consumed/Remaining) before continuing.
	//
	// GenResume 在 Run() 期间最多调用一次。配置 CheckpointID 时，Run() 会向 Store 查询该 checkpoint：
	// - 如果 checkpoint 包含 runner 状态（即 agent 在轮次中途被中断或取消），Run() 会调用 GenResume 来规划恢复轮次。
	// - 否则（无 checkpoint，或轮次之间的 checkpoint），不会调用 GenResume，循环会通过 GenInput 继续。
	// 它接收：
	// - interruptedItems：上次运行被 interrupted / canceled 时正在处理的项
	// - unhandledItems：上次运行退出时已缓冲但未处理的项
	// - newItems：Run() 调用前通过 Push() 推入的项
	// 它返回 GenResumeResult，描述如何恢复被中断的 agent 轮次（可选 ResumeParams），以及继续前如何操作 buffer（Consumed/Remaining）。
	GenResume func(ctx context.Context, loop *TurnLoop[T, M], interruptedItems, unhandledItems, newItems []T) (*GenResumeResult[T, M], error)

	// PrepareAgent returns an Agent configured to handle the consumed items.
	// This callback should set up the agent with appropriate system prompt,
	// tools, and middlewares based on what items are being processed.
	// Called once per turn with the items that GenInput decided to consume.
	// The loop parameter allows calling Push() or Stop() directly from within the callback.
	// Required.
	//
	// PrepareAgent 返回配置好的 Agent，用于处理已消费的项。
	// 此回调应根据正在处理的项，为 agent 设置合适的 system prompt、tools 和 middlewares。
	// 每个轮次调用一次，传入 GenInput 决定消费的项。
	// loop 参数允许在回调内直接调用 Push() 或 Stop()。
	// 必需。
	PrepareAgent func(ctx context.Context, loop *TurnLoop[T, M], consumed []T) (TypedAgent[M], error)

	// OnAgentEvents is called to handle events emitted by the agent.
	// The TurnContext provides per-turn info and control:
	//   - tc.Consumed: items that triggered this agent execution
	//   - tc.Loop: allows calling Push() or Stop() directly from within the callback
	//   - tc.Preempted / tc.Stopped: signals while processing events
	//
	// Error handling: the returned error is only used when the callback itself
	// wants to abort the TurnLoop. The callback should NEVER propagate
	// CancelError — the framework handles it automatically:
	//   - Stop: the framework propagates CancelError as ExitReason, loop exits.
	//   - Preempt: the framework does not propagate CancelError; if the callback
	//     also returns nil, the loop continues with the next turn.
	// In practice, return a non-nil error only for callback-internal failures
	// that should terminate the loop.
	//
	// Optional. If not provided, events are drained and the first error
	// (including CancelError from Stop) is returned as ExitReason.
	//
	// OnAgentEvents 用于处理 agent 发出的事件。
	// TurnContext 提供每轮次的信息和控制：
	// - tc.Consumed：触发本次 agent 执行的项
	// - tc.Loop：允许在回调内直接调用 Push() 或 Stop()
	// - tc.Preempted / tc.Stopped：处理事件时的信号
	// 错误处理：返回的 error 仅在回调自身想中止 TurnLoop 时使用。回调绝不应传播 CancelError —— 框架会自动处理：
	// - Stop：框架将 CancelError 作为 ExitReason 传播，循环退出。
	// - Preempt：框架不传播 CancelError；如果回调也返回 nil，循环会进入下一轮。
	// 实践中，仅在应终止循环的回调内部失败时返回非 nil error。
	// 可选。若未提供，事件会被 drain，并将第一个 error（包括来自 Stop 的 CancelError）作为 ExitReason 返回。
	OnAgentEvents func(ctx context.Context, tc *TurnContext[T, M], events *AsyncIterator[*TypedAgentEvent[M]]) error

	// Store is the checkpoint store for persistence and resume. Optional.
	// When set together with CheckpointID, enables automatic checkpoint-based resume.
	// The TurnLoop always persists both runner checkpoint bytes and item bookkeeping
	// (InterruptedItems, UnhandledItems) via gob encoding, so T must be gob-encodable
	// when Store is used.
	//
	// Store 是用于持久化和恢复的 checkpoint store。可选。
	// 与 CheckpointID 一起设置时，启用基于 checkpoint 的自动恢复。
	// TurnLoop 始终通过 gob 编码持久化 runner checkpoint bytes 和项记账信息（InterruptedItems、UnhandledItems），因此使用 Store 时 T 必须可被 gob 编码。
	Store CheckPointStore

	// CheckpointID, when set together with Store, enables automatic
	// checkpoint-based resume. On Run(), the TurnLoop queries Store for this ID:
	//   - If a checkpoint exists with runner state (mid-turn interrupt / cancel),
	//     GenResume is called to plan the resume turn.
	//   - If a checkpoint exists without runner state (between-turns),
	//     the stored unhandled items are buffered and the loop proceeds
	//     normally via GenInput.
	//   - If no checkpoint exists, the loop starts fresh.
	//
	// On exit, if the TurnLoop saved a new checkpoint, it is saved under this
	// same CheckpointID. On clean exit (no checkpoint saved), the existing
	// checkpoint under CheckpointID is deleted to prevent stale resumption.
	//
	// CheckpointID 与 Store 一起设置时，启用基于 checkpoint 的自动恢复。Run() 时，TurnLoop 会用此 ID 查询 Store：
	// - 如果存在包含 runner 状态的 checkpoint（轮次中途 interrupt / cancel），会调用 GenResume 来规划恢复轮次。
	// - 如果存在不含 runner 状态的 checkpoint（轮次之间），会缓冲已存储的未处理项，并正常通过 GenInput 继续循环。
	// - 如果不存在 checkpoint，循环从头开始。
	// 退出时，如果 TurnLoop 保存了新的 checkpoint，会使用同一个 CheckpointID 保存。正常退出（未保存 checkpoint）时，会删除 CheckpointID 下的现有 checkpoint，以避免过期恢复。
	CheckpointID string
}

// GenInputResult contains the result of GenInput processing.
// GenInputResult 包含 GenInput 处理的结果。
type GenInputResult[T any, M MessageType] struct {
	// RunCtx, if non-nil, overrides the context for this turn's execution
	// (PrepareAgent, agent run, OnAgentEvents).
	//
	// Must be derived from the ctx passed to GenInput to preserve the
	// TurnLoop's cancellation semantics and inherited values. For example:
	//
	//   runCtx := context.WithValue(ctx, traceKey{}, extractTraceID(items))
	//   return &GenInputResult[T]{RunCtx: runCtx, ...}, nil
	//
	// If nil, the TurnLoop's context is used unchanged.
	//
	// RunCtx 若非 nil，会覆盖本轮次执行（PrepareAgent、agent run、OnAgentEvents）的 context。
	// 必须派生自传给 GenInput 的 ctx，以保留 TurnLoop 的取消语义和继承值。例如：
	// runCtx := context.WithValue(ctx, traceKey{}, extractTraceID(items))
	// return &GenInputResult[T]{RunCtx: runCtx, ...}, nil
	// 若为 nil，则原样使用 TurnLoop 的 context。
	RunCtx context.Context

	// Input is the agent input to execute
	// Input 是要执行的 agent input
	Input *TypedAgentInput[M]

	// RunOpts are the options for this agent run.
	// Note: do not pass WithCheckPointID here; the TurnLoop automatically
	// injects the checkpointID into the Runner.
	//
	// RunOpts 是本次 agent run 的选项。
	// 注意：不要在这里传入 WithCheckPointID；TurnLoop 会自动将 checkpointID 注入 Runner。
	RunOpts []AgentRunOption

	// Consumed are the items selected for this turn.
	// They are removed from the buffer and passed to PrepareAgent.
	//
	// Consumed 是本轮次选择的项。
	// 它们会从 buffer 中移除，并传给 PrepareAgent。
	Consumed []T

	// Remaining are the items to keep in the buffer for a future turn.
	// TurnLoop pushes Remaining back into the buffer before running the agent.
	//
	// Items from the GenInput input slice that are in neither Consumed nor Remaining
	// are dropped by the loop.
	//
	// Remaining 是保留到未来轮次的项。
	// TurnLoop 会在运行 agent 前将 Remaining 推回 buffer。
	// GenInput 输入切片中既不在 Consumed 也不在 Remaining 的项，会被循环丢弃。
	Remaining []T
}

// GenResumeResult contains the result of GenResume processing.
// GenResumeResult 包含 GenResume 处理的结果。
type GenResumeResult[T any, M MessageType] struct {
	// RunCtx, if non-nil, overrides the context for this resumed turn's execution
	// (PrepareAgent, agent resume, OnAgentEvents).
	//
	// RunCtx 若非 nil，则覆盖本次恢复回合执行所用的 context
	// （PrepareAgent、agent resume、OnAgentEvents）。
	RunCtx context.Context

	// RunOpts are the options for this agent resume run.
	// Note: do not pass WithCheckPointID here; the TurnLoop automatically
	// injects the checkpointID into the Runner.
	//
	// RunOpts 是本次 agent resume 运行的选项。
	// 注意：不要在这里传入 WithCheckPointID；TurnLoop 会自动将 checkpointID 注入 Runner。
	RunOpts []AgentRunOption

	// ResumeParams are optional parameters for resuming an interrupted agent.
	// ResumeParams 是恢复被中断智能体时的可选参数。
	ResumeParams *ResumeParams

	// Consumed are the items selected for this resumed turn.
	// They are removed from the buffer and passed to PrepareAgent.
	//
	// Consumed 是本次恢复回合选中的条目。
	// 它们会从缓冲区移除并传给 PrepareAgent。
	Consumed []T

	// Remaining are the items to keep in the buffer for a future turn.
	// TurnLoop pushes Remaining back into the buffer before resuming the agent.
	//
	// Items from (interruptedItems, unhandledItems, newItems) that are in neither Consumed
	// nor Remaining are dropped by the loop.
	//
	// Remaining 是要保留在缓冲区中供未来回合使用的条目。
	// TurnLoop 会在恢复智能体前将 Remaining 推回缓冲区。
	// 来自 (interruptedItems, unhandledItems, newItems) 但既不在 Consumed
	// 也不在 Remaining 中的条目会被循环丢弃。
	Remaining []T
}

type turnRunSpec[T any, M MessageType] struct {
	runCtx       context.Context
	input        *TypedAgentInput[M]
	runOpts      []AgentRunOption
	resumeParams *ResumeParams
	isResume     bool
	consumed     []T
	resumeBytes  []byte
}

type turnPlan[T any, M MessageType] struct {
	turnCtx   context.Context
	remaining []T
	spec      *turnRunSpec[T, M]
}

func (l *TurnLoop[T, M]) planTurn(
	ctx context.Context,
	isResume bool,
	items []T,
	pr *turnLoopPendingResume[T],
) (*turnPlan[T, M], error) {
	if !isResume {
		result, err := l.config.GenInput(ctx, l, items)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, errors.New("GenInputResult is nil")
		}
		if result.Input == nil {
			return nil, errors.New("agent input is nil")
		}
		turnCtx := ctx
		if result.RunCtx != nil {
			turnCtx = result.RunCtx
		}
		return &turnPlan[T, M]{
			turnCtx:   turnCtx,
			remaining: result.Remaining,
			spec: &turnRunSpec[T, M]{
				runCtx:   result.RunCtx,
				input:    result.Input,
				runOpts:  result.RunOpts,
				consumed: result.Consumed,
			},
		}, nil
	}
	if pr == nil {
		return nil, errors.New("resume payload is nil")
	}
	if l.config.GenResume == nil {
		return nil, errors.New("GenResume is required for resume")
	}
	resumeResult, err := l.config.GenResume(ctx, l, pr.interrupted, pr.unhandled, pr.newItems)
	if err != nil {
		return nil, err
	}
	if resumeResult == nil {
		return nil, errors.New("GenResumeResult is nil")
	}
	turnCtx := ctx
	if resumeResult.RunCtx != nil {
		turnCtx = resumeResult.RunCtx
	}
	return &turnPlan[T, M]{
		turnCtx:   turnCtx,
		remaining: resumeResult.Remaining,
		spec: &turnRunSpec[T, M]{
			runCtx:       resumeResult.RunCtx,
			runOpts:      resumeResult.RunOpts,
			resumeParams: resumeResult.ResumeParams,
			isResume:     true,
			consumed:     resumeResult.Consumed,
			resumeBytes:  pr.resumeBytes,
		},
	}, nil
}

// InterruptError is the ExitReason when the TurnLoop exits due to a business
// interrupt (AgentAction.Interrupted). It carries InterruptContexts needed for
// targeted resumption via ResumeParams, parallel to CancelError.
//
// Unlike CancelError (which indicates forceful cancellation), InterruptError
// indicates the agent voluntarily paused execution at a business-defined point.
//
// 当 TurnLoop 因业务中断（AgentAction.Interrupted）退出时，InterruptError 是其 ExitReason。
// 它携带通过 ResumeParams 进行定向恢复所需的 InterruptContexts，与 CancelError 并行。
// 不同于 CancelError（表示强制取消），InterruptError 表示智能体在业务定义的点自愿暂停执行。
type InterruptError struct {
	// InterruptContexts provides the interrupt contexts needed for targeted
	// resumption via ResumeParams. Each context represents a step in the agent
	// hierarchy that was interrupted. Use each InterruptCtx.ID as a key in
	// ResumeParams.Targets.
	//
	// InterruptContexts 提供通过 ResumeParams 进行定向恢复所需的中断上下文。
	// 每个 context 表示被中断的智能体层级中的一个步骤。
	// 使用每个 InterruptCtx.ID 作为 ResumeParams.Targets 中的键。
	InterruptContexts []*InterruptCtx
}

func (e *InterruptError) Error() string {
	return fmt.Sprintf("agent interrupted: %d context(s)", len(e.InterruptContexts))
}

// TurnLoopExitState is returned when TurnLoop exits, containing the exit reason
// and any items that were not processed.
//
// TurnLoopExitState 在 TurnLoop 退出时返回，包含退出原因和所有未处理的条目。
type TurnLoopExitState[T any, M MessageType] struct {
	// ExitReason indicates why the loop exited.
	// nil means clean exit (Stop() was called without cancel options, or the
	// agent completed normally before Stop took effect).
	// Non-nil values include context errors, callback errors, *CancelError, etc.
	// When Stop(WithImmediate()) or Stop(WithGraceful()) cancels a running
	// agent, ExitReason will be a *CancelError.
	// This never contains checkpoint errors — see CheckpointErr for those.
	//
	// ExitReason 表示循环退出的原因。
	// nil 表示正常退出（调用了不带 cancel 选项的 Stop()，或智能体在 Stop 生效前正常完成）。
	// 非 nil 值包括 context 错误、回调错误、*CancelError 等。
	// 当 Stop(WithImmediate()) 或 Stop(WithGraceful()) 取消正在运行的智能体时，ExitReason 将是 *CancelError。
	// 这里绝不会包含 checkpoint 错误 —— 相关错误请见 CheckpointErr。
	ExitReason error

	// UnhandledItems contains items that were buffered but not processed.
	// These are items for which Push returned true but were never consumed by a turn.
	// This is always valid regardless of ExitReason.
	//
	// UnhandledItems 包含已缓冲但未处理的条目。
	// 这些条目是 Push 返回 true 但从未被任何回合消费的条目。
	// 无论 ExitReason 如何，该字段始终有效。
	UnhandledItems []T

	// InterruptedItems contains the items whose turn was interrupted — either by
	// a cancel (Stop with cancel options → *CancelError) or by a business
	// interrupt (AgentAction.Interrupted → *InterruptError).
	// On resume, these are passed to GenResume's interruptedItems parameter.
	//
	// InterruptedItems 包含其回合被中断的条目 —— 可能是由 cancel（带 cancel 选项的 Stop → *CancelError）引起，
	// 也可能是由业务中断（AgentAction.Interrupted → *InterruptError）引起。
	// 恢复时，这些条目会传给 GenResume 的 interruptedItems 参数。
	InterruptedItems []T

	// StopCause is the business-supplied reason passed via WithStopCause.
	// Empty if Stop was not called or no cause was provided.
	//
	// StopCause 是通过 WithStopCause 传入的业务侧原因。
	// 如果未调用 Stop 或未提供原因，则为空。
	StopCause string

	// CheckpointAttempted indicates whether a checkpoint save was attempted when the loop exited.
	// True when Store is configured, CheckpointID is set, the loop was not idle
	// at exit time, WithSkipCheckpoint was not used, and the exit was caused by
	// Stop() (clean or cancel) or a business interrupt (*InterruptError).
	//
	// CheckpointAttempted 表示循环退出时是否尝试保存检查点。
	// 当配置了 Store、设置了 CheckpointID、循环在退出时非空闲、未使用 WithSkipCheckpoint，且退出由
	// Stop()（正常或 cancel）或业务中断（*InterruptError）导致时，为 true。
	CheckpointAttempted bool

	// CheckpointErr is the error from checkpoint save, if any.
	// nil when CheckpointAttempted is false (no attempt was made) or when the save succeeded.
	//
	// CheckpointErr 是检查点保存产生的错误（如果有）。
	// 当 CheckpointAttempted 为 false（未尝试）或保存成功时为 nil。
	CheckpointErr error

	// TakeLateItems returns items that were pushed after the loop stopped
	// (i.e., Push returned false for these items). These items are NOT included
	// in the checkpoint.
	//
	// This function is idempotent: the first call computes and caches the result;
	// subsequent calls return the same slice.
	//
	// After TakeLateItems is called, any subsequent Push() will panic to
	// prevent items from being silently lost.
	//
	// It is safe to call TakeLateItems from any goroutine after Wait() returns.
	// If TakeLateItems is never called, late items are simply garbage collected.
	//
	// TakeLateItems 返回循环停止后推入的条目
	// （即这些条目的 Push 返回 false）。这些条目不会包含在检查点中。
	// 此函数是幂等的：首次调用会计算并缓存结果；
	// 后续调用返回相同的 slice。
	// 调用 TakeLateItems 后，任何后续 Push() 都会 panic，
	// 以防止条目被静默丢失。
	// Wait() 返回后，可安全地从任何 goroutine 调用 TakeLateItems。
	// 如果从未调用 TakeLateItems，late items 会被直接垃圾回收。
	TakeLateItems func() []T
}

// TurnContext provides per-turn context to the OnAgentEvents callback.
// TurnContext 为 OnAgentEvents 回调提供每回合 context。
type TurnContext[T any, M MessageType] struct {
	// Loop is the TurnLoop instance, allowing Push() or Stop() calls.
	// Loop 是 TurnLoop 实例，允许调用 Push() 或 Stop()。
	Loop *TurnLoop[T, M]

	// Consumed contains items that triggered this agent execution.
	// Consumed 包含触发本次智能体执行的条目。
	Consumed []T

	// Preempted is closed when a preempt signal fires for the current turn
	// (via Push with WithPreempt/WithPreemptTimeout) and at least one
	// preemptive Push contributed to the CancelError for the current turn.
	// "Contributed" means the preempt's cancel options were included in the
	// CancelError before it was finalized. Remains open if no preempt contributed.
	// Use in a select to detect preemption while processing events.
	//
	// Both Preempted and Stopped may be closed within the same turn if both
	// signals arrive while the agent is still being cancelled. Whichever
	// arrives after the cancel is fully handled will not contribute.
	//
	// 当当前回合触发抢占信号（通过带 WithPreempt/WithPreemptTimeout 的 Push）且至少一个抢占式 Push
	// 参与生成当前回合的 CancelError 时，Preempted 会被关闭。
	// “参与”表示该抢占的 cancel 选项在 CancelError 最终确定前已被包含其中。
	// 如果没有抢占参与，则保持打开。
	// 在 select 中使用它可在处理事件时检测抢占。
	// 如果智能体仍在被取消时两个信号都到达，Preempted 和 Stopped 可能在同一回合内都被关闭。
	// 无论哪个信号在 cancel 完全处理后到达，都不会参与。
	Preempted <-chan struct{}

	// Stopped is closed when a Stop() call contributed to the CancelError for the
	// current turn.
	// "Contributed" means Stop's cancel options were included in the CancelError
	// before it was finalized. Remains open if Stop did not contribute.
	// Use in a select to detect stop while processing events.
	//
	// See Preempted for the relationship between the two channels.
	//
	// Stopped 会在 Stop() 调用参与生成当前轮次的 CancelError 时关闭。
	// “参与”表示 Stop 的取消选项在 CancelError 最终确定前已被包含。若 Stop 未参与，则保持打开。
	// 可在 select 中使用，以便在处理事件时检测停止。
	// 参见 Preempted 了解这两个 channel 的关系。
	Stopped <-chan struct{}

	// StopCause returns the business-supplied reason from WithStopCause.
	// This value is only meaningful after the Stopped channel is closed.
	// Before that, it returns an empty string.
	//
	// StopCause 返回 WithStopCause 提供的业务原因。
	// 该值仅在 Stopped channel 关闭后才有意义。
	// 在此之前返回空字符串。
	StopCause func() string
}

// TurnLoop is a push-based event loop for agent execution.
// Users push items via Push() and the loop processes them through the agent.
//
// Create with NewTurnLoop, then start with Run:
//
//	loop := NewTurnLoop(cfg)
//	// pass loop to other components, push initial items, etc.
//	loop.Run(ctx)
//
// # Permissive API
//
// All methods are valid on a not-yet-running loop:
//   - Push: items are buffered and will be processed once Run is called.
//   - Stop: sets the stopped flag; a subsequent Run will exit immediately.
//   - Wait: blocks until Run is called AND the loop exits. If Run is never
//     called, Wait blocks forever (this is a programming error, analogous
//     to reading from a channel that nobody writes to).
//
// TurnLoop 是用于智能体执行的基于 push 的事件循环。
// 用户通过 Push() 推入条目，循环会通过智能体处理它们。
// 使用 NewTurnLoop 创建，然后用 Run 启动：
// loop := NewTurnLoop(cfg)
// pass loop to other components, push initial items, etc.
// loop.Run(ctx)
// # 宽松 API
// 所有方法都可在尚未运行的 loop 上调用：
// - Push：条目会被缓冲，并在调用 Run 后处理。
// - Stop：设置 stopped 标志；后续 Run 会立即退出。
// - Wait：阻塞直到 Run 被调用且 loop 退出。如果 Run 从未被调用，Wait 会永久阻塞（这是编程错误，类似于从无人写入的 channel 中读取）。
type TurnLoop[T any, M MessageType] struct {
	config TurnLoopConfig[T, M]

	buffer *turnBuffer[T]

	stopped int32
	started int32

	done chan struct{}

	result *TurnLoopExitState[T, M]

	runOnce sync.Once

	stopCtrl *stopController

	preemptCtrl *preemptController

	runErr error

	interruptedItems []T

	checkPointRunnerBytes []byte
	interruptContexts     []*InterruptCtx
	capturedCancelErr     *CancelError

	pendingResume *turnLoopPendingResume[T]

	loadCheckpointID string

	onAgentEvents func(ctx context.Context, tc *TurnContext[T, M], events *AsyncIterator[*TypedAgentEvent[M]]) error

	lateMu     sync.Mutex
	lateItems  []T
	lateSealed bool
}

func (l *TurnLoop[T, M]) appendLate(item T) {
	l.lateMu.Lock()
	defer l.lateMu.Unlock()
	if l.lateSealed {
		panic("TurnLoop: Push called after TakeLateItems")
	}
	l.lateItems = append(l.lateItems, item)
}

type turnLoopCheckpoint[T any] struct {
	RunnerCheckpoint []byte
	// HasRunnerState reports whether RunnerCheckpoint contains resumable runner state.
	// It is false for "between turns" checkpoints where no agent execution was
	// interrupted (e.g. Stop() before the first turn or between turns).
	//
	// HasRunnerState 报告 RunnerCheckpoint 是否包含可恢复的 runner 状态。
	// 对于没有智能体执行被中断的“轮次之间”检查点为 false（例如第一次轮次前或轮次之间的 Stop()）。
	HasRunnerState bool
	UnhandledItems []T
	CanceledItems  []T // gob-compat: kept as CanceledItems for deserialization of existing checkpoints
	// gob 兼容：保留为 CanceledItems，用于反序列化已有检查点
}

func marshalTurnLoopCheckpoint[T any](c *turnLoopCheckpoint[T]) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalTurnLoopCheckpoint[T any](data []byte) (*turnLoopCheckpoint[T], error) {
	var c turnLoopCheckpoint[T]
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (l *TurnLoop[T, M]) saveTurnLoopCheckpoint(ctx context.Context, checkPointID string, c *turnLoopCheckpoint[T]) error {
	if l.config.Store == nil {
		return errors.New("checkpoint store is nil")
	}
	data, err := marshalTurnLoopCheckpoint(c)
	if err != nil {
		return err
	}
	return l.config.Store.Set(ctx, checkPointID, data)
}

func (l *TurnLoop[T, M]) deleteTurnLoopCheckpoint(ctx context.Context, checkPointID string) error {
	if l.config.Store == nil {
		return nil
	}
	if deleter, ok := l.config.Store.(CheckPointDeleter); ok {
		return deleter.Delete(ctx, checkPointID)
	}
	return nil
}

func (l *TurnLoop[T, M]) tryLoadCheckpoint(ctx context.Context) error {
	checkPointID := l.config.CheckpointID
	if checkPointID == "" || l.config.Store == nil {
		return nil
	}

	l.loadCheckpointID = checkPointID

	data, existed, err := l.config.Store.Get(ctx, checkPointID)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint[%s]: %w", checkPointID, err)
	}
	if !existed {
		return nil
	}

	var cp *turnLoopCheckpoint[T]
	if len(data) == 0 {
		return nil
	}
	cp, err = unmarshalTurnLoopCheckpoint[T](data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal checkpoint[%s]: %w", checkPointID, err)
	}

	newItems := l.buffer.TakeAll()

	if cp.HasRunnerState {
		if len(cp.RunnerCheckpoint) == 0 {
			l.buffer.PushFront(newItems)
			return fmt.Errorf("checkpoint[%s] has runner state but bytes are empty", checkPointID)
		}
		l.pendingResume = &turnLoopPendingResume[T]{
			interrupted: append([]T{}, cp.CanceledItems...),
			unhandled:   append([]T{}, cp.UnhandledItems...),
			newItems:    append([]T{}, newItems...),
			resumeBytes: append([]byte{}, cp.RunnerCheckpoint...),
		}
	} else {
		items := make([]T, 0, len(cp.UnhandledItems)+len(newItems))
		items = append(items, cp.UnhandledItems...)
		items = append(items, newItems...)
		l.buffer.PushFront(items)
	}

	return nil
}

type turnLoopPendingResume[T any] struct {
	interrupted []T
	unhandled   []T
	newItems    []T
	resumeBytes []byte
}

// SafePoint describes at which boundary the agent may be cancelled.
// It is a bitmask: values can be combined with bitwise OR to accept multiple
// safe points (e.g. AfterToolCalls | AfterChatModel). Internally, SafePoint
// is translated to CancelMode via toCancelMode().
//
// SafePoint is used only in the preemption API (WithPreempt/WithPreemptTimeout).
// A key design constraint: preemption always targets a safe point — the user's
// intent is to cancel at a well-defined boundary, never to abort immediately.
// Immediate cancellation is only reachable as an automatic timeout escalation
// (via WithPreemptTimeout), not as a direct user choice. This is why SafePoint
// has no "immediate" value and why WithPreempt requires a non-zero SafePoint
// (panics otherwise).
//
// SafePoint 描述智能体可在哪个边界被取消。
// 它是一个 bitmask：可用按位 OR 组合多个值以接受多个安全点（例如 AfterToolCalls | AfterChatModel）。内部会通过 toCancelMode() 将 SafePoint 转换为 CancelMode。
// SafePoint 仅用于抢占 API（WithPreempt/WithPreemptTimeout）。
// 一个关键设计约束：抢占始终针对安全点——用户意图是在明确定义的边界取消，而不是立即中止。
// 立即取消只能作为自动超时升级（通过 WithPreemptTimeout）触发，不能由用户直接选择。因此 SafePoint 没有“immediate”值，且 WithPreempt 要求非零 SafePoint（否则 panic）。
type SafePoint int

const (
	// AfterChatModel allows the agent to finish the current chat-model
	// call before being cancelled.
	//
	// AfterChatModel 允许智能体在取消前完成当前 chat-model 调用。
	AfterChatModel SafePoint = 1 << iota
	// AfterToolCalls allows the agent to finish the current tool-call round
	// before being cancelled.
	//
	// AfterToolCalls 允许智能体在取消前完成当前工具调用轮次。
	AfterToolCalls
	// AnySafePoint is shorthand for AfterChatModel | AfterToolCalls.
	// AnySafePoint 是 AfterChatModel | AfterToolCalls 的简写。
	AnySafePoint = AfterChatModel | AfterToolCalls
)

func (sp SafePoint) toCancelMode() CancelMode {
	var mode CancelMode
	if sp&AfterToolCalls != 0 {
		mode |= CancelAfterToolCalls
	}
	if sp&AfterChatModel != 0 {
		mode |= CancelAfterChatModel
	}
	return mode
}

type stopConfig struct {
	agentCancelOpts []AgentCancelOption
	skipCheckpoint  bool
	stopCause       string
	idleFor         time.Duration
}

// StopOption is an option for Stop().
// StopOption 是 Stop() 的选项。
type StopOption func(*stopConfig)

// WithGraceful requests a graceful stop that waits at the nearest safe point
// (after tool calls or after a chat-model call) and propagates recursively to
// nested agents. It does not impose a time limit; use WithGracefulTimeout to
// add a grace period after which the stop escalates to immediate cancellation.
//
// WithGraceful and WithGracefulTimeout are mutually exclusive; if both are
// passed to the same Stop call, the last one wins.
//
// WithGraceful 请求优雅停止：等待最近的安全点（工具调用后或 chat-model 调用后），并递归传播到嵌套智能体。它不设置时间限制；可使用 WithGracefulTimeout 添加宽限期，超过后停止会升级为立即取消。
// WithGraceful 和 WithGracefulTimeout 互斥；若传给同一次 Stop 调用，最后一个生效。
func WithGraceful() StopOption {
	return func(cfg *stopConfig) {
		cfg.agentCancelOpts = []AgentCancelOption{
			WithAgentCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
			WithRecursive(),
		}
	}
}

// WithImmediate aborts the running agent turn as soon as possible.
// The agent is cancelled immediately without waiting for any safe point.
// Nested agents inside AgentTools will also receive the cancel signal
// and be torn down.
//
// This is the most aggressive stop mode — typically used when the caller
// wants to shut down the TurnLoop with no intention of resuming.
//
// WithImmediate 会尽快中止正在运行的智能体轮次。
// 智能体会立即被取消，不等待任何安全点。
// AgentTools 内的嵌套智能体也会收到取消信号并被拆除。
// 这是最激进的停止模式——通常用于调用方想关闭 TurnLoop 且无意恢复的场景。
func WithImmediate() StopOption {
	return func(cfg *stopConfig) {
		cfg.agentCancelOpts = []AgentCancelOption{
			WithRecursive(),
		}
	}
}

// WithGracefulTimeout is like WithGraceful but adds a grace period.
// If the agent has not reached a safe point within gracePeriod, the stop
// escalates to immediate cancellation.
//
// gracePeriod must be positive; passing a zero or negative duration panics.
//
// WithGraceful and WithGracefulTimeout are mutually exclusive; if both are
// passed to the same Stop call, the last one wins.
//
// WithGracefulTimeout 类似 WithGraceful，但会添加宽限期。
// 如果智能体在 gracePeriod 内未到达安全点，停止会升级为立即取消。
// gracePeriod 必须为正；传入零或负 duration 会 panic。
// WithGraceful 和 WithGracefulTimeout 互斥；若传给同一次 Stop 调用，最后一个生效。
func WithGracefulTimeout(gracePeriod time.Duration) StopOption {
	if gracePeriod <= 0 {
		panic("adk: WithGracefulTimeout: gracePeriod must be positive")
	}
	return func(cfg *stopConfig) {
		cfg.agentCancelOpts = []AgentCancelOption{
			WithAgentCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
			WithRecursive(),
			WithAgentCancelTimeout(gracePeriod),
		}
	}
}

// WithSkipCheckpoint tells the TurnLoop not to persist a checkpoint for this
// Stop call. Use this when the caller does not intend to resume in the future.
// The flag is sticky: once any Stop() call sets it, subsequent calls cannot undo it.
//
// WithSkipCheckpoint 告诉 TurnLoop 不要为此次 Stop 调用持久化检查点。
// 当调用方以后不打算恢复时使用。
// 该标志是粘性的：一旦任何 Stop() 调用设置了它，后续调用无法撤销。
func WithSkipCheckpoint() StopOption {
	return func(cfg *stopConfig) {
		cfg.skipCheckpoint = true
	}
}

// WithStopCause attaches a business-supplied reason string to this Stop call.
// The cause is surfaced in TurnLoopExitState.StopCause and, after the Stopped
// channel closes, via TurnContext.StopCause().
// If multiple Stop() calls provide a cause, the first non-empty value wins.
//
// WithStopCause 为此次 Stop 调用附加业务原因字符串。
// 该 cause 会在 TurnLoopExitState.StopCause 中暴露，并在 Stopped channel 关闭后通过 TurnContext.StopCause() 暴露。
// 如果多个 Stop() 调用提供 cause，首个非空值生效。
func WithStopCause(cause string) StopOption {
	return func(cfg *stopConfig) {
		cfg.stopCause = cause
	}
}

// UntilIdleFor defers the stop until the TurnLoop has been continuously idle
// (blocked between turns with no pending items) for at least the given
// duration. Each time a new item arrives the timer resets from zero.
//
// This is useful when business code monitors agent activity externally and
// wants to shut down the loop once there has been no work for a while, without
// racing with concurrent Push calls.
//
// UntilIdleFor does not impact a running agent. It only takes effect when the
// loop is idle between turns. Cancel options (WithImmediate, WithGraceful,
// WithGracefulTimeout) in the same Stop call are silently ignored — they are
// meaningless alongside UntilIdleFor.
//
// To escalate after a prior UntilIdleFor, issue a separate Stop call:
//
//	loop.Stop(UntilIdleFor(30 * time.Second))  // wait for idle
//	// ... later, if you need to abort immediately:
//	loop.Stop(WithImmediate())                 // overrides the idle wait
//
// Only the first UntilIdleFor duration takes effect; subsequent calls with
// a different duration are ignored. A Stop() call without UntilIdleFor always
// shuts down the loop immediately regardless of any pending idle timer.
//
// UntilIdleFor is combinable with non-cancel StopOptions (WithSkipCheckpoint,
// WithStopCause) in the same call.
//
// duration must be positive; passing a zero or negative value panics.
//
// UntilIdleFor 将停止延后到 TurnLoop 已连续空闲（在轮次之间阻塞且无待处理条目）至少给定时长之后。
// 每次有新条目到达时，计时器都会从零重置。
// 当业务代码在外部监控智能体活动，并希望在一段时间无工作后关闭 loop，同时避免与并发 Push 调用竞争时，这很有用。
// UntilIdleFor 不影响正在运行的智能体。它只在 loop 于轮次之间空闲时生效。同一次 Stop 调用中的取消选项（WithImmediate、WithGraceful、WithGracefulTimeout）会被静默忽略——它们与 UntilIdleFor 同用没有意义。
// 要在先前 UntilIdleFor 之后升级，请发起单独的 Stop 调用：
// loop.Stop(UntilIdleFor(30 * time.Second))  // wait for idle
// ... later, if you need to abort immediately:
// loop.Stop(WithImmediate())                 // overrides the idle wait
// 只有第一次 UntilIdleFor duration 会生效；后续不同 duration 的调用会被忽略。不带 UntilIdleFor 的 Stop() 调用总会立即关闭 loop，无论是否存在待处理的 idle timer。
// UntilIdleFor 可与非取消类 StopOptions（WithSkipCheckpoint、WithStopCause）在同一次调用中组合使用。
// duration 必须为正；传入零或负值会 panic。
func UntilIdleFor(duration time.Duration) StopOption {
	if duration <= 0 {
		panic("adk: UntilIdleFor: duration must be positive")
	}
	return func(cfg *stopConfig) {
		cfg.idleFor = duration
	}
}

type pushConfig[T any, M MessageType] struct {
	preempt         bool
	preemptDelay    time.Duration
	agentCancelOpts []AgentCancelOption
	pushStrategy    func(context.Context, *TurnContext[T, M]) []PushOption[T, M]
}

// PushOption is an option for Push().
// PushOption 是 Push() 的选项。
type PushOption[T any, M MessageType] func(*pushConfig[T, M])

// WithPreempt signals that the current agent turn should be cancelled at the
// specified safePoint after pushing the new item. The loop cancels the current
// turn and starts a new one, where GenInput will see all buffered items
// including the newly pushed one.
// Use WithPreemptTimeout to add a timeout that escalates to immediate abort.
//
// Because safe points fire at turn-level boundaries (after the chat model
// returns or after all tool calls complete), no nested agent is running at
// the moment of cancellation — nested agents within AgentTools have either
// not started yet (AfterChatModel) or already finished (AfterToolCalls).
// Note: WithPreempt does NOT include WithRecursive (no escalation path exists).
// WithPreemptTimeout DOES include WithRecursive so that on timeout escalation,
// nested agents are properly torn down.
//
// WithPreempt and WithPreemptTimeout are mutually exclusive; if both are
// passed to the same Push call, the last one wins.
//
// safePoint must not be zero; passing SafePoint(0) panics.
//
// WithPreempt 表示在推入新条目后，应在指定 safePoint 取消当前智能体轮次。loop 会取消当前轮次并启动新轮次，GenInput 将看到所有已缓冲条目，包括新推入的条目。
// 使用 WithPreemptTimeout 可添加超时，并在超时后升级为立即中止。
// 由于安全点在轮次级边界触发（chat model 返回后，或所有工具调用完成后），取消时没有嵌套智能体正在运行——AgentTools 内的嵌套智能体要么尚未启动（AfterChatModel），要么已经完成（AfterToolCalls）。
// 注意：WithPreempt 不包含 WithRecursive（不存在升级路径）。
// WithPreemptTimeout 包含 WithRecursive，因此在超时升级时，嵌套智能体会被正确拆除。
// WithPreempt 和 WithPreemptTimeout 互斥；若传给同一次 Push 调用，最后一个生效。
// safePoint 不得为零；传入 SafePoint(0) 会 panic。
func WithPreempt[T any, M MessageType](safePoint SafePoint) PushOption[T, M] {
	if safePoint == 0 {
		panic("adk: SafePoint must not be zero; use AfterToolCalls, AfterChatModel, or AnySafePoint")
	}
	return func(cfg *pushConfig[T, M]) {
		cfg.preempt = true
		cfg.agentCancelOpts = []AgentCancelOption{
			WithAgentCancelMode(safePoint.toCancelMode()),
		}
	}
}

// WithPreemptTimeout is like WithPreempt but adds a timeout. If the agent has
// not reached the safe point within timeout, the preemption escalates to
// immediate cancellation. On escalation, nested agents inside AgentTools will
// also receive the cancel signal and be torn down.
//
// safePoint must not be zero; passing SafePoint(0) panics.
//
// WithPreemptTimeout 类似 WithPreempt，但会添加超时。
// 如果智能体在 timeout 内未到达安全点，抢占会升级为立即取消。升级时，AgentTools 内的嵌套智能体也会收到取消信号并被拆除。
// safePoint 不得为零；传入 SafePoint(0) 会 panic。
func WithPreemptTimeout[T any, M MessageType](safePoint SafePoint, timeout time.Duration) PushOption[T, M] {
	if safePoint == 0 {
		panic("adk: SafePoint must not be zero; use AfterToolCalls, AfterChatModel, or AnySafePoint")
	}
	return func(cfg *pushConfig[T, M]) {
		cfg.preempt = true
		cfg.agentCancelOpts = []AgentCancelOption{
			WithAgentCancelMode(safePoint.toCancelMode()),
			WithAgentCancelTimeout(timeout),
			WithRecursive(),
		}
	}
}

// WithPreemptDelay sets a delay duration before resolving a preemptive Push.
// When used with WithPreempt or WithPreemptTimeout, the pushed item is buffered
// immediately, while the preempt request is resolved after the delay against the
// turn observed by Push. If that captured turn has already ended, the request is
// resolved as a no-op and must not cancel a later turn.
//
// WithPreemptDelay 设置解析抢占式 Push 前的延迟时长。
// 与 WithPreempt 或 WithPreemptTimeout 一起使用时，推入的条目会立即缓冲，而抢占请求会在延迟后根据 Push 观察到的轮次进行解析。如果捕获的轮次已结束，该请求会解析为 no-op，且不得取消后续轮次。
func WithPreemptDelay[T any, M MessageType](delay time.Duration) PushOption[T, M] {
	return func(cfg *pushConfig[T, M]) {
		cfg.preemptDelay = delay
	}
}

// WithPushStrategy provides dynamic push option resolution based on the current turn state.
// The callback receives the current turn's context and TurnContext (nil if no turn is active)
// and returns the actual PushOptions to apply. When WithPushStrategy is used, all other
// PushOptions passed to the same Push call are ignored.
//
// The returned options must not contain another WithPushStrategy; any nested
// strategy is silently stripped.
//
// Example: preempt only if the current turn is processing low-priority items:
//
//	loop.Push(urgentItem, WithPushStrategy(func(ctx context.Context, tc *TurnContext[MyItem, *schema.Message]) []PushOption[MyItem, *schema.Message] {
//	    if tc == nil {
//	        return nil // between turns, plain push
//	    }
//	    if isLowPriority(tc.Consumed) {
//	        return []PushOption[MyItem, *schema.Message]{WithPreempt[MyItem, *schema.Message](AnySafePoint)}
//	    }
//	    return nil // don't preempt high-priority work
//	}))
//
// WithPushStrategy 会根据当前轮次状态动态解析 push 选项。
// 回调接收当前轮次的 context 和 TurnContext（若没有活动轮次则为 nil），并返回实际要应用的 PushOptions。使用 WithPushStrategy 时，同一次 Push 调用传入的其他所有 PushOptions 都会被忽略。
// 返回的选项不得包含另一个 WithPushStrategy；任何嵌套 strategy 都会被静默剥离。
// 示例：仅当当前轮次正在处理低优先级项时抢占：
// loop.Push(urgentItem, WithPushStrategy(func(ctx context.Context, tc *TurnContext[MyItem, *schema.Message]) []PushOption[MyItem, *schema.Message] {
// if tc == nil {
// return nil // 轮次之间，普通 push
// }
// if isLowPriority(tc.Consumed) {
// return []PushOption[MyItem, *schema.Message]{WithPreempt[MyItem, *schema.Message](AnySafePoint)}
// }
// return nil // 不抢占高优先级工作
// }))
func WithPushStrategy[T any, M MessageType](fn func(ctx context.Context, tc *TurnContext[T, M]) []PushOption[T, M]) PushOption[T, M] {
	return func(cfg *pushConfig[T, M]) {
		cfg.pushStrategy = fn
	}
}

func defaultTurnLoopOnAgentEvents[T any, M MessageType](_ context.Context, _ *TurnContext[T, M], events *AsyncIterator[*TypedAgentEvent[M]]) error {
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			return event.Err
		}
	}
	return nil
}

// NewTurnLoop creates a new TurnLoop without starting it.
// The returned loop accepts Push and Stop calls immediately; pushed items
// are buffered until Run is called.
// Call Run to start the processing goroutine.
//
// NewTurnLoop panics if GenInput or PrepareAgent is nil.
//
// NewTurnLoop 创建一个新的 TurnLoop，但不启动它。
// 返回的 loop 会立即接受 Push 和 Stop 调用；push 的项会被缓冲，直到调用 Run。
// 调用 Run 以启动处理 goroutine。
// 如果 GenInput 或 PrepareAgent 为 nil，NewTurnLoop 会 panic。
func NewTurnLoop[T any, M MessageType](cfg TurnLoopConfig[T, M]) *TurnLoop[T, M] {
	if cfg.GenInput == nil {
		panic("adk: NewTurnLoop: GenInput is required")
	}
	if cfg.PrepareAgent == nil {
		panic("adk: NewTurnLoop: PrepareAgent is required")
	}

	l := &TurnLoop[T, M]{
		config:      cfg,
		buffer:      newTurnBuffer[T](),
		done:        make(chan struct{}),
		stopCtrl:    newStopController(),
		preemptCtrl: newPreemptController(),
	}
	if cfg.OnAgentEvents != nil {
		l.onAgentEvents = cfg.OnAgentEvents
	} else {
		l.onAgentEvents = defaultTurnLoopOnAgentEvents[T, M]
	}
	return l
}

func (l *TurnLoop[T, M]) start(ctx context.Context) {
	l.runOnce.Do(func() {
		atomic.StoreInt32(&l.started, 1)
		go l.run(ctx)
	})
}

// Run starts the loop's processing goroutine. It is non-blocking: the loop
// runs in the background and results are obtained via Wait.
//
// If CheckpointID is configured in TurnLoopConfig and a matching checkpoint
// exists in Store, the loop automatically resumes from that checkpoint.
// Otherwise it starts fresh with whatever items were Push()-ed.
//
// Calling Run more than once is a no-op: only the first call starts the loop.
//
// Run 启动 loop 的处理 goroutine。它是非阻塞的：loop 在后台运行，结果通过 Wait 获取。
// 如果 TurnLoopConfig 中配置了 CheckpointID，且 Store 中存在匹配的 checkpoint，loop 会自动从该 checkpoint 恢复。否则，它会使用已 Push() 的项全新启动。
// 多次调用 Run 是 no-op：只有第一次调用会启动 loop。
func (l *TurnLoop[T, M]) Run(ctx context.Context) {
	l.start(ctx)
}

// Push adds an item to the loop's buffer for processing.
// This method is non-blocking and thread-safe.
// Returns false if the loop has stopped, true otherwise. If a preemptive push
// succeeds, the second return value is a channel that callers can wait on to
// confirm the preempt request has been resolved. Specifically:
//   - If Push observes a planning or active turn that is still the target when
//     the request resolves, the channel closes after TurnLoop attempts to submit
//     cancel for that target turn.
//   - If Push observes no target turn, the loop has not started, the preempt
//     subsystem is closed, or a delayed target is already gone, the channel
//     closes as a no-op resolution.
//
// If the loop has not been started yet (Run not called), items are buffered
// and will be processed once Run is called.
// After Wait() returns, failed pushes can be recovered via TurnLoopExitState.TakeLateItems().
// Once TakeLateItems() has been called, any subsequent push that would become a
// late item will panic instead of being silently dropped.
//
// Use WithPreempt() or WithPreemptTimeout() to atomically push an item and signal
// preemption of the current agent. This is useful for urgent items that should
// interrupt the current processing.
// The returned channel may be waited on if the caller needs to ensure the preempt
// signal has been observed.
//
// Use WithPreemptDelay() together with WithPreempt()/WithPreemptTimeout() to delay
// request resolution. Push returns immediately after the item is buffered, and
// the delayed request remains bound to the turn observed by Push.
//
// Push 将一个项加入 loop 的缓冲区以供处理。
// 此方法非阻塞且线程安全。
// 如果 loop 已停止则返回 false，否则返回 true。如果抢占式 push 成功，第二个返回值是一个 channel，调用方可等待它以确认抢占请求已解析。具体来说：
// - 如果 Push 观察到一个 planning 或 active turn，且请求解析时它仍是目标，则 channel 会在 TurnLoop 尝试为该目标轮次提交 cancel 后关闭。
// - 如果 Push 未观察到目标轮次、loop 尚未启动、抢占子系统已关闭，或延迟目标已不存在，则 channel 会以 no-op 解析方式关闭。
// 如果 loop 尚未启动（未调用 Run），项会被缓冲，并在调用 Run 后处理。
// Wait() 返回后，失败的 push 可通过 TurnLoopExitState.TakeLateItems() 恢复。
// 一旦调用 TakeLateItems()，之后任何会成为 late item 的 push 都会 panic，而不是被静默丢弃。
// 使用 WithPreempt() 或 WithPreemptTimeout() 可原子地 push 一个项并向当前 agent 发出抢占信号。这适用于应中断当前处理的紧急项。
// 如果调用方需要确保已观察到抢占信号，可以等待返回的 channel。
// 将 WithPreemptDelay() 与 WithPreempt()/WithPreemptTimeout() 一起使用可延迟请求解析。Push 会在项被缓冲后立即返回，延迟请求仍绑定到 Push 观察到的轮次。
func (l *TurnLoop[T, M]) Push(item T, opts ...PushOption[T, M]) (bool, <-chan struct{}) {
	cfg := &pushConfig[T, M]{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.pushStrategy != nil {
		return l.pushWithStrategy(item, cfg)
	}

	return l.pushWithConfig(item, cfg)
}

// pushWithStrategy snapshots the current target turn while the strategy decides
// how to enqueue the item. If it requests preempt, that request is bound to the
// captured turn identity, including delayed preempt requests.
//
// pushWithStrategy 会在 strategy 决定如何将项入队时快照当前目标轮次。
// 如果它请求抢占，该请求会绑定到捕获的轮次身份，包括延迟抢占请求。
func (l *TurnLoop[T, M]) pushWithStrategy(item T, cfg *pushConfig[T, M]) (bool, <-chan struct{}) {
	strategy := cfg.pushStrategy

	snapshot := l.preemptCtrl.beginPush()
	defer l.preemptCtrl.endPush()

	runCtx := snapshot.ctx
	if runCtx == nil {
		runCtx = context.Background()
	}
	var tc *TurnContext[T, M]
	if snapshot.tc != nil {
		tc = snapshot.tc.(*TurnContext[T, M])
	}
	realOpts := strategy(runCtx, tc)
	cfg = &pushConfig[T, M]{}
	for _, opt := range realOpts {
		opt(cfg)
	}
	cfg.pushStrategy = nil

	if !cfg.preempt {
		if !l.buffer.TrySend(item) {
			l.appendLate(item)
			return false, nil
		}
		return true, nil
	}

	if atomic.LoadInt32(&l.stopped) != 0 {
		l.appendLate(item)
		return false, nil
	}

	if !l.buffer.TrySend(item) {
		l.appendLate(item)
		return false, nil
	}

	ack := make(chan struct{})
	if atomic.LoadInt32(&l.started) == 0 {
		close(ack)
		return true, ack
	}

	if cfg.preemptDelay > 0 {
		go func() {
			select {
			case <-time.After(cfg.preemptDelay):
				l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
			case <-l.done:
				close(ack)
			}
		}()
	} else {
		l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
	}
	return true, ack
}

func (l *TurnLoop[T, M]) pushWithConfig(item T, cfg *pushConfig[T, M]) (bool, <-chan struct{}) {
	if atomic.LoadInt32(&l.stopped) != 0 {
		l.appendLate(item)
		return false, nil
	}

	if cfg.preempt {
		snapshot := l.preemptCtrl.beginPush()
		defer l.preemptCtrl.endPush()

		if !l.buffer.TrySend(item) {
			l.appendLate(item)
			return false, nil
		}

		ack := make(chan struct{})
		if atomic.LoadInt32(&l.started) == 0 {
			close(ack)
			return true, ack
		}

		if cfg.preemptDelay > 0 {
			go func() {
				select {
				case <-time.After(cfg.preemptDelay):
					l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
				case <-l.done:
					close(ack)
				}
			}()
		} else {
			l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
		}
		return true, ack
	}

	if !l.buffer.TrySend(item) {
		l.appendLate(item)
		return false, nil
	}
	return true, nil
}

// Stop signals the loop to stop and returns immediately (non-blocking).
// Without options, the current agent turn runs to completion and the loop
// exits at the turn boundary without starting a new turn. ExitReason is nil.
//
// Use WithImmediate() to abort the running agent turn immediately.
// Use WithGraceful() to cancel at the nearest safe point with recursive
// propagation to nested agents.
// Use WithGracefulTimeout() for safe-point cancel with an escalation deadline.
// Use UntilIdleFor() to defer the stop until the loop has been continuously
// idle for a given duration; the loop shuts down automatically once the idle
// timer fires.
//
// This method may be called multiple times; subsequent calls update cancel options.
// A Stop() call without UntilIdleFor shuts down the loop immediately, even if
// a prior UntilIdleFor is still waiting.
// Call Wait() to block until the loop has fully exited and get the result.
//
// Stop may be called before Run. In that case, the stopped flag is set and
// a subsequent Run will exit the loop immediately.
//
// If the running agent does not support the WithCancel AgentRunOption,
// all cancel-related options (WithImmediate, WithGraceful, WithGracefulTimeout)
// degrade to "exit the loop on entering the next iteration" — the current
// agent turn runs to completion before the loop exits.
//
// Stop 向 loop 发送停止信号并立即返回（非阻塞）。
// 不带选项时，当前 agent 轮次会运行到完成，loop 在轮次边界退出且不启动新轮次。ExitReason 为 nil。
// 使用 WithImmediate() 可立即中止正在运行的 agent 轮次。
// 使用 WithGraceful() 可在最近的安全点取消，并递归传播到嵌套 agents。
// 使用 WithGracefulTimeout() 可进行带升级截止时间的安全点取消。
// 使用 UntilIdleFor() 可将停止延后到 loop 连续空闲指定时长；空闲定时器触发后 loop 会自动关闭。
// 此方法可多次调用；后续调用会更新 cancel 选项。
// 不带 UntilIdleFor 的 Stop() 调用会立即关闭 loop，即使之前的 UntilIdleFor 仍在等待。
// 调用 Wait() 可阻塞直到 loop 完全退出并获取结果。
// Stop 可在 Run 之前调用。此时会设置 stopped 标志，之后的 Run 会立即退出 loop。
// 如果正在运行的 agent 不支持 WithCancel AgentRunOption，所有 cancel 相关选项（WithImmediate、WithGraceful、WithGracefulTimeout）都会降级为“进入下一次迭代时退出 loop”——当前 agent 轮次会运行到完成后 loop 才退出。
func (l *TurnLoop[T, M]) Stop(opts ...StopOption) {
	cfg := &stopConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// UntilIdleFor is incompatible with cancel options (WithImmediate,
	// WithGraceful, WithGracefulTimeout) in the same call. Cancel opts only
	// make sense for an immediate or escalated stop; UntilIdleFor defers the
	// stop until idle, and must not impact a running agent. Drop them silently.
	//
	// UntilIdleFor 与同一次调用中的 cancel 选项（WithImmediate、WithGraceful、WithGracefulTimeout）不兼容。Cancel opts 只对立即停止或升级停止有意义；UntilIdleFor 会将停止延后到空闲，并且不得影响正在运行的 agent。静默丢弃它们。
	if cfg.idleFor > 0 {
		cfg.agentCancelOpts = nil
	}

	decision := l.stopCtrl.requestStop(cfg)
	if decision.wakeIdle {
		l.buffer.Wakeup()
	}
	if decision.commit {
		l.finishStopCommit()
	}
}

func (l *TurnLoop[T, M]) commitStop() {
	if !l.stopCtrl.commit() {
		return
	}
	l.finishStopCommit()
}

func (l *TurnLoop[T, M]) finishStopCommit() {
	atomic.StoreInt32(&l.stopped, 1)
	l.buffer.Close()
}

// Wait blocks until the loop exits and returns the result.
// This method is safe to call from multiple goroutines.
// All callers will receive the same result.
//
// Wait blocks until Run is called AND the loop exits. If Run is
// never called, Wait blocks forever.
//
// Wait 会阻塞直到 loop 退出并返回结果。
// 此方法可安全地从多个 goroutine 调用。
// 所有调用方都会收到相同的结果。
// Wait 会阻塞直到 Run 被调用且 loop 退出。如果从未调用 Run，Wait 会永久阻塞。
func (l *TurnLoop[T, M]) Wait() *TurnLoopExitState[T, M] {
	<-l.done
	return l.result
}

func (l *TurnLoop[T, M]) run(ctx context.Context) {
	defer l.cleanup(ctx)

	if err := l.tryLoadCheckpoint(ctx); err != nil {
		l.runErr = err
		return
	}

	// Monitor context cancellation: close the buffer so that a blocking
	// Receive() unblocks. The loop will then check ctx.Err() and exit.
	//
	// 监控 context 取消：关闭缓冲区，使阻塞的 Receive() 解除阻塞。随后 loop 会检查 ctx.Err() 并退出。
	go func() {
		select {
		case <-ctx.Done():
			l.buffer.Close()
		case <-l.done:
		}
	}()

	for {
		if l.stopCtrl.isCommitted() {
			return
		}

		isResume := false
		var pr *turnLoopPendingResume[T]
		var items []T
		var pushBack []T

		if l.pendingResume != nil {
			isResume = true
			pr = l.pendingResume
			l.pendingResume = nil

			l.preemptCtrl.waitForPushes()
			pr.newItems = append(pr.newItems, l.buffer.TakeAll()...)

			pushBack = make([]T, 0, len(pr.interrupted)+len(pr.unhandled)+len(pr.newItems))
			pushBack = append(pushBack, pr.interrupted...)
			pushBack = append(pushBack, pr.unhandled...)
			pushBack = append(pushBack, pr.newItems...)
		} else {
			var first T
			var ok bool

			if idleFor := l.stopCtrl.idleDuration(); idleFor > 0 {
				l.buffer.ClearWakeup()
				idleTimer := time.NewTimer(idleFor)
				cancelIdle := make(chan struct{})
				// When the idle timer fires, commitStop closes the buffer via
				// buffer.Close(), which broadcasts to unblock the pending
				// Receive() call below.
				//
				// 空闲定时器触发时，commitStop 会通过 buffer.Close() 关闭缓冲区，这会广播以解除下面挂起的 Receive() 调用。
				go func() {
					select {
					case <-idleTimer.C:
						l.commitStop()
					case <-cancelIdle:
					}
				}()

				first, ok = l.buffer.Receive()

				idleTimer.Stop()
				close(cancelIdle)

				// A spurious wakeup can occur if Stop(UntilIdleFor) called
				// buffer.Wakeup() after ClearWakeup() above but before
				// Receive() entered its wait. In that case, Receive returns
				// !ok from the woken flag, not from buffer closure.
				// Re-enter the loop so the idle timer restarts cleanly.
				//
				// 如果 Stop(UntilIdleFor) 在上面的 ClearWakeup() 之后、Receive() 进入等待之前调用了 buffer.Wakeup()，可能发生虚假唤醒。此时 Receive 返回 !ok 是因为 woken 标志，而不是因为缓冲区关闭。
				// 重新进入 loop，让空闲定时器干净地重启。
				if !ok && !l.buffer.IsClosed() {
					continue
				}
			} else {
				first, ok = l.buffer.Receive()
				// Woken up by Stop(UntilIdleFor); re-enter loop to start the idle timer.
				// 被 Stop(UntilIdleFor) 唤醒；重新进入 loop 以启动空闲定时器。
				if !ok && l.stopCtrl.idleDuration() > 0 {
					continue
				}
			}

			if !ok {
				if err := ctx.Err(); err != nil {
					l.runErr = err
				}
				return
			}

			if err := ctx.Err(); err != nil {
				l.buffer.PushFront([]T{first})
				l.runErr = err
				return
			}

			if l.stopCtrl.isCommitted() {
				l.buffer.PushFront([]T{first})
				return
			}

			l.preemptCtrl.waitForPushes()
			rest := l.buffer.TakeAll()
			items = append([]T{first}, rest...)
			pushBack = items
		}

		l.preemptCtrl.beginPlanningTurn()
		abortPlanning := func() {
			l.preemptCtrl.abortPlanningTurn().ack()
		}

		plan, err := l.planTurn(ctx, isResume, items, pr)
		if err != nil {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			l.runErr = err
			return
		}

		if l.stopCtrl.isCommitted() {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			return
		}

		agent, err := l.config.PrepareAgent(plan.turnCtx, l, plan.spec.consumed)
		if err != nil {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			l.runErr = err
			return
		}

		if l.stopCtrl.isCommitted() {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			return
		}

		l.buffer.PushFront(plan.remaining)

		runErr := l.runAgentAndHandleEvents(plan.turnCtx, agent, plan.spec)

		if runErr != nil {
			// Set interruptedItems when a cancel or interrupt was captured from the
			// event stream, regardless of what the user's callback returned. The items
			// were factually mid-execution when the signal arrived.
			//
			// 当从事件流捕获到 cancel 或 interrupt 时设置 interruptedItems，无论用户的回调返回什么。这些项在信号到达时事实上正处于执行中。
			if l.capturedCancelErr != nil || l.interruptContexts != nil {
				l.interruptedItems = append([]T{}, plan.spec.consumed...)
			}
			l.runErr = runErr
			return
		}

		// Business interrupt: agent produced an Interrupted action, exit to persist checkpoint.
		// 业务中断：agent 产生了 Interrupted action，退出以持久化 checkpoint。
		if l.interruptContexts != nil {
			l.interruptedItems = append([]T{}, plan.spec.consumed...)
			l.runErr = &InterruptError{InterruptContexts: l.interruptContexts}
			return
		}
	}
}

func (l *TurnLoop[T, M]) setupBridgeStore(spec *turnRunSpec[T, M], runOpts []AgentRunOption) ([]AgentRunOption, *bridgeStore, error) {
	store := l.config.Store
	if store == nil && spec.isResume {
		return nil, nil, fmt.Errorf("failed to resume agent: checkpoint store is nil")
	}
	if store == nil {
		return runOpts, nil, nil
	}
	runOpts = append(runOpts, WithCheckPointID(bridgeCheckpointID))
	if spec.isResume {
		if len(spec.resumeBytes) == 0 {
			return nil, nil, fmt.Errorf("resume checkpoint is empty")
		}
		return runOpts, newResumeBridgeStore(bridgeCheckpointID, spec.resumeBytes), nil
	}
	return runOpts, newBridgeStore(), nil
}

// watchPreempt runs for the lifetime of a single active turn. It consumes
// pending preempt requests exactly once and submits cancel for that turn.
//
// watchPreempt 在单个活动轮次的生命周期内运行。它只消费一次待处理的抢占请求，并为该轮次提交 cancel。
func (l *TurnLoop[T, M]) watchPreempt(done <-chan struct{}, agentCancelFunc AgentCancelFunc, preemptDone chan struct{}) {
	preemptDoneClosed := false
	for {
		select {
		case <-done:
			return
		case <-l.preemptCtrl.notify:
			req, ok := l.preemptCtrl.receivePreempt()
			if !ok {
				continue
			}
			// CancelHandle is intentionally not awaited here: agentCancelFunc commits the cancel signal synchronously,
			// while waiting would block until the turn finishes and can deadlock this watcher against the done signal.
			//
			// 这里有意不等待 CancelHandle：agentCancelFunc 会同步提交 cancel 信号，而等待会阻塞到轮次结束，并可能让此 watcher 与 done 信号互相死锁。
			_, contributed := agentCancelFunc(req.cancelOptions(time.Now())...)
			if contributed && !preemptDoneClosed {
				close(preemptDone)
				preemptDoneClosed = true
			}
			req.ack()
		}
	}
}

// watchStop runs for the lifetime of a single active turn. It consumes pending
// Stop cancel requests exactly once and submits them to that turn.
//
// watchStop 在单个活动轮次的生命周期内运行。它只消费一次待处理的 Stop cancel 请求，并将其提交给该轮次。
func (l *TurnLoop[T, M]) watchStop(done <-chan struct{}, agentCancelFunc AgentCancelFunc, stoppedDone chan struct{}) {
	stoppedClosed := false

	submit := func(req *stopCancelRequest) {
		_, contributed := agentCancelFunc(req.cancelOptions(time.Now())...)
		if contributed && !stoppedClosed {
			close(stoppedDone)
			stoppedClosed = true
		}
	}

	for {
		if req, ok := l.stopCtrl.receiveCancel(); ok {
			submit(req)
			continue
		}

		select {
		case <-done:
			return
		case <-l.stopCtrl.notify:
		}
	}
}

func (l *TurnLoop[T, M]) runAgentAndHandleEvents(
	ctx context.Context,
	agent TypedAgent[M],
	spec *turnRunSpec[T, M],
) error {
	l.interruptContexts = nil
	l.capturedCancelErr = nil
	l.checkPointRunnerBytes = nil

	var iter *AsyncIterator[*TypedAgentEvent[M]]

	runOpts, ms, err := l.setupBridgeStore(spec, spec.runOpts)
	if err != nil {
		l.preemptCtrl.abortPlanningTurn().ack()
		return err
	}
	store := l.config.Store
	cancelOpt, agentCancelFunc := WithCancel()
	runOpts = append(runOpts, cancelOpt)

	// For Run path the streaming mode comes from the input. For Resume path the
	// runner reads the streaming mode persisted in the checkpoint, so the value we
	// pass here is irrelevant.
	//
	// 对于 Run 路径，streaming mode 来自输入。对于 Resume 路径，runner 会读取 checkpoint 中持久化的 streaming mode，因此这里传入的值无关紧要。
	enableStreaming := false
	if spec.input != nil {
		enableStreaming = spec.input.EnableStreaming
	}
	runner := NewTypedRunner(TypedRunnerConfig[M]{
		EnableStreaming: enableStreaming,
		Agent:           agent,
		CheckPointStore: ms,
	})

	preemptDone := make(chan struct{})
	stoppedDone := make(chan struct{})

	tc := &TurnContext[T, M]{
		Loop:      l,
		Consumed:  spec.consumed,
		Preempted: preemptDone,
		Stopped:   stoppedDone,
		StopCause: l.stopCtrl.cause,
	}
	l.preemptCtrl.beginActiveTurn(ctx, tc)
	l.stopCtrl.beginActiveTurn()
	defer func() {
		l.stopCtrl.endActiveTurn()
		l.preemptCtrl.endActiveTurn().ack()
	}()

	if spec.isResume {
		var err error
		if spec.resumeParams != nil {
			iter, err = runner.ResumeWithParams(ctx, bridgeCheckpointID, spec.resumeParams, runOpts...)
		} else {
			iter, err = runner.Resume(ctx, bridgeCheckpointID, runOpts...)
		}
		if err != nil {
			return fmt.Errorf("failed to resume agent: %w", err)
		}
	} else {
		iter = runner.Run(ctx, spec.input.Messages, runOpts...)
	}

	// Wrap iterator to capture framework-level signals (CancelError, InterruptContexts)
	// from events before they flow to OnAgentEvents. This ensures the framework can
	// track these signals independently of what the user's callback returns.
	//
	// 包装 iterator，以便在事件流向 OnAgentEvents 之前捕获框架级信号（CancelError、InterruptContexts）。这确保框架可以独立跟踪这些信号，不受用户回调返回值影响。
	srcIter := iter
	proxyIter, proxyGen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go func() {
		defer proxyGen.Close()
		for {
			event, ok := srcIter.Next()
			if !ok {
				break
			}
			if event != nil {
				if event.Err != nil {
					var cancelErr *CancelError
					if errors.As(event.Err, &cancelErr) {
						l.capturedCancelErr = cancelErr
					}
				}
				if event.Action != nil && event.Action.Interrupted != nil {
					l.interruptContexts = event.Action.Interrupted.InterruptContexts
				}
			}
			proxyGen.Send(event)
		}
	}()
	iter = proxyIter

	handleEvents := func() error {
		return l.onAgentEvents(ctx, tc, iter)
	}

	done := make(chan struct{})
	var handleErr error

	go func() {
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				handleErr = safe.NewPanicErr(panicErr, debug.Stack())
			}
			close(done)
		}()
		handleErr = handleEvents()
	}()
	go l.watchPreempt(done, agentCancelFunc, preemptDone)
	go l.watchStop(done, agentCancelFunc, stoppedDone)

	finalizeCheckpoint := func() error {
		if store != nil && ms != nil {
			data, ok, err := ms.Get(ctx, bridgeCheckpointID)
			if err != nil {
				return fmt.Errorf("failed to read runner checkpoint: %w", err)
			}
			if ok {
				l.checkPointRunnerBytes = append([]byte{}, data...)
			}
		}
		return nil
	}

	// Wait for the turn to end. Three outcomes:
	//
	// done:         Events fully handled (normal or error). If Stop() was
	//               called, save checkpoint so the caller can resume later.
	//               Also handle the select race: if preemptDone is closed
	//               too, treat as a preempt (return nil) instead of leaking
	//               the CancelError.
	//
	// preemptDone:  A preemptive Push successfully cancelled the agent.
	//               Wait for the handleEvents goroutine to drain, then
	//               return nil — the run loop will start a new turn.
	//
	// stoppedDone:  Stop() cancelled the agent. Save checkpoint so the
	//               caller can resume later.
	//
	// 等待轮次结束。三种结果：
	// done:         Events 已完全处理（正常或出错）。如果调用了 Stop()，保存 checkpoint，使调用方之后可以恢复。还要处理 select 竞争：如果 preemptDone 也已关闭，则视为抢占（返回 nil），而不是泄漏 CancelError。
	// preemptDone:  抢占式 Push 已成功取消 agent。等待 handleEvents goroutine 排空，然后返回 nil——run loop 将启动新轮次。
	// stoppedDone:  Stop() 已取消 agent。保存 checkpoint，使调用方之后可以恢复。
	select {
	case <-done:
		select {
		case <-preemptDone:
			return nil
		default:
		}
		if err := finalizeCheckpoint(); err != nil {
			if handleErr != nil {
				handleErr = fmt.Errorf("%w; checkpoint error: %v", handleErr, err)
			} else {
				handleErr = err
			}
		}
		return l.applyFrameworkCapturedError(handleErr)
	case <-preemptDone:
		<-done
		return nil
	case <-stoppedDone:
		<-done
		if err := finalizeCheckpoint(); err != nil {
			if handleErr != nil {
				handleErr = fmt.Errorf("%w; checkpoint error: %v", handleErr, err)
			} else {
				handleErr = err
			}
		}
		return l.applyFrameworkCapturedError(handleErr)
	}
}

// applyFrameworkCapturedError resolves the final error for runAgentAndHandleEvents.
// Priority scheme:
//   - If handleErr != nil: the user's callback error wins (framework does not overwrite).
//   - If handleErr == nil and a CancelError was captured: use the captured CancelError.
//   - If handleErr == nil and interrupt contexts were captured: this is handled by the
//     caller (run loop) via l.interruptContexts, so return nil here.
//
// In all cases, the caller uses l.capturedCancelErr and l.interruptContexts to
// determine interruptedItems independently of the returned error.
//
// applyFrameworkCapturedError 为 runAgentAndHandleEvents 解析最终错误。
// 优先级规则：
// - 如果 handleErr != nil：用户的回调错误优先（框架不覆盖）。
// - 如果 handleErr == nil 且捕获到 CancelError：使用捕获到的 CancelError。
// - 如果 handleErr == nil 且捕获到 interrupt contexts：由调用方（run loop）通过 l.interruptContexts 处理，因此这里返回 nil。
// 所有情况下，调用方都会使用 l.capturedCancelErr 和 l.interruptContexts，独立于返回的错误来确定 interruptedItems。
func (l *TurnLoop[T, M]) applyFrameworkCapturedError(handleErr error) error {
	if handleErr != nil {
		return handleErr
	}
	if l.capturedCancelErr != nil {
		return l.capturedCancelErr
	}
	return nil
}

func (l *TurnLoop[T, M]) cleanup(ctx context.Context) {
	atomic.StoreInt32(&l.stopped, 1)

	unhandled := l.buffer.TakeAll()
	checkpointID := l.config.CheckpointID
	isIdle := len(l.checkPointRunnerBytes) == 0 && len(unhandled) == 0 && len(l.interruptedItems) == 0

	// Only save checkpoint when the loop exited due to an explicit Stop(),
	// a CancelError, or a business interrupt (InterruptError).
	// Also checkpoint when a cancel/interrupt was captured from the event stream
	// but the user's callback returned a custom error (the items were still in-flight).
	//
	// 仅当循环因显式 Stop()、CancelError 或业务中断（InterruptError）退出时保存检查点。
	// 当从事件流中捕获到取消/中断，但用户回调返回自定义错误时也保存检查点（这些项仍在处理中）。
	exitCausedByStop := l.runErr == nil || errors.As(l.runErr, new(*CancelError)) || l.capturedCancelErr != nil
	businessInterrupt := errors.As(l.runErr, new(*InterruptError)) || l.interruptContexts != nil
	shouldSaveCheckpoint := l.config.Store != nil && checkpointID != "" &&
		((l.stopCtrl.isCommitted() && exitCausedByStop) || businessInterrupt) &&
		!isIdle && !l.stopCtrl.skipCheckpointEnabled()

	var checkpointed bool
	var checkpointErr error

	if shouldSaveCheckpoint {
		cp := &turnLoopCheckpoint[T]{
			RunnerCheckpoint: l.checkPointRunnerBytes,
			HasRunnerState:   len(l.checkPointRunnerBytes) > 0,
			UnhandledItems:   unhandled,
			CanceledItems:    l.interruptedItems,
		}
		checkpointed = true
		checkpointErr = l.saveTurnLoopCheckpoint(ctx, checkpointID, cp)
	} else if l.loadCheckpointID != "" {
		_ = l.deleteTurnLoopCheckpoint(ctx, l.loadCheckpointID)
	}

	var takeLateOnce sync.Once
	var takeLateResult []T

	l.result = &TurnLoopExitState[T, M]{
		ExitReason:          l.runErr,
		UnhandledItems:      unhandled,
		InterruptedItems:    l.interruptedItems,
		StopCause:           l.stopCtrl.cause(),
		CheckpointAttempted: checkpointed,
		CheckpointErr:       checkpointErr,
		TakeLateItems: func() []T {
			takeLateOnce.Do(func() {
				l.lateMu.Lock()
				takeLateResult = append([]T{}, l.lateItems...)
				l.lateSealed = true
				l.lateMu.Unlock()
			})
			return takeLateResult
		},
	}

	l.stopCtrl.closeForLoopExit()
	l.preemptCtrl.closeForLoopExit()
	l.buffer.Close()
	close(l.done)
}
