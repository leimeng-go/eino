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
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/cloudwego/eino/internal/core"
	"github.com/cloudwego/eino/internal/safe"
	"github.com/cloudwego/eino/schema"
)

func errorIterator[M MessageType](err error) *AsyncIterator[*TypedAgentEvent[M]] {
	iter, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	gen.Send(&TypedAgentEvent[M]{Err: err})
	gen.Close()
	return iter
}

func newUserMessage[M MessageType](query string) (M, error) {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return any(schema.UserMessage(query)).(M), nil
	case *schema.AgenticMessage:
		return any(schema.UserAgenticMessage(query)).(M), nil
	default:
		return zero, fmt.Errorf("unsupported message type %T", zero)
	}
}

// TypedRunner is the primary entry point for executing an Agent.
// It manages the agent's lifecycle, including starting, resuming, and checkpointing.
//
// Execution always goes through the flowAgent pipeline, which handles
// multi-agent orchestration, callbacks, agent naming, run paths, and cancellation.
//
// TypedRunner 是执行 Agent 的主要入口。
// 它管理智能体的生命周期，包括启动、恢复和创建检查点。
// 执行始终经过 flowAgent pipeline，由它处理多智能体编排、回调、智能体命名、运行路径和取消。
type TypedRunner[M MessageType] struct {
	a               TypedAgent[M]
	enableStreaming bool
	store           CheckPointStore
}

// Runner is the default runner type using *schema.Message.
// Runner 是使用 *schema.Message 的默认 runner 类型。
type Runner = TypedRunner[*schema.Message]

type CheckPointStore = core.CheckPointStore

type CheckPointDeleter = core.CheckPointDeleter

type TypedRunnerConfig[M MessageType] struct {
	Agent           TypedAgent[M]
	EnableStreaming bool

	CheckPointStore CheckPointStore
}

// RunnerConfig is the default runner config type using *schema.Message.
// RunnerConfig 是使用 *schema.Message 的默认 runner 配置类型。
type RunnerConfig = TypedRunnerConfig[*schema.Message]

// ResumeParams contains all parameters needed to resume an execution.
// This struct provides an extensible way to pass resume parameters without
// requiring breaking changes to method signatures.
//
// ResumeParams 包含恢复执行所需的所有参数。
// 该结构体提供了可扩展的恢复参数传递方式，无需破坏性修改方法签名。
type ResumeParams struct {
	// Targets contains the addresses of components to be resumed as keys,
	// with their corresponding resume data as values
	//
	// Targets 以待恢复组件的地址为键，以对应的恢复数据为值。
	Targets map[string]any
	// Future extensible fields can be added here without breaking changes
	// 以后可在此添加可扩展字段而不造成破坏性变更。
}

// NewRunner creates a new Runner with the given config.
// NewRunner 使用给定配置创建新的 Runner。
func NewRunner(_ context.Context, conf RunnerConfig) *Runner {
	return NewTypedRunner(conf)
}

// NewTypedRunner creates a new TypedRunner with the given config.
// NewTypedRunner 使用给定配置创建新的 TypedRunner。
func NewTypedRunner[M MessageType](conf TypedRunnerConfig[M]) *TypedRunner[M] {
	return &TypedRunner[M]{
		enableStreaming: conf.EnableStreaming,
		a:               conf.Agent,
		store:           conf.CheckPointStore,
	}
}

func (r *TypedRunner[M]) Run(ctx context.Context, messages []M,
	opts ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	return typedRunnerRunImpl(r.a, r.enableStreaming, r.store, ctx, messages, opts...)
}

// Query is a convenience method that starts a new execution with a single user query string.
// Query 是一个便捷方法，用单个用户查询字符串启动新的执行。
func (r *TypedRunner[M]) Query(ctx context.Context,
	query string, opts ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	msgs, err := newUserMessage[M](query)
	if err != nil {
		return errorIterator[M](err)
	}
	return r.Run(ctx, []M{msgs}, opts...)
}

// Resume continues an interrupted execution from a checkpoint, using an "Implicit Resume All" strategy.
// This method is best for simpler use cases where the act of resuming implies that all previously
// interrupted points should proceed without specific data.
//
// When using this method, all interrupted agents will receive `isResumeFlow = false` when they
// call `GetResumeContext`, as no specific agent was targeted. This is suitable for the "Simple Confirmation"
// pattern where an agent only needs to know `wasInterrupted` is true to continue.
//
// Resume 使用“Implicit Resume All”策略，从检查点继续被中断的执行。
// 该方法最适合较简单的用例：恢复动作本身就表示所有先前的中断点都应继续执行，且无需特定数据。
// 使用此方法时，所有被中断的智能体调用 GetResumeContext 时都会收到 `isResumeFlow = false`，因为没有指定特定智能体。
// 这适用于“Simple Confirmation”模式：智能体只需知道 `wasInterrupted` 为 true 即可继续。
func (r *TypedRunner[M]) Resume(ctx context.Context, checkPointID string, opts ...AgentRunOption) (
	*AsyncIterator[*TypedAgentEvent[M]], error) {
	return r.resumeInternal(ctx, checkPointID, nil, opts...)
}

// ResumeWithParams continues an interrupted execution from a checkpoint with specific parameters.
// This is the most common and powerful way to resume, allowing you to target specific interrupt points
// (identified by their address/ID) and provide them with data.
//
// The params.Targets map should contain the addresses of the components to be resumed as keys. These addresses
// can point to any interruptible component in the entire execution graph, including ADK agents, compose
// graph nodes, or tools. The value can be the resume data for that component, or `nil` if no data is needed.
//
// When using this method:
//   - Components whose addresses are in the params.Targets map will receive `isResumeFlow = true` when they
//     call `GetResumeContext`.
//   - Interrupted components whose addresses are NOT in the params.Targets map must decide how to proceed:
//     -- "Leaf" components (the actual root causes of the original interrupt) MUST re-interrupt themselves
//     to preserve their state.
//     -- "Composite" agents (like SequentialAgent or ChatModelAgent) should generally proceed with their
//     execution. They act as conduits, allowing the resume signal to flow to their children. They will
//     naturally re-interrupt if one of their interrupted children re-interrupts, as they receive the
//     new `CompositeInterrupt` signal from them.
//
// ResumeWithParams 使用指定参数，从检查点继续被中断的执行。
// 这是最常见且最强大的恢复方式，可定位特定中断点（由其地址/ID 标识）并向其提供数据。
// params.Targets map 应以待恢复组件的地址为键。这些地址可以指向整个执行图中的任意可中断组件，包括 ADK 智能体、compose 图节点或工具。值可以是该组件的恢复数据；若不需要数据，则为 `nil`。
// 使用此方法时：
// - 地址在 params.Targets map 中的组件调用 GetResumeContext 时会收到 `isResumeFlow = true`。
// - 地址不在 params.Targets map 中的被中断组件必须自行决定如何继续：
// -- “Leaf”组件（原始中断的实际根因）必须重新中断自身，以保留其状态。
// -- “Composite”智能体（如 SequentialAgent 或 ChatModelAgent）通常应继续执行。它们充当通道，让恢复信号传递给子组件。如果某个被中断的子组件重新中断，它们会收到该子组件发出的新 `CompositeInterrupt` 信号，并自然地再次中断。
func (r *TypedRunner[M]) ResumeWithParams(ctx context.Context, checkPointID string, params *ResumeParams, opts ...AgentRunOption) (*AsyncIterator[*TypedAgentEvent[M]], error) {
	return r.resumeInternal(ctx, checkPointID, params.Targets, opts...)
}

func (r *TypedRunner[M]) resumeInternal(ctx context.Context, checkPointID string, resumeData map[string]any,
	opts ...AgentRunOption) (*AsyncIterator[*TypedAgentEvent[M]], error) {
	return typedRunnerResumeInternalImpl(r.a, r.store, ctx, checkPointID, resumeData, opts...)
}

func typedRunnerRunImpl[M MessageType](a TypedAgent[M], enableStreaming bool, store CheckPointStore, ctx context.Context, messages []M, opts ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	o := getCommonOptions(nil, opts...)

	input := &TypedAgentInput[M]{
		Messages:        messages,
		EnableStreaming: enableStreaming,
	}

	var zero M
	if _, ok := any(zero).(*schema.Message); ok {
		concreteAgent, _ := any(a).(Agent)
		fa := toFlowAgent(ctx, concreteAgent)
		if store != nil {
			fa.checkPointStore = store
		}
		concreteInput := any(input).(*AgentInput)
		ctx = ctxWithNewTypedRunCtx(ctx, input, o.sharedParentSession)
		AddSessionValues(ctx, o.sessionValues)

		iter := fa.Run(ctx, concreteInput, opts...)

		if store == nil && o.cancelCtx == nil {
			return any(iter).(*AsyncIterator[*TypedAgentEvent[M]])
		}

		niter, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
		go typedRunnerHandleIterImpl(enableStreaming, store, ctx, any(iter).(*AsyncIterator[*TypedAgentEvent[M]]), gen, o.checkPointID, o.cancelCtx)
		return niter
	}

	fa := toTypedFlowAgent(a)
	if store != nil {
		fa.checkPointStore = store
	}

	ctx = ctxWithNewTypedRunCtx(ctx, input, o.sharedParentSession)
	AddSessionValues(ctx, o.sessionValues)

	iter := fa.Run(ctx, input, opts...)

	if store == nil && o.cancelCtx == nil {
		return iter
	}

	niter, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go typedRunnerHandleIterImpl(enableStreaming, store, ctx, iter, gen, o.checkPointID, o.cancelCtx)
	return niter
}

func typedRunnerResumeInternalImpl[M MessageType](a TypedAgent[M], store CheckPointStore, ctx context.Context, checkPointID string, resumeData map[string]any, //nolint:revive // argument-limit
	opts ...AgentRunOption) (*AsyncIterator[*TypedAgentEvent[M]], error) {
	if store == nil {
		return nil, fmt.Errorf("failed to resume: store is nil")
	}

	ctx, runCtx, resumeInfo, err := runnerLoadCheckPointImpl(store, ctx, checkPointID)
	if err != nil {
		return nil, fmt.Errorf("failed to load from checkpoint: %w", err)
	}

	// Resume uses the streaming mode persisted in the checkpoint, not the value the
	// caller passed when constructing the runner. This is the runner's own invariant:
	// the checkpoint is the source of truth for what mode the original execution was
	// running in, and any new checkpoint written during this resume must preserve it.
	//
	// Resume 使用检查点中持久化的流式模式，而不是调用者构造 runner 时传入的值。
	// 这是 runner 自身的不变式：检查点是原始执行所运行模式的事实来源，恢复期间写入的任何新检查点都必须保留该模式。
	enableStreaming := resumeInfo.EnableStreaming

	o := getCommonOptions(nil, opts...)
	if o.sharedParentSession {
		parentSession := getSession(ctx)
		if parentSession != nil {
			runCtx.Session.Values = parentSession.Values
			runCtx.Session.valuesMtx = parentSession.valuesMtx
		}
	}
	if runCtx.Session.valuesMtx == nil {
		runCtx.Session.valuesMtx = &sync.Mutex{}
	}
	if runCtx.Session.Values == nil {
		runCtx.Session.Values = make(map[string]any)
	}

	ctx = setRunCtx(ctx, runCtx)
	AddSessionValues(ctx, o.sessionValues)

	if len(resumeData) > 0 {
		ctx = core.BatchResumeWithData(ctx, resumeData)
	}

	var zero M
	if _, ok := any(zero).(*schema.Message); ok {
		concreteAgent, _ := any(a).(Agent)
		fa := toFlowAgent(ctx, concreteAgent)
		ra, ok := Agent(fa).(ResumableAgent)
		if !ok {
			return nil, fmt.Errorf("agent %T does not support resume", a)
		}
		aIter := ra.Resume(ctx, resumeInfo, opts...)

		niter, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
		go typedRunnerHandleIterImpl(enableStreaming, store, ctx, any(aIter).(*AsyncIterator[*TypedAgentEvent[M]]), gen, &checkPointID, o.cancelCtx)
		return niter, nil
	}

	fa := toTypedFlowAgent(a)
	ra, ok := TypedAgent[M](fa).(TypedResumableAgent[M])
	if !ok {
		return nil, fmt.Errorf("agent %T does not support resume", a)
	}
	aIter := ra.Resume(ctx, resumeInfo, opts...)

	niter, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go typedRunnerHandleIterImpl(enableStreaming, store, ctx, aIter, gen, &checkPointID, o.cancelCtx)
	return niter, nil
}

func typedRunnerHandleIterImpl[M MessageType](enableStreaming bool, store CheckPointStore, ctx context.Context, aIter *AsyncIterator[*TypedAgentEvent[M]], //nolint:revive // argument-limit
	gen *AsyncGenerator[*TypedAgentEvent[M]], checkPointID *string, cancelCtx *cancelContext) {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			e := safe.NewPanicErr(panicErr, debug.Stack())
			gen.Send(&TypedAgentEvent[M]{Err: e})
		}

		gen.Close()
	}()
	var (
		interruptSignal *core.InterruptSignal
		legacyData      any
	)
	for {
		event, ok := aIter.Next()
		if !ok {
			break
		}

		if event.Err != nil {
			var cancelErr *CancelError
			if errors.As(event.Err, &cancelErr) {
				if cancelCtx != nil && cancelCtx.isRoot() && cancelCtx.shouldCancel() {
					cancelCtx.markCancelHandled()
				}
				if cancelErr.interruptSignal != nil && checkPointID != nil {
					cancelErr.InterruptContexts = core.ToInterruptContexts(cancelErr.interruptSignal, allowedAddressSegmentTypes)
					err := runnerSaveCheckPointImpl(enableStreaming, store, ctx, *checkPointID, &InterruptInfo{}, cancelErr.interruptSignal)
					if err != nil {
						gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("failed to save checkpoint on cancel: %w", err)})
					}
				}
				gen.Send(event)
				break
			}
		}

		if event.Action != nil && event.Action.internalInterrupted != nil {
			if interruptSignal != nil {
				panic("multiple interrupt actions should not happen in Runner")
			}
			interruptSignal = event.Action.internalInterrupted
			interruptContexts := core.ToInterruptContexts(interruptSignal, allowedAddressSegmentTypes)
			event = &TypedAgentEvent[M]{
				AgentName: event.AgentName,
				RunPath:   event.RunPath,
				Output:    event.Output,
				Action: &AgentAction{
					Interrupted: &InterruptInfo{
						Data:              event.Action.Interrupted.Data,
						InterruptContexts: interruptContexts,
					},
					internalInterrupted: interruptSignal,
				},
			}
			legacyData = event.Action.Interrupted.Data

			if checkPointID != nil {
				err := runnerSaveCheckPointImpl(enableStreaming, store, ctx, *checkPointID, &InterruptInfo{
					Data: legacyData,
				}, interruptSignal)
				if err != nil {
					gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("failed to save checkpoint: %w", err)})
				}
			}
		}

		gen.Send(event)
	}
}
