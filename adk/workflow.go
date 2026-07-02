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
	"runtime/debug"
	"sync"

	"github.com/cloudwego/eino/internal/core"
	"github.com/cloudwego/eino/internal/safe"
	"github.com/cloudwego/eino/schema"
)

type workflowAgentMode int

const (
	workflowAgentModeUnknown workflowAgentMode = iota
	workflowAgentModeSequential
	workflowAgentModeLoop
	workflowAgentModeParallel
)

type workflowAgent struct {
	name        string
	description string
	subAgents   []*flowAgent

	mode workflowAgentMode

	maxIterations int
}

func (a *workflowAgent) Name(_ context.Context) string {
	return a.name
}

func (a *workflowAgent) Description(_ context.Context) string {
	return a.description
}

func (a *workflowAgent) GetType() string {
	switch a.mode {
	case workflowAgentModeSequential:
		return "Sequential"
	case workflowAgentModeParallel:
		return "Parallel"
	case workflowAgentModeLoop:
		return "Loop"
	default:
		return "WorkflowAgent"
	}
}

func (a *workflowAgent) Run(ctx context.Context, _ *AgentInput, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iterator, generator := NewAsyncIteratorPair[*AgentEvent]()

	go func() {

		var err error
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				e := safe.NewPanicErr(panicErr, debug.Stack())
				generator.Send(&AgentEvent{Err: e})
			} else if err != nil {
				generator.Send(&AgentEvent{Err: err})
			}

			generator.Close()
		}()

		// Different workflow execution based on mode
		// 根据模式执行不同的 workflow
		switch a.mode {
		case workflowAgentModeSequential:
			err = a.runSequential(ctx, generator, nil, nil, opts...)
		case workflowAgentModeLoop:
			err = a.runLoop(ctx, generator, nil, nil, opts...)
		case workflowAgentModeParallel:
			err = a.runParallel(ctx, generator, nil, nil, opts...)
		default:
			err = fmt.Errorf("unsupported workflow agent mode: %d", a.mode)
		}
	}()

	return iterator
}

type sequentialWorkflowState struct {
	InterruptIndex int
}

type parallelWorkflowState struct {
	SubAgentEvents map[int][]*agentEventWrapper
}

type loopWorkflowState struct {
	LoopIterations int
	SubAgentIndex  int
}

func init() {
	schema.RegisterName[*sequentialWorkflowState]("eino_adk_sequential_workflow_state")
	schema.RegisterName[*parallelWorkflowState]("eino_adk_parallel_workflow_state")
	schema.RegisterName[*loopWorkflowState]("eino_adk_loop_workflow_state")
}

func (a *workflowAgent) Resume(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iterator, generator := NewAsyncIteratorPair[*AgentEvent]()

	go func() {
		var err error
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				e := safe.NewPanicErr(panicErr, debug.Stack())
				generator.Send(&AgentEvent{Err: e})
			} else if err != nil {
				generator.Send(&AgentEvent{Err: err})
			}

			generator.Close()
		}()

		state := info.InterruptState
		if state == nil {
			panic(fmt.Sprintf("workflowAgent.Resume: agent '%s' was asked to resume but has no state", a.Name(ctx)))
		}

		// Different workflow execution based on the type of our restored state.
		// 根据恢复状态的类型执行不同的 workflow。
		switch s := state.(type) {
		case *sequentialWorkflowState:
			err = a.runSequential(ctx, generator, s, info, opts...)
		case *parallelWorkflowState:
			err = a.runParallel(ctx, generator, s, info, opts...)
		case *loopWorkflowState:
			err = a.runLoop(ctx, generator, s, info, opts...)
		default:
			err = fmt.Errorf("unsupported workflow agent state type: %T", s)
		}
	}()
	return iterator
}

// WorkflowInterruptInfo stores interrupt information for workflow agents.
// CheckpointSchema: persisted via InterruptInfo.Data (gob).
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// WorkflowInterruptInfo 存储 workflow 智能体的中断信息。
// CheckpointSchema: 通过 InterruptInfo.Data 持久化（gob）。
// 不推荐：Workflow 智能体基于共享完整上下文的 agent transfer，
// 实证上未证明更有效。大多数多智能体场景建议改用
// ChatModelAgent 搭配 AgentTool 或 DeepAgent。
type WorkflowInterruptInfo struct {
	OrigInput *AgentInput

	SequentialInterruptIndex int
	SequentialInterruptInfo  *InterruptInfo

	LoopIterations int

	ParallelInterruptInfo map[int] /*index*/ *InterruptInfo
}

func (a *workflowAgent) runSequential(ctx context.Context,
	generator *AsyncGenerator[*AgentEvent], seqState *sequentialWorkflowState, info *ResumeInfo,
	opts ...AgentRunOption) (err error) {

	startIdx := 0

	seqCtx := ctx

	// If we are resuming, find which sub-agent to start from and prepare its context.
	// 如果正在恢复，找到要从哪个子智能体开始，并准备其上下文。
	if seqState != nil {
		startIdx = seqState.InterruptIndex

		var steps []string
		for i := 0; i < startIdx; i++ {
			steps = append(steps, a.subAgents[i].Name(seqCtx))
		}

		seqCtx = updateRunPathOnly(seqCtx, steps...)
	}

	for i := startIdx; i < len(a.subAgents); i++ {
		subAgent := a.subAgents[i]

		// Cancel check at transition boundary between sub-agents.
		// Transition boundaries are always safe to cancel at — no sub-agent
		// work is in progress, so any cancel mode is honoured.
		//
		// 在子智能体之间的转换边界检查取消。
		// 转换边界始终可以安全取消——没有子智能体
		// 正在执行工作，因此会遵循任何取消模式。
		if cancelCtx := getCancelContext(ctx); cancelCtx != nil && cancelCtx.shouldCancel() {
			state := &sequentialWorkflowState{InterruptIndex: i}
			event := cancelAtTransition(ctx, "Sequential workflow cancel at transition", state)
			generator.Send(event)
			return nil
		}

		var subIterator *AsyncIterator[*AgentEvent]
		if seqState != nil {
			wfInfo, _ := info.Data.(*WorkflowInterruptInfo)
			if wfInfo != nil && wfInfo.SequentialInterruptInfo != nil {
				// Sub-agent was interrupted — resume it.
				// 子智能体已中断——恢复它。
				subIterator = subAgent.Resume(seqCtx, &ResumeInfo{
					EnableStreaming: info.EnableStreaming,
					InterruptInfo:   wfInfo.SequentialInterruptInfo,
				}, opts...)
			} else {
				subIterator = subAgent.Run(seqCtx, nil, opts...)
			}
			seqState = nil
		} else {
			subIterator = subAgent.Run(seqCtx, nil, opts...)
		}

		seqCtx = updateRunPathOnly(seqCtx, subAgent.Name(seqCtx))

		var lastActionEvent *AgentEvent
		for {
			event, ok := subIterator.Next()
			if !ok {
				break
			}

			if event.Err != nil {
				// exit if report error
				// 如果 report 出错则退出
				generator.Send(event)
				return nil
			}

			if lastActionEvent != nil {
				generator.Send(lastActionEvent)
				lastActionEvent = nil
			}

			if event.Action != nil {
				lastActionEvent = event
				continue
			}
			generator.Send(event)
		}

		if lastActionEvent != nil {
			if lastActionEvent.Action.internalInterrupted != nil {
				// A sub-agent interrupted. Wrap it with our own state, including the index.
				// 子智能体发生中断。用我们自己的状态包装它，包括索引。
				state := &sequentialWorkflowState{
					InterruptIndex: i,
				}
				// Use CompositeInterrupt to funnel the sub-interrupt and add our own state.
				// The context for the composite interrupt must be the one from *before* the sub-agent ran.
				//
				// 使用 CompositeInterrupt 汇聚子中断，并添加我们自己的状态。
				// 复合中断的上下文必须是子智能体运行之前的上下文。
				event := CompositeInterrupt(ctx, "Sequential workflow interrupted", state,
					lastActionEvent.Action.internalInterrupted)

				// For backward compatibility, populate the deprecated Data field.
				// 为保持向后兼容，填充已弃用的 Data 字段。
				event.Action.Interrupted.Data = &WorkflowInterruptInfo{
					OrigInput:                getRunCtx(ctx).RootInput,
					SequentialInterruptIndex: i,
					SequentialInterruptInfo:  lastActionEvent.Action.Interrupted,
				}
				event.AgentName = lastActionEvent.AgentName
				event.RunPath = lastActionEvent.RunPath

				generator.Send(event)
				return nil
			}

			if lastActionEvent.Action.Exit {
				// Forward the event
				// 转发事件
				generator.Send(lastActionEvent)
				return nil
			}

			generator.Send(lastActionEvent)
		}
	}

	return nil
}

// BreakLoopAction is a programmatic-only agent action used to prematurely
// terminate the execution of a loop workflow agent.
// When a loop workflow agent receives this action from a sub-agent, it will stop its
// current iteration and will not proceed to the next one.
// It will mark the BreakLoopAction as Done, signalling to any 'upper level' loop agent
// that this action has been processed and should be ignored further up.
// This action is not intended to be used by LLMs.
//
// BreakLoopAction 是一种仅供程序使用的智能体动作，用于提前
// 终止 loop workflow 智能体的执行。
// 当 loop workflow 智能体从子智能体收到此动作时，会停止
// 当前迭代，并且不会进入下一次迭代。
// 它会将 BreakLoopAction 标记为 Done，向任何“上层”loop 智能体表明
// 此动作已被处理，后续应忽略。
// 此动作不应由 LLM 使用。
type BreakLoopAction struct {
	// From records the name of the agent that initiated the break loop action.
	// From 记录发起 break loop 动作的智能体名称。
	From string
	// Done is a state flag that can be used by the framework to mark when the
	// action has been handled.
	//
	// Done 是一个状态标志，框架可用它标记
	// 该动作何时已被处理。
	Done bool
	// CurrentIterations is populated by the framework to record at which
	// iteration the loop was broken.
	//
	// CurrentIterations 由框架填充，用于记录循环在哪次迭代被中断。
	CurrentIterations int
}

// NewBreakLoopAction creates a new BreakLoopAction, signaling a request
// to terminate the current loop.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// NewBreakLoopAction 创建一个新的 BreakLoopAction，表示请求终止当前循环。
// 不推荐：Workflow agents 基于共享完整上下文的 agent transfer 构建，实证上并未证明更有效。对于大多数多智能体场景，建议改用 ChatModelAgent 搭配 AgentTool 或 DeepAgent。
func NewBreakLoopAction(agentName string) *AgentAction {
	return &AgentAction{BreakLoop: &BreakLoopAction{
		From: agentName,
	}}
}

func (a *workflowAgent) runLoop(ctx context.Context, generator *AsyncGenerator[*AgentEvent],
	loopState *loopWorkflowState, resumeInfo *ResumeInfo, opts ...AgentRunOption) (err error) {

	if len(a.subAgents) == 0 {
		return nil
	}

	startIter := 0
	startIdx := 0

	loopCtx := ctx

	if loopState != nil {
		// We are resuming.
		// 正在恢复。
		startIter = loopState.LoopIterations
		startIdx = loopState.SubAgentIndex

		// Rebuild the loopCtx to have the correct RunPath up to the point of resumption.
		// 重建 loopCtx，使 RunPath 正确到达恢复点。
		var steps []string
		for i := 0; i < startIter; i++ {
			for _, subAgent := range a.subAgents {
				steps = append(steps, subAgent.Name(loopCtx))
			}
		}
		for i := 0; i < startIdx; i++ {
			steps = append(steps, a.subAgents[i].Name(loopCtx))
		}
		loopCtx = updateRunPathOnly(loopCtx, steps...)
	}

	for i := startIter; i < a.maxIterations || a.maxIterations == 0; i++ {
		for j := startIdx; j < len(a.subAgents); j++ {
			subAgent := a.subAgents[j]

			if cancelCtx := getCancelContext(ctx); cancelCtx != nil && cancelCtx.shouldCancel() {
				state := &loopWorkflowState{LoopIterations: i, SubAgentIndex: j}
				event := cancelAtTransition(ctx, "Loop workflow cancel at transition", state)
				generator.Send(event)
				return nil
			}

			var subIterator *AsyncIterator[*AgentEvent]
			if loopState != nil {
				wfInfo, _ := resumeInfo.Data.(*WorkflowInterruptInfo)
				if wfInfo != nil && wfInfo.SequentialInterruptInfo != nil {
					// Sub-agent was interrupted — resume it.
					// 子智能体已中断——恢复它。
					subIterator = subAgent.Resume(loopCtx, &ResumeInfo{
						EnableStreaming: resumeInfo.EnableStreaming,
						InterruptInfo:   wfInfo.SequentialInterruptInfo,
					}, opts...)
				} else {
					subIterator = subAgent.Run(loopCtx, nil, opts...)
				}
				loopState = nil // Only resume the first time.
				// 只在第一次恢复。
			} else {
				subIterator = subAgent.Run(loopCtx, nil, opts...)
			}

			loopCtx = updateRunPathOnly(loopCtx, subAgent.Name(loopCtx))

			var lastActionEvent *AgentEvent
			var breakLoopEvent *AgentEvent
			for {
				event, ok := subIterator.Next()
				if !ok {
					break
				}

				if event.Err != nil {
					generator.Send(event)
					return nil
				}

				if lastActionEvent != nil {
					if lastActionEvent.Action.BreakLoop != nil && !lastActionEvent.Action.BreakLoop.Done {
						lastActionEvent.Action.BreakLoop.Done = true
						lastActionEvent.Action.BreakLoop.CurrentIterations = i
						breakLoopEvent = lastActionEvent
					}
					generator.Send(lastActionEvent)
					lastActionEvent = nil
				}

				if event.Action != nil {
					lastActionEvent = event
					continue
				}
				generator.Send(event)
			}

			if lastActionEvent != nil {
				if lastActionEvent.Action.BreakLoop != nil && !lastActionEvent.Action.BreakLoop.Done {
					lastActionEvent.Action.BreakLoop.Done = true
					lastActionEvent.Action.BreakLoop.CurrentIterations = i
					breakLoopEvent = lastActionEvent
				}

				if lastActionEvent.Action.internalInterrupted != nil {
					// A sub-agent interrupted. Wrap it with our own loop state.
					// 子智能体发生中断。用我们自己的循环状态包装它。
					state := &loopWorkflowState{
						LoopIterations: i,
						SubAgentIndex:  j,
					}
					// Use CompositeInterrupt to funnel the sub-interrupt and add our own state.
					// 使用 CompositeInterrupt 汇聚子中断，并添加我们自己的状态。
					event := CompositeInterrupt(ctx, "Loop workflow interrupted", state,
						lastActionEvent.Action.internalInterrupted)

					// For backward compatibility, populate the deprecated Data field.
					// 为向后兼容，填充已弃用的 Data 字段。
					event.Action.Interrupted.Data = &WorkflowInterruptInfo{
						OrigInput:                getRunCtx(ctx).RootInput,
						LoopIterations:           i,
						SequentialInterruptIndex: j,
						SequentialInterruptInfo:  lastActionEvent.Action.Interrupted,
					}
					event.AgentName = lastActionEvent.AgentName
					event.RunPath = lastActionEvent.RunPath

					generator.Send(event)
					return
				}

				if lastActionEvent.Action.Exit {
					generator.Send(lastActionEvent)
					return
				}

				generator.Send(lastActionEvent)
			}

			if breakLoopEvent != nil {
				return
			}
		}

		// Reset the sub-agent index for the next iteration of the outer loop.
		// 重置子智能体索引，用于外层循环的下一次迭代。
		startIdx = 0
	}

	return nil
}

func (a *workflowAgent) runParallel(ctx context.Context, generator *AsyncGenerator[*AgentEvent],
	parState *parallelWorkflowState, resumeInfo *ResumeInfo, opts ...AgentRunOption) error {

	if len(a.subAgents) == 0 {
		return nil
	}

	var (
		wg                  sync.WaitGroup
		subInterruptSignals []*core.InterruptSignal
		dataMap             = make(map[int]*InterruptInfo)
		mu                  sync.Mutex
		agentNames          map[string]bool
		err                 error
		childContexts       = make([]context.Context, len(a.subAgents))
	)

	// If resuming, get the scoped ResumeInfo for each child that needs to be resumed.
	// 如果正在恢复，为每个需要恢复的子项获取其作用域内的 ResumeInfo。
	if parState != nil {
		agentNames, err = getNextResumeAgents(ctx, resumeInfo)
		if err != nil {
			return err
		}
	}

	// Fork contexts for each sub-agent
	// 为每个子智能体派生上下文
	for i := range a.subAgents {
		childContexts[i] = forkRunCtx(ctx)

		// If we're resuming and this agent has existing events, add them to the child context
		// 如果正在恢复且该 agent 已有事件，将它们添加到子上下文
		if parState != nil && parState.SubAgentEvents != nil {
			if existingEvents, ok := parState.SubAgentEvents[i]; ok {
				// Add existing events to the child's lane events
				// 将已有事件添加到子项的 lane events
				childRunCtx := getRunCtx(childContexts[i])
				if childRunCtx != nil && childRunCtx.Session != nil {
					if childRunCtx.Session.LaneEvents == nil {
						childRunCtx.Session.LaneEvents = &laneEvents{}
					}
					childRunCtx.Session.LaneEvents.Events = append(childRunCtx.Session.LaneEvents.Events, existingEvents...)
				}
			}
		}
	}

	// Cancel check before spawning parallel goroutines. No sub-agent work
	// is in progress, so any cancel mode is honoured at this boundary.
	//
	// 在启动并行 goroutine 前检查取消。此时没有子智能体工作在进行，因此任何取消模式都会在该边界生效。
	if cancelCtx := getCancelContext(ctx); cancelCtx != nil && cancelCtx.shouldCancel() {
		state := &parallelWorkflowState{}
		event := cancelAtTransition(ctx, "Parallel workflow cancel before spawn", state)
		generator.Send(event)
		return nil
	}

	for i := range a.subAgents {
		wg.Add(1)
		go func(idx int, agent *flowAgent) {
			defer func() {
				panicErr := recover()
				if panicErr != nil {
					e := safe.NewPanicErr(panicErr, debug.Stack())
					generator.Send(&AgentEvent{Err: e})
				}
				wg.Done()
			}()

			var iterator *AsyncIterator[*AgentEvent]

			if _, ok := agentNames[agent.Name(ctx)]; ok {
				childResumeInfo := &ResumeInfo{
					EnableStreaming: resumeInfo.EnableStreaming,
				}
				if wfInfo, ok := resumeInfo.Data.(*WorkflowInterruptInfo); ok && wfInfo != nil {
					childResumeInfo.InterruptInfo = wfInfo.ParallelInterruptInfo[idx]
				}
				iterator = agent.Resume(childContexts[idx], childResumeInfo, opts...)
			} else if parState != nil {
				// We are resuming, but this child is not in the next points map.
				// This means it finished successfully, so we don't run it.
				//
				// 正在恢复，但这个子项不在 next points map 中。
				// 这表示它已成功完成，因此不运行它。
				return
			} else {
				iterator = agent.Run(childContexts[idx], nil, opts...)
			}

			for {
				event, ok := iterator.Next()
				if !ok {
					break
				}
				if event.Action != nil && event.Action.internalInterrupted != nil {
					mu.Lock()
					subInterruptSignals = append(subInterruptSignals, event.Action.internalInterrupted)
					dataMap[idx] = event.Action.Interrupted
					mu.Unlock()
					break
				}
				generator.Send(event)
			}
		}(i, a.subAgents[i])
	}

	wg.Wait()

	if len(subInterruptSignals) == 0 {
		// Join all child contexts back to the parent
		// 将所有子上下文合并回父上下文
		joinRunCtxs(ctx, childContexts...)
		return nil
	}

	if len(subInterruptSignals) > 0 {
		// Before interrupting, collect the current events from each child context
		// 中断前，收集每个子上下文中的当前事件
		subAgentEvents := make(map[int][]*agentEventWrapper)
		for i, childCtx := range childContexts {
			childRunCtx := getRunCtx(childCtx)
			if childRunCtx != nil && childRunCtx.Session != nil && childRunCtx.Session.LaneEvents != nil {
				subAgentEvents[i] = childRunCtx.Session.LaneEvents.Events
			}
		}

		state := &parallelWorkflowState{
			SubAgentEvents: subAgentEvents,
		}
		event := CompositeInterrupt(ctx, "Parallel workflow interrupted", state, subInterruptSignals...)

		// For backward compatibility, populate the deprecated Data field.
		// 为向后兼容，填充已弃用的 Data 字段。
		event.Action.Interrupted.Data = &WorkflowInterruptInfo{
			OrigInput:             getRunCtx(ctx).RootInput,
			ParallelInterruptInfo: dataMap,
		}
		event.AgentName = a.Name(ctx)
		event.RunPath = getRunCtx(ctx).RunPath

		generator.Send(event)
	}

	return nil
}

func cancelAtTransition(ctx context.Context, info string, state any) *AgentEvent {
	// state is the workflow checkpoint state (e.g. sequentialWorkflowState);
	// nil for subContexts because this is a leaf interrupt with no child signals.
	//
	// state 是 workflow 检查点状态（例如 sequentialWorkflowState）；
	// subContexts 为 nil，因为这是没有子信号的叶子中断。
	is, err := core.Interrupt(ctx, info, state, nil,
		core.WithLayerPayload(getRunCtx(ctx).RunPath))
	if err != nil {
		return &AgentEvent{Err: err}
	}

	contexts := core.ToInterruptContexts(is, allowedAddressSegmentTypes)

	return &AgentEvent{
		Action: &AgentAction{
			Interrupted: &InterruptInfo{
				InterruptContexts: contexts,
			},
			internalInterrupted: is,
		},
	}
}

// SequentialAgentConfig is the configuration for NewSequentialAgent.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// SequentialAgentConfig 是 NewSequentialAgent 的配置。
// 不推荐：Workflow 智能体基于共享完整上下文的智能体转移构建，
// 实证上尚未证明更有效。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
type SequentialAgentConfig struct {
	Name        string
	Description string
	SubAgents   []Agent
}

// ParallelAgentConfig is the configuration for NewParallelAgent.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// ParallelAgentConfig 是 NewParallelAgent 的配置。
// 不推荐：Workflow 智能体基于共享完整上下文的智能体转移构建，
// 实证上尚未证明更有效。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
type ParallelAgentConfig struct {
	Name        string
	Description string
	SubAgents   []Agent
}

// LoopAgentConfig is the configuration for NewLoopAgent.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// LoopAgentConfig 是 NewLoopAgent 的配置。
// 不推荐：Workflow 智能体基于共享完整上下文的智能体转移构建，
// 实证上尚未证明更有效。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
type LoopAgentConfig struct {
	Name        string
	Description string
	SubAgents   []Agent

	MaxIterations int
}

func newWorkflowAgent(ctx context.Context, name, desc string,
	subAgents []Agent, mode workflowAgentMode, maxIterations int) (*flowAgent, error) {

	wa := &workflowAgent{
		name:        name,
		description: desc,
		mode:        mode,

		maxIterations: maxIterations,
	}

	fas := make([]Agent, len(subAgents))
	for i, subAgent := range subAgents {
		fas[i] = toFlowAgent(ctx, subAgent, WithDisallowTransferToParent())
	}

	fa, err := setSubAgents(ctx, wa, fas)
	if err != nil {
		return nil, err
	}

	wa.subAgents = fa.subAgents

	return fa, nil
}

// NewSequentialAgent creates an agent that runs sub-agents sequentially.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// NewSequentialAgent 创建一个按顺序运行子智能体的智能体。
// 不推荐：Workflow 智能体基于共享完整上下文的智能体转移构建，
// 实证上尚未证明更有效。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
func NewSequentialAgent(ctx context.Context, config *SequentialAgentConfig) (ResumableAgent, error) {
	return newWorkflowAgent(ctx, config.Name, config.Description, config.SubAgents, workflowAgentModeSequential, 0)
}

// NewParallelAgent creates an agent that runs sub-agents in parallel.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// NewParallelAgent 创建一个并行运行子智能体的智能体。
// 不推荐：Workflow 智能体基于共享完整上下文的智能体转移构建，
// 实证上尚未证明更有效。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
func NewParallelAgent(ctx context.Context, config *ParallelAgentConfig) (ResumableAgent, error) {
	return newWorkflowAgent(ctx, config.Name, config.Description, config.SubAgents, workflowAgentModeParallel, 0)
}

// NewLoopAgent creates an agent that loops over sub-agents with a max iteration limit.
//
// NOT RECOMMENDED: Workflow agents are built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// NewLoopAgent 创建一个按最大迭代次数限制循环运行子智能体的智能体。
// 不推荐：Workflow 智能体基于共享完整上下文的智能体转移构建，
// 实证上尚未证明更有效。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
func NewLoopAgent(ctx context.Context, config *LoopAgentConfig) (ResumableAgent, error) {
	return newWorkflowAgent(ctx, config.Name, config.Description, config.SubAgents, workflowAgentModeLoop, config.MaxIterations)
}
