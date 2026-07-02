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

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	icb "github.com/cloudwego/eino/internal/callbacks"
)

// AgentCallbackInput represents the input passed to agent callbacks during OnStart.
// Use ConvAgentCallbackInput to safely convert from callbacks.CallbackInput.
//
// AgentCallbackInput 表示 OnStart 期间传给智能体回调的输入。
// 使用 ConvAgentCallbackInput 可从 callbacks.CallbackInput 安全转换。
type AgentCallbackInput struct {
	// Input contains the agent input for a new run. Nil when resuming.
	// Input 包含新 run 的智能体输入。恢复时为 nil。
	Input *AgentInput
	// ResumeInfo contains resume information when resuming from an interrupt. Nil for new runs.
	// ResumeInfo 包含从中断恢复时的恢复信息。新 run 时为 nil。
	ResumeInfo *ResumeInfo
}

// AgentCallbackOutput represents the output passed to agent callbacks during OnEnd.
// Use ConvAgentCallbackOutput to safely convert from callbacks.CallbackOutput.
//
// Important: The Events iterator should be consumed asynchronously to avoid blocking
// the agent execution. Each callback handler receives an independent copy of the iterator.
//
// AgentCallbackOutput 表示 OnEnd 期间传给智能体回调的输出。
// 使用 ConvAgentCallbackOutput 可从 callbacks.CallbackOutput 安全转换。
// 重要：Events 迭代器应异步消费，以避免阻塞智能体执行。每个回调处理器都会收到迭代器的独立副本。
type AgentCallbackOutput struct {
	// Events provides a stream of agent events. Each handler receives its own copy.
	// Events 提供智能体事件流。每个处理器都会收到自己的副本。
	Events *AsyncIterator[*AgentEvent]
}

func copyTypedEventIterator[M MessageType](iter *AsyncIterator[*TypedAgentEvent[M]], n int) []*AsyncIterator[*TypedAgentEvent[M]] {
	if n <= 0 {
		return nil
	}
	if n == 1 {
		return []*AsyncIterator[*TypedAgentEvent[M]]{iter}
	}

	iterators := make([]*AsyncIterator[*TypedAgentEvent[M]], n)
	generators := make([]*AsyncGenerator[*TypedAgentEvent[M]], n)
	for i := 0; i < n; i++ {
		iterators[i], generators[i] = NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	}

	go func() {
		defer func() {
			for _, g := range generators {
				g.Close()
			}
		}()

		for {
			event, ok := iter.Next()
			if !ok {
				break
			}
			for i := 0; i < n-1; i++ {
				generators[i].Send(copyTypedAgentEvent(event))
			}
			generators[n-1].Send(event)
		}
	}()

	return iterators
}

func copyAgentCallbackOutput(out *AgentCallbackOutput, n int) []*AgentCallbackOutput {
	if out == nil || out.Events == nil {
		result := make([]*AgentCallbackOutput, n)
		for i := 0; i < n; i++ {
			result[i] = out
		}
		return result
	}
	iters := copyTypedEventIterator(out.Events, n)
	result := make([]*AgentCallbackOutput, n)
	for i, iter := range iters {
		result[i] = &AgentCallbackOutput{Events: iter}
	}
	return result
}

// ConvAgentCallbackInput converts a generic CallbackInput to AgentCallbackInput.
// Returns nil if the input is not an AgentCallbackInput.
//
// ConvAgentCallbackInput 将通用 CallbackInput 转换为 AgentCallbackInput。
// 如果输入不是 AgentCallbackInput，则返回 nil。
func ConvAgentCallbackInput(input callbacks.CallbackInput) *AgentCallbackInput {
	if v, ok := input.(*AgentCallbackInput); ok {
		return v
	}
	return nil
}

// ConvAgentCallbackOutput converts a generic CallbackOutput to AgentCallbackOutput.
// Returns nil if the output is not an AgentCallbackOutput.
//
// ConvAgentCallbackOutput 将通用 CallbackOutput 转换为 AgentCallbackOutput。
// 如果输出不是 AgentCallbackOutput，则返回 nil。
func ConvAgentCallbackOutput(output callbacks.CallbackOutput) *AgentCallbackOutput {
	if v, ok := output.(*AgentCallbackOutput); ok {
		return v
	}
	return nil
}

func initAgentCallbacks(ctx context.Context, agentName, agentType string, opts ...AgentRunOption) context.Context {
	ri := &callbacks.RunInfo{
		Name:      agentName,
		Type:      agentType,
		Component: ComponentOfAgent,
	}

	o := getCommonOptions(nil, opts...)
	if len(o.handlers) == 0 {
		return icb.ReuseHandlers(ctx, ri)
	}
	return icb.AppendHandlers(ctx, ri, o.handlers...)
}

func getAgentType(agent Agent) string {
	if typer, ok := agent.(components.Typer); ok {
		return typer.GetType()
	}
	return ""
}

// TypedAgentCallbackInput represents the input passed to typed agent callbacks during OnStart.
// Use ConvTypedCallbackInput to safely convert from callbacks.CallbackInput.
//
// TypedAgentCallbackInput 表示 OnStart 期间传给 typed agent 回调的输入。
// 使用 ConvTypedCallbackInput 可从 callbacks.CallbackInput 安全转换。
type TypedAgentCallbackInput[M MessageType] struct {
	// Input contains the agent input for a new run. Nil when resuming.
	// Input 包含新 run 的智能体输入。恢复时为 nil。
	Input *TypedAgentInput[M]
	// ResumeInfo contains resume information when resuming from an interrupt. Nil for new runs.
	// ResumeInfo 包含从中断恢复时的恢复信息。新 run 时为 nil。
	ResumeInfo *ResumeInfo
}

// TypedAgentCallbackOutput represents the output passed to typed agent callbacks during OnEnd.
// Use ConvTypedCallbackOutput to safely convert from callbacks.CallbackOutput.
//
// Important: The Events iterator should be consumed asynchronously to avoid blocking
// the agent execution. Each callback handler receives an independent copy of the iterator.
//
// TypedAgentCallbackOutput 表示 OnEnd 期间传给类型化智能体回调的输出。
// 使用 ConvTypedCallbackOutput 可安全地从 callbacks.CallbackOutput 转换。
// 重要：Events 迭代器应异步消费，以避免阻塞智能体执行。每个回调处理器都会收到该迭代器的独立副本。
type TypedAgentCallbackOutput[M MessageType] struct {
	// Events provides a stream of agent events. Each handler receives its own copy.
	// Events 提供智能体事件流。每个处理器都会收到自己的副本。
	Events *AsyncIterator[*TypedAgentEvent[M]]
}

// ConvTypedCallbackInput converts a callbacks.CallbackInput to *TypedAgentCallbackInput[M].
// Returns nil if the input is not of the expected type.
//
// ConvTypedCallbackInput 将 callbacks.CallbackInput 转换为 *TypedAgentCallbackInput[M]。
// 如果输入不是期望类型，则返回 nil。
func ConvTypedCallbackInput[M MessageType](input callbacks.CallbackInput) *TypedAgentCallbackInput[M] {
	if v, ok := input.(*TypedAgentCallbackInput[M]); ok {
		return v
	}
	return nil
}

// ConvTypedCallbackOutput converts a callbacks.CallbackOutput to *TypedAgentCallbackOutput[M].
// Returns nil if the output is not of the expected type.
//
// ConvTypedCallbackOutput 将 callbacks.CallbackOutput 转换为 *TypedAgentCallbackOutput[M]。
// 如果输出不是期望类型，则返回 nil。
func ConvTypedCallbackOutput[M MessageType](output callbacks.CallbackOutput) *TypedAgentCallbackOutput[M] {
	if v, ok := output.(*TypedAgentCallbackOutput[M]); ok {
		return v
	}
	return nil
}

func copyTypedCallbackOutput[M MessageType](out *TypedAgentCallbackOutput[M], n int) []*TypedAgentCallbackOutput[M] {
	if out == nil || out.Events == nil {
		result := make([]*TypedAgentCallbackOutput[M], n)
		for i := 0; i < n; i++ {
			result[i] = out
		}
		return result
	}
	iters := copyTypedEventIterator(out.Events, n)
	result := make([]*TypedAgentCallbackOutput[M], n)
	for i, iter := range iters {
		result[i] = &TypedAgentCallbackOutput[M]{Events: iter}
	}
	return result
}

func initAgenticCallbacks(ctx context.Context, agentName, agentType string, opts ...AgentRunOption) context.Context {
	ri := &callbacks.RunInfo{
		Name:      agentName,
		Type:      agentType,
		Component: ComponentOfAgenticAgent,
	}

	o := getCommonOptions(nil, opts...)
	if len(o.handlers) == 0 {
		return icb.ReuseHandlers(ctx, ri)
	}
	return icb.AppendHandlers(ctx, ri, o.handlers...)
}
