/*
 * Copyright 2024 CloudWeGo Authors
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

// Package host implements the host pattern for multi-agent system.
// Package host 实现多智能体系统的 host 模式。
package host

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/flow/agent"
	"github.com/cloudwego/eino/schema"
)

// MultiAgent is a host multi-agent system.
// A host agent is responsible for deciding which specialist to 'hand off' the task to.
// One or more specialist agents are responsible for completing the task.
//
// MultiAgent 是一个 host 多智能体系统。
// host 智能体负责决定将任务“移交”给哪个专家。
// 一个或多个专家智能体负责完成任务。
type MultiAgent struct {
	runnable         compose.Runnable[[]*schema.Message, *schema.Message]
	graph            *compose.Graph[[]*schema.Message, *schema.Message]
	graphAddNodeOpts []compose.GraphAddNodeOpt
}

// Generate runs the multi-agent synchronously and returns the final message.
// Generate 同步运行多智能体并返回最终消息。
func (ma *MultiAgent) Generate(ctx context.Context, input []*schema.Message, opts ...agent.AgentOption) (*schema.Message, error) {
	composeOptions := agent.GetComposeOptions(opts...)

	handler := convertCallbacks(opts...)
	if handler != nil {
		composeOptions = append(composeOptions, compose.WithCallbacks(handler).DesignateNode(ma.HostNodeKey()))
	}

	return ma.runnable.Invoke(ctx, input, composeOptions...)
}

// Stream runs the multi-agent in streaming mode and returns a message stream.
// Stream 以流式模式运行多智能体并返回消息流。
func (ma *MultiAgent) Stream(ctx context.Context, input []*schema.Message, opts ...agent.AgentOption) (*schema.StreamReader[*schema.Message], error) {
	composeOptions := agent.GetComposeOptions(opts...)

	handler := convertCallbacks(opts...)
	if handler != nil {
		composeOptions = append(composeOptions, compose.WithCallbacks(handler).DesignateNode(ma.HostNodeKey()))
	}

	return ma.runnable.Stream(ctx, input, composeOptions...)
}

// ExportGraph exports the underlying graph from MultiAgent, along with the []compose.GraphAddNodeOpt to be used when adding this graph to another graph.
// ExportGraph 导出 MultiAgent 的底层图，以及将该图添加到另一个图时使用的 []compose.GraphAddNodeOpt。
func (ma *MultiAgent) ExportGraph() (compose.AnyGraph, []compose.GraphAddNodeOpt) {
	return ma.graph, ma.graphAddNodeOpts
}

// HostNodeKey returns the graph node key used for the host agent.
// HostNodeKey 返回 host 智能体使用的图节点 key。
func (ma *MultiAgent) HostNodeKey() string {
	return defaultHostNodeKey
}

// MultiAgentConfig is the config for host multi-agent system.
// MultiAgentConfig 是 host 多智能体系统的配置。
type MultiAgentConfig struct {
	Host        Host
	Specialists []*Specialist

	Name string // the name of the host multi-agent
	// host 多智能体的名称
	HostNodeName string // the name of the host node in the graph, default is "host"
	// 图中 host 节点的名称，默认为 "host"
	// StreamToolCallChecker is a function to determine whether the model's streaming output contains tool calls.
	// Different models have different ways of outputting tool calls in streaming mode:
	// - Some models (like OpenAI) output tool calls directly
	// - Others (like Claude) output text first, then tool calls
	// This handler allows custom logic to check for tool calls in the stream.
	// It should return:
	// - true if the output contains tool calls and agent should continue processing
	// - false if no tool calls and agent should stop
	// Note: This field only needs to be configured when using streaming mode
	// Note: The handler MUST close the modelOutput stream before returning
	// Optional. By default, it checks if the first chunk contains tool calls.
	// Note: The default implementation does not work well with Claude, which typically outputs tool calls after text content.
	// Note: If your ChatModel doesn't output tool calls first, you can try adding prompts to constrain the model from generating extra text during the tool call.
	//
	// StreamToolCallChecker 是一个函数，用于判断模型的流式输出是否包含工具调用。
	// 不同模型在流式模式下输出工具调用的方式不同：
	// - 有些模型（如 OpenAI）会直接输出工具调用
	// - 其他模型（如 Claude）会先输出文本，再输出工具调用
	// 该处理器允许使用自定义逻辑检查流中是否有工具调用。
	// 它应返回：
	// - 如果输出包含工具调用且智能体应继续处理，则返回 true
	// - 如果没有工具调用且智能体应停止，则返回 false
	// 注意：仅在使用流式模式时才需要配置此字段
	// 注意：该处理器返回前必须关闭 modelOutput 流
	// 可选。默认会检查第一个 chunk 是否包含工具调用。
	// 注意：默认实现不太适用于 Claude，因为 Claude 通常会在文本内容之后输出工具调用。
	// 注意：如果你的 ChatModel 不会先输出工具调用，可以尝试添加提示，约束模型在工具调用期间不要生成额外文本。
	StreamToolCallChecker func(ctx context.Context, modelOutput *schema.StreamReader[*schema.Message]) (bool, error)

	// Summarizer is the summarizer agent that will summarize the outputs of all the chosen specialist agents.
	// Only when the Host agent picks multiple Specialist will this be called.
	// If you do not provide a summarizer, a default summarizer that simply concatenates all the output messages into one message will be used.
	// Note: the default summarizer do not support streaming.
	//
	// Summarizer 是汇总智能体，用于汇总所有被选中专家智能体的输出。
	// 只有当 Host 智能体选择多个 Specialist 时才会调用它。
	// 如果未提供 summarizer，将使用默认 summarizer，它只是将所有输出消息拼接成一条消息。
	// 注意：默认 summarizer 不支持流式。
	Summarizer *Summarizer
}

func (conf *MultiAgentConfig) validate() error {
	if conf == nil {
		return errors.New("host multi agent config is nil")
	}

	if conf.Host.ChatModel == nil && conf.Host.ToolCallingModel == nil {
		return errors.New("host multi agent host ChatModel is nil")
	}

	if len(conf.Specialists) == 0 {
		return errors.New("host multi agent specialists are empty")
	}

	for _, s := range conf.Specialists {
		if s.ChatModel == nil && s.Invokable == nil && s.Streamable == nil {
			return fmt.Errorf("specialist %s has no chat model or Invokable or Streamable", s.Name)
		}

		if err := s.AgentMeta.validate(); err != nil {
			return err
		}
	}

	return nil
}

// AgentMeta is the meta information of an agent within a multi-agent system.
// AgentMeta 是多智能体系统中某个智能体的元信息。
type AgentMeta struct {
	Name string // the name of the agent, should be unique within multi-agent system
	// 智能体名称，在多智能体系统内应唯一
	IntendedUse string // the intended use-case of the agent, used as the reason for the multi-agent system to hand over control to this agent
	// 智能体的预期用例，用作多智能体系统将控制权移交给该智能体的原因
}

func (am AgentMeta) validate() error {
	if len(am.Name) == 0 {
		return errors.New("agent meta name is empty")
	}

	if len(am.IntendedUse) == 0 {
		return errors.New("agent meta intended use is empty")
	}

	return nil
}

// Host is the host agent within a multi-agent system.
// Currently, it can only be a model.ChatModel.
//
// Host 是多智能体系统中的主智能体。
// 目前只能是 model.ChatModel。
type Host struct {
	ToolCallingModel model.ToolCallingChatModel
	// Deprecated: ChatModel is deprecated, please use ToolCallingModel instead.
	// This field will be removed in a future release.
	//
	// Deprecated: ChatModel 已废弃，请改用 ToolCallingModel。
	// 该字段将在未来版本中移除。
	ChatModel    model.ChatModel
	SystemPrompt string
}

// Specialist is a specialist agent within a host multi-agent system.
// It can be a model.ChatModel or any Invokable and/or Streamable, such as react.Agent.
// ChatModel and (Invokable / Streamable) are mutually exclusive, only one should be provided.
// notice: SystemPrompt only effects when ChatModel has been set.
// If Invokable is provided but not Streamable, then the Specialist will be 'compose.InvokableLambda'.
// If Streamable is provided but not Invokable, then the Specialist will be 'compose.StreamableLambda'.
// if Both Invokable and Streamable is provided, then the Specialist will be 'compose.AnyLambda'.
//
// Specialist 是 host 多智能体系统中的专家智能体。
// 它可以是 model.ChatModel，或任意 Invokable 和/或 Streamable，例如 react.Agent。
// ChatModel 与 (Invokable / Streamable) 互斥，只应提供其中一种。
// 注意：SystemPrompt 仅在设置了 ChatModel 时生效。
// 如果提供了 Invokable 但未提供 Streamable，则 Specialist 将是 'compose.InvokableLambda'。
// 如果提供了 Streamable 但未提供 Invokable，则 Specialist 将是 'compose.StreamableLambda'。
// 如果同时提供 Invokable 和 Streamable，则 Specialist 将是 'compose.AnyLambda'。
type Specialist struct {
	AgentMeta

	ChatModel    model.BaseChatModel
	SystemPrompt string

	Invokable  compose.Invoke[[]*schema.Message, *schema.Message, agent.AgentOption]
	Streamable compose.Stream[[]*schema.Message, *schema.Message, agent.AgentOption]
}

// Summarizer defines a lightweight agent used to summarize
// conversations or tool outputs using a chat model and prompt.
//
// Summarizer 定义了一个轻量智能体，使用聊天模型和提示来总结
// 对话或工具输出。
type Summarizer struct {
	ChatModel    model.BaseChatModel
	SystemPrompt string
}

func firstChunkStreamToolCallChecker(_ context.Context, sr *schema.StreamReader[*schema.Message]) (bool, error) {
	defer sr.Close()

	for {
		msg, err := sr.Recv()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}

		if len(msg.ToolCalls) > 0 {
			return true, nil
		}

		if len(msg.Content) == 0 { // skip empty chunks at the front
			// 跳过开头的空 chunk
			continue
		}

		return false, nil
	}
}
