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

package react

import (
	"context"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/flow/agent"
	"github.com/cloudwego/eino/internal"
	"github.com/cloudwego/eino/schema"
	ub "github.com/cloudwego/eino/utils/callbacks"
)

// WithToolOptions returns an agent option that specifies tool.Option for the tools in agent.
// WithToolOptions 返回一个智能体 option，用于为智能体中的工具指定 tool.Option。
func WithToolOptions(opts ...tool.Option) agent.AgentOption {
	return agent.WithComposeOptions(compose.WithToolsNodeOption(compose.WithToolOption(opts...)))
}

// WithChatModelOptions returns an agent option that specifies model.Option for the chat model in agent.
// WithChatModelOptions 返回一个智能体 option，用于为智能体中的聊天模型指定 model.Option。
func WithChatModelOptions(opts ...model.Option) agent.AgentOption {
	return agent.WithComposeOptions(compose.WithChatModelOption(opts...))
}

// WithToolList returns an agent option that specifies compose.ToolsNodeOption for ToolsNode in agent.
// If you also need to pass ToolInfo to the chat model, use WithTools instead.
// Deprecated: This changes tool list for ToolsNode ONLY.
//
// WithToolList 返回一个智能体 option，用于为智能体中的 ToolsNode 指定 compose.ToolsNodeOption。
// 如果还需要将 ToolInfo 传给聊天模型，请使用 WithTools。
// Deprecated: 这只会更改 ToolsNode 的工具列表。
func WithToolList(tools ...tool.BaseTool) agent.AgentOption {
	return agent.WithComposeOptions(compose.WithToolsNodeOption(compose.WithToolList(tools...)))
}

// WithTools is a convenience function that configures a React agent with a list of tools.
// It performs two essential operations:
//  1. Extracts tool information for the chat model to understand available tools
//  2. Registers the actual tool implementations for execution
//
// Parameters:
//   - ctx: The context for the operation, used when calling Info() on each tool
//   - tools: A variadic list of tools that must implement either InvokableTool or StreamableTool interfaces
//
// Returns:
//   - []agent.AgentOption: A slice containing exactly 2 agent options:
//   - Option 1: Configures the chat model with tool schemas via model.WithTools(toolInfos)
//   - Option 2: Registers the tool implementations via compose.WithToolList(tools...)
//   - error: Returns an error if any tool's Info() method fails
//
// Usage Example:
//
//	ctx := context.Background()
//	agentOptions, err := WithTools(ctx, myTool1, myTool2, myTool3)
//	if err != nil {
//	    return fmt.Errorf("failed to configure tools: %w", err)
//	}
//
//	agent, err := react.NewAgent(ctx, &react.AgentConfig{
//	    ToolCallingModel: myModel,
//	    // other config...
//	})
//	if err != nil {
//	    return fmt.Errorf("failed to create agent: %w", err)
//	}
//
//	// Use the tool options with Generate or Stream methods
//	msg, err := agent.Generate(ctx, messages, agentOptions...)
//	// or
//	stream, err := agent.Stream(ctx, messages, agentOptions...)
//
// Comparison with Related Functions:
//   - WithToolList: Only registers tool implementations, doesn't configure the chat model
//   - WithTools: Comprehensive setup that handles both chat model configuration and tool registration
//
// Notes:
//   - The function always returns exactly 2 options when successful
//   - Both returned options should be applied to the agent for proper tool functionality
//
// WithTools 是一个便捷函数，用工具列表配置 React 智能体。
// 它执行两个核心操作：
// 1. 提取工具信息，使聊天模型了解可用工具
// 2. 注册实际的工具实现以供执行
// 参数：
// - ctx: 操作的 context，在调用每个工具的 Info() 时使用
// - tools: 可变参数工具列表，必须实现 InvokableTool 或 StreamableTool 接口
// 返回：
// - []agent.AgentOption: 一个正好包含 2 个智能体 option 的切片：
// - Option 1: 通过 model.WithTools(toolInfos) 为聊天模型配置工具 schema
// - Option 2: 通过 compose.WithToolList(tools...) 注册工具实现
// - error: 如果任意工具的 Info() 方法失败，则返回错误
// 用法示例：
// ctx := context.Background()
// agentOptions, err := WithTools(ctx, myTool1, myTool2, myTool3)
// if err != nil {
// return fmt.Errorf("failed to configure tools: %w", err)
// }
// agent, err := react.NewAgent(ctx, &react.AgentConfig{
// ToolCallingModel: myModel,
// other config...
// })
// if err != nil {
// return fmt.Errorf("failed to create agent: %w", err)
// }
// Use the tool options with Generate or Stream methods
// msg, err := agent.Generate(ctx, messages, agentOptions...)
// or
// stream, err := agent.Stream(ctx, messages, agentOptions...)
// 与相关函数对比：
// - WithToolList: 只注册工具实现，不配置聊天模型
// - WithTools: 完整设置，同时处理聊天模型配置和工具注册
// 说明：
// - 成功时该函数总是正好返回 2 个 option
// - 应将两个返回的 option 都应用到智能体，才能保证工具功能正常
func WithTools(ctx context.Context, tools ...tool.BaseTool) ([]agent.AgentOption, error) {
	toolInfos := make([]*schema.ToolInfo, 0, len(tools))
	for _, tl := range tools {
		info, err := tl.Info(ctx)
		if err != nil {
			return nil, err
		}

		toolInfos = append(toolInfos, info)
	}

	opts := make([]agent.AgentOption, 2)
	opts[0] = agent.WithComposeOptions(compose.WithChatModelOption(model.WithTools(toolInfos)))
	opts[1] = agent.WithComposeOptions(compose.WithToolsNodeOption(compose.WithToolList(tools...)))
	return opts, nil
}

// Iterator provides a lightweight FIFO stream of values and errors
// produced during agent execution.
//
// Iterator 提供一个轻量级 FIFO 流，用于传递智能体执行期间
// 产生的值和错误。
type Iterator[T any] struct {
	ch *internal.UnboundedChan[item[T]]
}

// Next retrieves the next value from the iterator.
// It returns the zero value and false when the stream is exhausted.
//
// Next 从 iterator 中获取下一个值。
// 当流耗尽时，返回零值和 false。
func (iter *Iterator[T]) Next() (T, bool, error) {
	ch := iter.ch
	if ch == nil {
		var zero T
		return zero, false, nil
	}

	i, ok := ch.Receive()
	if !ok {
		var zero T
		return zero, false, nil
	}

	return i.v, true, i.err
}

// MessageFuture exposes asynchronous accessors for messages produced
// by Generate and Stream calls.
//
// MessageFuture 提供对 Generate 和 Stream 调用产生的消息的异步访问方法。
type MessageFuture interface {
	// GetMessages returns an iterator for retrieving messages generated during "agent.Generate" calls.
	// GetMessages 返回一个 iterator，用于获取 "agent.Generate" 调用期间生成的消息。
	GetMessages() *Iterator[*schema.Message]

	// GetMessageStreams returns an iterator for retrieving streaming messages generated during "agent.Stream" calls.
	// GetMessageStreams 返回一个 iterator，用于获取 "agent.Stream" 调用期间生成的流式消息。
	GetMessageStreams() *Iterator[*schema.StreamReader[*schema.Message]]
}

// WithMessageFuture returns an agent option and a MessageFuture interface instance.
// The option configures the agent to collect messages generated during execution,
// while the MessageFuture interface allows users to asynchronously retrieve these messages.
//
// This function works correctly both when the agent is used directly and when it is
// embedded as a subgraph within another graph. Graph callbacks are filtered using
// the address in the context: only callbacks whose address contains a runnable segment
// matching the agent's configured graph name are processed.
//
// WithMessageFuture 返回一个智能体 option 和一个 MessageFuture 接口实例。
// 该 option 配置智能体收集执行期间生成的消息，
// 而 MessageFuture 接口允许用户异步获取这些消息。
// 无论智能体被直接使用，还是作为子图嵌入到另一个图中，
// 该函数都能正常工作。Graph 回调会使用 context 中的地址进行过滤：
// 只处理其地址包含与智能体配置的 graph 名称匹配的 runnable 段的回调。
func WithMessageFuture() (agent.AgentOption, MessageFuture) {
	h := &cbHandler{started: make(chan struct{}), graphName: GraphName}

	cmHandler := &ub.ModelCallbackHandler{
		OnEnd:                 h.onChatModelEnd,
		OnEndWithStreamOutput: h.onChatModelEndWithStreamOutput,
	}
	createToolResultSender := func() toolResultSender {
		return func(toolName, callID, result string) {
			msg := schema.ToolMessage(result, callID, schema.WithToolName(toolName))
			h.sendMessage(msg)
		}
	}
	createStreamToolResultSender := func() streamToolResultSender {
		return func(toolName, callID string, resultStream *schema.StreamReader[string]) {
			cvt := func(in string) (*schema.Message, error) {
				return schema.ToolMessage(in, callID, schema.WithToolName(toolName)), nil
			}
			msgStream := schema.StreamReaderWithConvert(resultStream, cvt)
			h.sendMessageStream(msgStream)
		}
	}

	createEnhancedToolResultSender := func() enhancedToolResultSender {
		return func(toolName, callID string, result *schema.ToolResult) {
			var err error
			msg := schema.ToolMessage("", callID, schema.WithToolName(toolName))
			msg.UserInputMultiContent, err = result.ToMessageInputParts()
			if err != nil {
				return
			}
			h.sendMessage(msg)
		}
	}

	createEnhancedStreamToolResultSender := func() enhancedStreamToolResultSender {
		return func(toolName, callID string, resultStream *schema.StreamReader[*schema.ToolResult]) {
			cvt := func(result *schema.ToolResult) (*schema.Message, error) {
				var err error
				msg := schema.ToolMessage("", callID, schema.WithToolName(toolName))
				msg.UserInputMultiContent, err = result.ToMessageInputParts()
				if err != nil {
					return nil, err
				}
				return msg, nil
			}
			msgStream := schema.StreamReaderWithConvert(resultStream, cvt)
			h.sendMessageStream(msgStream)
		}
	}

	graphHandler := callbacks.NewHandlerBuilder().
		OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
			ctx = h.onGraphStart(ctx, info, input)
			return setToolResultSendersToCtx(ctx, &toolResultSenders{
				sender:                         createToolResultSender(),
				streamSender:                   createStreamToolResultSender(),
				enhancedResultSender:           createEnhancedToolResultSender(),
				enhancedStreamToolResultSender: createEnhancedStreamToolResultSender(),
			})
		}).
		OnStartWithStreamInputFn(func(ctx context.Context, info *callbacks.RunInfo, input *schema.StreamReader[callbacks.CallbackInput]) context.Context {
			ctx = h.onGraphStartWithStreamInput(ctx, info, input)
			return setToolResultSendersToCtx(ctx, &toolResultSenders{
				sender:                         createToolResultSender(),
				streamSender:                   createStreamToolResultSender(),
				enhancedResultSender:           createEnhancedToolResultSender(),
				enhancedStreamToolResultSender: createEnhancedStreamToolResultSender(),
			})
		}).
		OnEndFn(h.onGraphEnd).
		OnEndWithStreamOutputFn(h.onGraphEndWithStreamOutput).
		OnErrorFn(h.onGraphError).Build()
	cb := ub.NewHandlerHelper().ChatModel(cmHandler).Graph(graphHandler).Handler()
	option := agent.WithComposeOptions(compose.WithCallbacks(cb))

	return option, h
}

type item[T any] struct {
	v   T
	err error
}

type cbHandler struct {
	graphName string

	ownAddress compose.Address
	ownClaimed bool

	msgs  *internal.UnboundedChan[item[*schema.Message]]
	sMsgs *internal.UnboundedChan[item[*schema.StreamReader[*schema.Message]]]

	started chan struct{}
}

func (h *cbHandler) GetMessages() *Iterator[*schema.Message] {
	<-h.started

	return &Iterator[*schema.Message]{ch: h.msgs}
}

func (h *cbHandler) GetMessageStreams() *Iterator[*schema.StreamReader[*schema.Message]] {
	<-h.started

	return &Iterator[*schema.StreamReader[*schema.Message]]{ch: h.sMsgs}
}

// isOwnGraph reports whether the callback is being invoked for the React agent's own graph.
//
// After the first onGraphStart call records h.ownAddress, subsequent calls compare the
// current context address against that exact recorded address. This handles nested React
// agents correctly: each cbHandler instance records its own unique full address path, so
// two React agents at different nesting depths (e.g., an outer agent whose tool invokes
// another React agent) will never interfere with each other even if they share the same
// graph name.
//
// isOwnGraph 报告该回调是否针对 React 智能体自身的图被调用。
// 第一次 onGraphStart 调用记录 h.ownAddress 后，后续调用会将
// 当前 context 地址与该精确记录的地址进行比较。这能正确处理嵌套 React
// 智能体：每个 cbHandler 实例都会记录自己唯一的完整地址路径，因此
// 位于不同嵌套深度的两个 React 智能体（例如外层智能体的工具调用了
// 另一个 React 智能体）即使共享相同的 graph 名称，也不会相互干扰。
func (h *cbHandler) isOwnGraph(ctx context.Context) bool {
	if !h.ownClaimed {
		return false
	}
	return compose.GetCurrentAddress(ctx).Equals(h.ownAddress)
}

// claimOwnership is called exclusively from onGraphStart / onGraphStartWithStreamInput
// to record this handler's address on first invocation. It returns true if the handler
// has not yet been initialised, records the current address, and returns false on all
// subsequent calls.
//
// claimOwnership 仅由 onGraphStart / onGraphStartWithStreamInput 调用，
// 用于在首次调用时记录此处理器的地址。如果处理器尚未初始化，它返回 true，
// 记录当前地址，并在所有后续调用中返回 false。
func (h *cbHandler) claimOwnership(ctx context.Context) bool {
	if h.ownClaimed {
		return false
	}
	h.ownAddress = compose.GetCurrentAddress(ctx)
	h.ownClaimed = true
	return true
}

func (h *cbHandler) onChatModelEnd(ctx context.Context,
	_ *callbacks.RunInfo, input *model.CallbackOutput) context.Context {

	h.sendMessage(input.Message)

	return ctx
}

func (h *cbHandler) onChatModelEndWithStreamOutput(ctx context.Context,
	_ *callbacks.RunInfo, input *schema.StreamReader[*model.CallbackOutput]) context.Context {

	c := func(output *model.CallbackOutput) (*schema.Message, error) {
		return output.Message, nil
	}
	s := schema.StreamReaderWithConvert(input, c)

	h.sendMessageStream(s)

	return ctx
}

func (h *cbHandler) onGraphError(ctx context.Context,
	_ *callbacks.RunInfo, err error) context.Context {

	if !h.isOwnGraph(ctx) {
		return ctx
	}

	if h.msgs != nil {
		h.msgs.Send(item[*schema.Message]{err: err})
	} else if h.sMsgs != nil {
		h.sMsgs.Send(item[*schema.StreamReader[*schema.Message]]{err: err})
	}

	return ctx
}

func (h *cbHandler) onGraphEnd(ctx context.Context,
	_ *callbacks.RunInfo, _ callbacks.CallbackOutput) context.Context {

	if !h.isOwnGraph(ctx) {
		return ctx
	}

	if h.msgs != nil {
		h.msgs.Close()
	}

	return ctx
}

func (h *cbHandler) onGraphEndWithStreamOutput(ctx context.Context,
	_ *callbacks.RunInfo, _ *schema.StreamReader[callbacks.CallbackOutput]) context.Context {

	if !h.isOwnGraph(ctx) {
		return ctx
	}

	if h.sMsgs != nil {
		h.sMsgs.Close()
	}

	return ctx
}

func (h *cbHandler) onGraphStart(ctx context.Context,
	_ *callbacks.RunInfo, _ callbacks.CallbackInput) context.Context {

	if !h.claimOwnership(ctx) {
		return ctx
	}

	h.msgs = internal.NewUnboundedChan[item[*schema.Message]]()
	close(h.started)

	return ctx
}

func (h *cbHandler) onGraphStartWithStreamInput(ctx context.Context, _ *callbacks.RunInfo,
	input *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	input.Close()

	if !h.claimOwnership(ctx) {
		return ctx
	}

	h.sMsgs = internal.NewUnboundedChan[item[*schema.StreamReader[*schema.Message]]]()
	close(h.started)

	return ctx
}

func (h *cbHandler) sendMessage(msg *schema.Message) {
	if h.msgs != nil {
		h.msgs.Send(item[*schema.Message]{v: msg})
	} else {
		sMsg := schema.StreamReaderFromArray([]*schema.Message{msg})
		h.sMsgs.Send(item[*schema.StreamReader[*schema.Message]]{v: sMsg})
	}
}

func (h *cbHandler) sendMessageStream(sMsg *schema.StreamReader[*schema.Message]) {
	if h.sMsgs != nil {
		h.sMsgs.Send(item[*schema.StreamReader[*schema.Message]]{v: sMsg})
	} else {
		// concat
		msg, err := schema.ConcatMessageStream(sMsg)

		if err != nil {
			h.msgs.Send(item[*schema.Message]{err: err})
		} else {
			h.msgs.Send(item[*schema.Message]{v: msg})
		}
	}
}
