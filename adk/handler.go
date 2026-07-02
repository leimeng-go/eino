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
	"encoding/gob"
	"fmt"
	"io"
	"reflect"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// InvokableToolCallEndpoint is the function signature for invoking a tool synchronously.
// Middleware authors implement wrappers around this endpoint to add custom behavior.
//
// InvokableToolCallEndpoint 是同步调用工具的函数签名。
// 中间件作者可围绕此 endpoint 实现包装器，以添加自定义行为。
type InvokableToolCallEndpoint func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error)

// StreamableToolCallEndpoint is the function signature for invoking a tool with streaming output.
// Middleware authors implement wrappers around this endpoint to add custom behavior.
//
// StreamableToolCallEndpoint 是以流式输出调用工具的函数签名。
// 中间件作者可围绕此 endpoint 实现包装器，以添加自定义行为。
type StreamableToolCallEndpoint func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*schema.StreamReader[string], error)

type EnhancedInvokableToolCallEndpoint func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error)

type EnhancedStreamableToolCallEndpoint func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error)

// ToolContext provides metadata about the tool being wrapped.
// ToolContext 提供被包装工具的元数据。
type ToolContext struct {
	Name   string
	CallID string
}

// ToolCallsContext contains metadata about the tool calls that just completed.
// ToolCallsContext 包含刚完成的工具调用的元数据。
type ToolCallsContext struct {
	// ToolCalls contains the tool call metadata from the model's response.
	// ToolCalls 包含模型响应中的工具调用元数据。
	ToolCalls []ToolContext
}

// TypedModelContext contains context information passed to WrapModel.
// TypedModelContext 包含传递给 WrapModel 的上下文信息。
type TypedModelContext[M MessageType] struct {
	// Tools contains the current tool list configured for the agent.
	// This is populated at request time with the tools that will be sent to the model.
	//
	// Deprecated: Use TypedChatModelAgentState.ToolInfos in BeforeModelRewriteState instead.
	// ModelContext.Tools remains populated for backward compatibility with existing WrapModel handlers,
	// but new code should read and modify state.ToolInfos which is the source of truth for the model call.
	//
	// Tools 包含当前为智能体配置的工具列表。
	// 它会在请求时填充为将发送给模型的工具。
	// Deprecated: 请改用 BeforeModelRewriteState 中的 TypedChatModelAgentState.ToolInfos。
	// ModelContext.Tools 仍会填充，以兼容现有 WrapModel 处理器，
	// 但新代码应读取并修改 state.ToolInfos；它才是模型调用的事实来源。
	Tools []*schema.ToolInfo

	// ModelRetryConfig contains the retry configuration for the model.
	// This is populated at request time from the agent's ModelRetryConfig.
	// Used by EventSenderModelWrapper to wrap stream errors appropriately.
	//
	// ModelRetryConfig 包含模型的重试配置。
	// 它会在请求时从智能体的 ModelRetryConfig 填充。
	// EventSenderModelWrapper 使用它来适当地包装流错误。
	ModelRetryConfig *TypedModelRetryConfig[M]

	// ModelFailoverConfig contains the failover configuration for the model.
	// This is populated at request time from the agent's ModelFailoverConfig.
	// Used by EventSenderModelWrapper to wrap stream errors so that failed failover
	// attempts are skipped (not treated as fatal) by the flow event processor.
	//
	// ModelFailoverConfig 包含模型的 failover 配置。
	// 它会在请求时从智能体的 ModelFailoverConfig 填充。
	// EventSenderModelWrapper 使用它来包装流错误，使失败的 failover
	// 尝试会被流事件处理器跳过（不视为致命错误）。
	ModelFailoverConfig *ModelFailoverConfig[M]

	cancelContext *cancelContext
}

// ModelContext is the default model context type using *schema.Message.
// ModelContext 是使用 *schema.Message 的默认模型上下文类型。
type ModelContext = TypedModelContext[*schema.Message]

// ChatModelAgentContext contains runtime information passed to handlers before each ChatModelAgent run.
// Handlers can modify Instruction, Tools, and ReturnDirectly to customize agent behavior.
//
// This type is specific to ChatModelAgent. Other agent types may define their own context types.
//
// ChatModelAgentContext 包含每次 ChatModelAgent 运行前传递给处理器的运行时信息。
// 处理器可以修改 Instruction、Tools 和 ReturnDirectly 来自定义智能体行为。
// 此类型专用于 ChatModelAgent。其他智能体类型可以定义自己的上下文类型。
type ChatModelAgentContext struct {
	// Instruction is the current instruction for the Agent execution.
	// It includes the instruction configured for the agent, additional instructions appended by framework
	// and AgentMiddleware, and modifications applied by previous BeforeAgent handlers.
	// The finalized instruction after all BeforeAgent handlers are then passed to GenModelInput,
	// to be (optionally) formatted with SessionValues and converted to system message.
	//
	// Instruction 是智能体执行的当前指令。
	// 它包含为智能体配置的指令、框架和 AgentMiddleware 追加的额外指令，
	// 以及之前 BeforeAgent 处理器应用的修改。
	// 所有 BeforeAgent 处理器处理后的最终指令随后会传给 GenModelInput，
	// 用于（可选地）结合 SessionValues 格式化并转换为 system message。
	Instruction string

	// Tools are the raw tools (without any wrapper or tool middleware) currently configured for the Agent execution.
	// They includes tools passed in AgentConfig, implicit tools added by framework such as transfer / exit tools,
	// and other tools already added by middlewares.
	//
	// Tools 是当前为智能体执行配置的原始工具（未经过任何包装器或工具中间件）。
	// 它们包括 AgentConfig 中传入的工具、框架添加的隐式工具（如 transfer / exit 工具），
	// 以及中间件已添加的其他工具。
	Tools []tool.BaseTool

	// ReturnDirectly is the set of tool names currently configured to cause the Agent to return directly.
	// This is based on the return directly map configured for the agent, plus any modifications
	// by previous BeforeAgent handlers.
	//
	// ReturnDirectly 是当前配置为使 Agent 直接返回的工具名称集合。
	// 它基于为 agent 配置的 return directly map，以及之前 BeforeAgent handlers 所做的任何修改。
	ReturnDirectly map[string]bool

	// ToolSearchTool is the tool info for the model's native tool search capability.
	// When set by a BeforeAgent handler, the framework passes it to the model via model.WithToolSearchTool.
	//
	// ToolSearchTool 是模型原生工具搜索能力的工具信息。
	// 当 BeforeAgent handler 设置它时，框架会通过 model.WithToolSearchTool 将其传给模型。
	ToolSearchTool *schema.ToolInfo
}

// TypedChatModelAgentMiddleware defines the interface for customizing TypedChatModelAgent behavior.
//
// IMPORTANT: This interface is specifically designed for TypedChatModelAgent and agents built
// on top of it (e.g., DeepAgent).
//
// Why TypedChatModelAgentMiddleware instead of AgentMiddleware?
//
// AgentMiddleware is a struct type, which has inherent limitations:
//   - Struct types are closed: users cannot add new methods to extend functionality
//   - The framework only recognizes AgentMiddleware's fixed fields, so even if users
//     embed AgentMiddleware in a custom struct and add methods, the framework cannot
//     call those methods (config.Middlewares is []AgentMiddleware, not a user type)
//   - Callbacks in AgentMiddleware only return error, cannot return modified context
//
// TypedChatModelAgentMiddleware is an interface type, which is open for extension:
//   - Users can implement custom handlers with arbitrary internal state and methods
//   - Hook methods return (context.Context, ..., error) for direct context propagation
//   - Wrapper methods (WrapToolCall, WrapModel) enable context propagation through the
//     wrapped endpoint chain: wrappers can pass modified context to the next wrapper
//   - Configuration is centralized in struct fields rather than scattered in closures
//
// TypedChatModelAgentMiddleware vs AgentMiddleware:
//   - Use AgentMiddleware for simple, static additions (extra instruction/tools)
//   - Use TypedChatModelAgentMiddleware for dynamic behavior, context modification, or call wrapping
//   - AgentMiddleware is kept for backward compatibility with existing users
//   - Both can be used together; see AgentMiddleware documentation for execution order
//
// Use *TypedBaseChatModelAgentMiddleware as an embedded struct to provide default no-op
// implementations for all methods.
//
// TypedChatModelAgentMiddleware 定义了用于自定义 TypedChatModelAgent 行为的接口。
// 重要：此接口专为 TypedChatModelAgent 及基于它构建的 agents（例如 DeepAgent）设计。
// 为什么使用 TypedChatModelAgentMiddleware 而不是 AgentMiddleware？
// AgentMiddleware 是 struct 类型，存在固有限制：
// - Struct 类型是封闭的：用户无法添加新方法来扩展功能
// - 框架只识别 AgentMiddleware 的固定字段，因此即使用户在自定义 struct 中嵌入 AgentMiddleware 并添加方法，框架也无法调用这些方法（config.Middlewares 是 []AgentMiddleware，不是用户类型）
// - AgentMiddleware 中的 callbacks 只返回 error，不能返回修改后的 context
// TypedChatModelAgentMiddleware 是接口类型，可开放扩展：
// - 用户可以实现带有任意内部状态和方法的自定义 handlers
// - Hook 方法返回 (context.Context, ..., error)，可直接传播 context
// - Wrapper 方法（WrapToolCall、WrapModel）支持通过被包装的 endpoint chain 传播 context：wrappers 可以将修改后的 context 传给下一个 wrapper
// - 配置集中在 struct 字段中，而不是分散在 closures 中
// TypedChatModelAgentMiddleware 与 AgentMiddleware：
// - 简单、静态的添加（额外 instruction/tools）使用 AgentMiddleware
// - 动态行为、context 修改或调用包装使用 TypedChatModelAgentMiddleware
// - AgentMiddleware 保留用于兼容现有用户
// - 两者可以一起使用；执行顺序见 AgentMiddleware 文档
// 使用 *TypedBaseChatModelAgentMiddleware 作为嵌入 struct，为所有方法提供默认 no-op 实现。
type TypedChatModelAgentMiddleware[M MessageType] interface {
	// BeforeAgent is called before each agent run, allowing modification of
	// the agent's instruction and tools configuration.
	//
	// BeforeAgent 在每次 agent run 前调用，可修改 agent 的 instruction 和 tools 配置。
	BeforeAgent(ctx context.Context, runCtx *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error)

	// AfterAgent is called after the agent run reaches a successful terminal state.
	// Successful terminal states are: final answer (model response with no tool calls),
	// and return-directly tool result.
	//
	// AfterAgent is NOT called when the agent terminates with an error (e.g.,
	// ErrExceedMaxIterations, context cancellation, model errors).
	//
	// The state parameter contains the final conversation state, including all messages
	// from the completed run.
	//
	// AfterAgent handlers are called in the same order as BeforeAgent handlers
	// (first registered = first called). Consistent with all other middleware hooks,
	// if any handler returns an error, subsequent handlers are NOT called (fail-fast)
	// and the error is sent to the event stream.
	//
	// AfterAgent 在 agent run 达到成功终止状态后调用。
	// 成功终止状态包括：最终答案（无 tool calls 的模型响应）和 return-directly 工具结果。
	// 当 agent 因错误终止时，不会调用 AfterAgent（例如 ErrExceedMaxIterations、context cancellation、模型错误）。
	// state 参数包含最终会话状态，包括已完成 run 中的所有消息。
	// AfterAgent handlers 的调用顺序与 BeforeAgent handlers 相同（先注册 = 先调用）。与所有其他 middleware hooks 一致，如果任一 handler 返回错误，后续 handlers 不会被调用（fail-fast），该错误会发送到 event stream。
	AfterAgent(ctx context.Context, state *TypedChatModelAgentState[M]) (context.Context, error)

	// BeforeModelRewriteState is called before each model invocation.
	// The returned state is persisted to the agent's internal state and passed to the model.
	// The returned context is propagated to the model call and subsequent handlers.
	//
	// The ChatModelAgentState struct provides access to:
	//   - Messages: the conversation history
	//   - ToolInfos: the tool list that will be sent to the model (modifiable)
	//   - DeferredToolInfos: tools for server-side search (modifiable, nil if unused)
	//
	// This is the recommended place to modify messages and tools before a model call.
	// Changes here are persisted in state and reflected in subsequent iterations.
	//
	// BeforeModelRewriteState 在每次模型调用前调用。
	// 返回的 state 会持久化到 agent 的内部状态，并传给模型。
	// 返回的 context 会传播到模型调用和后续 handlers。
	// ChatModelAgentState struct 提供以下访问：
	// - Messages：会话历史
	// - ToolInfos：将发送给模型的工具列表（可修改）
	// - DeferredToolInfos：用于服务端搜索的工具（可修改；未使用时为 nil）
	// 这是在模型调用前修改 messages 和 tools 的推荐位置。
	// 这里的更改会持久化到 state，并反映到后续迭代中。
	BeforeModelRewriteState(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error)

	// AfterModelRewriteState is called after each model invocation.
	// The input state includes the model's response as the last message.
	// The returned state is persisted to the agent's internal state.
	//
	// The ChatModelAgentState struct provides access to:
	//   - Messages: the conversation history including the model's response
	//   - ToolInfos: the tool list that was sent to the model
	//   - DeferredToolInfos: tools for server-side search (nil if unused)
	//
	// AfterModelRewriteState 在每次模型调用后调用。
	// 输入 state 包含模型响应作为最后一条消息。
	// 返回的 state 会持久化到 agent 的内部状态。
	// ChatModelAgentState struct 提供以下访问：
	// - Messages：会话历史，包括模型响应
	// - ToolInfos：已发送给模型的工具列表
	// - DeferredToolInfos：用于服务端搜索的工具（未使用时为 nil）
	AfterModelRewriteState(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error)

	// WrapInvokableToolCall wraps a tool's synchronous execution with custom behavior.
	// Return the input endpoint unchanged and nil error if no wrapping is needed.
	//
	// This method is only called for tools that implement InvokableTool.
	// If a tool only implements StreamableTool, this method will not be called for that tool.
	//
	// This method is called at request time when the tool is about to be executed.
	// The tCtx parameter provides metadata about the tool:
	//   - Name: The name of the tool being wrapped
	//   - CallID: The unique identifier for this specific tool call
	//
	// WrapInvokableToolCall 用自定义行为包装工具的同步执行。
	// 如果不需要包装，返回未修改的输入 endpoint 和 nil error。
	// 此方法只会对实现 InvokableTool 的工具调用。
	// 如果工具只实现 StreamableTool，则不会为该工具调用此方法。
	// 此方法在请求时、工具即将执行时调用。
	// tCtx 参数提供工具的元数据：
	// - Name：被包装工具的名称
	// - CallID：此特定 tool call 的唯一标识符
	WrapInvokableToolCall(ctx context.Context, endpoint InvokableToolCallEndpoint, tCtx *ToolContext) (InvokableToolCallEndpoint, error)

	// WrapStreamableToolCall wraps a tool's streaming execution with custom behavior.
	// Return the input endpoint unchanged and nil error if no wrapping is needed.
	//
	// This method is only called for tools that implement StreamableTool.
	// If a tool only implements InvokableTool, this method will not be called for that tool.
	//
	// This method is called at request time when the tool is about to be executed.
	// The tCtx parameter provides metadata about the tool:
	//   - Name: The name of the tool being wrapped
	//   - CallID: The unique identifier for this specific tool call
	//
	// WrapStreamableToolCall 用自定义行为包装工具的流式执行。
	// 如果不需要包装，返回未修改的输入 endpoint 和 nil error。
	// 此方法只会对实现 StreamableTool 的工具调用。
	// 如果工具只实现 InvokableTool，则不会为该工具调用此方法。
	// 此方法在请求时、工具即将执行时调用。
	// tCtx 参数提供工具的元数据：
	// - Name：被包装工具的名称
	// - CallID：此特定 tool call 的唯一标识符
	WrapStreamableToolCall(ctx context.Context, endpoint StreamableToolCallEndpoint, tCtx *ToolContext) (StreamableToolCallEndpoint, error)

	// WrapEnhancedInvokableToolCall wraps an enhanced tool's synchronous execution with custom behavior.
	// Return the input endpoint unchanged and nil error if no wrapping is needed.
	//
	// This method is only called for tools that implement EnhancedInvokableTool.
	// If a tool only implements EnhancedStreamableTool, this method will not be called for that tool.
	//
	// This method is called at request time when the tool is about to be executed.
	// The tCtx parameter provides metadata about the tool:
	//   - Name: The name of the tool being wrapped
	//   - CallID: The unique identifier for this specific tool call
	//
	// WrapEnhancedInvokableToolCall 用自定义行为包装增强工具的同步执行。
	// 如果不需要包装，返回未修改的输入 endpoint 和 nil error。
	// 此方法只会对实现 EnhancedInvokableTool 的工具调用。
	// 如果工具只实现 EnhancedStreamableTool，则不会为该工具调用此方法。
	// 此方法在请求时、工具即将执行时调用。
	// tCtx 参数提供工具的元数据：
	// - Name：被包装工具的名称
	// - CallID：此特定 tool call 的唯一标识符
	WrapEnhancedInvokableToolCall(ctx context.Context, endpoint EnhancedInvokableToolCallEndpoint, tCtx *ToolContext) (EnhancedInvokableToolCallEndpoint, error)

	// WrapEnhancedStreamableToolCall wraps an enhanced tool's streaming execution with custom behavior.
	// Return the input endpoint unchanged and nil error if no wrapping is needed.
	//
	// This method is only called for tools that implement EnhancedStreamableTool.
	// If a tool only implements EnhancedInvokableTool, this method will not be called for that tool.
	//
	// This method is called at request time when the tool is about to be executed.
	// The tCtx parameter provides metadata about the tool:
	//   - Name: The name of the tool being wrapped
	//   - CallID: The unique identifier for this specific tool call
	//
	// WrapEnhancedStreamableToolCall 用自定义行为包装增强工具的流式执行。
	// 如果不需要包装，返回未修改的输入 endpoint 和 nil error。
	// 此方法只会对实现 EnhancedStreamableTool 的工具调用。
	// 如果工具只实现 EnhancedInvokableTool，则不会为该工具调用此方法。
	// 此方法在请求时、工具即将执行时调用。
	// tCtx 参数提供工具的元数据：
	// - Name：被包装工具的名称
	// - CallID：此特定 tool call 的唯一标识符
	WrapEnhancedStreamableToolCall(ctx context.Context, endpoint EnhancedStreamableToolCallEndpoint, tCtx *ToolContext) (EnhancedStreamableToolCallEndpoint, error)

	// WrapModel wraps a chat model with custom behavior around the actual model call.
	// Return the input model unchanged and nil error if no wrapping is needed.
	//
	// This method is called at request time when the model is about to be invoked.
	// Note: The parameter is model.BaseModel[M] (not ToolCallingChatModel) because wrappers
	// only need to intercept Generate/Stream calls. Tool binding (WithTools) is handled
	// separately by the framework and does not flow through user wrappers.
	//
	// Recommended use cases (behavior around the model call itself):
	//   - Model call retry logic
	//   - Model failover (switching to a backup model)
	//   - Sending events (e.g. streaming progress)
	//   - Processing or transforming the response stream
	//   - Changing call configurations (temperature, top_p, etc.)
	//
	// Discouraged use cases (use BeforeModelRewriteState instead):
	//   - Modifying input messages: changes here are NOT persisted in state, only
	//     affect a single model call, and break prompt cache across iterations.
	//   - Modifying the tool list: use state.ToolInfos / state.DeferredToolInfos in
	//     BeforeModelRewriteState, which is the source of truth for tool configuration.
	//
	// The mc parameter provides read-only context about the current model call:
	//   - Tools: The tool infos that will be sent to the model (Deprecated: read state.ToolInfos instead)
	//
	// WrapModel 用自定义行为包装聊天模型，包裹实际模型调用。
	// 如果不需要包装，返回未修改的输入 model 和 nil error。
	// 此方法在请求时、模型即将被调用时调用。
	// 注意：参数是 model.BaseModel[M]（不是 ToolCallingChatModel），因为 wrappers 只需要拦截 Generate/Stream 调用。工具绑定（WithTools）由框架单独处理，不会流经用户 wrappers。
	// 推荐用例（围绕模型调用本身的行为）：
	// - 模型调用重试逻辑
	// - 模型 failover（切换到备用模型）
	// - 发送事件（例如流式进度）
	// - 处理或转换响应流
	// - 修改调用配置（temperature、top_p 等）
	// 不推荐用例（改用 BeforeModelRewriteState）：
	// - 修改输入 messages：这里的更改不会持久化到 state，只影响单次模型调用，并会破坏迭代间的 prompt cache。
	// - 修改工具列表：使用 BeforeModelRewriteState 中的 state.ToolInfos / state.DeferredToolInfos，它是工具配置的事实来源。
	// mc 参数提供当前模型调用的只读上下文：
	// - Tools：将发送给模型的工具信息（Deprecated：请改读 state.ToolInfos）
	WrapModel(ctx context.Context, m model.BaseModel[M], mc *TypedModelContext[M]) (model.BaseModel[M], error)
}

// ChatModelAgentMiddleware is the default middleware type using *schema.Message.
// See TypedChatModelAgentMiddleware for full documentation.
//
// ChatModelAgentMiddleware 是使用 *schema.Message 的默认 middleware 类型。
// 完整文档见 TypedChatModelAgentMiddleware。
type ChatModelAgentMiddleware = TypedChatModelAgentMiddleware[*schema.Message]

type TypedBaseChatModelAgentMiddleware[M MessageType] struct{}

// BaseChatModelAgentMiddleware provides default no-op implementations for ChatModelAgentMiddleware.
// Embed *BaseChatModelAgentMiddleware in custom handlers to only override the methods you need.
//
// Example:
//
//	type MyHandler struct {
//		*adk.BaseChatModelAgentMiddleware
//		// custom fields
//	}
//
//	func (h *MyHandler) BeforeModelRewriteState(ctx context.Context, state *adk.ChatModelAgentState, mc *adk.ModelContext) (context.Context, *adk.ChatModelAgentState, error) {
//		// custom logic
//		return ctx, state, nil
//	}
//
// BaseChatModelAgentMiddleware 为 ChatModelAgentMiddleware 提供默认 no-op 实现。
// 在自定义 handlers 中嵌入 *BaseChatModelAgentMiddleware，只重写所需方法。
// 示例：
// type MyHandler struct {
// *adk.BaseChatModelAgentMiddleware
// 自定义字段
// }
// func (h *MyHandler) BeforeModelRewriteState(ctx context.Context, state *adk.ChatModelAgentState, mc *adk.ModelContext) (context.Context, *adk.ChatModelAgentState, error) {
// 自定义逻辑
// return ctx, state, nil
// }
type BaseChatModelAgentMiddleware = TypedBaseChatModelAgentMiddleware[*schema.Message]

func (b *TypedBaseChatModelAgentMiddleware[M]) WrapInvokableToolCall(_ context.Context, endpoint InvokableToolCallEndpoint, _ *ToolContext) (InvokableToolCallEndpoint, error) {
	return endpoint, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) WrapStreamableToolCall(_ context.Context, endpoint StreamableToolCallEndpoint, _ *ToolContext) (StreamableToolCallEndpoint, error) {
	return endpoint, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) WrapEnhancedInvokableToolCall(_ context.Context, endpoint EnhancedInvokableToolCallEndpoint, _ *ToolContext) (EnhancedInvokableToolCallEndpoint, error) {
	return endpoint, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) WrapEnhancedStreamableToolCall(_ context.Context, endpoint EnhancedStreamableToolCallEndpoint, _ *ToolContext) (EnhancedStreamableToolCallEndpoint, error) {
	return endpoint, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) WrapModel(_ context.Context, m model.BaseModel[M], _ *TypedModelContext[M]) (model.BaseModel[M], error) {
	return m, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) BeforeAgent(ctx context.Context, runCtx *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) {
	return ctx, runCtx, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) AfterAgent(ctx context.Context, state *TypedChatModelAgentState[M]) (context.Context, error) {
	return ctx, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) BeforeModelRewriteState(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error) {
	return ctx, state, nil
}

func (b *TypedBaseChatModelAgentMiddleware[M]) AfterModelRewriteState(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error) {
	return ctx, state, nil
}

func processTypedState(ctx context.Context, fn func(extra map[string]any) map[string]any) error {
	runCtx := getRunCtx(ctx)
	if runCtx != nil && runCtx.AgenticRootInput != nil {
		return compose.ProcessState(ctx, func(_ context.Context, st *typedState[*schema.AgenticMessage]) error {
			st.Extra = fn(st.Extra)
			return nil
		})
	}
	return compose.ProcessState(ctx, func(_ context.Context, st *typedState[*schema.Message]) error {
		st.Extra = fn(st.Extra)
		return nil
	})
}

// SetRunLocalValue sets a key-value pair that persists for the duration of the current agent Run() invocation.
// The value is scoped to this specific execution and is not shared across different Run() calls or agent instances.
//
// Values stored here are compatible with interrupt/resume cycles - they will be serialized and restored
// when the agent is resumed. For custom types, you must register them using schema.RegisterName[T]()
// in an init() function to ensure proper serialization.
//
// This function can only be called from within a ChatModelAgentMiddleware during agent execution.
// Returns an error if called outside of an agent execution context.
//
// SetRunLocalValue 设置一个键值对，在当前 agent Run() 调用期间保持有效。
// 该值的作用域限定于此次执行，不会在不同 Run() 调用或 agent 实例之间共享。
// 这里存储的值兼容 interrupt/resume 周期：agent 恢复时会被序列化并还原。对于自定义类型，必须在 init() 函数中使用 schema.RegisterName[T]() 注册，以确保正确序列化。
// 此函数只能在 agent 执行期间从 ChatModelAgentMiddleware 内部调用。
// 如果在 agent 执行 context 外调用，则返回错误。
func SetRunLocalValue(ctx context.Context, key string, value any) error {
	if err := checkGobEncodability(key, value); err != nil {
		return err
	}

	err := processTypedState(ctx, func(extra map[string]any) map[string]any {
		if extra == nil {
			extra = make(map[string]any)
		}
		extra[key] = value
		return extra
	})
	if err != nil {
		return fmt.Errorf("SetRunLocalValue failed: must be called within a ChatModelAgent Run() or Resume() execution context: %w", err)
	}

	return nil
}

// GetRunLocalValue retrieves a value that was set during the current agent Run() invocation.
// The value is scoped to this specific execution and is not shared across different Run() calls or agent instances.
//
// Values stored via SetRunLocalValue are compatible with interrupt/resume cycles - they will be serialized
// and restored when the agent is resumed. For custom types, you must register them using schema.RegisterName[T]()
// in an init() function to ensure proper serialization.
//
// This function can only be called from within a ChatModelAgentMiddleware during agent execution.
// Returns the value and true if found, or nil and false if not found or if called outside of an agent execution context.
//
// GetRunLocalValue 获取当前 agent Run() 调用期间设置的值。
// 该值的作用域限定于此次执行，不会在不同 Run() 调用或 agent 实例之间共享。
// 通过 SetRunLocalValue 存储的值兼容 interrupt/resume 周期：agent 恢复时会被序列化并还原。对于自定义类型，必须在 init() 函数中使用 schema.RegisterName[T]() 注册，以确保正确序列化。
// 此函数只能在 agent 执行期间从 ChatModelAgentMiddleware 内部调用。
// 如果找到则返回该值和 true；如果未找到或在 agent 执行 context 外调用，则返回 nil 和 false。
func GetRunLocalValue(ctx context.Context, key string) (any, bool, error) {
	var val any
	var found bool
	err := processTypedState(ctx, func(extra map[string]any) map[string]any {
		if extra != nil {
			val, found = extra[key]
		}
		return extra
	})
	if err != nil {
		return nil, false, fmt.Errorf("GetRunLocalValue failed: must be called within a ChatModelAgent Run() or Resume() execution context: %w", err)
	}
	return val, found, nil
}

// DeleteRunLocalValue removes a value that was set during the current agent Run() invocation.
//
// This function can only be called from within a ChatModelAgentMiddleware during agent execution.
// Returns an error if called outside of an agent execution context.
//
// DeleteRunLocalValue 删除当前 agent Run() 调用期间设置的值。
// 此函数只能在 agent 执行期间从 ChatModelAgentMiddleware 内部调用。
// 如果在 agent 执行 context 外调用，则返回错误。
func DeleteRunLocalValue(ctx context.Context, key string) error {
	err := processTypedState(ctx, func(extra map[string]any) map[string]any {
		if extra != nil {
			delete(extra, key)
		}
		return extra
	})
	if err != nil {
		return fmt.Errorf("DeleteRunLocalValue failed: must be called within a ChatModelAgent Run() or Resume() execution context: %w", err)
	}
	return nil
}

// TypedSendEvent sends a custom TypedAgentEvent to the event stream during agent execution.
// This allows TypedChatModelAgentMiddleware implementations to emit custom events that will be
// received by the caller iterating over the agent's event stream.
//
// Note: TypedSendEvent is a pure transport — it does NOT auto-assign message IDs.
// Framework-created messages (model output, tool results) receive IDs automatically
// via internal wrapper layers. If your middleware constructs its own messages, call
// EnsureMessageID before sending to assign an ID.
//
// This function can only be called from within a TypedChatModelAgentMiddleware during agent execution.
// Returns an error if called outside of an agent execution context.
//
// TypedSendEvent 在 agent 执行期间向 event stream 发送自定义 TypedAgentEvent。
// 这允许 TypedChatModelAgentMiddleware 实现发出自定义事件，供调用方在遍历 agent 的 event stream 时接收。
// 注意：TypedSendEvent 是纯传输机制，不会自动分配 message IDs。
// 框架创建的消息（模型输出、工具结果）会通过内部 wrapper layers 自动获得 IDs。若 middleware 构造自己的消息，请在发送前调用 EnsureMessageID 分配 ID。
// 此函数只能在 agent 执行期间从 TypedChatModelAgentMiddleware 内部调用。
// 如果在 agent 执行 context 外调用，则返回错误。
func TypedSendEvent[M MessageType](ctx context.Context, event *TypedAgentEvent[M]) error {
	execCtx := getTypedChatModelAgentExecCtx[M](ctx)
	if execCtx == nil || execCtx.generator == nil {
		return fmt.Errorf("TypedSendEvent failed: must be called within a ChatModelAgent Run() or Resume() execution context")
	}

	execCtx.send(event)
	return nil
}

// SendEvent sends a custom AgentEvent to the event stream during agent execution.
// This allows ChatModelAgentMiddleware implementations to emit custom events that will be
// received by the caller iterating over the agent's event stream.
//
// This function can only be called from within a ChatModelAgentMiddleware during agent execution.
// Returns an error if called outside of an agent execution context.
//
// SendEvent 在 agent 执行期间向 event stream 发送自定义 AgentEvent。
// 这允许 ChatModelAgentMiddleware 实现发出自定义事件，供调用方在遍历 agent 的 event stream 时接收。
// 此函数只能在 agent 执行期间从 ChatModelAgentMiddleware 内部调用。
// 如果在 agent 执行 context 外调用，则返回错误。
func SendEvent(ctx context.Context, event *AgentEvent) error {
	return TypedSendEvent(ctx, event)
}

// checkGobEncodability probes whether the value can be gob-encoded as part of
// a map[string]any, which is exactly how State.Extra is serialized during
// checkpoint. This catches unregistered types early at Set time, rather than
// letting them fail at checkpoint/resume time with a confusing error.
//
// checkGobEncodability 探测该值是否可作为 map[string]any 的一部分进行 gob 编码，这正是 State.Extra 在 checkpoint 期间的序列化方式。
// 这样可以在 Set 时尽早发现未注册类型，而不是等到 checkpoint/resume 时才因令人困惑的错误失败。
func checkGobEncodability(key string, value any) error {
	probe := map[string]any{key: value}
	if err := gob.NewEncoder(io.Discard).Encode(probe); err != nil {
		typeName := reflect.TypeOf(value).String()
		return fmt.Errorf("SetRunLocalValue: the value (type %s) for key %q is not gob-serializable, "+
			"which means it will fail when the agent checkpoint is saved or resumed.\n\n"+
			"To fix this, register the type in an init() function in your package:\n\n"+
			"  func init() {\n"+
			"      schema.RegisterName[%s](\"a_unique_name_for_this_type\")\n"+
			"  }\n\n"+
			"This is required because agent state (including values set via SetRunLocalValue) is "+
			"persisted using gob encoding for interrupt/resume support. All concrete types stored "+
			"in interface-typed fields (like map[string]any) must be registered with gob.\n\n"+
			"If this value does not need to survive interrupt/resume, store it on the context instead, "+
			"for example via context.WithValue, so you don't need gob registration.\n\n"+
			"Underlying error: %w", typeName, key, typeName, err)
	}
	return nil
}
