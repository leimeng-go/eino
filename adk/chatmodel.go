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
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bytedance/sonic"

	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/internal/safe"
	"github.com/cloudwego/eino/schema"
)

var _ ResumableAgent = &TypedChatModelAgent[*schema.Message]{}
var _ TypedResumableAgent[*schema.AgenticMessage] = &TypedChatModelAgent[*schema.AgenticMessage]{}

type typedChatModelAgentExecCtx[M MessageType] struct {
	runtimeReturnDirectly map[string]bool
	generator             *AsyncGenerator[*TypedAgentEvent[M]]
	cancelCtx             *cancelContext

	failoverLastSuccessModel model.BaseModel[M]

	// suppressEventSend prevents eventSenderModel from emitting AgentEvents for the current
	// Generate call. Set to true before each rejected retry attempt and reset to false after.
	// Invariant: any code path that emits model output events MUST check this flag.
	//
	// suppressEventSend 阻止 eventSenderModel 为当前 Generate 调用发出 AgentEvents。
	// 每次被拒绝的重试前设为 true，之后重置为 false。
	// 不变量：任何发出模型输出事件的代码路径都必须检查此标志。
	suppressEventSend  bool
	retryVerdictSignal *retryVerdictSignal

	afterToolCallsHook func(ctx context.Context) error
}

func (e *typedChatModelAgentExecCtx[M]) send(event *TypedAgentEvent[M]) {
	if e == nil || e.generator == nil {
		return
	}
	if e.cancelCtx != nil && e.cancelCtx.isImmediateCancelled() {
		return
	}
	e.generator.trySend(event)
}

type chatModelAgentExecCtx = typedChatModelAgentExecCtx[*schema.Message]

type typedChatModelAgentExecCtxKey[M MessageType] struct{}

func withTypedChatModelAgentExecCtx[M MessageType](ctx context.Context, execCtx *typedChatModelAgentExecCtx[M]) context.Context {
	return context.WithValue(ctx, typedChatModelAgentExecCtxKey[M]{}, execCtx)
}

func getTypedChatModelAgentExecCtx[M MessageType](ctx context.Context) *typedChatModelAgentExecCtx[M] {
	if v := ctx.Value(typedChatModelAgentExecCtxKey[M]{}); v != nil {
		return v.(*typedChatModelAgentExecCtx[M])
	}
	return nil
}

type chatModelAgentRunOptions struct {
	chatModelOptions []model.Option
	toolOptions      []tool.Option
	agentToolOptions map[string][]AgentRunOption

	historyModifier func(context.Context, []Message) []Message

	afterToolCallsHook func(ctx context.Context) error
}

// WithChatModelOptions sets options for the underlying chat model.
// WithChatModelOptions 设置底层 chat model 的选项。
func WithChatModelOptions(opts []model.Option) AgentRunOption {
	return WrapImplSpecificOptFn(func(t *chatModelAgentRunOptions) {
		t.chatModelOptions = opts
	})
}

// WithToolOptions sets options for tools used by the chat model agent.
// WithToolOptions 设置 chat model 智能体所用工具的选项。
func WithToolOptions(opts []tool.Option) AgentRunOption {
	return WrapImplSpecificOptFn(func(t *chatModelAgentRunOptions) {
		t.toolOptions = opts
	})
}

// WithAgentToolRunOptions specifies per-tool run options for the agent.
// WithAgentToolRunOptions 为智能体指定每个工具的运行选项。
func WithAgentToolRunOptions(opts map[string][]AgentRunOption) AgentRunOption {
	return WrapImplSpecificOptFn(func(t *chatModelAgentRunOptions) {
		t.agentToolOptions = opts
	})
}

// WithHistoryModifier sets a function to modify history during resume.
// Deprecated: use ResumeWithData and ChatModelAgentResumeData instead.
//
// WithHistoryModifier 设置在恢复期间修改历史记录的函数。
// 已废弃：请改用 ResumeWithData 和 ChatModelAgentResumeData。
func WithHistoryModifier(f func(context.Context, []Message) []Message) AgentRunOption {
	return WrapImplSpecificOptFn(func(t *chatModelAgentRunOptions) {
		t.historyModifier = f
	})
}

// WithAfterToolCallsHook registers a per-run hook that fires synchronously after
// all tool calls in a react iteration complete, before the next ChatModel call.
//
// This is suitable for TurnLoop Push+Preempt patterns where the pushed item
// must be visible to the next turn's GenInput.
//
// WithAfterToolCallsHook 注册一个每次运行的 hook，在 react 迭代中的所有工具调用完成后、下一次 ChatModel 调用前同步触发。
// 适用于 TurnLoop Push+Preempt 模式，其中推送的项必须对下一轮的 GenInput 可见。
func WithAfterToolCallsHook(fn func(ctx context.Context) error) AgentRunOption {
	return WrapImplSpecificOptFn(func(t *chatModelAgentRunOptions) {
		t.afterToolCallsHook = fn
	})
}

type ToolsConfig struct {
	compose.ToolsNodeConfig

	// ReturnDirectly specifies tools that cause the agent to return immediately when called.
	// The map keys are tool names indicate whether the tool should trigger immediate return.
	//
	// ReturnDirectly 指定调用后会使智能体立即返回的工具。
	// map 的 key 是工具名称，表示该工具是否应触发立即返回。
	ReturnDirectly map[string]bool

	// EmitInternalEvents indicates whether internal events from agentTool should be emitted
	// to the parent agent's AsyncGenerator, allowing real-time streaming of nested agent output
	// to the end-user via Runner.
	//
	// Note that these forwarded events are NOT recorded in the parent agent's runSession.
	// They are only emitted to the end-user and have no effect on the parent agent's state
	// or checkpoint.
	//
	// Action Scoping:
	// Actions emitted by the inner agent are scoped to the agent tool boundary:
	//   - Interrupted: Propagated via CompositeInterrupt to allow proper interrupt/resume
	//   - Exit, TransferToAgent, BreakLoop: Ignored outside the agent tool
	//
	// EmitInternalEvents 表示是否将 agentTool 的内部事件发出到父智能体的 AsyncGenerator，从而允许通过 Runner 将嵌套智能体输出实时流式传给最终用户。
	// 注意，这些转发事件不会记录在父智能体的 runSession 中。
	// 它们只会发送给最终用户，不会影响父智能体的状态或检查点。
	// Action 作用域：
	// 内部智能体发出的 Actions 会被限定在 agent tool 边界内：
	// - Interrupted: 通过 CompositeInterrupt 传播，以便正确中断/恢复
	// - Exit, TransferToAgent, BreakLoop: 在 agent tool 外部忽略
	EmitInternalEvents bool
}

// TypedGenModelInput transforms the agent's system instruction and user input into model input
// messages ([]M). This is the primary customization point for controlling what the model sees.
// The default implementation prepends a system message (if instruction is non-empty),
// followed by the user's input messages.
//
// TypedGenModelInput 将智能体的系统指令和用户输入转换为模型输入消息 ([]M)。
// 这是控制模型可见内容的主要自定义点。
// 默认实现会先添加系统消息（如果 instruction 非空），然后添加用户的输入消息。
type TypedGenModelInput[M MessageType] func(ctx context.Context, instruction string, input *TypedAgentInput[M]) ([]M, error)

// GenModelInput transforms agent instructions and input into a format suitable for the model.
// GenModelInput 将智能体指令和输入转换为适合模型的格式。
type GenModelInput = TypedGenModelInput[*schema.Message]

func defaultGenModelInput(ctx context.Context, instruction string, input *AgentInput) ([]Message, error) {
	msgs := make([]Message, 0, len(input.Messages)+1)

	if instruction != "" {
		sp := schema.SystemMessage(instruction)

		vs := GetSessionValues(ctx)
		if len(vs) > 0 {
			ct := prompt.FromMessages(schema.FString, sp)
			ms, err := ct.Format(ctx, vs)
			if err != nil {
				return nil, fmt.Errorf("defaultGenModelInput: failed to format instruction using FString template. "+
					"This formatting is triggered automatically when SessionValues are present. "+
					"If your instruction contains literal curly braces (e.g., JSON), provide a custom GenModelInput that uses another format. If you are using "+
					"SessionValues for purposes other than instruction formatting, provide a custom GenModelInput that does no formatting at all: %w", err)
			}

			sp = ms[0]
		}

		msgs = append(msgs, sp)
	}

	msgs = append(msgs, input.Messages...)

	return msgs, nil
}

func newDefaultGenModelInput[M MessageType]() TypedGenModelInput[M] {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return any(GenModelInput(defaultGenModelInput)).(TypedGenModelInput[M])
	case *schema.AgenticMessage:
		return any(TypedGenModelInput[*schema.AgenticMessage](func(_ context.Context, instruction string, input *TypedAgentInput[*schema.AgenticMessage]) ([]*schema.AgenticMessage, error) {
			msgs := make([]*schema.AgenticMessage, 0, len(input.Messages)+1)
			if instruction != "" {
				msgs = append(msgs, schema.SystemAgenticMessage(instruction))
			}
			msgs = append(msgs, input.Messages...)
			return msgs, nil
		})).(TypedGenModelInput[M])
	default:
		panic("unreachable: unknown MessageType")
	}
}

// TypedChatModelAgentState represents the state of a chat model agent during conversation.
// This is the primary state type for both TypedChatModelAgentMiddleware and AgentMiddleware callbacks.
//
// TypedChatModelAgentState 表示 chat model 智能体在对话期间的状态。
// 这是 TypedChatModelAgentMiddleware 和 AgentMiddleware 回调的主要状态类型。
type TypedChatModelAgentState[M MessageType] struct {
	// Messages contains all messages in the current conversation session.
	// Messages 包含当前对话会话中的所有消息。
	Messages []M

	// ToolInfos contains the tool definitions passed to the model via model.WithTools.
	// BeforeModelRewriteState handlers can read and modify this field to control which tools
	// the model sees on each call.
	//
	// ToolInfos 包含通过 model.WithTools 传给模型的工具定义。
	// BeforeModelRewriteState 处理器可以读取并修改此字段，以控制模型每次调用时可见的工具。
	ToolInfos []*schema.ToolInfo

	// DeferredToolInfos contains tool definitions for server-side deferred retrieval,
	// passed to the model via model.WithDeferredTools. These tools are not included in the
	// immediate tool list but can be discovered by the model through its native search capability.
	// Nil when not in use.
	//
	// DeferredToolInfos 包含用于服务端延迟检索的工具定义，通过 model.WithDeferredTools 传给模型。
	// 这些工具不包含在即时工具列表中，但模型可以通过其原生搜索能力发现它们。
	// 未使用时为 nil。
	DeferredToolInfos []*schema.ToolInfo
}

// ChatModelAgentState is the default state type using *schema.Message.
// ChatModelAgentState 是使用 *schema.Message 的默认状态类型。
type ChatModelAgentState = TypedChatModelAgentState[*schema.Message]

// Deprecated: Use ChatModelAgentMiddleware (interface-based Handlers) instead.
// AgentMiddleware will be removed in a future release.
//
// AgentMiddleware provides hooks to customize agent behavior at various stages of execution.
//
// 已废弃：请改用 ChatModelAgentMiddleware（基于接口的 Handlers）。
// AgentMiddleware 将在未来版本中移除。
// AgentMiddleware 提供 hook，用于在执行的各个阶段自定义智能体行为。
type AgentMiddleware struct {
	// AdditionalInstruction adds supplementary text to the agent's system instruction.
	// This instruction is concatenated with the base instruction before each chat model call.
	//
	// AdditionalInstruction 向智能体的系统指令添加补充文本。
	// 该指令会在每次 chat model 调用前与基础指令拼接。
	AdditionalInstruction string

	// AdditionalTools adds supplementary tools to the agent's available toolset.
	// These tools are combined with the tools configured for the agent.
	//
	// AdditionalTools 向智能体的可用工具集添加补充工具。
	// 这些工具会与为智能体配置的工具合并。
	AdditionalTools []tool.BaseTool

	// BeforeChatModel is called before each ChatModel invocation, allowing modification of the agent state.
	// BeforeChatModel 在每次调用 ChatModel 前调用，允许修改智能体状态。
	BeforeChatModel func(context.Context, *ChatModelAgentState) error

	// AfterChatModel is called after each ChatModel invocation, allowing modification of the agent state.
	// AfterChatModel 在每次调用 ChatModel 后调用，允许修改智能体状态。
	AfterChatModel func(context.Context, *ChatModelAgentState) error

	// WrapToolCall wraps tool calls with custom middleware logic.
	// Each middleware contains Invokable and/or Streamable functions for tool calls.
	//
	// WrapToolCall 用自定义中间件逻辑包装工具调用。
	// 每个中间件包含用于工具调用的 Invokable 和/或 Streamable 函数。
	WrapToolCall compose.ToolMiddleware
}

// TypedChatModelAgentConfig is the generic configuration for ChatModelAgent.
// TypedChatModelAgentConfig 是 ChatModelAgent 的泛型配置。
type TypedChatModelAgentConfig[M MessageType] struct {
	// Name of the agent. Better be unique across all agents.
	// Optional. If empty, the agent can still run standalone but cannot be used as
	// a sub-agent tool via NewAgentTool (which requires a non-empty Name).
	//
	// 智能体名称。最好在所有智能体中唯一。
	// 可选。若为空，智能体仍可独立运行，但不能通过 NewAgentTool 用作子智能体工具（这要求 Name 非空）。
	Name string
	// Description of the agent's capabilities.
	// Helps other agents determine whether to transfer tasks to this agent.
	// Optional. If empty, the agent can still run standalone but cannot be used as
	// a sub-agent tool via NewAgentTool (which requires a non-empty Description).
	//
	// 智能体能力描述。
	// 帮助其他智能体判断是否将任务转交给该智能体。
	// 可选。若为空，智能体仍可独立运行，但不能通过 NewAgentTool 用作子智能体工具（这要求 Description 非空）。
	Description string
	// Instruction used as the system prompt for this agent.
	// Optional. If empty, no system prompt will be used.
	// Supports f-string placeholders for session values in default GenModelInput, for example:
	// "You are a helpful assistant. The current time is {Time}. The current user is {User}."
	// These placeholders will be replaced with session values for "Time" and "User".
	//
	// 用作该智能体系统提示的指令。
	// 可选。若为空，将不使用系统提示。
	// 在默认 GenModelInput 中支持会话值的 f-string 占位符，例如：
	// "You are a helpful assistant. The current time is {Time}. The current user is {User}."
	// 这些占位符会被替换为 "Time" 和 "User" 的会话值。
	Instruction string

	// Model is the chat model used by the agent.
	// If your ChatModelAgent uses any tools, this model must support the model.WithTools
	// call option, as that's how ChatModelAgent configures the model with tool information.
	//
	// Model 是智能体使用的聊天模型。
	// 如果你的 ChatModelAgent 使用任何工具，该模型必须支持 model.WithTools 调用选项，因为 ChatModelAgent 通过它为模型配置工具信息。
	Model model.BaseModel[M]

	ToolsConfig ToolsConfig

	// GenModelInput transforms instructions and input messages into the model's input format.
	// Optional. Defaults to defaultGenModelInput which combines instruction and messages.
	//
	// GenModelInput 将指令和输入消息转换为模型的输入格式。
	// 可选。默认使用 defaultGenModelInput，它会组合指令和消息。
	GenModelInput TypedGenModelInput[M]

	// Exit defines the tool used to terminate the agent process.
	// Optional. If nil, no Exit Action will be generated.
	// You can use the provided 'ExitTool' implementation directly.
	//
	// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
	// to be more effective empirically. Consider using ChatModelAgent with AgentTool
	// or DeepAgent instead for most multi-agent scenarios.
	//
	// Exit 定义用于终止智能体流程的工具。
	// 可选。若为 nil，则不会生成 Exit Action。
	// 可以直接使用提供的 'ExitTool' 实现。
	// 不推荐：智能体之间共享完整上下文的转交在经验上并未证明更有效。多数多智能体场景建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
	Exit tool.BaseTool

	// OutputKey stores the agent's response in the session.
	// Optional. When set, stores output via AddSessionValue(ctx, outputKey, msg.Content).
	//
	// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
	// to be more effective empirically. Consider using ChatModelAgent with AgentTool
	// or DeepAgent instead for most multi-agent scenarios.
	//
	// OutputKey 将智能体的响应存储到会话中。
	// 可选。设置后，通过 AddSessionValue(ctx, outputKey, msg.Content) 存储输出。
	// 不推荐：智能体之间共享完整上下文的转交在经验上并未证明更有效。多数多智能体场景建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
	OutputKey string

	// MaxIterations defines the upper limit of ChatModel generation cycles.
	// The agent will terminate with an error if this limit is exceeded.
	// Optional. Defaults to 20.
	//
	// MaxIterations 定义 ChatModel 生成周期的上限。
	// 如果超过此限制，智能体将以错误终止。
	// 可选。默认值为 20。
	MaxIterations int

	// Deprecated: Use Handlers instead. Middlewares will be removed in a future release.
	// Handlers provides a more flexible interface-based approach for extending agent behavior.
	//
	// Deprecated: 请改用 Handlers。Middlewares 将在未来版本中移除。
	// Handlers 提供了更灵活的基于接口的方式来扩展智能体行为。
	Middlewares []AgentMiddleware

	// Handlers configures interface-based handlers for extending agent behavior.
	// Unlike Middlewares (struct-based), Handlers allow users to:
	//   - Add custom methods to their handler implementations
	//   - Return modified context from handler methods
	//   - Centralize configuration in struct fields instead of closures
	//
	// Handlers are processed after Middlewares, in registration order.
	// See ChatModelAgentMiddleware documentation for when to use Handlers vs Middlewares.
	//
	// Execution Order (relative to AgentMiddleware and ToolsConfig):
	//
	// Model call lifecycle (outermost to innermost wrapper chain):
	//  1. AgentMiddleware.BeforeChatModel (hook, runs before model call)
	//  2. ChatModelAgentMiddleware.BeforeModelRewriteState (hook, can modify state before model call)
	//  3. failoverModelWrapper (internal - failover between models, if configured)
	//  4. retryModelWrapper (internal - retries on failure, if configured)
	//  5. eventSenderModelWrapper (internal - sends model response events)
	//  6. ChatModelAgentMiddleware.WrapModel (wrapper, first registered is outermost)
	//  7. callbackInjectionModelWrapper (internal - injects callbacks if not enabled; when failover is enabled, this is handled per-model inside failoverProxyModel instead)
	//  8. failoverProxyModel (internal - dispatches to selected failover model, if configured) / Model.Generate/Stream
	//  9. ChatModelAgentMiddleware.AfterModelRewriteState (hook, can modify state after model call)
	// 10. AgentMiddleware.AfterChatModel (hook, runs after model call)
	//
	// Custom Event Sender Position:
	// By default, events are sent after all user middlewares (WrapModel) have processed the output,
	// containing the modified messages. To send events with original (unmodified) output, pass
	// NewEventSenderModelWrapper as a Handler after the modifying middleware:
	//
	//   agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
	//       Handlers: []adk.ChatModelAgentMiddleware{
	//           myCustomHandler,                   // First registered = outermost wrapper
	//           adk.NewEventSenderModelWrapper(),  // Last registered = innermost, events sent with original output
	//       },
	//   })
	//
	// Handler order: first registered is outermost. So [A, B, C] becomes A(B(C(model))).
	// EventSenderModelWrapper sends events in post-processing, so placing it innermost
	// means it receives the original model output before outer handlers modify it.
	//
	// When EventSenderModelWrapper is detected in Handlers, the framework skips
	// the default event sender to avoid duplicate events.
	//
	// Tool call lifecycle (outermost to innermost):
	//  1. eventSenderToolWrapper (internal ToolMiddleware - sends tool result events after all processing)
	//  2. ToolsConfig.ToolCallMiddlewares (ToolMiddleware)
	//  3. AgentMiddleware.WrapToolCall (ToolMiddleware)
	//  4. ChatModelAgentMiddleware.WrapToolCall (wrapper, first registered is outermost)
	//  5. callbackInjectedToolCall (internal - injects callbacks if tool doesn't handle them)
	//  6. Tool.InvokableRun/StreamableRun
	//
	// Custom Tool Event Sender Position:
	// By default, tool result events are emitted by an internal event sender placed before
	// all user middlewares (outermost), so events reflect the fully processed tool output.
	// To control exactly where in the handler chain tool events are emitted, pass
	// NewEventSenderToolWrapper() as one of the Handlers. Its position determines which
	// middlewares' effects are visible in the emitted event:
	//
	//   agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
	//       Handlers: []adk.ChatModelAgentMiddleware{
	//           loggingHandler,                      // Outermost: sees event-sender output
	//           adk.NewEventSenderToolWrapper(),     // Events reflect output from handlers below
	//           sanitizationHandler,                 // Innermost: runs first, modifies tool output
	//       },
	//   })
	//
	// Handler order: first registered is outermost. So [A, B, C] wraps as A(B(C(tool))).
	// The event sender captures tool output in post-processing, so its position controls
	// which handlers' modifications are included in the emitted events.
	//
	// When NewEventSenderToolWrapper is detected in Handlers, the framework skips
	// the default event sender to avoid duplicate events.
	//
	// Tool List Modification:
	//
	// There are two ways to modify the tool list:
	//
	//  1. In BeforeAgent: Modify ChatModelAgentContext.Tools ([]tool.BaseTool) directly. This affects
	//     both the tool info list passed to ChatModel AND the actual tools available for
	//     execution. Changes persist for the entire agent run.
	//
	//  2. In BeforeModelRewriteState: Modify state.ToolInfos and state.DeferredToolInfos directly.
	//     This affects the tool info list passed to ChatModel for this and all subsequent model
	//     calls (changes are persisted in state). This is the recommended approach for dynamic
	//     tool filtering/selection based on conversation context.
	//
	// Modifying tools in WrapModel (e.g. via model.WithTools) is discouraged: changes there
	// are NOT persisted in state, only affect a single model call, and break prompt cache.
	//
	// Handlers 配置用于扩展智能体行为的基于接口的处理器。
	// 与 Middlewares（基于结构体）不同，Handlers 允许用户：
	// - 向其处理器实现添加自定义方法
	// - 从处理器方法返回修改后的 context
	// - 将配置集中在结构体字段中，而不是闭包里
	// Handlers 会在 Middlewares 之后按注册顺序处理。
	// 何时使用 Handlers 或 Middlewares，请参阅 ChatModelAgentMiddleware 文档。
	// 执行顺序（相对于 AgentMiddleware 和 ToolsConfig）：
	// 模型调用生命周期（从最外层到最内层包装链）：
	// 1. AgentMiddleware.BeforeChatModel（hook，在模型调用前运行）
	// 2. ChatModelAgentMiddleware.BeforeModelRewriteState（hook，可在模型调用前修改状态）
	// 3. failoverModelWrapper（内部 - 如已配置，在模型间 failover）
	// 4. retryModelWrapper（内部 - 如已配置，失败时重试）
	// 5. eventSenderModelWrapper（内部 - 发送模型响应事件）
	// 6. ChatModelAgentMiddleware.WrapModel（wrapper，最先注册的是最外层）
	// 7. callbackInjectionModelWrapper（内部 - 未启用时注入回调；启用 failover 时，这会在 failoverProxyModel 内按模型处理）
	// 8. failoverProxyModel（内部 - 如已配置，分发到选定的 failover 模型）/ Model.Generate/Stream
	// 9. ChatModelAgentMiddleware.AfterModelRewriteState（hook，可在模型调用后修改状态）
	// 10. AgentMiddleware.AfterChatModel（hook，在模型调用后运行）
	// 自定义事件发送器位置：
	// 默认情况下，事件会在所有用户中间件（WrapModel）处理完输出后发送，
	// 并包含修改后的消息。若要用原始（未修改）输出发送事件，请在修改型中间件之后传入
	// NewEventSenderModelWrapper 作为 Handler：
	// agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
	// Handlers: []adk.ChatModelAgentMiddleware{
	// myCustomHandler,                   // 最先注册 = 最外层 wrapper
	// adk.NewEventSenderModelWrapper(),  // 最后注册 = 最内层，事件使用原始输出发送
	// },
	// })
	// Handler 顺序：最先注册的是最外层。因此 [A, B, C] 会变为 A(B(C(model)))。
	// EventSenderModelWrapper 在后处理阶段发送事件，因此将其放在最内层
	// 表示它会在外层处理器修改之前收到原始模型输出。
	// 当在 Handlers 中检测到 EventSenderModelWrapper 时，框架会跳过
	// 默认事件发送器以避免重复事件。
	// 工具调用生命周期（从最外层到最内层）：
	// 1. eventSenderToolWrapper（内部 ToolMiddleware - 在所有处理之后发送工具结果事件）
	// 2. ToolsConfig.ToolCallMiddlewares（ToolMiddleware）
	// 3. AgentMiddleware.WrapToolCall（ToolMiddleware）
	// 4. ChatModelAgentMiddleware.WrapToolCall（wrapper，最先注册的是最外层）
	// 5. callbackInjectedToolCall（内部 - 如果工具未处理回调则注入回调）
	// 6. Tool.InvokableRun/StreamableRun
	// 自定义工具事件发送器位置：
	// 默认情况下，工具结果事件由位于所有用户中间件之前（最外层）的内部事件发送器发出，
	// 因此事件反映完全处理后的工具输出。
	// 若要精确控制工具事件在处理器链中的发出位置，请将
	// NewEventSenderToolWrapper() 作为 Handlers 之一传入。它的位置决定哪些
	// 中间件效果会体现在发出的事件中：
	// agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
	// Handlers: []adk.ChatModelAgentMiddleware{
	// loggingHandler,                      // 最外层：看到 event-sender 输出
	// adk.NewEventSenderToolWrapper(),     // 事件反映其下方处理器的输出
	// sanitizationHandler,                 // 最内层：最先运行，修改工具输出
	// },
	// })
	// Handler 顺序：最先注册的是最外层。因此 [A, B, C] 会包装为 A(B(C(tool)))。
	// 事件发送器在后处理阶段捕获工具输出，因此其位置控制
	// 哪些处理器的修改会包含在发出的事件中。
	// 当在 Handlers 中检测到 NewEventSenderToolWrapper 时，框架会跳过
	// 默认事件发送器以避免重复事件。
	// 工具列表修改：
	// 有两种方式可以修改工具列表：
	// 1. 在 BeforeAgent 中：直接修改 ChatModelAgentContext.Tools ([]tool.BaseTool)。这会同时影响
	// 传给 ChatModel 的工具信息列表以及实际可用于执行的工具。
	// 变更会在整个智能体运行期间持续生效。
	// 2. 在 BeforeModelRewriteState 中：直接修改 state.ToolInfos 和 state.DeferredToolInfos。
	// 这会影响本次及后续所有模型调用传给 ChatModel 的工具信息列表
	// （变更会持久化在 state 中）。这是基于对话上下文进行动态
	// 工具过滤/选择的推荐方式。
	// 不建议在 WrapModel 中修改工具（例如通过 model.WithTools）：那里的变更
	// 不会持久化到 state 中，只影响单次模型调用，并且会破坏提示缓存。
	Handlers []TypedChatModelAgentMiddleware[M]

	// ModelRetryConfig configures retry behavior for the ChatModel.
	// When set, the agent will automatically retry failed ChatModel calls
	// based on the configured policy.
	// Optional. If nil, no retry will be performed.
	//
	// ModelRetryConfig 配置 ChatModel 的重试行为。
	// 设置后，智能体会根据配置的策略自动重试失败的 ChatModel 调用。
	// 可选。若为 nil，则不执行重试。
	ModelRetryConfig *TypedModelRetryConfig[M]

	// ModelFailoverConfig configures failover behavior for the ChatModel.
	// When set, the agent will first try the last successful model (initially the configured Model),
	// and on failure, call GetFailoverModel to select alternate models.
	// Model field is still required as it serves as the initial model.
	// Optional. If nil, no failover will be performed.
	//
	// ModelFailoverConfig 配置 ChatModel 的 failover 行为。
	// 设置后，智能体会先尝试上次成功的模型（初始为配置的 Model），
	// 失败时调用 GetFailoverModel 来选择备用模型。
	// Model 字段仍然必需，因为它作为初始模型。
	// 可选。若为 nil，则不执行 failover。
	ModelFailoverConfig *ModelFailoverConfig[M]
}

type ChatModelAgentConfig = TypedChatModelAgentConfig[*schema.Message]

// TypedChatModelAgent is a chat model-backed agent parameterized by message type.
//
// For M = *schema.Message, the full ReAct loop (model → tool calls → model) is used.
// For M = *schema.AgenticMessage, a single-shot chain is used since agentic models
// handle tool calling internally. Cancel monitoring and retry on the model stream
// are not yet supported for agentic models.
//
// TypedChatModelAgent 是由聊天模型驱动、按消息类型参数化的智能体。
// 当 M = *schema.Message 时，使用完整的 ReAct 循环（模型 → 工具调用 → 模型）。
// 当 M = *schema.AgenticMessage 时，由于 agentic 模型会在内部处理工具调用，
// 因此使用单次链。agentic 模型尚不支持模型流的取消监控和重试。
type TypedChatModelAgent[M MessageType] struct {
	name        string
	description string
	instruction string

	model       model.BaseModel[M]
	toolsConfig ToolsConfig

	genModelInput TypedGenModelInput[M]

	outputKey     string
	maxIterations int

	subAgents   []TypedAgent[M]
	parentAgent TypedAgent[M]

	disallowTransferToParent bool

	exit tool.BaseTool

	handlers    []TypedChatModelAgentMiddleware[M]
	middlewares []AgentMiddleware

	modelRetryConfig    *TypedModelRetryConfig[M]
	modelFailoverConfig *ModelFailoverConfig[M]

	once   sync.Once
	run    typedRunFunc[M]
	frozen uint32
	exeCtx *execContext
}

type ChatModelAgent = TypedChatModelAgent[*schema.Message]

// typedRunParams holds the parameters for a typedRunFunc invocation.
// typedRunParams 保存 typedRunFunc 调用的参数。
type typedRunParams[M MessageType] struct {
	input          *TypedAgentInput[M]
	generator      *AsyncGenerator[*TypedAgentEvent[M]]
	store          *bridgeStore
	instruction    string
	returnDirectly map[string]bool
	cancelCtx      *cancelContext
	cancelCtxOwned bool
	composeOpts    []compose.Option

	afterToolCallsHook func(ctx context.Context) error
}

type typedRunFunc[M MessageType] func(ctx context.Context, p *typedRunParams[M])

func resolveRunCancelContext(ctx context.Context, o *options) (*cancelContext, bool) {
	inherited := getCancelContext(ctx)
	if o.cancelCtx != nil {
		return o.cancelCtx, o.cancelCtx != inherited
	}
	return inherited, false
}

// NewChatModelAgent creates a new ChatModelAgent with the given config.
// NewChatModelAgent 使用给定配置创建新的 ChatModelAgent。
func NewChatModelAgent(ctx context.Context, config *ChatModelAgentConfig) (*ChatModelAgent, error) {
	return NewTypedChatModelAgent[*schema.Message](ctx, config)
}

// NewTypedChatModelAgent creates a new TypedChatModelAgent with the given config.
// NewTypedChatModelAgent 使用给定配置创建新的 TypedChatModelAgent。
func NewTypedChatModelAgent[M MessageType](ctx context.Context, config *TypedChatModelAgentConfig[M]) (*TypedChatModelAgent[M], error) {
	if config.ModelFailoverConfig != nil {
		if config.ModelFailoverConfig.GetFailoverModel == nil {
			return nil, errors.New("ModelFailoverConfig.GetFailoverModel is required when ModelFailoverConfig is set")
		}

		// ShouldFailover is required when ModelFailoverConfig is set
		// 设置 ModelFailoverConfig 时必须提供 ShouldFailover
		if config.ModelFailoverConfig.ShouldFailover == nil {
			return nil, errors.New("ModelFailoverConfig.ShouldFailover is required when ModelFailoverConfig is set")
		}
	}

	if config.Model == nil {
		return nil, errors.New("agent 'Model' is required")
	}

	var genInput TypedGenModelInput[M]
	if config.GenModelInput != nil {
		genInput = config.GenModelInput
	} else {
		genInput = newDefaultGenModelInput[M]()
	}

	tc := config.ToolsConfig

	// Tool call middleware execution order (outermost to innermost):
	// 1. eventSenderToolWrapper (internal - sends tool result events after all modifications)
	// 2. User-provided ToolsConfig.ToolCallMiddlewares (original order preserved)
	// 3. Middlewares' WrapToolCall (in registration order)
	// 4. ChatModelAgentMiddleware.WrapToolCall (in registration order)
	// 5. callbackInjectedToolCall (internal - injects callbacks if tool doesn't handle them)
	//
	// 工具调用中间件执行顺序（从最外层到最内层）：
	// 1. eventSenderToolWrapper（内部 - 在所有修改后发送工具结果事件）
	// 2. 用户提供的 ToolsConfig.ToolCallMiddlewares（保持原始顺序）
	// 3. Middlewares 的 WrapToolCall（按注册顺序）
	// 4. ChatModelAgentMiddleware.WrapToolCall（按注册顺序）
	// 5. callbackInjectedToolCall（内部 - 如果工具未处理回调则注入回调）
	if !hasUserEventSenderToolWrapper(config.Handlers) {
		defaultToolEventSender := handlersToToolMiddlewares([]TypedChatModelAgentMiddleware[M]{newTypedEventSenderToolWrapper[M]()})
		tc.ToolCallMiddlewares = append(defaultToolEventSender, tc.ToolCallMiddlewares...)
	}
	tc.ToolCallMiddlewares = append(tc.ToolCallMiddlewares, collectToolMiddlewaresFromMiddlewares(config.Middlewares)...)

	// Cancel monitoring middleware (innermost — close to the tool endpoint).
	// This allows early abort of the raw tool result stream when immediateChan fires
	// (CancelImmediate or timeout escalation), while requiring outer wrappers to
	// propagate stream errors such as ErrStreamCanceled without swallowing them.
	//
	// 取消监控中间件（最内层——靠近工具端点）。
	// 这允许在 immediateChan 触发（CancelImmediate 或超时升级）时提前中止原始工具结果流，同时要求外层包装器传播 ErrStreamCanceled 等流错误而不是吞掉它们。
	cancelToolHandler := &cancelMonitoredToolHandler{}
	tc.ToolCallMiddlewares = append(tc.ToolCallMiddlewares, compose.ToolMiddleware{
		Streamable:         cancelToolHandler.WrapStreamableToolCall,
		EnhancedStreamable: cancelToolHandler.WrapEnhancedStreamableToolCall,
	})

	return &TypedChatModelAgent[M]{
		name:                config.Name,
		description:         config.Description,
		instruction:         config.Instruction,
		model:               config.Model,
		toolsConfig:         tc,
		genModelInput:       genInput,
		exit:                config.Exit,
		outputKey:           config.OutputKey,
		maxIterations:       config.MaxIterations,
		handlers:            config.Handlers,
		middlewares:         config.Middlewares,
		modelRetryConfig:    config.ModelRetryConfig,
		modelFailoverConfig: config.ModelFailoverConfig,
	}, nil
}

func collectToolMiddlewaresFromMiddlewares(mws []AgentMiddleware) []compose.ToolMiddleware {
	var middlewares []compose.ToolMiddleware
	for _, m := range mws {
		if m.WrapToolCall.Invokable == nil && m.WrapToolCall.Streamable == nil && m.WrapToolCall.EnhancedStreamable == nil && m.WrapToolCall.EnhancedInvokable == nil {
			continue
		}
		middlewares = append(middlewares, m.WrapToolCall)
	}
	return middlewares
}

const (
	TransferToAgentToolName        = "transfer_to_agent"
	TransferToAgentToolDesc        = "Transfer the question to another agent."
	TransferToAgentToolDescChinese = "将问题移交给其他 Agent。"
)

var (
	toolInfoTransferToAgent = &schema.ToolInfo{
		Name: TransferToAgentToolName,

		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"agent_name": {
				Desc:     "the name of the agent to transfer to",
				Required: true,
				Type:     schema.String,
			},
		}),
	}

	ToolInfoExit = &schema.ToolInfo{
		Name: "exit",
		Desc: "Exit the agent process and return the final result.",

		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"final_result": {
				Desc:     "the final result to return",
				Required: true,
				Type:     schema.String,
			},
		}),
	}
)

type ExitTool struct{}

func (et ExitTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return ToolInfoExit, nil
}

func (et ExitTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	type exitParams struct {
		FinalResult string `json:"final_result"`
	}

	params := &exitParams{}
	err := sonic.UnmarshalString(argumentsInJSON, params)
	if err != nil {
		return "", err
	}

	err = SendToolGenAction(ctx, "exit", NewExitAction())
	if err != nil {
		return "", err
	}

	return params.FinalResult, nil
}

type transferToAgent struct{}

func (tta transferToAgent) Info(_ context.Context) (*schema.ToolInfo, error) {
	desc := internal.SelectPrompt(internal.I18nPrompts{
		English: TransferToAgentToolDesc,
		Chinese: TransferToAgentToolDescChinese,
	})
	info := *toolInfoTransferToAgent
	info.Desc = desc
	return &info, nil
}

func transferToAgentToolOutput(destName string) string {
	tpl := internal.SelectPrompt(internal.I18nPrompts{
		English: "successfully transferred to agent [%s]",
		Chinese: "成功移交任务至 agent [%s]",
	})
	return fmt.Sprintf(tpl, destName)
}

func (tta transferToAgent) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	type transferParams struct {
		AgentName string `json:"agent_name"`
	}

	params := &transferParams{}
	err := sonic.UnmarshalString(argumentsInJSON, params)
	if err != nil {
		return "", err
	}

	err = SendToolGenAction(ctx, TransferToAgentToolName, NewTransferToAgentAction(params.AgentName))
	if err != nil {
		return "", err
	}

	return transferToAgentToolOutput(params.AgentName), nil
}

func (a *TypedChatModelAgent[M]) Name(_ context.Context) string {
	return a.name
}

func (a *TypedChatModelAgent[M]) Description(_ context.Context) string {
	return a.description
}

func (a *TypedChatModelAgent[M]) GetType() string {
	return "ChatModel"
}

// OnSetSubAgents implements OnSubAgents.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// OnSetSubAgents 实现 OnSubAgents。
// 不推荐：在智能体之间进行完整上下文共享的智能体转移，在经验上尚未证明更有效。大多数多智能体场景请考虑改用 ChatModelAgent 搭配 AgentTool，或使用 DeepAgent。
func (a *TypedChatModelAgent[M]) OnSetSubAgents(_ context.Context, subAgents []TypedAgent[M]) error {
	if atomic.LoadUint32(&a.frozen) == 1 {
		return errors.New("agent has been frozen after run")
	}

	if len(a.subAgents) > 0 {
		return errors.New("agent's sub-agents has already been set")
	}

	a.subAgents = subAgents
	return nil
}

// OnSetAsSubAgent implements OnSubAgents.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// OnSetAsSubAgent 实现 OnSubAgents。
// 不推荐：在智能体之间进行完整上下文共享的智能体转移，在经验上尚未证明更有效。大多数多智能体场景请考虑改用 ChatModelAgent 搭配 AgentTool，或使用 DeepAgent。
func (a *TypedChatModelAgent[M]) OnSetAsSubAgent(_ context.Context, parent TypedAgent[M]) error {
	if atomic.LoadUint32(&a.frozen) == 1 {
		return errors.New("agent has been frozen after run")
	}

	if a.parentAgent != nil {
		return errors.New("agent has already been set as a sub-agent of another agent")
	}

	a.parentAgent = parent
	return nil
}

// OnDisallowTransferToParent implements OnSubAgents.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// OnDisallowTransferToParent 实现 OnSubAgents。
// 不推荐：在智能体之间进行完整上下文共享的智能体转移，在经验上尚未证明更有效。大多数多智能体场景请考虑改用 ChatModelAgent 搭配 AgentTool，或使用 DeepAgent。
func (a *TypedChatModelAgent[M]) OnDisallowTransferToParent(_ context.Context) error {
	if atomic.LoadUint32(&a.frozen) == 1 {
		return errors.New("agent has been frozen after run")
	}

	a.disallowTransferToParent = true

	return nil
}

type ChatModelAgentInterruptInfo struct {
	Info *compose.InterruptInfo
	Data []byte
}

func init() {
	schema.RegisterName[*ChatModelAgentInterruptInfo]("_eino_adk_chat_model_agent_interrupt_info")
}

func extractTextContent[M MessageType](msg M) string {
	switch v := any(msg).(type) {
	case *schema.Message:
		return v.Content
	case *schema.AgenticMessage:
		var texts []string
		for _, block := range v.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeAssistantGenText && block.AssistantGenText != nil {
				texts = append(texts, block.AssistantGenText.Text)
			}
		}
		return strings.Join(texts, "\n")
	default:
		return ""
	}
}

func setOutputToSession[M MessageType](ctx context.Context, msg M, msgStream *schema.StreamReader[M], outputKey string) error {
	if !isNilMessage(msg) {
		AddSessionValue(ctx, outputKey, extractTextContent(msg))
		return nil
	}

	concatenated, err := concatMessageStream(msgStream)
	if err != nil {
		return err
	}

	AddSessionValue(ctx, outputKey, extractTextContent(concatenated))
	return nil
}

func typedErrFunc[M MessageType](err error) typedRunFunc[M] {
	return func(ctx context.Context, p *typedRunParams[M]) {
		p.generator.Send(&TypedAgentEvent[M]{Err: err})
	}
}

// ChatModelAgentResumeData holds data that can be provided to a ChatModelAgent during a resume operation
// to modify its behavior. It is provided via the adk.ResumeWithData function.
//
// ChatModelAgentResumeData 保存可在恢复操作期间提供给 ChatModelAgent、用于修改其行为的数据。
// 它通过 adk.ResumeWithData 函数提供。
type ChatModelAgentResumeData struct {
	// HistoryModifier is a function that can transform the agent's message history before it is sent to the model.
	// This allows for adding new information or context upon resumption.
	//
	// HistoryModifier 是一个函数，可在智能体的消息历史发送给模型前对其进行转换。
	// 这允许在恢复时添加新信息或上下文。
	HistoryModifier func(ctx context.Context, history []Message) []Message
}

type execContext struct {
	instruction    string
	toolsNodeConf  compose.ToolsNodeConfig
	returnDirectly map[string]bool

	toolInfos      []*schema.ToolInfo
	unwrappedTools []tool.BaseTool

	toolSearchTool *schema.ToolInfo // set by BeforeAgent when the model supports native tool search
	// 当模型支持原生工具搜索时由 BeforeAgent 设置

	rebuildGraph bool // whether needs to instantiate a new graph because of topology changes due to tool modifications
	// 是否因工具修改导致拓扑变化而需要实例化新的图
	toolUpdated bool // whether needs to pass a compose.WithToolList option to ToolsNode due to tool list change
	// 是否因工具列表变化而需要向 ToolsNode 传入 compose.WithToolList option
}

func (a *TypedChatModelAgent[M]) applyBeforeAgent(ctx context.Context, ec *execContext) (context.Context, *execContext, error) {
	runCtx := &ChatModelAgentContext{
		Instruction:    ec.instruction,
		Tools:          cloneSlice(ec.unwrappedTools),
		ReturnDirectly: copyMap(ec.returnDirectly),
	}

	var err error
	for i, handler := range a.handlers {
		ctx, runCtx, err = handler.BeforeAgent(ctx, runCtx)
		if err != nil {
			return ctx, nil, fmt.Errorf("handler[%d] (%T) BeforeAgent failed: %w", i, handler, err)
		}
	}

	toolsNodeConf := ec.toolsNodeConf
	toolsNodeConf.Tools = runCtx.Tools
	toolsNodeConf.ToolCallMiddlewares = cloneSlice(ec.toolsNodeConf.ToolCallMiddlewares)

	runtimeEC := &execContext{
		instruction:    runCtx.Instruction,
		toolsNodeConf:  toolsNodeConf,
		returnDirectly: runCtx.ReturnDirectly,
		toolSearchTool: runCtx.ToolSearchTool,
		toolUpdated:    true,
		rebuildGraph: (len(ec.toolsNodeConf.Tools) == 0 && len(runCtx.Tools) > 0) ||
			(len(ec.returnDirectly) == 0 && len(runCtx.ReturnDirectly) > 0),
	}

	toolInfos, err := genToolInfos(ctx, &runtimeEC.toolsNodeConf)
	if err != nil {
		return ctx, nil, err
	}

	runtimeEC.toolInfos = toolInfos

	return ctx, runtimeEC, nil
}

func (a *TypedChatModelAgent[M]) applyAfterAgent(ctx context.Context) (context.Context, error) {
	if len(a.handlers) == 0 {
		return ctx, nil
	}

	var state TypedChatModelAgentState[M]
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		state.Messages = st.Messages
		state.ToolInfos = st.ToolInfos
		state.DeferredToolInfos = st.DeferredToolInfos
		return nil
	})

	var err error
	for i, handler := range a.handlers {
		ctx, err = handler.AfterAgent(ctx, &state)
		if err != nil {
			return ctx, fmt.Errorf("handler[%d] (%T) AfterAgent failed: %w", i, handler, err)
		}
	}
	return ctx, nil
}

func (a *TypedChatModelAgent[M]) prepareExecContext(ctx context.Context) (*execContext, error) {
	instruction := a.instruction
	toolsNodeConf := a.toolsConfig.ToolsNodeConfig
	toolsNodeConf.Tools = cloneSlice(a.toolsConfig.Tools)
	toolsNodeConf.ToolCallMiddlewares = cloneSlice(a.toolsConfig.ToolCallMiddlewares)
	returnDirectly := copyMap(a.toolsConfig.ReturnDirectly)

	transferToAgents := a.subAgents
	if a.parentAgent != nil && !a.disallowTransferToParent {
		transferToAgents = append(transferToAgents, a.parentAgent)
	}

	if len(transferToAgents) > 0 {
		transferInstruction := genTransferToAgentInstruction(ctx, transferToAgents)
		instruction = concatInstructions(instruction, transferInstruction)

		toolsNodeConf.Tools = append(toolsNodeConf.Tools, &transferToAgent{})
		returnDirectly[TransferToAgentToolName] = true
	}

	if a.exit != nil {
		toolsNodeConf.Tools = append(toolsNodeConf.Tools, a.exit)
		exitInfo, err := a.exit.Info(ctx)
		if err != nil {
			return nil, err
		}
		returnDirectly[exitInfo.Name] = true
	}

	for _, m := range a.middlewares {
		if m.AdditionalInstruction != "" {
			instruction = concatInstructions(instruction, m.AdditionalInstruction)
		}
		toolsNodeConf.Tools = append(toolsNodeConf.Tools, m.AdditionalTools...)
	}

	unwrappedTools := cloneSlice(toolsNodeConf.Tools)

	handlerMiddlewares := handlersToToolMiddlewares(a.handlers)
	toolsNodeConf.ToolCallMiddlewares = append(toolsNodeConf.ToolCallMiddlewares, handlerMiddlewares...)

	toolInfos, err := genToolInfos(ctx, &toolsNodeConf)
	if err != nil {
		return nil, err
	}

	return &execContext{
		instruction:    instruction,
		toolsNodeConf:  toolsNodeConf,
		returnDirectly: returnDirectly,
		toolInfos:      toolInfos,
		unwrappedTools: unwrappedTools,
	}, nil
}

// handleRunFuncError is the common error handler for buildNoToolsRunFunc and buildReActRunFunc.
// It handles compose interrupts (both cancel-triggered and business)
// and generic errors, sending the appropriate event to the generator.
//
// handleRunFuncError 是 buildNoToolsRunFunc 和 buildReActRunFunc 的通用错误处理器。
// 它处理 compose 中断（包括取消触发的中断和业务中断）以及普通错误，并向生成器发送相应事件。
func (a *TypedChatModelAgent[M]) handleRunFuncError(
	ctx context.Context,
	err error,
	cancelCtx *cancelContext,
	cancelCtxOwned bool,
	store *bridgeStore,
	generator *AsyncGenerator[*TypedAgentEvent[M]],
) {
	info, ok := compose.ExtractInterruptInfo(err)
	if ok {
		if cancelCtx != nil {
			if !cancelCtx.shouldCancel() {
				// Note: there is a benign TOCTOU window here. Between shouldCancel()
				// returning false and markDone() executing, a concurrent cancel could
				// transition stateRunning→stateCancelling. markDone() then does
				// stateCancelling→stateDone, and the cancel func receives
				// ErrExecutionEnded (execution finished before cancel took effect).
				//
				// 注意：这里存在一个无害的 TOCTOU 窗口。在 shouldCancel() 返回 false 到 markDone() 执行之间，并发取消可能会将 stateRunning→stateCancelling。随后 markDone() 执行 stateCancelling→stateDone，cancel func 会收到 ErrExecutionEnded（执行在取消生效前已结束）。
				cancelCtx.markDone()
			}
		}

		data, existed, sErr := store.Get(ctx, bridgeCheckpointID)
		if sErr != nil {
			generator.Send(&TypedAgentEvent[M]{AgentName: a.name, Err: fmt.Errorf("failed to get interrupt info: %w", sErr)})
			return
		}
		if !existed {
			generator.Send(&TypedAgentEvent[M]{AgentName: a.name, Err: fmt.Errorf("interrupt occurred but checkpoint data is missing")})
			return
		}

		is := FromInterruptContexts(info.InterruptContexts)
		event := TypedCompositeInterrupt[M](ctx, info, data, is)
		event.Action.Interrupted.Data = &ChatModelAgentInterruptInfo{
			Info: info,
			Data: data,
		}
		event.AgentName = a.name
		generator.Send(event)
		return
	}

	if cancelCtxOwned && cancelCtx != nil {
		cancelCtx.markDone()
	}
	generator.Send(&TypedAgentEvent[M]{Err: err})
}

type typedNoToolsInput[M MessageType] struct {
	input       *TypedAgentInput[M]
	instruction string
}

func appendModelToChain[I, O any, M MessageType](chain *compose.Chain[I, O], m model.BaseModel[M]) {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		chain.AppendChatModel(any(m).(model.BaseChatModel))
	case *schema.AgenticMessage:
		chain.AppendAgenticModel(any(m).(model.AgenticModel))
	}
}

func (a *TypedChatModelAgent[M]) buildNoToolsRunFunc(_ context.Context) (typedRunFunc[M], error) {
	return func(ctx context.Context, p *typedRunParams[M]) {
		cancelCtx := p.cancelCtx
		ctx = withCancelContext(ctx, cancelCtx)

		wrappedModel := buildModelWrappers(a.model, &typedModelWrapperConfig[M]{
			handlers:       a.handlers,
			middlewares:    a.middlewares,
			retryConfig:    a.modelRetryConfig,
			failoverConfig: a.modelFailoverConfig,
			cancelContext:  cancelCtx,
		})

		chain := compose.NewChain[typedNoToolsInput[M], M](
			compose.WithGenLocalState(func(ctx context.Context) (state *typedState[M]) {
				return &typedState[M]{}
			}))

		chain.AppendLambda(compose.InvokableLambda(func(ctx context.Context, in typedNoToolsInput[M]) ([]M, error) {
			messages, err := a.genModelInput(ctx, in.instruction, in.input)
			if err != nil {
				return nil, err
			}
			if err := compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
				st.Messages = append(st.Messages, messages...)
				return nil
			}); err != nil {
				return nil, err
			}
			return messages, nil
		}))

		appendModelToChain(chain, wrappedModel)

		if len(a.handlers) > 0 {
			chain.AppendLambda(compose.InvokableLambda(func(ctx context.Context, msg M) (M, error) {
				_, err := a.applyAfterAgent(ctx)
				return msg, err
			}))
		}

		var compileOptions []compose.GraphCompileOption
		compileOptions = append(compileOptions,
			compose.WithGraphName(a.name),
			compose.WithCheckPointStore(p.store),
			compose.WithSerializer(&gobSerializer{}))

		if cancelCtx != nil {
			var interrupt func(...compose.GraphInterruptOption)
			ctx, interrupt = compose.WithGraphInterrupt(ctx)
			cancelCtx.setGraphInterruptFunc(interrupt)
		}

		r, err := chain.Compile(ctx, compileOptions...)
		if err != nil {
			p.generator.Send(&TypedAgentEvent[M]{Err: err})
			return
		}

		ctx = withTypedChatModelAgentExecCtx(ctx, &typedChatModelAgentExecCtx[M]{
			generator:                p.generator,
			cancelCtx:                cancelCtx,
			failoverLastSuccessModel: a.model,
		})

		// Pre-execution cancel check
		// 执行前取消检查
		if cancelCtx != nil && cancelCtx.shouldCancel() {
			if cancelCtx.getMode() == CancelImmediate || atomic.LoadInt32(&cancelCtx.escalated) == 1 {
				cancelErr, ok := cancelCtx.createAndMarkCancelHandled()
				if !ok {
					return
				}
				p.generator.Send(&TypedAgentEvent[M]{Err: cancelErr})
				return
			}
		}

		in := typedNoToolsInput[M]{input: p.input, instruction: p.instruction}

		var msg M
		var msgStream *schema.StreamReader[M]
		if p.input.EnableStreaming {
			msgStream, err = r.Stream(ctx, in, p.composeOpts...)
		} else {
			msg, err = r.Invoke(ctx, in, p.composeOpts...)
		}

		if err == nil {
			if a.outputKey != "" {
				err = setOutputToSession(ctx, msg, msgStream, a.outputKey)
				if err != nil {
					p.generator.Send(&TypedAgentEvent[M]{Err: err})
				}
			} else if msgStream != nil {
				msgStream.Close()
			}
			return
		}

		a.handleRunFuncError(ctx, err, cancelCtx, p.cancelCtxOwned, p.store, p.generator)
	}, nil
}

func (a *TypedChatModelAgent[M]) buildReActRunFunc(ctx context.Context, bc *execContext) (typedRunFunc[M], error) {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return a.buildMessageReActRunFunc(ctx, bc)
	case *schema.AgenticMessage:
		// single-shot: agentic models handle tool calling internally
		// 单次调用：agentic 模型内部处理工具调用
		return a.buildAgenticReActRunFunc(ctx, bc)
	default:
		return nil, fmt.Errorf("unsupported message type %T for ReAct run mode", zero)
	}
}

type reactRunInput struct {
	input       *AgentInput
	instruction string
}

func (a *TypedChatModelAgent[M]) buildMessageReActRunFunc(_ context.Context, bc *execContext) (typedRunFunc[M], error) {
	// safe: only called when M = *schema.Message (guarded by type switch in buildReActRunFunc)
	// 安全：仅在 M = *schema.Message 时调用（由 buildReActRunFunc 中的 type switch 保护）
	msgModel := any(a.model).(model.BaseChatModel)
	msgHandlers := any(a.handlers).([]ChatModelAgentMiddleware)
	genModelInputFn := any(a.genModelInput).(GenModelInput)
	msgConf := &reactConfig{
		model:       msgModel,
		toolsConfig: &bc.toolsNodeConf,
		modelWrapperConf: &modelWrapperConfig{
			handlers:       msgHandlers,
			middlewares:    a.middlewares,
			retryConfig:    any(a.modelRetryConfig).(*ModelRetryConfig),
			failoverConfig: any(a.modelFailoverConfig).(*ModelFailoverConfig[*schema.Message]),
			toolInfos:      bc.toolInfos,
		},
		toolsReturnDirectly: bc.returnDirectly,
		agentName:           a.name,
		maxIterations:       a.maxIterations,
	}
	if len(a.handlers) > 0 {
		msgAgent := any(a).(*TypedChatModelAgent[*schema.Message])
		msgConf.afterAgentFunc = func(ctx context.Context, msg *schema.Message) (*schema.Message, error) {
			_, err := msgAgent.applyAfterAgent(ctx)
			return msg, err
		}
	}

	return func(ctx context.Context, p *typedRunParams[M]) {
		mp := any(p).(*typedRunParams[*schema.Message])
		cancelCtx := mp.cancelCtx
		msgConf.cancelCtx = cancelCtx
		if msgConf.modelWrapperConf != nil {
			msgConf.modelWrapperConf.cancelContext = cancelCtx
		}
		ctx = withCancelContext(ctx, cancelCtx)

		g, err := newReact(ctx, msgConf)
		if err != nil {
			mp.generator.Send(&AgentEvent{Err: err})
			return
		}

		chain := compose.NewChain[reactRunInput, Message]().
			AppendLambda(
				compose.InvokableLambda(func(ctx context.Context, in reactRunInput) (*reactInput, error) {
					messages, genErr := genModelInputFn(ctx, in.instruction, in.input)
					if genErr != nil {
						return nil, genErr
					}
					return &reactInput{
						Messages: messages,
					}, nil
				}),
			).
			AppendGraph(g, compose.WithNodeName("ReAct"), compose.WithGraphCompileOptions(compose.WithMaxRunSteps(math.MaxInt)))

		var compileOptions []compose.GraphCompileOption
		compileOptions = append(compileOptions,
			compose.WithGraphName(a.name),
			compose.WithCheckPointStore(mp.store),
			compose.WithSerializer(&gobSerializer{}),
			compose.WithMaxRunSteps(math.MaxInt))

		if cancelCtx != nil {
			var interrupt func(...compose.GraphInterruptOption)
			ctx, interrupt = compose.WithGraphInterrupt(ctx)
			cancelCtx.setGraphInterruptFunc(interrupt)
		}

		runnable, err_ := chain.Compile(ctx, compileOptions...)
		if err_ != nil {
			mp.generator.Send(&AgentEvent{Err: err_})
			return
		}

		ctx = withTypedChatModelAgentExecCtx(ctx, &chatModelAgentExecCtx{
			runtimeReturnDirectly:    mp.returnDirectly,
			generator:                mp.generator,
			cancelCtx:                cancelCtx,
			failoverLastSuccessModel: msgModel,
			afterToolCallsHook:       mp.afterToolCallsHook,
		})

		// Pre-execution cancel check
		// 执行前取消检查
		if cancelCtx != nil && cancelCtx.shouldCancel() {
			if cancelCtx.getMode() == CancelImmediate || atomic.LoadInt32(&cancelCtx.escalated) == 1 {
				cancelErr, ok := cancelCtx.createAndMarkCancelHandled()
				if !ok {
					return
				}
				mp.generator.Send(&AgentEvent{Err: cancelErr})
				return
			}
		}

		in := reactRunInput{
			input:       mp.input,
			instruction: mp.instruction,
		}

		var runOpts []compose.Option
		runOpts = append(runOpts, mp.composeOpts...)
		if a.toolsConfig.EmitInternalEvents {
			runOpts = append(runOpts, compose.WithToolsNodeOption(compose.WithToolOption(withAgentToolEventGenerator(mp.generator))))
		}
		if mp.input.EnableStreaming {
			runOpts = append(runOpts, compose.WithToolsNodeOption(compose.WithToolOption(withAgentToolEnableStreaming(true))))
		}

		var msg Message
		var msgStream MessageStream
		if mp.input.EnableStreaming {
			msgStream, err_ = runnable.Stream(ctx, in, runOpts...)
		} else {
			msg, err_ = runnable.Invoke(ctx, in, runOpts...)
		}

		if err_ == nil {
			if a.outputKey != "" {
				err_ = setOutputToSession[*schema.Message](ctx, msg, msgStream, a.outputKey)
				if err_ != nil {
					mp.generator.Send(&AgentEvent{Err: err_})
				}
			} else if msgStream != nil {
				msgStream.Close()
			}

			return
		}

		a.handleRunFuncError(ctx, err_, cancelCtx, mp.cancelCtxOwned, mp.store, p.generator)
	}, nil
}

type agenticReactRunInput struct {
	input       *TypedAgentInput[*schema.AgenticMessage]
	instruction string
}

func (a *TypedChatModelAgent[M]) buildAgenticReActRunFunc(_ context.Context, bc *execContext) (typedRunFunc[M], error) {
	agenticModel := any(a.model).(model.AgenticModel)
	agenticHandlers := any(a.handlers).([]TypedChatModelAgentMiddleware[*schema.AgenticMessage])
	genModelInputFn := any(a.genModelInput).(TypedGenModelInput[*schema.AgenticMessage])
	agenticConf := &agenticReactConfig{
		model:       agenticModel,
		toolsConfig: &bc.toolsNodeConf,
		modelWrapperConf: &typedModelWrapperConfig[*schema.AgenticMessage]{
			handlers:       agenticHandlers,
			middlewares:    a.middlewares,
			retryConfig:    any(a.modelRetryConfig).(*TypedModelRetryConfig[*schema.AgenticMessage]),
			failoverConfig: any(a.modelFailoverConfig).(*ModelFailoverConfig[*schema.AgenticMessage]),
			toolInfos:      bc.toolInfos,
		},
		toolsReturnDirectly: bc.returnDirectly,
		agentName:           a.name,
		maxIterations:       a.maxIterations,
	}
	if len(a.handlers) > 0 {
		agenticAgent := any(a).(*TypedChatModelAgent[*schema.AgenticMessage])
		agenticConf.afterAgentFunc = func(ctx context.Context, msg *schema.AgenticMessage) (*schema.AgenticMessage, error) {
			_, err := agenticAgent.applyAfterAgent(ctx)
			return msg, err
		}
	}

	return func(ctx context.Context, p *typedRunParams[M]) {
		ap := any(p).(*typedRunParams[*schema.AgenticMessage])
		cancelCtx := ap.cancelCtx
		agenticConf.cancelCtx = cancelCtx
		if agenticConf.modelWrapperConf != nil {
			agenticConf.modelWrapperConf.cancelContext = cancelCtx
		}
		ctx = withCancelContext(ctx, cancelCtx)

		g, err := newAgenticReact(ctx, agenticConf)
		if err != nil {
			ap.generator.Send(&TypedAgentEvent[*schema.AgenticMessage]{Err: err})
			return
		}

		chain := compose.NewChain[agenticReactRunInput, *schema.AgenticMessage]().
			AppendLambda(
				compose.InvokableLambda(func(ctx context.Context, in agenticReactRunInput) (*agenticReactInput, error) {
					messages, genErr := genModelInputFn(ctx, in.instruction, in.input)
					if genErr != nil {
						return nil, genErr
					}
					return &agenticReactInput{
						Messages: messages,
					}, nil
				}),
			).
			AppendGraph(g, compose.WithNodeName("ReAct"), compose.WithGraphCompileOptions(compose.WithMaxRunSteps(math.MaxInt)))

		var compileOptions []compose.GraphCompileOption
		compileOptions = append(compileOptions,
			compose.WithGraphName(a.name),
			compose.WithCheckPointStore(ap.store),
			compose.WithSerializer(&gobSerializer{}),
			compose.WithMaxRunSteps(math.MaxInt))

		if cancelCtx != nil {
			var interrupt func(...compose.GraphInterruptOption)
			ctx, interrupt = compose.WithGraphInterrupt(ctx)
			cancelCtx.setGraphInterruptFunc(interrupt)
		}

		runnable, err_ := chain.Compile(ctx, compileOptions...)
		if err_ != nil {
			ap.generator.Send(&TypedAgentEvent[*schema.AgenticMessage]{Err: err_})
			return
		}

		ctx = withTypedChatModelAgentExecCtx(ctx, &typedChatModelAgentExecCtx[*schema.AgenticMessage]{
			runtimeReturnDirectly:    ap.returnDirectly,
			generator:                ap.generator,
			cancelCtx:                cancelCtx,
			failoverLastSuccessModel: agenticModel,
			afterToolCallsHook:       ap.afterToolCallsHook,
		})

		// Pre-execution cancel check
		// 执行前取消检查
		if cancelCtx != nil && cancelCtx.shouldCancel() {
			if cancelCtx.getMode() == CancelImmediate || atomic.LoadInt32(&cancelCtx.escalated) == 1 {
				cancelErr, ok := cancelCtx.createAndMarkCancelHandled()
				if !ok {
					return
				}
				ap.generator.Send(&TypedAgentEvent[*schema.AgenticMessage]{Err: cancelErr})
				return
			}
		}

		in := agenticReactRunInput{input: ap.input, instruction: ap.instruction}

		var runOpts []compose.Option
		runOpts = append(runOpts, ap.composeOpts...)
		if a.toolsConfig.EmitInternalEvents {
			runOpts = append(runOpts, compose.WithToolsNodeOption(compose.WithToolOption(withTypedAgentToolEventGenerator[*schema.AgenticMessage](ap.generator))))
		}
		if ap.input.EnableStreaming {
			runOpts = append(runOpts, compose.WithToolsNodeOption(compose.WithToolOption(withAgentToolEnableStreaming(true))))
		}

		var msg *schema.AgenticMessage
		var msgStream *schema.StreamReader[*schema.AgenticMessage]
		if ap.input.EnableStreaming {
			msgStream, err_ = runnable.Stream(ctx, in, runOpts...)
		} else {
			msg, err_ = runnable.Invoke(ctx, in, runOpts...)
		}

		if err_ == nil {
			if a.outputKey != "" {
				err_ = setOutputToSession(ctx, msg, msgStream, a.outputKey)
				if err_ != nil {
					ap.generator.Send(&TypedAgentEvent[*schema.AgenticMessage]{Err: err_})
				}
			} else if msgStream != nil {
				msgStream.Close()
			}

			return
		}

		a.handleRunFuncError(ctx, err_, cancelCtx, ap.cancelCtxOwned, ap.store, p.generator)
	}, nil
}

func (a *TypedChatModelAgent[M]) buildRunFunc(ctx context.Context) typedRunFunc[M] {
	a.once.Do(func() {
		ec, err := a.prepareExecContext(ctx)
		if err != nil {
			a.run = typedErrFunc[M](err)
			return
		}

		a.exeCtx = ec

		if len(ec.toolsNodeConf.Tools) == 0 {
			var run typedRunFunc[M]
			run, err = a.buildNoToolsRunFunc(ctx)
			if err != nil {
				a.run = typedErrFunc[M](err)
				return
			}
			a.run = run
			return
		}

		var run typedRunFunc[M]
		run, err = a.buildReActRunFunc(ctx, ec)
		if err != nil {
			a.run = typedErrFunc[M](err)
			return
		}
		a.run = run
	})

	atomic.StoreUint32(&a.frozen, 1)

	return a.run
}

func (a *TypedChatModelAgent[M]) getRunFunc(ctx context.Context) (context.Context, typedRunFunc[M], *execContext, error) {
	defaultRun := a.buildRunFunc(ctx)
	bc := a.exeCtx

	if bc == nil {
		return ctx, defaultRun, bc, nil
	}

	if len(a.handlers) == 0 {
		runtimeBC := &execContext{
			instruction:    bc.instruction,
			toolsNodeConf:  bc.toolsNodeConf,
			returnDirectly: bc.returnDirectly,
			toolInfos:      bc.toolInfos,
		}
		return ctx, defaultRun, runtimeBC, nil
	}

	ctx, runtimeBC, err := a.applyBeforeAgent(ctx, bc)
	if err != nil {
		return ctx, nil, nil, err
	}

	if !runtimeBC.rebuildGraph {
		return ctx, defaultRun, runtimeBC, nil
	}

	var tempRun typedRunFunc[M]
	if len(runtimeBC.toolsNodeConf.Tools) == 0 {
		tempRun, err = a.buildNoToolsRunFunc(ctx)
		if err != nil {
			return ctx, nil, nil, err
		}
	} else {
		tempRun, err = a.buildReActRunFunc(ctx, runtimeBC)
		if err != nil {
			return ctx, nil, nil, err
		}
	}

	return ctx, tempRun, runtimeBC, nil
}

func (a *TypedChatModelAgent[M]) Run(ctx context.Context, input *TypedAgentInput[M], opts ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	iterator, generator := NewAsyncIteratorPair[*TypedAgentEvent[M]]()

	o := getCommonOptions(nil, opts...)
	cancelCtx, cancelCtxOwned := resolveRunCancelContext(ctx, o)

	ctx, run, bc, err := a.getRunFunc(ctx)
	if err != nil {
		go func() {
			if cancelCtxOwned && cancelCtx != nil {
				defer cancelCtx.markDone()
			}
			generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("ChatModelAgent getRunFunc error: %w", err)})
			generator.Close()
		}()
		return iterator
	}

	co := getComposeOptions(opts)
	co = append(co, compose.WithCheckPointID(bridgeCheckpointID))
	runOps := GetImplSpecificOptions[chatModelAgentRunOptions](nil, opts...)

	if bc != nil {
		if len(bc.toolInfos) > 0 {
			co = append(co, compose.WithChatModelOption(model.WithTools(bc.toolInfos)))
		}
		if bc.toolSearchTool != nil {
			co = append(co, compose.WithChatModelOption(model.WithToolSearchTool(bc.toolSearchTool)))
		}
		if bc.toolUpdated {
			co = append(co, compose.WithToolsNodeOption(compose.WithToolList(bc.toolsNodeConf.Tools...)))
		}
	}

	go func() {
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				e := safe.NewPanicErr(panicErr, debug.Stack())
				generator.Send(&TypedAgentEvent[M]{Err: e})
			}

			generator.Close()
		}()

		var (
			instruction    string
			returnDirectly map[string]bool
		)

		if bc != nil {
			instruction = bc.instruction
			returnDirectly = bc.returnDirectly
		}

		run(ctx, &typedRunParams[M]{
			input:              input,
			generator:          generator,
			store:              newBridgeStore(),
			instruction:        instruction,
			returnDirectly:     returnDirectly,
			cancelCtx:          cancelCtx,
			cancelCtxOwned:     cancelCtxOwned,
			composeOpts:        co,
			afterToolCallsHook: runOps.afterToolCallsHook,
		})
	}()

	if cancelCtxOwned {
		return wrapIterWithCancelCtx(iterator, cancelCtx)
	}
	return iterator
}

func (a *TypedChatModelAgent[M]) Resume(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	iterator, generator := NewAsyncIteratorPair[*TypedAgentEvent[M]]()

	o := getCommonOptions(nil, opts...)
	cancelCtx, cancelCtxOwned := resolveRunCancelContext(ctx, o)

	ctx, run, bc, err := a.getRunFunc(ctx)
	if err != nil {
		go func() {
			if cancelCtxOwned && cancelCtx != nil {
				defer cancelCtx.markDone()
			}
			generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("ChatModelAgent getRunFunc error: %w", err)})
			generator.Close()
		}()
		return iterator
	}

	co := getComposeOptions(opts)
	co = append(co, compose.WithCheckPointID(bridgeCheckpointID))
	resumeRunOps := GetImplSpecificOptions[chatModelAgentRunOptions](nil, opts...)

	if bc != nil {
		if len(bc.toolInfos) > 0 {
			co = append(co, compose.WithChatModelOption(model.WithTools(bc.toolInfos)))
		}
		if bc.toolSearchTool != nil {
			co = append(co, compose.WithChatModelOption(model.WithToolSearchTool(bc.toolSearchTool)))
		}
		if bc.toolUpdated {
			co = append(co, compose.WithToolsNodeOption(compose.WithToolList(bc.toolsNodeConf.Tools...)))
		}
	}

	if info == nil {
		panic(fmt.Sprintf("ChatModelAgent.Resume: agent '%s' was asked to resume but info is nil", a.Name(ctx)))
	}

	if info.InterruptState == nil {
		panic(fmt.Sprintf("ChatModelAgent.Resume: agent '%s' was asked to resume but has no state", a.Name(ctx)))
	}

	stateByte, ok := info.InterruptState.([]byte)
	if !ok {
		panic(fmt.Sprintf("ChatModelAgent.Resume: agent '%s' was asked to resume but has invalid interrupt state type: %T",
			a.Name(ctx), info.InterruptState))
	}

	// Migrate legacy checkpoints before resume.
	// This covers both:
	// - v0.7.*: state is stored as a struct wire type (stateV07) under the legacy name.
	// - v0.8.0-v0.8.3: state is stored as a GobEncoder payload under the same legacy name and must
	//   be routed to a GobDecode-compatible compat type via byte-patching.
	// The result is re-encoded so the resume path always operates on the current *State.
	//
	// 恢复前迁移旧版检查点。
	// 这同时覆盖：
	// - v0.7.*：状态以结构体 wire 类型（stateV07）存储在旧名称下。
	// - v0.8.0-v0.8.3：状态以 GobEncoder payload 存储在同一旧名称下，必须通过字节补丁路由到兼容 GobDecode 的 compat 类型。
	// 结果会被重新编码，因此恢复路径始终基于当前的 *State 操作。
	stateByte, err = preprocessComposeCheckpoint(stateByte)
	if err != nil {
		go func() {
			generator.Send(&TypedAgentEvent[M]{Err: err})
			generator.Close()
		}()
		return iterator
	}

	var historyModifier func(ctx context.Context, history []Message) []Message
	if info.ResumeData != nil {
		resumeData, ok := info.ResumeData.(*ChatModelAgentResumeData)
		if !ok {
			panic(fmt.Sprintf("ChatModelAgent.Resume: agent '%s' was asked to resume but has invalid resume data type: %T",
				a.Name(ctx), info.ResumeData))
		}
		historyModifier = resumeData.HistoryModifier
	}

	if historyModifier != nil {
		co = append(co, compose.WithStateModifier(func(ctx context.Context, path compose.NodePath, state any) error {
			s, ok := state.(*State)
			if !ok {
				return nil
			}
			s.Messages = historyModifier(ctx, s.Messages)
			return nil
		}))
	}

	go func() {
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				e := safe.NewPanicErr(panicErr, debug.Stack())
				generator.Send(&TypedAgentEvent[M]{Err: e})
			}

			generator.Close()
		}()

		var (
			instruction    string
			returnDirectly map[string]bool
		)

		if bc != nil {
			instruction = bc.instruction
			returnDirectly = bc.returnDirectly
		}

		run(ctx, &typedRunParams[M]{
			input:              &TypedAgentInput[M]{EnableStreaming: info.EnableStreaming},
			generator:          generator,
			store:              newResumeBridgeStore(bridgeCheckpointID, stateByte),
			instruction:        instruction,
			returnDirectly:     returnDirectly,
			cancelCtx:          cancelCtx,
			cancelCtxOwned:     cancelCtxOwned,
			composeOpts:        co,
			afterToolCallsHook: resumeRunOps.afterToolCallsHook,
		})
	}()

	if cancelCtxOwned {
		return wrapIterWithCancelCtx(iterator, cancelCtx)
	}
	return iterator
}

func getComposeOptions(opts []AgentRunOption) []compose.Option {
	o := GetImplSpecificOptions[chatModelAgentRunOptions](nil, opts...)
	var co []compose.Option
	if len(o.chatModelOptions) > 0 {
		co = append(co, compose.WithChatModelOption(o.chatModelOptions...))
	}
	var to []tool.Option
	if len(o.toolOptions) > 0 {
		to = append(to, o.toolOptions...)
	}
	for toolName, atos := range o.agentToolOptions {
		to = append(to, withAgentToolOptions(toolName, atos))
	}
	if len(to) > 0 {
		co = append(co, compose.WithToolsNodeOption(compose.WithToolOption(to...)))
	}
	if o.historyModifier != nil {
		co = append(co, compose.WithStateModifier(func(ctx context.Context, path compose.NodePath, state any) error {
			s, ok := state.(*State)
			if !ok {
				return fmt.Errorf("unexpected state type: %T, expected: %T", state, &State{})
			}
			s.Messages = o.historyModifier(ctx, s.Messages)
			return nil
		}))
	}
	return co
}

type gobSerializer struct{}

func (g *gobSerializer) Marshal(v any) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *gobSerializer) Unmarshal(data []byte, v any) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(v)
}

// preprocessComposeCheckpoint migrates legacy compose checkpoints to the current format.
// It handles the v0.8.0-v0.8.3 format:
//   - gob name "_eino_adk_state_v080_" (already byte-patched by preprocessADKCheckpoint
//     from "_eino_adk_react_state"), opaque-bytes wire format → decoded as *stateV080
//
// v0.7 checkpoints need no migration — State is now a plain struct registered under the
// same gob name, and gob handles missing fields gracefully.
//
// Fast path: if the legacy name is not present, skip entirely.
//
// preprocessComposeCheckpoint 将旧版 compose 检查点迁移到当前格式。
// 它处理 v0.8.0-v0.8.3 格式：
// - gob 名称 "_eino_adk_state_v080_"（已由 preprocessADKCheckpoint 从 "_eino_adk_react_state" 字节补丁得到），opaque-bytes wire 格式 → 解码为 *stateV080
// v0.7 检查点无需迁移——State 现在是注册在同一 gob 名称下的普通结构体，gob 会优雅处理缺失字段。
// 快速路径：如果不存在旧名称，则完全跳过。
func preprocessComposeCheckpoint(data []byte) ([]byte, error) {
	const lenPrefixedCompatName = "\x15" + stateGobNameV080
	if bytes.Contains(data, []byte(lenPrefixedCompatName)) {
		// v0.8.0-v0.8.3: already byte-patched by preprocessADKCheckpoint; decode as *stateV080.
		// v0.8.0-v0.8.3：已由 preprocessADKCheckpoint 字节补丁；按 *stateV080 解码。
		migrated, err := compose.MigrateCheckpointState(data, &gobSerializer{}, func(state any) (any, bool, error) {
			sc, ok := state.(*stateV080)
			if !ok {
				return state, false, nil
			}
			return stateV080ToState(sc), true, nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to migrate v0.8.0-v0.8.3 compose checkpoint: %w", err)
		}
		return migrated, nil
	}

	return data, nil
}
