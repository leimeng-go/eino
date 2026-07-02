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
	"io"
	"reflect"
	"sync"

	"github.com/google/uuid"

	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/internal/generic"
	"github.com/cloudwego/eino/schema"
)

type typedGenerateEndpoint[M MessageType] func(ctx context.Context, input []M, opts ...model.Option) (M, error)
type typedStreamEndpoint[M MessageType] func(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error)

type typedModelWrapperConfig[M MessageType] struct {
	handlers       []TypedChatModelAgentMiddleware[M]
	middlewares    []AgentMiddleware
	retryConfig    *TypedModelRetryConfig[M]
	failoverConfig *ModelFailoverConfig[M]
	toolInfos      []*schema.ToolInfo
	cancelContext  *cancelContext
}

type modelWrapperConfig = typedModelWrapperConfig[*schema.Message]

func buildModelWrappers[M MessageType](m model.BaseModel[M], config *typedModelWrapperConfig[M]) model.BaseModel[M] {
	return buildModelWrappersImpl(m, config)
}

func buildModelWrappersImpl[M MessageType](m model.BaseModel[M], config *typedModelWrapperConfig[M]) model.BaseModel[M] {
	var wrapped = m

	if config.failoverConfig != nil {
		wrapped = &typedFailoverProxyModel[M]{}
	}

	if !components.IsCallbacksEnabled(wrapped) {
		wrapped = typedCallbackInjectionModelWrapper[M]{}.wrapModel(wrapped)
	}

	wrapped = &typedStateModelWrapper[M]{
		inner:               wrapped,
		original:            m,
		handlers:            config.handlers,
		middlewares:         config.middlewares,
		toolInfos:           config.toolInfos,
		modelRetryConfig:    config.retryConfig,
		modelFailoverConfig: config.failoverConfig,
		cancelContext:       config.cancelContext,
	}

	return wrapped
}

type typedCallbackInjectionModelWrapper[M MessageType] struct{}

func (w typedCallbackInjectionModelWrapper[M]) wrapModel(m model.BaseModel[M]) model.BaseModel[M] {
	return &typedCallbackInjectedModel[M]{inner: m}
}

type typedCallbackInjectedModel[M MessageType] struct {
	inner model.BaseModel[M]
}

func (m *typedCallbackInjectedModel[M]) Generate(ctx context.Context, input []M, opts ...model.Option) (M, error) {
	ctx = callbacks.OnStart(ctx, input)
	result, err := m.inner.Generate(ctx, input, opts...)
	if err != nil {
		callbacks.OnError(ctx, err)
		var zero M
		return zero, err
	}
	callbacks.OnEnd(ctx, result)
	return result, nil
}

func (m *typedCallbackInjectedModel[M]) Stream(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
	ctx = callbacks.OnStart(ctx, input)
	result, err := m.inner.Stream(ctx, input, opts...)
	if err != nil {
		callbacks.OnError(ctx, err)
		return nil, err
	}
	_, wrappedStream := callbacks.OnEndWithStreamOutput(ctx, result)
	return wrappedStream, nil
}

func handlersToToolMiddlewares[M MessageType](handlers []TypedChatModelAgentMiddleware[M]) []compose.ToolMiddleware {
	var middlewares []compose.ToolMiddleware
	// Forward iteration: compose.wrapToolCall applies middlewares in reverse order
	// (len-1 down to 0), so keeping the original handler order here means
	// handlers[0] ends up outermost — matching the model wrapping convention.
	//
	// 正向迭代：compose.wrapToolCall 会按相反顺序应用 middleware
	// （从 len-1 到 0），因此这里保持原始 handler 顺序意味着
	// handlers[0] 最终位于最外层——与模型包装约定一致。
	for _, handler := range handlers {

		m := compose.ToolMiddleware{}

		h := handler
		m.Invokable = func(next compose.InvokableToolEndpoint) compose.InvokableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.ToolOutput, error) {
				tCtx := &ToolContext{
					Name:   input.Name,
					CallID: input.CallID,
				}
				wrappedEndpoint, err := h.WrapInvokableToolCall(
					ctx,
					func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
						output, err := next(ctx, &compose.ToolInput{
							Name:        input.Name,
							CallID:      input.CallID,
							Arguments:   argumentsInJSON,
							CallOptions: opts,
						})
						if err != nil {
							return "", err
						}
						return output.Result, nil
					},
					tCtx,
				)
				if err != nil {
					return nil, err
				}
				result, err := wrappedEndpoint(ctx, input.Arguments, input.CallOptions...)
				if err != nil {
					return nil, err
				}
				return &compose.ToolOutput{Result: result}, nil
			}
		}

		m.Streamable = func(next compose.StreamableToolEndpoint) compose.StreamableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.StreamToolOutput, error) {
				tCtx := &ToolContext{
					Name:   input.Name,
					CallID: input.CallID,
				}
				wrappedEndpoint, err := h.WrapStreamableToolCall(
					ctx,
					func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*schema.StreamReader[string], error) {
						output, err := next(ctx, &compose.ToolInput{
							Name:        input.Name,
							CallID:      input.CallID,
							Arguments:   argumentsInJSON,
							CallOptions: opts,
						})
						if err != nil {
							return nil, err
						}
						return output.Result, nil
					},
					tCtx,
				)
				if err != nil {
					return nil, err
				}
				result, err := wrappedEndpoint(ctx, input.Arguments, input.CallOptions...)
				if err != nil {
					return nil, err
				}
				return &compose.StreamToolOutput{Result: result}, nil
			}
		}

		m.EnhancedInvokable = func(next compose.EnhancedInvokableToolEndpoint) compose.EnhancedInvokableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.EnhancedInvokableToolOutput, error) {
				tCtx := &ToolContext{
					Name:   input.Name,
					CallID: input.CallID,
				}
				wrappedEndpoint, err := h.WrapEnhancedInvokableToolCall(
					ctx,
					func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
						output, err := next(ctx, &compose.ToolInput{
							Name:        input.Name,
							CallID:      input.CallID,
							Arguments:   toolArgument.Text,
							CallOptions: opts,
						})
						if err != nil {
							return nil, err
						}
						return output.Result, nil
					},
					tCtx,
				)
				if err != nil {
					return nil, err
				}
				result, err := wrappedEndpoint(ctx, &schema.ToolArgument{Text: input.Arguments}, input.CallOptions...)
				if err != nil {
					return nil, err
				}
				return &compose.EnhancedInvokableToolOutput{Result: result}, nil
			}
		}

		m.EnhancedStreamable = func(next compose.EnhancedStreamableToolEndpoint) compose.EnhancedStreamableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.EnhancedStreamableToolOutput, error) {
				tCtx := &ToolContext{
					Name:   input.Name,
					CallID: input.CallID,
				}
				wrappedEndpoint, err := h.WrapEnhancedStreamableToolCall(
					ctx,
					func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error) {
						output, err := next(ctx, &compose.ToolInput{
							Name:        input.Name,
							CallID:      input.CallID,
							Arguments:   toolArgument.Text,
							CallOptions: opts,
						})
						if err != nil {
							return nil, err
						}
						return output.Result, nil
					},
					tCtx,
				)
				if err != nil {
					return nil, err
				}
				result, err := wrappedEndpoint(ctx, &schema.ToolArgument{Text: input.Arguments}, input.CallOptions...)
				if err != nil {
					return nil, err
				}
				return &compose.EnhancedStreamableToolOutput{Result: result}, nil
			}
		}

		middlewares = append(middlewares, m)
	}
	return middlewares
}

type typedEventSenderModelWrapper[M MessageType] struct {
	*TypedBaseChatModelAgentMiddleware[M]
}

// NewEventSenderModelWrapper creates a ChatModelAgentMiddleware that sends model output as agent events.
// NewEventSenderModelWrapper 创建一个 ChatModelAgentMiddleware，用于将模型输出作为智能体事件发送。
func NewEventSenderModelWrapper() ChatModelAgentMiddleware {
	return &typedEventSenderModelWrapper[*schema.Message]{
		TypedBaseChatModelAgentMiddleware: &TypedBaseChatModelAgentMiddleware[*schema.Message]{},
	}
}

func (w *typedEventSenderModelWrapper[M]) WrapModel(_ context.Context, m model.BaseModel[M], mc *TypedModelContext[M]) (model.BaseModel[M], error) {
	inner := m
	if mc != nil && mc.cancelContext != nil {
		inner = &typedCancelMonitoredModel[M]{
			inner:         inner,
			cancelContext: mc.cancelContext,
		}
	}
	var retryConfig *TypedModelRetryConfig[M]
	if mc != nil {
		retryConfig = mc.ModelRetryConfig
	}
	var failoverConfig *ModelFailoverConfig[M]
	if mc != nil {
		failoverConfig = mc.ModelFailoverConfig
	}
	return &typedEventSenderModel[M]{inner: inner, modelRetryConfig: retryConfig, modelFailoverConfig: failoverConfig}, nil
}

type typedEventSenderModel[M MessageType] struct {
	inner               model.BaseModel[M]
	modelRetryConfig    *TypedModelRetryConfig[M]
	modelFailoverConfig *ModelFailoverConfig[M]
}

func (m *typedEventSenderModel[M]) Generate(ctx context.Context, input []M, opts ...model.Option) (M, error) {
	result, err := m.inner.Generate(ctx, input, opts...)
	if err != nil {
		var zero M
		return zero, err
	}

	execCtx := getTypedChatModelAgentExecCtx[M](ctx)
	if execCtx != nil && execCtx.suppressEventSend {
		return result, nil
	}
	if execCtx == nil || execCtx.generator == nil {
		var zero M
		return zero, errors.New("generator is nil when sending event in Generate: ensure agent state is properly initialized")
	}

	event := typedModelOutputEvent(copyMessage(result), nil)
	execCtx.send(event)

	return result, nil
}

func (m *typedEventSenderModel[M]) Stream(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
	result, err := m.inner.Stream(ctx, input, opts...)
	if err != nil {
		return nil, err
	}

	execCtx := getTypedChatModelAgentExecCtx[M](ctx)
	if execCtx == nil || execCtx.generator == nil {
		result.Close()
		return nil, errors.New("generator is nil when sending event in Stream: ensure agent state is properly initialized")
	}

	streams := result.Copy(2)

	eventStream := streams[0]
	if convertOpts := m.buildStreamConvertOptions(ctx); len(convertOpts) > 0 {
		eventStream = schema.StreamReaderWithConvert(streams[0],
			func(msg M) (M, error) { return msg, nil },
			convertOpts...)
	}

	var zero M
	event := typedModelOutputEvent[M](zero, eventStream)
	execCtx.send(event)

	return streams[1], nil
}

// buildStreamConvertOptions constructs ConvertOption hooks that gate stream termination behind
// the retry verdict signal protocol.
//
// Verdict signal lifecycle:
//   - streamWithShouldRetry creates a new retryVerdictSignal per retry attempt, stores it in
//     execCtx.retryVerdictSignal, and sends exactly one retryVerdict after ShouldRetry decides.
//   - The closures below capture a *retryVerdictSignal that is nil at closure-creation time; they
//     read the live value from execCtx.retryVerdictSignal, which is set before each model call.
//
// Two hooks cooperate to cover all stream termination paths:
//   - WithErrWrapper intercepts mid-stream errors. It blocks on the verdict to decide
//     whether to wrap the error as WillRetryError (rejected attempt) or pass it through (accepted).
//   - WithOnEOF intercepts clean EOF (successful stream). It blocks on the verdict to
//     either inject a WillRetryError (rejected) or pass through io.EOF (accepted).
//
// Both hooks share a sync.Once-guarded reader so the verdict channel is read at most once.
// This prevents a goroutine leak when a mid-stream error is followed by EOF: errWrapper fires
// first (caching the verdict), and onEOF reuses the cached value instead of blocking on a
// drained channel.
//
// buildStreamConvertOptions 构造 ConvertOption hook，用于通过
// 重试判定信号协议控制流终止。
// 判定信号生命周期：
// - streamWithShouldRetry 为每次重试尝试创建新的 retryVerdictSignal，将其存入
// execCtx.retryVerdictSignal，并在 ShouldRetry 决定后只发送一个 retryVerdict。
// - 下面的闭包捕获的是创建闭包时为 nil 的 *retryVerdictSignal；它们会
// 从 execCtx.retryVerdictSignal 读取实时值，该值会在每次模型调用前设置。
// 两个 hook 协同覆盖所有流终止路径：
// - WithErrWrapper 拦截流中错误。它会阻塞等待判定，以决定
// 将错误包装为 WillRetryError（被拒绝的尝试）还是原样传递（被接受）。
// - WithOnEOF 拦截正常 EOF（成功的流）。它会阻塞等待判定，以
// 注入 WillRetryError（被拒绝）或传递 io.EOF（被接受）。
// 两个 hook 共享一个由 sync.Once 保护的读取器，因此判定 channel 最多只读取一次。
// 这可避免流中错误后跟 EOF 时发生 goroutine 泄漏：errWrapper 会
// 先触发（缓存判定），onEOF 复用缓存值，而不是在
// 已耗尽的 channel 上阻塞。
func (m *typedEventSenderModel[M]) buildStreamConvertOptions(ctx context.Context) []schema.ConvertOption {
	var retryAttempt int
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		retryAttempt = st.getRetryAttempt()
		return nil
	})

	wrapWithCancelGuard := func(inner func(error) error) func(error) error {
		return func(err error) error {
			if errors.Is(err, ErrStreamCanceled) {
				return err
			}
			return inner(err)
		}
	}

	var opts []schema.ConvertOption

	var retryWrapper func(error) error
	if m.modelRetryConfig != nil {
		if m.modelRetryConfig.ShouldRetry != nil {
			execCtx := getTypedChatModelAgentExecCtx[M](ctx)
			signal := (*retryVerdictSignal)(nil)
			if execCtx != nil {
				signal = execCtx.retryVerdictSignal
			}
			if signal != nil {
				var (
					verdictOnce   sync.Once
					cachedVerdict retryVerdict
				)
				readVerdict := func() retryVerdict {
					verdictOnce.Do(func() {
						cachedVerdict = <-signal.ch
					})
					return cachedVerdict
				}

				retryWrapper = wrapWithCancelGuard(func(err error) error {
					verdict := readVerdict()
					if verdict.WillRetry {
						return &WillRetryError{
							ErrStr:       err.Error(),
							RetryAttempt: verdict.RetryAttempt,
							rejectReason: verdict.RejectReason,
							err:          err,
						}
					}
					return err
				})

				opts = append(opts, schema.WithOnEOF(func() (any, error) {
					verdict := readVerdict()
					if verdict.WillRetry {
						return nil, &WillRetryError{
							ErrStr:       verdict.Err.Error(),
							RetryAttempt: verdict.RetryAttempt,
							rejectReason: verdict.RejectReason,
							err:          verdict.Err,
						}
					}
					return nil, io.EOF
				}))
			}
		} else {
			retryWrapper = wrapWithCancelGuard(
				genErrWrapper(ctx, m.modelRetryConfig.MaxRetries, retryAttempt, m.modelRetryConfig.IsRetryAble),
			)
		}
	}

	hasFailover := m.modelFailoverConfig != nil
	// failoverHasMoreAttempts is set by failoverModelWrapper before each inner call.
	// It is true when additional failover attempts remain after the current one,
	// meaning stream errors should be wrapped as WillRetryError so the flow layer
	// skips them. On the final attempt it is false, so the error propagates normally.
	//
	// failoverHasMoreAttempts 由 failoverModelWrapper 在每次内部调用前设置。
	// 若当前尝试之后仍有额外 failover 尝试，则为 true，
	// 表示流错误应包装为 WillRetryError，以便 flow 层
	// 跳过它们。最后一次尝试时为 false，错误会正常传播。
	failoverHasMore := getFailoverHasMoreAttempts(ctx)

	if retryWrapper == nil && !(hasFailover && failoverHasMore) {
		return opts
	}

	combinedErrWrapper := func(err error) error {
		// If retry is configured and will retry this error, use the retry wrapper's WillRetryError.
		// 如果已配置 retry 且会重试此错误，则使用 retry wrapper 的 WillRetryError。
		if retryWrapper != nil {
			wrapped := retryWrapper(err)
			if errors.As(wrapped, new(*WillRetryError)) {
				return wrapped
			}
		}
		// Retry won't handle this error (either exhausted or not configured), but
		// failover still has more attempts remaining. Wrap it as WillRetryError so
		// the flow layer skips this event from the failed attempt.
		//
		// retry 不会处理此错误（已耗尽或未配置），但
		// failover 仍有更多尝试。将其包装为 WillRetryError，以便
		// flow 层跳过失败尝试产生的此事件。
		if hasFailover && failoverHasMore {
			if errors.Is(err, ErrStreamCanceled) {
				return err
			}
			return &WillRetryError{ErrStr: err.Error(), err: err}
		}
		return err
	}
	opts = append(opts, schema.WithErrWrapper(combinedErrWrapper))

	return opts
}

func copyMessage[M MessageType](msg M) M {
	switch v := any(msg).(type) {
	case *schema.Message:
		cp := *v
		return any(&cp).(M)
	case *schema.AgenticMessage:
		cp := *v
		return any(&cp).(M)
	default:
		return msg
	}
}

// typedSetMessageID sets a specific message ID in Extra.
// typedSetMessageID 在 Extra 中设置指定的消息 ID。
func typedSetMessageID[M MessageType](msg M, id string) {
	switch v := any(msg).(type) {
	case *schema.Message:
		v.Extra = internal.SetMessageID(v.Extra, id)
	case *schema.AgenticMessage:
		v.Extra = internal.SetMessageID(v.Extra, id)
	}
}

// GetMessageID returns the eino-internal message ID from the given message, or "".
// GetMessageID 返回给定消息中的 eino 内部消息 ID，若无则返回 ""。
func GetMessageID[M MessageType](msg M) string {
	switch v := any(msg).(type) {
	case *schema.Message:
		return internal.GetMessageID(v.Extra)
	case *schema.AgenticMessage:
		return internal.GetMessageID(v.Extra)
	default:
		return ""
	}
}

// EnsureMessageID assigns a UUID v4 message ID if the message doesn't have one.
// Idempotent: if ID already set, no-op.
// Middleware authors should call this before SendEvent if they create messages.
//
// EnsureMessageID 在消息没有 ID 时分配 UUID v4 消息 ID。
// 幂等：如果已设置 ID，则不执行任何操作。
// Middleware 作者如果创建消息，应在 SendEvent 前调用它。
func EnsureMessageID[M MessageType](msg M) {
	switch v := any(msg).(type) {
	case *schema.Message:
		v.Extra = internal.EnsureMessageID(v.Extra)
	case *schema.AgenticMessage:
		v.Extra = internal.EnsureMessageID(v.Extra)
	}
}

func typedPopToolGenAction[M MessageType](ctx context.Context, toolName string) *AgentAction {
	toolCallID := compose.GetToolCallID(ctx)

	var action *AgentAction
	_ = compose.ProcessState(ctx, func(ctx context.Context, st *typedState[M]) error {
		if len(toolCallID) > 0 {
			if a := st.popToolGenAction(toolCallID); a != nil {
				action = a
				return nil
			}
		}

		if a := st.popToolGenAction(toolName); a != nil {
			action = a
		}

		return nil
	})

	return action
}

type typedEventSenderToolWrapper[M MessageType] struct {
	*TypedBaseChatModelAgentMiddleware[M]
}

func (*typedEventSenderToolWrapper[M]) isEventSenderToolWrapper() {}

// eventSenderToolWrapperMarker enables cross-type detection of eventSenderToolWrapper
// in generic contexts. hasUserEventSenderToolWrapper[M] receives
// []TypedChatModelAgentMiddleware[M], so when M is *schema.AgenticMessage, a direct
// type assertion to *eventSenderToolWrapper (which implements the *schema.Message alias)
// would fail. The marker interface bridges this gap.
//
// eventSenderToolWrapperMarker 支持在泛型上下文中跨类型检测 eventSenderToolWrapper。
// hasUserEventSenderToolWrapper[M] 接收
// []TypedChatModelAgentMiddleware[M]，因此当 M 为 *schema.AgenticMessage 时，直接
// 类型断言为 *eventSenderToolWrapper（它实现的是 *schema.Message 别名）
// 会失败。该 marker interface 用于弥合此差异。
type eventSenderToolWrapperMarker interface{ isEventSenderToolWrapper() }

// NewEventSenderToolWrapper returns a ChatModelAgentMiddleware that sends tool result events.
// By default, the framework places this before all user middlewares (outermost), so events
// reflect the fully processed tool output. To control exactly where events are emitted,
// include this in ChatModelAgentConfig.Handlers at the desired position.
// When detected in Handlers, the framework skips the default event sender to avoid duplicates.
//
// NewEventSenderToolWrapper 返回一个 ChatModelAgentMiddleware，用于发送工具结果事件。
// 默认情况下，框架会将其放在所有用户 middleware 之前（最外层），因此事件
// 反映完全处理后的工具输出。若要精确控制事件发出位置，
// 请将其放入 ChatModelAgentConfig.Handlers 的目标位置。
// 在 Handlers 中检测到它时，框架会跳过默认事件发送器以避免重复。
func NewEventSenderToolWrapper() ChatModelAgentMiddleware {
	return newTypedEventSenderToolWrapper[*schema.Message]()
}

// newTypedEventSenderToolWrapper creates a typed event sender wrapper for the given message type.
// This is used internally to ensure the default event sender matches the agent's message type
// (e.g. *schema.AgenticMessage agents need an AgenticMessage-typed wrapper so that
// compose.ProcessState can access the correct state type).
//
// newTypedEventSenderToolWrapper 为给定消息类型创建一个带类型的事件发送 wrapper。
// 内部使用它来确保默认事件发送器匹配智能体的消息类型
// （例如 *schema.AgenticMessage 智能体需要 AgenticMessage 类型的 wrapper，以便
// compose.ProcessState 能访问正确的状态类型）。
func newTypedEventSenderToolWrapper[M MessageType]() *typedEventSenderToolWrapper[M] {
	return &typedEventSenderToolWrapper[M]{
		TypedBaseChatModelAgentMiddleware: &TypedBaseChatModelAgentMiddleware[M]{},
	}
}

// textToFunctionToolResultBlocks wraps a plain text string into FunctionToolResultBlocks.
// textToFunctionToolResultBlocks 将纯文本字符串包装为 FunctionToolResultBlocks。
func textToFunctionToolResultBlocks(text string) []*schema.FunctionToolResultContentBlock {
	if text == "" {
		return nil
	}
	return []*schema.FunctionToolResultContentBlock{
		{Type: schema.FunctionToolResultContentBlockTypeText, Text: &schema.UserInputText{Text: text}},
	}
}

// functionToolResultAgenticMessage constructs a function tool result message with AgenticRoleType "user".
// functionToolResultAgenticMessage 构造 AgenticRoleType 为 "user" 的函数工具结果消息。
func functionToolResultAgenticMessage(callID, name string, content []*schema.FunctionToolResultContentBlock) *schema.AgenticMessage {
	return &schema.AgenticMessage{
		Role: schema.AgenticRoleTypeUser,
		ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.FunctionToolResult{
				CallID:  callID,
				Name:    name,
				Content: content,
			}),
		},
	}
}

func markAgenticMessageStreamingMeta(msg *schema.AgenticMessage, index int) {
	if msg == nil {
		return
	}
	for _, block := range msg.ContentBlocks {
		if block == nil {
			continue
		}
		block.StreamingMeta = &schema.StreamingMeta{Index: index}
	}
}

func toolSearchResultAgenticMessage(callID, name string, tr *schema.ToolResult) (*schema.AgenticMessage, bool) {
	if tr == nil || len(tr.Parts) != 1 {
		return nil, false
	}

	part := tr.Parts[0]
	if part.Type != schema.ToolPartTypeToolSearchResult || part.ToolSearchResult == nil {
		return nil, false
	}

	return &schema.AgenticMessage{
		Role: schema.AgenticRoleTypeUser,
		ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.ToolSearchFunctionToolResult{
				CallID: callID,
				Name:   name,
				Result: part.ToolSearchResult,
			}),
		},
	}, true
}

func toolResultAgenticMessage(callID, name string, tr *schema.ToolResult) *schema.AgenticMessage {
	if msg, ok := toolSearchResultAgenticMessage(callID, name, tr); ok {
		return msg
	}
	return functionToolResultAgenticMessage(callID, name, toolResultToBlocks(tr))
}

// toolResultToBlocks converts a ToolResult's multimodal parts into FunctionToolResultBlocks.
// This preserves all media types (text, image, audio, video, file), unlike toolResultText
// which only extracts text.
//
// toolResultToBlocks 将 ToolResult 的多模态部分转换为 FunctionToolResultBlocks。
// 这会保留所有媒体类型（text、image、audio、video、file），不同于只提取文本的 toolResultText。
func toolResultToBlocks(tr *schema.ToolResult) []*schema.FunctionToolResultContentBlock {
	if tr == nil || len(tr.Parts) == 0 {
		return nil
	}
	blocks := make([]*schema.FunctionToolResultContentBlock, 0, len(tr.Parts))
	for _, p := range tr.Parts {
		var block *schema.FunctionToolResultContentBlock
		switch p.Type {
		case schema.ToolPartTypeText:
			block = &schema.FunctionToolResultContentBlock{
				Type:  schema.FunctionToolResultContentBlockTypeText,
				Text:  &schema.UserInputText{Text: p.Text},
				Extra: p.Extra,
			}
		case schema.ToolPartTypeImage:
			if p.Image != nil {
				block = &schema.FunctionToolResultContentBlock{
					Type: schema.FunctionToolResultContentBlockTypeImage,
					Image: &schema.UserInputImage{
						URL:        derefString(p.Image.URL),
						Base64Data: derefString(p.Image.Base64Data),
						MIMEType:   p.Image.MIMEType,
					},
					Extra: p.Extra,
				}
			}
		case schema.ToolPartTypeAudio:
			if p.Audio != nil {
				block = &schema.FunctionToolResultContentBlock{
					Type: schema.FunctionToolResultContentBlockTypeAudio,
					Audio: &schema.UserInputAudio{
						URL:        derefString(p.Audio.URL),
						Base64Data: derefString(p.Audio.Base64Data),
						MIMEType:   p.Audio.MIMEType,
					},
					Extra: p.Extra,
				}
			}
		case schema.ToolPartTypeVideo:
			if p.Video != nil {
				block = &schema.FunctionToolResultContentBlock{
					Type: schema.FunctionToolResultContentBlockTypeVideo,
					Video: &schema.UserInputVideo{
						URL:        derefString(p.Video.URL),
						Base64Data: derefString(p.Video.Base64Data),
						MIMEType:   p.Video.MIMEType,
					},
					Extra: p.Extra,
				}
			}
		case schema.ToolPartTypeFile:
			if p.File != nil {
				block = &schema.FunctionToolResultContentBlock{
					Type: schema.FunctionToolResultContentBlockTypeFile,
					File: &schema.UserInputFile{
						URL:        derefString(p.File.URL),
						Base64Data: derefString(p.File.Base64Data),
						MIMEType:   p.File.MIMEType,
					},
					Extra: p.Extra,
				}
			}
		}
		if block != nil {
			blocks = append(blocks, block)
		}
	}
	return blocks
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// typedToolInvokeEvent constructs the tool result event for the invoke path,
// dispatching on M to create the correct message and event types.
//
// typedToolInvokeEvent 为 invoke 路径构造工具结果事件，
// 根据 M 分派以创建正确的消息和事件类型。
func typedToolInvokeEvent[M MessageType](callID, toolName, result, toolMsgID string) *TypedAgentEvent[M] {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		msg := schema.ToolMessage(result, callID, schema.WithToolName(toolName))
		msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
		event := EventFromMessage(msg, nil, schema.Tool, toolName)
		return any(event).(*TypedAgentEvent[M])
	case *schema.AgenticMessage:
		msg := functionToolResultAgenticMessage(callID, toolName, textToFunctionToolResultBlocks(result))
		msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
		event := EventFromAgenticMessage(msg, nil, schema.AgenticRoleTypeUser)
		return any(event).(*TypedAgentEvent[M])
	default:
		return nil
	}
}

// typedToolStreamEvent constructs the tool result event for the stream path,
// dispatching on M to create the correct message stream and event types.
//
// typedToolStreamEvent 为 stream 路径构造工具结果事件，
// 根据 M 分派以创建正确的消息流和事件类型。
func typedToolStreamEvent[M MessageType](callID, toolName, toolMsgID string, stream *schema.StreamReader[string]) *TypedAgentEvent[M] {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		first := true
		cvt := func(in string) (Message, error) {
			msg := schema.ToolMessage(in, callID, schema.WithToolName(toolName))
			if first {
				first = false
				msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
			}
			return msg, nil
		}
		msgStream := schema.StreamReaderWithConvert(stream, cvt)
		event := EventFromMessage(nil, msgStream, schema.Tool, toolName)
		return any(event).(*TypedAgentEvent[M])
	case *schema.AgenticMessage:
		first := true
		cvt := func(in string) (*schema.AgenticMessage, error) {
			msg := functionToolResultAgenticMessage(callID, toolName, textToFunctionToolResultBlocks(in))
			markAgenticMessageStreamingMeta(msg, 0)
			if first {
				first = false
				msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
			}
			return msg, nil
		}
		msgStream := schema.StreamReaderWithConvert(stream, cvt)
		event := EventFromAgenticMessage(nil, msgStream, schema.AgenticRoleTypeUser)
		return any(event).(*TypedAgentEvent[M])
	default:
		return nil
	}
}

// typedToolEnhancedInvokeEvent constructs the tool result event for the enhanced invoke path.
// For *schema.Message it builds a multimodal tool message; for *schema.AgenticMessage it
// uses the string content of the result (AgenticToolsNode only uses the string path).
//
// typedToolEnhancedInvokeEvent 为 enhanced invoke 路径构造工具结果事件。
// 对于 *schema.Message，它构建多模态工具消息；对于 *schema.AgenticMessage，
// 它使用结果的字符串内容（AgenticToolsNode 只使用字符串路径）。
func typedToolEnhancedInvokeEvent[M MessageType](callID, toolName, toolMsgID string, result *schema.ToolResult) (*TypedAgentEvent[M], error) {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		msg := schema.ToolMessage("", callID, schema.WithToolName(toolName))
		var err error
		msg.UserInputMultiContent, err = result.ToMessageInputParts()
		if err != nil {
			return nil, err
		}
		msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
		event := EventFromMessage(msg, nil, schema.Tool, toolName)
		return any(event).(*TypedAgentEvent[M]), nil
	case *schema.AgenticMessage:
		msg := toolResultAgenticMessage(callID, toolName, result)
		msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
		event := EventFromAgenticMessage(msg, nil, schema.AgenticRoleTypeUser)
		return any(event).(*TypedAgentEvent[M]), nil
	default:
		return nil, nil
	}
}

// typedToolEnhancedStreamEvent constructs the tool result event for the enhanced stream path.
// For *schema.Message it builds multimodal tool messages; for *schema.AgenticMessage it
// converts each chunk's multimodal parts into FunctionToolResultBlocks.
//
// typedToolEnhancedStreamEvent 为 enhanced stream 路径构造工具结果事件。
// 对于 *schema.Message，它构建多模态工具消息；对于 *schema.AgenticMessage，
// 它将每个 chunk 的多模态部分转换为 FunctionToolResultBlocks。
func typedToolEnhancedStreamEvent[M MessageType](callID, toolName, toolMsgID string, stream *schema.StreamReader[*schema.ToolResult]) *TypedAgentEvent[M] {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		first := true
		cvt := func(in *schema.ToolResult) (Message, error) {
			msg := schema.ToolMessage("", callID, schema.WithToolName(toolName))
			var cvtErr error
			msg.UserInputMultiContent, cvtErr = in.ToMessageInputParts()
			if cvtErr != nil {
				return nil, cvtErr
			}
			if first {
				first = false
				msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
			}
			return msg, nil
		}
		msgStream := schema.StreamReaderWithConvert(stream, cvt)
		event := EventFromMessage(nil, msgStream, schema.Tool, toolName)
		return any(event).(*TypedAgentEvent[M])
	case *schema.AgenticMessage:
		first := true
		cvt := func(in *schema.ToolResult) (*schema.AgenticMessage, error) {
			msg := toolResultAgenticMessage(callID, toolName, in)
			markAgenticMessageStreamingMeta(msg, 0)
			if first {
				first = false
				msg.Extra = internal.SetMessageID(msg.Extra, toolMsgID)
			}
			return msg, nil
		}
		msgStream := schema.StreamReaderWithConvert(stream, cvt)
		event := EventFromAgenticMessage(nil, msgStream, schema.AgenticRoleTypeUser)
		return any(event).(*TypedAgentEvent[M])
	default:
		return nil
	}
}

func (w *typedEventSenderToolWrapper[M]) WrapInvokableToolCall(_ context.Context, endpoint InvokableToolCallEndpoint, tCtx *ToolContext) (InvokableToolCallEndpoint, error) {
	return func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
		result, err := endpoint(ctx, argumentsInJSON, opts...)
		if err != nil {
			return "", err
		}

		toolName := tCtx.Name
		callID := tCtx.CallID

		prePopAction := typedPopToolGenAction[M](ctx, toolName)
		toolMsgID := uuid.NewString()
		event := typedToolInvokeEvent[M](callID, toolName, result, toolMsgID)
		if prePopAction != nil {
			event.Action = prePopAction
		}

		execCtx := getTypedChatModelAgentExecCtx[M](ctx)
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setToolMsgID(toolName, callID, toolMsgID)
			if st.getReturnDirectlyToolCallID() == callID {
				st.setReturnDirectlyEvent(event)
			} else {
				execCtx.send(event)
			}
			return nil
		})

		return result, nil
	}, nil
}

func (w *typedEventSenderToolWrapper[M]) WrapStreamableToolCall(_ context.Context, endpoint StreamableToolCallEndpoint, tCtx *ToolContext) (StreamableToolCallEndpoint, error) {
	return func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		result, err := endpoint(ctx, argumentsInJSON, opts...)
		if err != nil {
			return nil, err
		}

		toolName := tCtx.Name
		callID := tCtx.CallID

		prePopAction := typedPopToolGenAction[M](ctx, toolName)
		streams := result.Copy(2)

		toolMsgID := uuid.NewString()
		event := typedToolStreamEvent[M](callID, toolName, toolMsgID, streams[0])
		event.Action = prePopAction

		execCtx := getTypedChatModelAgentExecCtx[M](ctx)
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setToolMsgID(toolName, callID, toolMsgID)
			if st.getReturnDirectlyToolCallID() == callID {
				st.setReturnDirectlyEvent(event)
			} else {
				execCtx.send(event)
			}
			return nil
		})

		return streams[1], nil
	}, nil
}

func (w *typedEventSenderToolWrapper[M]) WrapEnhancedInvokableToolCall(_ context.Context, endpoint EnhancedInvokableToolCallEndpoint, tCtx *ToolContext) (EnhancedInvokableToolCallEndpoint, error) {
	return func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
		result, err := endpoint(ctx, toolArgument, opts...)
		if err != nil {
			return nil, err
		}

		toolName := tCtx.Name
		callID := tCtx.CallID

		prePopAction := typedPopToolGenAction[M](ctx, toolName)
		toolMsgID := uuid.NewString()
		event, eventErr := typedToolEnhancedInvokeEvent[M](callID, toolName, toolMsgID, result)
		if eventErr != nil {
			return nil, eventErr
		}
		if prePopAction != nil {
			event.Action = prePopAction
		}

		execCtx := getTypedChatModelAgentExecCtx[M](ctx)
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setToolMsgID(toolName, callID, toolMsgID)
			if st.getReturnDirectlyToolCallID() == callID {
				st.setReturnDirectlyEvent(event)
			} else {
				execCtx.send(event)
			}
			return nil
		})

		return result, nil
	}, nil
}

func (w *typedEventSenderToolWrapper[M]) WrapEnhancedStreamableToolCall(_ context.Context, endpoint EnhancedStreamableToolCallEndpoint, tCtx *ToolContext) (EnhancedStreamableToolCallEndpoint, error) {
	return func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error) {
		result, err := endpoint(ctx, toolArgument, opts...)
		if err != nil {
			return nil, err
		}

		toolName := tCtx.Name
		callID := tCtx.CallID

		prePopAction := typedPopToolGenAction[M](ctx, toolName)
		streams := result.Copy(2)

		toolMsgID := uuid.NewString()
		event := typedToolEnhancedStreamEvent[M](callID, toolName, toolMsgID, streams[0])
		event.Action = prePopAction

		execCtx := getTypedChatModelAgentExecCtx[M](ctx)
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setToolMsgID(toolName, callID, toolMsgID)
			if st.getReturnDirectlyToolCallID() == callID {
				st.setReturnDirectlyEvent(event)
			} else {
				execCtx.send(event)
			}
			return nil
		})

		return streams[1], nil
	}, nil
}

func hasUserEventSenderToolWrapper[M MessageType](handlers []TypedChatModelAgentMiddleware[M]) bool {
	for _, handler := range handlers {
		if _, ok := any(handler).(eventSenderToolWrapperMarker); ok {
			return true
		}
	}
	return false
}

type typedStateModelWrapper[M MessageType] struct {
	inner               model.BaseModel[M]
	original            model.BaseModel[M]
	handlers            []TypedChatModelAgentMiddleware[M]
	middlewares         []AgentMiddleware
	toolInfos           []*schema.ToolInfo
	modelRetryConfig    *TypedModelRetryConfig[M]
	modelFailoverConfig *ModelFailoverConfig[M]
	cancelContext       *cancelContext
}

type stateModelWrapper = typedStateModelWrapper[*schema.Message]

func (w *typedStateModelWrapper[M]) IsCallbacksEnabled() bool {
	return true
}

func (w *typedStateModelWrapper[M]) GetType() string {
	if typer, ok := any(w.original).(components.Typer); ok {
		return typer.GetType()
	}
	return generic.ParseTypeName(reflect.ValueOf(w.original))
}

func (w *typedStateModelWrapper[M]) hasUserEventSender() bool {
	for _, handler := range w.handlers {
		if _, ok := any(handler).(*typedEventSenderModelWrapper[M]); ok {
			return true
		}
	}
	return false
}

func (w *typedStateModelWrapper[M]) wrapGenerateEndpoint(endpoint typedGenerateEndpoint[M]) typedGenerateEndpoint[M] {
	// === ID Assignment layer (innermost, framework-controlled) ===
	// Ensures model output has a message ID before any WrapModel handler or event sender sees it.
	// Copies the result to avoid mutating a potentially shared pointer returned by the model.
	//
	// === ID 分配层（最内层，由框架控制）===
	// 确保在任何 WrapModel 处理器或事件发送器看到模型输出前，它已有消息 ID。
	// 复制结果，避免修改模型返回的、可能被共享的指针。
	{
		realInner := endpoint
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (M, error) {
			result, err := realInner(ctx, input, opts...)
			if err != nil {
				return result, err
			}
			if GetMessageID(result) == "" {
				result = copyMessage(result)
				EnsureMessageID(result)
			}
			return result, nil
		}
	}

	hasUserEventSender := w.hasUserEventSender()
	retryConfig := w.modelRetryConfig
	failoverConfig := w.modelFailoverConfig
	cc := w.cancelContext

	for i := len(w.handlers) - 1; i >= 0; i-- {
		handler := w.handlers[i]
		innerEndpoint := endpoint
		baseToolInfos := w.toolInfos
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (M, error) {
			baseOpts := &model.Options{Tools: baseToolInfos}
			commonOpts := model.GetCommonOptions(baseOpts, opts...)
			mc := &TypedModelContext[M]{Tools: commonOpts.Tools, ModelRetryConfig: retryConfig, cancelContext: cc}
			wrappedModel, err := handler.WrapModel(ctx, &typedEndpointModel[M]{generate: innerEndpoint}, mc)
			if err != nil {
				var zero M
				return zero, err
			}
			return wrappedModel.Generate(ctx, input, opts...)
		}
	}

	if !hasUserEventSender {
		innerEndpoint := endpoint
		eventSender := &typedEventSenderModelWrapper[M]{
			TypedBaseChatModelAgentMiddleware: &TypedBaseChatModelAgentMiddleware[M]{},
		}
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (M, error) {
			execCtx := getTypedChatModelAgentExecCtx[M](ctx)
			if execCtx == nil || execCtx.generator == nil {
				return innerEndpoint(ctx, input, opts...)
			}
			mc := &TypedModelContext[M]{ModelRetryConfig: retryConfig, ModelFailoverConfig: failoverConfig, cancelContext: cc}
			wrappedModel, err := eventSender.WrapModel(ctx, &typedEndpointModel[M]{generate: innerEndpoint}, mc)
			if err != nil {
				var zero M
				return zero, err
			}
			return wrappedModel.Generate(ctx, input, opts...)
		}
	}

	if w.modelRetryConfig != nil {
		innerEndpoint := endpoint
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (M, error) {
			retryWrapper := newTypedRetryModelWrapper[M](&typedEndpointModel[M]{generate: innerEndpoint}, w.modelRetryConfig)
			return retryWrapper.Generate(ctx, input, opts...)
		}
	}

	if w.modelFailoverConfig != nil {
		config := w.modelFailoverConfig
		innerEndpoint := endpoint
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (M, error) {
			failoverWrapper := newFailoverModelWrapper[M](&typedEndpointModel[M]{generate: innerEndpoint}, config)
			return failoverWrapper.Generate(ctx, input, opts...)
		}
	}

	return endpoint
}

func (w *typedStateModelWrapper[M]) wrapStreamEndpoint(endpoint typedStreamEndpoint[M]) typedStreamEndpoint[M] {
	// === ID Assignment layer (innermost, framework-controlled) ===
	// Pre-allocates a UUID and injects it into the first chunk only.
	// Only the first chunk carries the ID in Extra to avoid concatStrings corruption
	// during ConcatMessages (which string-concatenates duplicate Extra keys).
	//
	// === ID 分配层（最内层，由框架控制）===
	// 预先分配 UUID，并仅注入到第一个 chunk。
	// 只有第一个 chunk 在 Extra 中携带 ID，以避免 ConcatMessages 期间出现 concatStrings 损坏
	// （它会对重复的 Extra key 做字符串拼接）。
	{
		realInner := endpoint
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
			reader, err := realInner(ctx, input, opts...)
			if err != nil {
				return nil, err
			}
			msgID := uuid.NewString()
			first := true
			return schema.StreamReaderWithConvert(reader, func(msg M) (M, error) {
				if first {
					first = false
					if GetMessageID(msg) == "" {
						typedSetMessageID(msg, msgID)
					}
				}
				return msg, nil
			}), nil
		}
	}

	hasUserEventSender := w.hasUserEventSender()
	retryConfig := w.modelRetryConfig
	failoverConfig := w.modelFailoverConfig
	cc := w.cancelContext

	for i := len(w.handlers) - 1; i >= 0; i-- {
		handler := w.handlers[i]
		innerEndpoint := endpoint
		baseToolInfos := w.toolInfos
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
			baseOpts := &model.Options{Tools: baseToolInfos}
			commonOpts := model.GetCommonOptions(baseOpts, opts...)
			mc := &TypedModelContext[M]{Tools: commonOpts.Tools, ModelRetryConfig: retryConfig, cancelContext: cc}
			wrappedModel, err := handler.WrapModel(ctx, &typedEndpointModel[M]{stream: innerEndpoint}, mc)
			if err != nil {
				return nil, err
			}
			return wrappedModel.Stream(ctx, input, opts...)
		}
	}

	if !hasUserEventSender {
		innerEndpoint := endpoint
		eventSender := &typedEventSenderModelWrapper[M]{
			TypedBaseChatModelAgentMiddleware: &TypedBaseChatModelAgentMiddleware[M]{},
		}
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
			execCtx := getTypedChatModelAgentExecCtx[M](ctx)
			if execCtx == nil || execCtx.generator == nil {
				return innerEndpoint(ctx, input, opts...)
			}
			mc := &TypedModelContext[M]{ModelRetryConfig: retryConfig, ModelFailoverConfig: failoverConfig, cancelContext: cc}
			wrappedModel, err := eventSender.WrapModel(ctx, &typedEndpointModel[M]{stream: innerEndpoint}, mc)
			if err != nil {
				return nil, err
			}
			return wrappedModel.Stream(ctx, input, opts...)
		}
	}

	if w.modelRetryConfig != nil {
		innerEndpoint := endpoint
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
			retryWrapper := newTypedRetryModelWrapper[M](&typedEndpointModel[M]{stream: innerEndpoint}, w.modelRetryConfig)
			return retryWrapper.Stream(ctx, input, opts...)
		}
	}

	if w.modelFailoverConfig != nil {
		config := w.modelFailoverConfig
		innerEndpoint := endpoint
		endpoint = func(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
			failoverWrapper := newFailoverModelWrapper[M](&typedEndpointModel[M]{stream: innerEndpoint}, config)
			return failoverWrapper.Stream(ctx, input, opts...)
		}
	}

	return endpoint
}

func (w *typedStateModelWrapper[M]) Generate(ctx context.Context, _ []M, opts ...model.Option) (M, error) {
	var (
		stateMessages          []M
		stateToolInfos         []*schema.ToolInfo
		stateDeferredToolInfos []*schema.ToolInfo
	)
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		stateMessages = st.Messages
		stateToolInfos = st.ToolInfos
		stateDeferredToolInfos = st.DeferredToolInfos
		return nil
	})

	// Backfill: old checkpoints or fresh starts have nil ToolInfos.
	// Use compose-level tools from opts (which always reflects the latest bc.toolInfos)
	// rather than w.toolInfos (which may be stale if the graph was reused).
	//
	// 回填：旧检查点或全新启动时 ToolInfos 为 nil。
	// 使用 opts 中的 compose 级工具（始终反映最新的 bc.toolInfos），
	// 而不是 w.toolInfos（图被复用时可能已过期）。
	if stateToolInfos == nil {
		composeLevelOpts := model.GetCommonOptions(&model.Options{}, opts...)
		if composeLevelOpts.Tools != nil {
			stateToolInfos = composeLevelOpts.Tools
		} else {
			stateToolInfos = w.toolInfos
		}
	}

	state := &TypedChatModelAgentState[M]{
		Messages:          stateMessages,
		ToolInfos:         stateToolInfos,
		DeferredToolInfos: stateDeferredToolInfos,
	}

	if msgState, ok := any(state).(*ChatModelAgentState); ok {
		for _, m := range w.middlewares {
			if m.BeforeChatModel != nil {
				if err := m.BeforeChatModel(ctx, msgState); err != nil {
					var zero M
					return zero, err
				}
			}
		}
	}

	baseOpts := &model.Options{Tools: w.toolInfos}
	commonOpts := model.GetCommonOptions(baseOpts, opts...)
	mc := &TypedModelContext[M]{Tools: commonOpts.Tools, ModelRetryConfig: w.modelRetryConfig, cancelContext: w.cancelContext}
	for _, handler := range w.handlers {
		var err error
		ctx, state, err = handler.BeforeModelRewriteState(ctx, state, mc)
		if err != nil {
			var zero M
			return zero, err
		}
	}

	// Persist state (including tool infos) after BeforeModelRewriteState.
	// 在 BeforeModelRewriteState 之后持久化状态（包括工具信息）。
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		st.Messages = state.Messages
		st.ToolInfos = state.ToolInfos
		st.DeferredToolInfos = state.DeferredToolInfos
		return nil
	})

	// Derive model options from state. Append after caller opts so state takes precedence
	// (model.GetCommonOptions applies left-to-right, last wins).
	// Use explicit copy to avoid mutating the caller's opts slice.
	//
	// 从状态派生模型选项。追加到调用方 opts 之后，使状态具有优先级
	// （model.GetCommonOptions 从左到右应用，后者胜出）。
	// 使用显式复制，避免修改调用方的 opts 切片。
	derivedOpts := make([]model.Option, len(opts), len(opts)+2)
	copy(derivedOpts, opts)
	derivedOpts = append(derivedOpts, model.WithTools(state.ToolInfos))
	if state.DeferredToolInfos != nil {
		derivedOpts = append(derivedOpts, model.WithDeferredTools(state.DeferredToolInfos))
	}

	wrappedEndpoint := w.wrapGenerateEndpoint(w.inner.Generate)
	result, err := wrappedEndpoint(ctx, state.Messages, derivedOpts...)
	if err != nil {
		var zero M
		return zero, err
	}

	// Re-read State.Messages after Generate completes: when ShouldRetry uses
	// PersistModifiedInputMessages, applyDecisionForRetry writes modified messages to State.
	// We must pick up those changes before appending the model result.
	//
	// Generate 完成后重新读取 State.Messages：当 ShouldRetry 使用
	// PersistModifiedInputMessages 时，applyDecisionForRetry 会将修改后的消息写入 State。
	// 在追加模型结果前，必须获取这些变更。
	if w.modelRetryConfig != nil && w.modelRetryConfig.ShouldRetry != nil {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			state.Messages = st.Messages
			return nil
		})
	}

	state.Messages = append(state.Messages, result)

	for _, handler := range w.handlers {
		ctx, state, err = handler.AfterModelRewriteState(ctx, state, mc)
		if err != nil {
			var zero M
			return zero, err
		}
	}

	if msgState, ok := any(state).(*ChatModelAgentState); ok {
		for _, m := range w.middlewares {
			if m.AfterChatModel != nil {
				if err := m.AfterChatModel(ctx, msgState); err != nil {
					var zero M
					return zero, err
				}
			}
		}
	}

	// Persist state (including tool infos) after AfterModelRewriteState.
	// 在 AfterModelRewriteState 之后持久化状态（包括工具信息）。
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		st.Messages = state.Messages
		st.ToolInfos = state.ToolInfos
		st.DeferredToolInfos = state.DeferredToolInfos
		return nil
	})

	if len(state.Messages) == 0 {
		var zero M
		return zero, errors.New("no messages left in state after model call")
	}
	return state.Messages[len(state.Messages)-1], nil
}

func (w *typedStateModelWrapper[M]) Stream(ctx context.Context, _ []M, opts ...model.Option) (*schema.StreamReader[M], error) {
	var (
		stateMessages          []M
		stateToolInfos         []*schema.ToolInfo
		stateDeferredToolInfos []*schema.ToolInfo
	)
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		stateMessages = st.Messages
		stateToolInfos = st.ToolInfos
		stateDeferredToolInfos = st.DeferredToolInfos
		return nil
	})

	// Backfill: old checkpoints or fresh starts have nil ToolInfos.
	// Use compose-level tools from opts (which always reflects the latest bc.toolInfos)
	// rather than w.toolInfos (which may be stale if the graph was reused).
	//
	// 回填：旧检查点或全新启动时 ToolInfos 为 nil。
	// 使用 opts 中的 compose 级工具（始终反映最新的 bc.toolInfos），
	// 而不是 w.toolInfos（图被复用时可能已过期）。
	if stateToolInfos == nil {
		composeLevelOpts := model.GetCommonOptions(&model.Options{}, opts...)
		if composeLevelOpts.Tools != nil {
			stateToolInfos = composeLevelOpts.Tools
		} else {
			stateToolInfos = w.toolInfos
		}
	}

	state := &TypedChatModelAgentState[M]{
		Messages:          stateMessages,
		ToolInfos:         stateToolInfos,
		DeferredToolInfos: stateDeferredToolInfos,
	}

	if msgState, ok := any(state).(*ChatModelAgentState); ok {
		for _, m := range w.middlewares {
			if m.BeforeChatModel != nil {
				if err := m.BeforeChatModel(ctx, msgState); err != nil {
					return nil, err
				}
			}
		}
	}

	baseOpts := &model.Options{Tools: w.toolInfos}
	commonOpts := model.GetCommonOptions(baseOpts, opts...)
	mc := &TypedModelContext[M]{Tools: commonOpts.Tools, ModelRetryConfig: w.modelRetryConfig, cancelContext: w.cancelContext}
	for _, handler := range w.handlers {
		var err error
		ctx, state, err = handler.BeforeModelRewriteState(ctx, state, mc)
		if err != nil {
			return nil, err
		}
	}

	// Persist state (including tool infos) after BeforeModelRewriteState.
	// 在 BeforeModelRewriteState 之后持久化状态（包括工具信息）。
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		st.Messages = state.Messages
		st.ToolInfos = state.ToolInfos
		st.DeferredToolInfos = state.DeferredToolInfos
		return nil
	})

	// Derive model options from state. Append after caller opts so state takes precedence
	// (model.GetCommonOptions applies left-to-right, last wins).
	// Use explicit copy to avoid mutating the caller's opts slice.
	//
	// 从状态派生模型选项。追加到调用方 opts 之后，使状态具有优先级
	// （model.GetCommonOptions 从左到右应用，后者胜出）。
	// 使用显式复制，避免修改调用方的 opts 切片。
	derivedOpts := make([]model.Option, len(opts), len(opts)+2)
	copy(derivedOpts, opts)
	derivedOpts = append(derivedOpts, model.WithTools(state.ToolInfos))
	if state.DeferredToolInfos != nil {
		derivedOpts = append(derivedOpts, model.WithDeferredTools(state.DeferredToolInfos))
	}

	wrappedEndpoint := w.wrapStreamEndpoint(w.inner.Stream)
	stream, err := wrappedEndpoint(ctx, state.Messages, derivedOpts...)
	if err != nil {
		return nil, err
	}
	result, err := concatMessageStream(stream)
	if err != nil {
		return nil, err
	}

	// Re-read State.Messages after Stream completes: same rationale as in Generate above.
	// Stream 完成后重新读取 State.Messages：理由同上面的 Generate。
	if w.modelRetryConfig != nil && w.modelRetryConfig.ShouldRetry != nil {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			state.Messages = st.Messages
			return nil
		})
	}

	state.Messages = append(state.Messages, result)

	for _, handler := range w.handlers {
		ctx, state, err = handler.AfterModelRewriteState(ctx, state, mc)
		if err != nil {
			return nil, err
		}
	}

	if msgState, ok := any(state).(*ChatModelAgentState); ok {
		for _, m := range w.middlewares {
			if m.AfterChatModel != nil {
				if err := m.AfterChatModel(ctx, msgState); err != nil {
					return nil, err
				}
			}
		}
	}

	// Persist state (including tool infos) after AfterModelRewriteState.
	// 在 AfterModelRewriteState 之后持久化状态（包括工具信息）。
	_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
		st.Messages = state.Messages
		st.ToolInfos = state.ToolInfos
		st.DeferredToolInfos = state.DeferredToolInfos
		return nil
	})

	if len(state.Messages) == 0 {
		return nil, errors.New("no messages left in state after model call")
	}
	return schema.StreamReaderFromArray([]M{state.Messages[len(state.Messages)-1]}), nil
}

type typedEndpointModel[M MessageType] struct {
	generate typedGenerateEndpoint[M]
	stream   typedStreamEndpoint[M]
}

func (m *typedEndpointModel[M]) Generate(ctx context.Context, input []M, opts ...model.Option) (M, error) {
	if m.generate != nil {
		return m.generate(ctx, input, opts...)
	}
	var zero M
	return zero, errors.New("generate endpoint not set")
}

func (m *typedEndpointModel[M]) Stream(ctx context.Context, input []M, opts ...model.Option) (*schema.StreamReader[M], error) {
	if m.stream != nil {
		return m.stream(ctx, input, opts...)
	}
	return nil, errors.New("stream endpoint not set")
}
