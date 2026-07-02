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
	"io"
	"math/rand"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

var (
	// ErrExceedMaxRetries is returned when the maximum number of retries has been exceeded.
	// Use errors.Is to check if an error is due to max retries being exceeded:
	//
	//   if errors.Is(err, adk.ErrExceedMaxRetries) {
	//       // handle max retries exceeded
	//   }
	//
	// Use errors.As to extract the underlying RetryExhaustedError for the last error details:
	//
	//   var retryErr *adk.RetryExhaustedError
	//   if errors.As(err, &retryErr) {
	//       fmt.Printf("last error was: %v\n", retryErr.LastErr)
	//   }
	//
	// ErrExceedMaxRetries 在超过最大重试次数时返回。
	// 使用 errors.Is 检查错误是否由于超过最大重试次数：
	// if errors.Is(err, adk.ErrExceedMaxRetries) {
	// 处理超过最大重试次数
	// }
	// 使用 errors.As 提取底层 RetryExhaustedError 以获取最后一次错误详情：
	// var retryErr *adk.RetryExhaustedError
	// if errors.As(err, &retryErr) {
	// fmt.Printf("last error was: %v\n", retryErr.LastErr)
	// }
	ErrExceedMaxRetries = errors.New("exceeds max retries")
)

// RetryExhaustedError is returned when all retry attempts have been exhausted.
// It wraps the last error that occurred during retry attempts.
//
// RetryExhaustedError 在所有重试尝试都耗尽时返回。
// 它包装了重试过程中发生的最后一个错误。
type RetryExhaustedError struct {
	LastErr      error
	TotalRetries int
}

func (e *RetryExhaustedError) Error() string {
	if e.LastErr != nil {
		return fmt.Sprintf("exceeds max retries: last error: %v", e.LastErr)
	}
	return "exceeds max retries"
}

func (e *RetryExhaustedError) Unwrap() error {
	return ErrExceedMaxRetries
}

// WillRetryError is emitted when a retryable error occurs and a retry will be attempted.
// It allows end-users to observe retry events in real-time via AgentEvent.
//
// Field design rationale:
//   - ErrStr (exported): Stores the error message string for Gob serialization during checkpointing.
//     This ensures the error message is preserved after checkpoint restore.
//   - err (unexported): Stores the original error for Unwrap() support at runtime.
//     This field is intentionally unexported because Gob serialization would fail for unregistered
//     concrete error types. Since end-users only need the original error when the AgentEvent first
//     occurs (not after restoring from checkpoint), skipping serialization is acceptable.
//     After checkpoint restore, err will be nil and Unwrap() returns nil.
//   - rejectReason (unexported): Stores a user-defined value set by the ShouldRetry callback
//     via RetryDecision.RejectReason. This is runtime-only observability data — after checkpoint
//     restore it will be nil. Unexported to avoid Gob serialization of arbitrary types.
//
// WillRetryError 在发生可重试错误且将进行重试时发出。
// 它允许最终用户通过 AgentEvent 实时观察重试事件。
// 字段设计原因：
// - ErrStr（导出）：存储错误消息字符串，用于检查点中的 Gob 序列化。
// 这确保检查点恢复后仍保留错误消息。
// - err（未导出）：存储原始错误，用于运行时支持 Unwrap()。
// 该字段故意不导出，因为未注册的具体错误类型会导致 Gob 序列化失败。
// 由于最终用户只需要在 AgentEvent 首次发生时获取原始错误（而不是从检查点恢复后），
// 因此跳过序列化是可接受的。检查点恢复后，err 将为 nil，Unwrap() 返回 nil。
// - rejectReason（未导出）：存储由 ShouldRetry 回调通过 RetryDecision.RejectReason
// 设置的用户自定义值。这是仅运行时的可观测性数据——检查点恢复后将为 nil。
// 不导出是为了避免对任意类型进行 Gob 序列化。
type WillRetryError struct {
	ErrStr       string
	RetryAttempt int
	rejectReason any
	err          error
}

func (e *WillRetryError) Error() string {
	return e.ErrStr
}

func (e *WillRetryError) Unwrap() error {
	return e.err
}

// RejectReason returns the user-defined rejection reason set by the ShouldRetry callback
// via RetryDecision.RejectReason. Returns nil if not set or after checkpoint restore.
//
// RejectReason 返回由 ShouldRetry 回调通过 RetryDecision.RejectReason 设置的用户自定义拒绝原因。
// 未设置或检查点恢复后返回 nil。
func (e *WillRetryError) RejectReason() any {
	return e.rejectReason
}

func init() {
	schema.RegisterName[*WillRetryError]("eino_adk_chatmodel_will_retry_error")
}

// TypedRetryContext contains context information passed to TypedModelRetryConfig.ShouldRetry
// during a retry decision.
//
// State combinations for OutputMessage and Err:
//
//	OutputMessage != nil, Err == nil  → successful call; inspect message quality
//	OutputMessage == nil, Err != nil  → failed call (Generate error or Stream() error)
//	OutputMessage != nil, Err != nil  → partial stream (chunks received before mid-stream error)
//	OutputMessage == nil, Err == nil  → empty stream (zero chunks before EOF)
//
// TypedRetryContext 包含在重试决策期间传递给 TypedModelRetryConfig.ShouldRetry 的上下文信息。
// OutputMessage 和 Err 的状态组合：
// OutputMessage != nil, Err == nil  → 调用成功；检查消息质量
// OutputMessage == nil, Err != nil  → 调用失败（Generate 错误或 Stream() 错误）
// OutputMessage != nil, Err != nil  → 部分流（流中途出错前已收到 chunk）
// OutputMessage == nil, Err == nil  → 空流（EOF 前零个 chunk）
type TypedRetryContext[M MessageType] struct {
	// RetryAttempt is the current retry attempt number (1-based).
	// For the first retry decision (after the initial call), this is 1.
	//
	// RetryAttempt 是当前重试尝试次数（从 1 开始）。
	// 对于首次重试决策（初始调用之后），该值为 1。
	RetryAttempt int

	// InputMessages is the input messages that were sent to the model for the current attempt.
	// InputMessages 是当前尝试发送给模型的输入消息。
	InputMessages []M

	// Options is the model options that were used for the current attempt.
	// Options 是当前尝试使用的模型选项。
	Options []model.Option

	// OutputMessage is the output message from the model, if any.
	// This is non-nil when the model returned a message successfully.
	// For streaming, this is the fully concatenated message (the entire stream is consumed
	// before ShouldRetry is called).
	// For streaming with mid-stream errors, this is the partial concatenation of chunks
	// received before the error occurred.
	// May be nil if the model returned an error without producing a message, or if the
	// stream was empty (zero chunks before EOF).
	//
	// OutputMessage 是模型的输出消息（如果有）。
	// 当模型成功返回消息时，该值非 nil。
	// 对于流式，这是完全拼接后的消息（在调用 ShouldRetry 之前会消费整个流）。
	// 对于流式且流中途出错的情况，这是错误发生前收到的 chunk 的部分拼接。
	// 如果模型返回错误且未产生消息，或流为空（EOF 前零个 chunk），则可能为 nil。
	OutputMessage M

	// Err is the error from the model call, if any.
	// May be nil if the model produced a message without error.
	// Note: both OutputMessage and Err can be nil simultaneously for empty streams.
	//
	// Err 是模型调用产生的错误（如果有）。
	// 如果模型无错误地产生了消息，则可能为 nil。
	// 注意：对于空流，OutputMessage 和 Err 可以同时为 nil。
	Err error
}

// RetryContext is the default retry context type using *schema.Message.
// RetryContext 是使用 *schema.Message 的默认重试上下文类型。
type RetryContext = TypedRetryContext[*schema.Message]

// TypedRetryDecision represents the decision made by TypedModelRetryConfig.ShouldRetry.
// TypedRetryDecision 表示 TypedModelRetryConfig.ShouldRetry 做出的决策。
type TypedRetryDecision[M MessageType] struct {
	// Retry indicates whether the model call should be retried.
	// If false, the model output (or error) is accepted as-is, unless RewriteError is set.
	//
	// Retry 表示是否应重试模型调用。
	// 如果为 false，则按原样接受模型输出（或错误），除非设置了 RewriteError。
	Retry bool

	// RewriteError, when non-nil, overrides the return value of the model call with this error.
	// The agent run will fail with this error.
	//
	// This is useful for two scenarios:
	//   - When the model returns a "seemingly correct" message (no error) that actually
	//     contains unrecoverable issues. RewriteError converts the successful output
	//     into a fatal error.
	//   - When the model returns an error, but you want to replace it with a different,
	//     more descriptive error (e.g., adding context or wrapping).
	//
	// When Retry is true, RewriteError is ignored.
	// When Retry is false and RewriteError is non-nil, the model call returns
	// RewriteError regardless of whether the original call had an error or a message.
	//
	// RewriteError 非 nil 时，会用此错误覆盖模型调用的返回值。
	// 智能体运行将以此错误失败。
	// 这适用于两种场景：
	// - 当模型返回“看似正确”的消息（无错误），但实际包含不可恢复的问题时。
	// RewriteError 会将成功输出转换为致命错误。
	// - 当模型返回错误，但你想用另一个更具描述性的错误替换它时
	// （例如添加上下文或包装）。
	// 当 Retry 为 true 时，RewriteError 会被忽略。
	// 当 Retry 为 false 且 RewriteError 非 nil 时，无论原始调用是错误还是消息，
	// 模型调用都会返回 RewriteError。
	RewriteError error

	// ModifiedInputMessages, when non-nil, replaces the input messages for the next retry.
	//
	// This enables advanced recovery strategies like context compression or message trimming.
	// Only used when Retry is true. Ignored when Retry is false.
	//
	// ModifiedInputMessages 非 nil 时，会替换下一次重试的输入消息。
	// 这支持上下文压缩或消息裁剪等高级恢复策略。
	// 仅在 Retry 为 true 时使用。Retry 为 false 时忽略。
	ModifiedInputMessages []M

	// PersistModifiedInputMessages controls whether ModifiedInputMessages are written
	// back to the agent's conversation history, affecting subsequent model calls in
	// the agent loop (not just the next retry attempt).
	//
	// When true, the modified messages replace the current conversation history.
	// When false (default), the modified messages are only used for the next retry attempt
	// within this retry cycle.
	//
	// Only used when Retry is true and ModifiedInputMessages is non-nil.
	//
	// PersistModifiedInputMessages 控制是否将 ModifiedInputMessages 写回智能体的对话历史，
	// 从而影响智能体循环中的后续模型调用（不只是下一次重试尝试）。
	// 为 true 时，修改后的消息会替换当前对话历史。
	// 为 false（默认）时，修改后的消息仅用于本次重试周期中的下一次重试尝试。
	// 仅在 Retry 为 true 且 ModifiedInputMessages 非 nil 时使用。
	PersistModifiedInputMessages bool

	// AdditionalOptions, when non-nil, provides additional model options for the next retry.
	// These options are appended to the existing options, taking precedence via last-wins semantics.
	//
	// This enables adjustments like increasing MaxTokens for the retry attempt.
	// Note: options accumulate across retries within a single retry cycle. If ShouldRetry
	// returns AdditionalOptions on every attempt, each set is appended to the previous ones.
	// Only the last value for each option key takes effect, but earlier values remain in the slice.
	// AdditionalOptions are scoped to the current retry cycle and do not persist to subsequent
	// agent iterations — each new model call in the agent loop starts with the original options.
	// Only used when Retry is true. Ignored when Retry is false.
	//
	// AdditionalOptions 非 nil 时，为下一次重试提供额外的模型选项。
	// 这些选项会追加到现有选项之后，并通过后者优先语义取得优先级。
	// 这支持为重试尝试增加 MaxTokens 等调整。
	// 注意：在单个重试周期内，options 会跨重试累积。如果 ShouldRetry
	// 每次尝试都返回 AdditionalOptions，每组选项都会追加到之前的选项之后。
	// 每个 option key 只有最后一个值生效，但较早的值仍保留在 slice 中。
	// AdditionalOptions 的作用域限定在当前重试周期内，不会持久到后续
	// 智能体迭代——智能体循环中的每次新模型调用都会从原始 options 开始。
	// 仅在 Retry 为 true 时使用。Retry 为 false 时忽略。
	AdditionalOptions []model.Option

	// Backoff specifies the duration to wait before the next retry attempt.
	// If zero, the default backoff function (from ModelRetryConfig.BackoffFunc or the
	// built-in exponential backoff) is used.
	//
	// This allows the ShouldRetry callback to dynamically control retry timing based on
	// the specific error or problematic message encountered.
	// Only used when Retry is true. Ignored when Retry is false.
	//
	// Backoff 指定下一次重试前等待的时长。
	// 如果为零，则使用默认退避函数（来自 ModelRetryConfig.BackoffFunc 或内置指数退避）。
	// 这允许 ShouldRetry 回调根据遇到的具体错误或问题消息动态控制重试时机。
	// 仅在 Retry 为 true 时使用。Retry 为 false 时忽略。
	Backoff time.Duration

	// RejectReason is an optional user-defined value describing why the output was rejected.
	// When Retry is true and the rejected stream/message is observed downstream via
	// AgentEvent, this value is attached to the WillRetryError emitted to the event stream.
	// Consumers can retrieve it via WillRetryError.RejectReason().
	//
	// The ShouldRetry callback has full access to the model output (via retryCtx.OutputMessage)
	// and error (via retryCtx.Err), so it can distill whatever information it wants into
	// RejectReason — a string, a struct, the output message itself, or nil.
	//
	// Only used when Retry is true. Ignored when Retry is false.
	//
	// RejectReason 是可选的用户自定义值，用于描述输出被拒绝的原因。
	// 当 Retry 为 true，且被拒绝的流/消息通过 AgentEvent 在下游被观察到时，该值会附加到发送到事件流的 WillRetryError 上。
	// 消费者可通过 WillRetryError.RejectReason() 获取它。
	// ShouldRetry 回调可完整访问模型输出（通过 retryCtx.OutputMessage）和错误（通过 retryCtx.Err），因此可将所需信息提炼到 RejectReason 中——字符串、结构体、输出消息本身或 nil。
	// 仅在 Retry 为 true 时使用。Retry 为 false 时忽略。
	RejectReason any
}

// RetryDecision is the default retry decision type using *schema.Message.
// RetryDecision 是使用 *schema.Message 的默认重试决策类型。
type RetryDecision = TypedRetryDecision[*schema.Message]

// TypedModelRetryConfig configures retry behavior for the ChatModel node.
// It defines how the agent should handle transient failures when calling the ChatModel.
//
// TypedModelRetryConfig 配置 ChatModel 节点的重试行为。
// 它定义智能体在调用 ChatModel 时应如何处理瞬时失败。
type TypedModelRetryConfig[M MessageType] struct {
	// MaxRetries specifies the maximum number of retry attempts.
	// A value of 0 means no retries will be attempted.
	// A value of 3 means up to 3 retry attempts (4 total calls including the initial attempt).
	//
	// MaxRetries 指定最大重试次数。
	// 值为 0 表示不进行重试。
	// 值为 3 表示最多重试 3 次（包含初始尝试共 4 次调用）。
	MaxRetries int

	// ShouldRetry determines how to handle a model call result.
	// It receives context information about the current attempt including the output message
	// and/or error, and returns a decision on whether to retry, what to modify, etc.
	// Returning nil is treated as &RetryDecision{Retry: false} (accept as-is).
	//
	// If nil, defaults to retrying on any non-nil error (backward compatible with IsRetryAble).
	//
	// Note: When ShouldRetry is set, IsRetryAble is ignored.
	// Note: In streaming mode, the entire stream is consumed before ShouldRetry is called.
	// The event stream is sent to the client in real time regardless; only the retry
	// decision is deferred until the full response is available.
	//
	// ShouldRetry 决定如何处理模型调用结果。
	// 它接收当前尝试的上下文信息，包括输出消息和/或错误，并返回是否重试、修改什么等决策。
	// 返回 nil 会被视为 &RetryDecision{Retry: false}（按原样接受）。
	// 如果为 nil，默认对任何非 nil 错误进行重试（与 IsRetryAble 向后兼容）。
	// 注意：设置 ShouldRetry 时，将忽略 IsRetryAble。
	// 注意：在流式模式下，会先消费完整个流，再调用 ShouldRetry。
	// 无论如何，事件流都会实时发送给客户端；只有重试决策会延迟到完整响应可用后再作出。
	ShouldRetry func(ctx context.Context, retryCtx *TypedRetryContext[M]) *TypedRetryDecision[M]

	// Deprecated: Use ShouldRetry instead for richer retry control including message
	// inspection, input modification, and option adjustment. When ShouldRetry is set,
	// IsRetryAble is ignored.
	//
	// Deprecated: 请改用 ShouldRetry，以获得更丰富的重试控制，包括消息检查、输入修改和 option 调整。设置 ShouldRetry 时，将忽略 IsRetryAble。
	IsRetryAble func(ctx context.Context, err error) bool

	// BackoffFunc calculates the delay before the next retry attempt.
	// The attempt parameter starts at 1 for the first retry.
	// Used as the default when RetryDecision.Backoff is zero.
	// If nil, a default exponential backoff with jitter is used:
	// base delay 100ms, exponentially increasing up to 10s max,
	// with random jitter (0-50% of delay) to prevent thundering herd.
	//
	// BackoffFunc 计算下一次重试前的延迟。
	// attempt 参数从 1 开始，表示第一次重试。
	// 当 RetryDecision.Backoff 为零时用作默认值。
	// 如果为 nil，则使用带抖动的默认指数退避：
	// 基础延迟 100ms，指数增长，最大到 10s，
	// 并带随机抖动（延迟的 0-50%），以避免 thundering herd。
	BackoffFunc func(ctx context.Context, attempt int) time.Duration
}

// ModelRetryConfig is the default retry config type using *schema.Message.
// ModelRetryConfig 是使用 *schema.Message 的默认重试配置类型。
type ModelRetryConfig = TypedModelRetryConfig[*schema.Message]

func defaultIsRetryAble(_ context.Context, err error) bool {
	return err != nil
}

func defaultBackoff(_ context.Context, attempt int) time.Duration {
	baseDelay := 100 * time.Millisecond
	maxDelay := 10 * time.Second

	if attempt <= 0 {
		return baseDelay
	}

	if attempt > 7 {
		return maxDelay + time.Duration(rand.Int63n(int64(maxDelay/2)))
	}

	delay := baseDelay * time.Duration(1<<uint(attempt-1))
	if delay > maxDelay {
		delay = maxDelay
	}

	jitter := time.Duration(rand.Int63n(int64(delay / 2)))
	return delay + jitter
}

func genErrWrapper(ctx context.Context, maxRetries, attempt int, isRetryAbleFunc func(ctx context.Context, err error) bool) func(error) error {
	return func(err error) error {
		isRetryAble := isRetryAbleFunc == nil || isRetryAbleFunc(ctx, err)
		hasRetriesLeft := attempt < maxRetries

		if isRetryAble && hasRetriesLeft {
			return &WillRetryError{ErrStr: err.Error(), RetryAttempt: attempt, err: err}
		}
		return err
	}
}

func consumeStreamForError[M any](stream *schema.StreamReader[M]) error {
	defer stream.Close()
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type retryVerdictSignal struct {
	ch chan retryVerdict
}

type retryVerdict struct {
	WillRetry    bool
	RetryAttempt int
	Err          error
	RejectReason any
}

// retryModelWrapper wraps a BaseChatModel with retry logic.
// This is used inside the model wrapper chain, positioned between eventSenderModelWrapper
// and stateModelWrapper, so that retry only affects the inner chain (event sending, user wrappers,
// callback injection) without re-running state management (BeforeModelRewriteState/AfterModelRewriteState).
//
// retryModelWrapper 用重试逻辑包装 BaseChatModel。
// 它用于模型包装器链内部，位于 eventSenderModelWrapper 和 stateModelWrapper 之间，因此重试只影响内部链（事件发送、用户包装器、回调注入），不会重新运行状态管理（BeforeModelRewriteState/AfterModelRewriteState）。
type typedRetryModelWrapper[M MessageType] struct {
	inner  model.BaseModel[M]
	config *TypedModelRetryConfig[M]
}

func newTypedRetryModelWrapper[M MessageType](inner model.BaseModel[M], config *TypedModelRetryConfig[M]) *typedRetryModelWrapper[M] {
	return &typedRetryModelWrapper[M]{inner: inner, config: config}
}

func (r *typedRetryModelWrapper[M]) Generate(ctx context.Context, input []M, opts ...model.Option) (M, error) {
	if r.config.ShouldRetry != nil {
		return generateWithShouldRetry(r, ctx, input, opts...)
	}
	return r.generateLegacy(ctx, input, opts...)
}

func (r *typedRetryModelWrapper[M]) generateLegacy(ctx context.Context, input []M, opts ...model.Option) (zero M, _ error) {
	isRetryAble := r.config.IsRetryAble
	if isRetryAble == nil {
		isRetryAble = defaultIsRetryAble
	}
	backoffFunc := r.config.BackoffFunc
	if backoffFunc == nil {
		backoffFunc = defaultBackoff
	}

	var lastErr error
	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		out, err := r.inner.Generate(ctx, input, opts...)
		if err == nil {
			return out, nil
		}

		if _, ok := compose.ExtractInterruptInfo(err); ok {
			return zero, err
		}

		if errors.Is(err, ErrStreamCanceled) {
			return zero, err
		}

		if !isRetryAble(ctx, err) {
			return zero, err
		}

		lastErr = err
		if attempt < r.config.MaxRetries {
			if err := r.contextAwareSleep(ctx, backoffFunc(ctx, attempt+1)); err != nil {
				return zero, err
			}
		}
	}

	return zero, &RetryExhaustedError{LastErr: lastErr, TotalRetries: r.config.MaxRetries}
}

func generateWithShouldRetry[M MessageType](r *typedRetryModelWrapper[M], ctx context.Context, input []M, opts ...model.Option) (M, error) {
	backoffFunc := r.config.BackoffFunc
	if backoffFunc == nil {
		backoffFunc = defaultBackoff
	}

	execCtx := getTypedChatModelAgentExecCtx[M](ctx)

	currentInput := input
	currentOpts := opts
	var lastErr error
	var zero M

	defer func() {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setRetryAttempt(0)
			return nil
		})
	}()

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setRetryAttempt(attempt)
			return nil
		})

		// Suppress event sending during Generate: the ShouldRetry callback must decide whether
		// to accept or reject the result before any event is emitted. If accepted, the event
		// is sent explicitly below (lines after decision check). If rejected, no event leaks.
		//
		// 在 Generate 期间抑制事件发送：ShouldRetry 回调必须在发出任何事件前决定接受还是拒绝结果。
		// 如果接受，事件会在下面显式发送（决策检查之后的行）。如果拒绝，则不会泄漏事件。
		if execCtx != nil {
			execCtx.suppressEventSend = true
		}
		out, err := r.inner.Generate(ctx, currentInput, currentOpts...)
		if execCtx != nil {
			execCtx.suppressEventSend = false
		}

		if err != nil {
			if _, ok := compose.ExtractInterruptInfo(err); ok {
				return zero, err
			}

			if errors.Is(err, ErrStreamCanceled) {
				return zero, err
			}
		}

		retryCtx := &TypedRetryContext[M]{
			RetryAttempt:  attempt + 1,
			InputMessages: currentInput,
			Options:       currentOpts,
			OutputMessage: out,
			Err:           err,
		}
		decision := r.config.ShouldRetry(ctx, retryCtx)
		if decision == nil {
			decision = &TypedRetryDecision[M]{}
		}

		if !decision.Retry {
			if decision.RewriteError != nil {
				return zero, decision.RewriteError
			}
			if err != nil {
				return zero, err
			}
			if execCtx != nil && execCtx.generator != nil && out != nil {
				event := typedModelOutputEvent(out, nil)
				execCtx.send(event)
			}
			return out, nil
		}

		lastErr = err
		if lastErr == nil {
			lastErr = fmt.Errorf("model output rejected by ShouldRetry at attempt %d", attempt+1)
		}

		if attempt >= r.config.MaxRetries {
			break
		}

		applyDecisionForRetry(&currentInput, &currentOpts, ctx, decision)

		delay := decision.Backoff
		if delay == 0 {
			delay = backoffFunc(ctx, attempt+1)
		}

		if err := r.contextAwareSleep(ctx, delay); err != nil {
			return zero, err
		}
	}

	return zero, &RetryExhaustedError{LastErr: lastErr, TotalRetries: r.config.MaxRetries}
}

func (r *typedRetryModelWrapper[M]) contextAwareSleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func streamWithShouldRetry[M MessageType](r *typedRetryModelWrapper[M], ctx context.Context, input []M, opts ...model.Option) (
	*schema.StreamReader[M], error) {

	backoffFunc := r.config.BackoffFunc
	if backoffFunc == nil {
		backoffFunc = defaultBackoff
	}

	defer func() {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setRetryAttempt(0)
			return nil
		})
	}()

	execCtx := getTypedChatModelAgentExecCtx[M](ctx)

	currentInput := input
	currentOpts := opts
	var lastErr error
	var curSignal *retryVerdictSignal

	// Panic recovery for verdict signal: if ShouldRetry panics, the onEOF/errWrapper closures in
	// buildStreamConvertOptions will block forever on signal.ch, causing a goroutine leak. This
	// defer ensures a verdict is always sent, even on panic, before re-panicking.
	//
	// verdict 信号的 panic 恢复：如果 ShouldRetry panic，buildStreamConvertOptions 中的 onEOF/errWrapper 闭包会一直阻塞在 signal.ch 上，导致 goroutine 泄漏。
	// 该 defer 确保即使发生 panic，也一定会在重新 panic 前发送 verdict。
	defer func() {
		if p := recover(); p != nil {
			if curSignal != nil {
				select {
				case curSignal.ch <- retryVerdict{WillRetry: false, Err: fmt.Errorf("panic: %v", p)}:
				default:
				}
			}
			panic(p)
		}
	}()

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setRetryAttempt(attempt)
			return nil
		})

		signal := &retryVerdictSignal{ch: make(chan retryVerdict, 1)}
		curSignal = signal
		if execCtx != nil {
			execCtx.retryVerdictSignal = signal
		}

		stream, err := r.inner.Stream(ctx, currentInput, currentOpts...)
		if err != nil {
			// Defensive no-op: when Stream() returns an error, no stream exists, so
			// eventSenderModel never creates the StreamReaderWithConvert hooks that would
			// read from signal.ch. This send has no consumer — it merely fills the
			// buffered(1) slot so the panic-recovery defer (select/default) won't block
			// if a later panic tries to send a second verdict. The signal is discarded
			// when the next iteration creates a new one.
			//
			// 防御性 no-op：当 Stream() 返回错误时，不存在流，因此 eventSenderModel 不会创建读取 signal.ch 的 StreamReaderWithConvert hook。
			// 这次发送没有消费者——只是填充 buffered(1) 槽位，使 panic-recovery defer（select/default）在之后发生 panic 并尝试发送第二个 verdict 时不会阻塞。
			// 当下一次迭代创建新的 signal 时，该信号会被丢弃。
			signal.ch <- retryVerdict{WillRetry: false}

			if _, ok := compose.ExtractInterruptInfo(err); ok {
				return nil, err
			}

			if errors.Is(err, ErrStreamCanceled) {
				return nil, err
			}

			retryCtx := &TypedRetryContext[M]{
				RetryAttempt:  attempt + 1,
				InputMessages: currentInput,
				Options:       currentOpts,
				Err:           err,
			}
			decision := r.config.ShouldRetry(ctx, retryCtx)
			if decision == nil {
				decision = &TypedRetryDecision[M]{}
			}

			if !decision.Retry {
				if decision.RewriteError != nil {
					return nil, decision.RewriteError
				}
				return nil, err
			}

			lastErr = err
			if attempt < r.config.MaxRetries {
				applyDecisionForRetry(&currentInput, &currentOpts, ctx, decision)
				delay := decision.Backoff
				if delay == 0 {
					delay = backoffFunc(ctx, attempt+1)
				}
				if err := r.contextAwareSleep(ctx, delay); err != nil {
					return nil, err
				}
			}
			continue
		}

		// Split the stream: checkCopy is consumed synchronously here to build the complete
		// message for ShouldRetry inspection; returnCopy is returned to the caller and may
		// already be consumed downstream in parallel. The verdict signal bridges the two:
		// once ShouldRetry decides, the signal tells returnCopy's errWrapper/onEOF whether
		// to pass through normally or inject a WillRetryError.
		//
		// 拆分流：checkCopy 在此处同步消费，用于构建完整消息供 ShouldRetry 检查；returnCopy 返回给调用方，并可能已在下游并行消费。
		// verdict 信号连接两者：ShouldRetry 作出决策后，该信号会告诉 returnCopy 的 errWrapper/onEOF 是正常透传，还是注入 WillRetryError。
		copies := stream.Copy(2)
		checkCopy := copies[0]
		returnCopy := copies[1]

		msg, streamErr := typedConsumeStream(checkCopy)

		if errors.Is(streamErr, ErrStreamCanceled) {
			signal.ch <- retryVerdict{WillRetry: false}
			returnCopy.Close()
			return nil, streamErr
		}

		retryCtx := &TypedRetryContext[M]{
			RetryAttempt:  attempt + 1,
			InputMessages: currentInput,
			Options:       currentOpts,
			OutputMessage: msg,
			Err:           streamErr,
		}
		decision := r.config.ShouldRetry(ctx, retryCtx)
		if decision == nil {
			decision = &TypedRetryDecision[M]{}
		}

		if !decision.Retry {
			signal.ch <- retryVerdict{WillRetry: false}

			if decision.RewriteError != nil {
				returnCopy.Close()
				return nil, decision.RewriteError
			}
			if streamErr != nil {
				returnCopy.Close()
				return nil, streamErr
			}
			return returnCopy, nil
		}

		verdictErr := streamErr
		if verdictErr == nil {
			verdictErr = fmt.Errorf("model output rejected by ShouldRetry at attempt %d", attempt+1)
		}
		signal.ch <- retryVerdict{
			WillRetry:    true,
			RetryAttempt: attempt,
			Err:          verdictErr,
			RejectReason: decision.RejectReason,
		}
		returnCopy.Close()

		lastErr = verdictErr

		if attempt < r.config.MaxRetries {
			applyDecisionForRetry(&currentInput, &currentOpts, ctx, decision)
			delay := decision.Backoff
			if delay == 0 {
				delay = backoffFunc(ctx, attempt+1)
			}
			if err := r.contextAwareSleep(ctx, delay); err != nil {
				return nil, err
			}
		}
	}

	return nil, &RetryExhaustedError{LastErr: lastErr, TotalRetries: r.config.MaxRetries}
}

func applyDecisionForRetry[M MessageType](currentInput *[]M, currentOpts *[]model.Option, ctx context.Context, decision *TypedRetryDecision[M]) {
	if decision.ModifiedInputMessages != nil {
		*currentInput = decision.ModifiedInputMessages
		if decision.PersistModifiedInputMessages {
			modifiedInput := *currentInput
			_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
				st.Messages = modifiedInput
				return nil
			})
		}
	}

	if decision.AdditionalOptions != nil {
		cloned := make([]model.Option, len(*currentOpts), len(*currentOpts)+len(decision.AdditionalOptions))
		copy(cloned, *currentOpts)
		*currentOpts = append(cloned, decision.AdditionalOptions...)
	}
}

func (r *typedRetryModelWrapper[M]) Stream(ctx context.Context, input []M, opts ...model.Option) (
	*schema.StreamReader[M], error) {

	if r.config.ShouldRetry != nil {
		return streamWithShouldRetry(r, ctx, input, opts...)
	}
	return r.streamLegacy(ctx, input, opts...)
}

func (r *typedRetryModelWrapper[M]) streamLegacy(ctx context.Context, input []M, opts ...model.Option) (
	*schema.StreamReader[M], error) {

	isRetryAble := r.config.IsRetryAble
	if isRetryAble == nil {
		isRetryAble = defaultIsRetryAble
	}
	backoffFunc := r.config.BackoffFunc
	if backoffFunc == nil {
		backoffFunc = defaultBackoff
	}

	defer func() {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setRetryAttempt(0)
			return nil
		})
	}()

	var lastErr error
	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		_ = compose.ProcessState(ctx, func(_ context.Context, st *typedState[M]) error {
			st.setRetryAttempt(attempt)
			return nil
		})

		stream, err := r.inner.Stream(ctx, input, opts...)
		if err != nil {
			if _, ok := compose.ExtractInterruptInfo(err); ok {
				return nil, err
			}
			if errors.Is(err, ErrStreamCanceled) {
				return nil, err
			}
			if !isRetryAble(ctx, err) {
				return nil, err
			}
			lastErr = err
			if attempt < r.config.MaxRetries {
				if err := r.contextAwareSleep(ctx, backoffFunc(ctx, attempt+1)); err != nil {
					return nil, err
				}
			}
			continue
		}

		copies := stream.Copy(2)
		checkCopy := copies[0]
		returnCopy := copies[1]

		streamErr := consumeStreamForError(checkCopy)
		if streamErr == nil {
			return returnCopy, nil
		}

		returnCopy.Close()
		if errors.Is(streamErr, ErrStreamCanceled) {
			return nil, streamErr
		}
		if !isRetryAble(ctx, streamErr) {
			return nil, streamErr
		}

		lastErr = streamErr
		if attempt < r.config.MaxRetries {
			if err := r.contextAwareSleep(ctx, backoffFunc(ctx, attempt+1)); err != nil {
				return nil, err
			}
		}
	}

	return nil, &RetryExhaustedError{LastErr: lastErr, TotalRetries: r.config.MaxRetries}
}
