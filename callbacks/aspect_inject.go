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

package callbacks

import (
	"context"

	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/internal/callbacks"
	"github.com/cloudwego/eino/schema"
)

// OnStart Fast inject callback input / output aspect for component developer
// e.g.
//
//	func (t *testChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (resp *schema.Message, err error) {
//		defer func() {
//			if err != nil {
//				callbacks.OnError(ctx, err)
//			}
//		}()
//
//		ctx = callbacks.OnStart(ctx, &model.CallbackInput{
//			Messages: input,
//			Tools:    nil,
//			Extra:    nil,
//		})
//
//		// do smt
//
//		ctx = callbacks.OnEnd(ctx, &model.CallbackOutput{
//			Message: resp,
//			Extra:   nil,
//		})
//
//		return resp, nil
//	}
//
// OnStart 为组件开发者快速注入回调输入/输出 aspect
// 例如：
// func (t *testChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (resp *schema.Message, err error) {
// defer func() {
// if err != nil {
// callbacks.OnError(ctx, err)
// }
// }()
// ctx = callbacks.OnStart(ctx, &model.CallbackInput{
// Messages: input,
// Tools:    nil,
// Extra:    nil,
// })
// 执行 smt
// ctx = callbacks.OnEnd(ctx, &model.CallbackOutput{
// Message: resp,
// Extra:   nil,
// })
// return resp, nil
// }

// OnStart invokes the OnStart timing for all registered handlers in the
// context. This is called by component implementations that manage their own
// callbacks (i.e. implement [components.Checker] and return true from
// IsCallbacksEnabled). The returned context must be propagated to subsequent
// OnEnd/OnError calls so handlers can correlate start and end events.
//
// Handlers are invoked in reverse registration order (last registered = first
// called) to match the middleware wrapping convention.
//
// Example — typical usage inside a component's Generate method:
//
//	func (m *myChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
//	    ctx = callbacks.OnStart(ctx, &model.CallbackInput{Messages: input})
//	    resp, err := m.doGenerate(ctx, input, opts...)
//	    if err != nil {
//	        callbacks.OnError(ctx, err)
//	        return nil, err
//	    }
//	    callbacks.OnEnd(ctx, &model.CallbackOutput{Message: resp})
//	    return resp, nil
//	}
//
// OnStart 会为 context 中所有已注册的处理器触发 OnStart 时机。
// 由自行管理回调的组件实现调用（即实现 [components.Checker]，并且
// IsCallbacksEnabled 返回 true）。返回的 context 必须继续传递给后续
// OnEnd/OnError 调用，以便处理器关联开始和结束事件。
// 处理器按注册顺序的逆序调用（最后注册 = 最先调用），
// 以匹配 middleware 包装约定。
// 示例——组件 Generate 方法中的典型用法：
// func (m *myChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
// ctx = callbacks.OnStart(ctx, &model.CallbackInput{Messages: input})
// resp, err := m.doGenerate(ctx, input, opts...)
// if err != nil {
// callbacks.OnError(ctx, err)
// return nil, err
// }
// callbacks.OnEnd(ctx, &model.CallbackOutput{Message: resp})
// return resp, nil
// }
func OnStart[T any](ctx context.Context, input T) context.Context {
	ctx, _ = callbacks.On(ctx, input, callbacks.OnStartHandle[T], TimingOnStart, true)

	return ctx
}

// OnEnd invokes the OnEnd timing for all registered handlers. Call this after
// the component produces a successful result. Handlers run in registration
// order (first registered = first called).
//
// Do not call both OnEnd and OnError for the same invocation — OnEnd signals
// success; OnError signals failure.
//
// OnEnd 会为所有已注册的处理器触发 OnEnd 时机。应在组件成功产出结果后调用。处理器按注册顺序运行（先注册 = 先调用）。
// 不要在同一次调用中同时调用 OnEnd 和 OnError —— OnEnd 表示成功；OnError 表示失败。
func OnEnd[T any](ctx context.Context, output T) context.Context {
	ctx, _ = callbacks.On(ctx, output, callbacks.OnEndHandle[T], TimingOnEnd, false)

	return ctx
}

// OnStartWithStreamInput invokes the OnStartWithStreamInput timing. Use this
// when the component's input is itself a stream (Collect / Transform
// paradigms). The framework automatically copies the stream so each handler
// receives an independent reader; handlers MUST close their copy or the
// underlying goroutine will leak.
//
// Returns the updated context and a new StreamReader that the component should
// use going forward (the original is consumed by the framework).
//
// OnStartWithStreamInput 会触发 OnStartWithStreamInput 时机。当组件输入本身是流（Collect / Transform 范式）时使用。框架会自动复制流，使每个处理器收到独立的读取器；处理器 MUST 关闭自己的副本，否则底层 goroutine 会泄漏。
// 返回更新后的 context，以及组件后续应使用的新 StreamReader（原始读取器已被框架消费）。
func OnStartWithStreamInput[T any](ctx context.Context, input *schema.StreamReader[T]) (
	nextCtx context.Context, newStreamReader *schema.StreamReader[T]) {

	return callbacks.On(ctx, input, callbacks.OnStartWithStreamInputHandle[T], TimingOnStartWithStreamInput, true)
}

// OnEndWithStreamOutput invokes the OnEndWithStreamOutput timing. Use this
// when the component produces a streaming output (Stream / Transform
// paradigms). Like OnStartWithStreamInput, stream copies are made per
// handler; each handler must close its copy.
//
// Returns the updated context and the StreamReader the component should return
// to its caller.
//
// OnEndWithStreamOutput 会触发 OnEndWithStreamOutput 时机。当组件产生流式输出（Stream / Transform 范式）时使用。与 OnStartWithStreamInput 一样，会为每个处理器复制流；每个处理器都必须关闭自己的副本。
// 返回更新后的 context，以及组件应返回给调用方的 StreamReader。
func OnEndWithStreamOutput[T any](ctx context.Context, output *schema.StreamReader[T]) (
	nextCtx context.Context, newStreamReader *schema.StreamReader[T]) {

	return callbacks.On(ctx, output, callbacks.OnEndWithStreamOutputHandle[T], TimingOnEndWithStreamOutput, false)
}

// OnError invokes the OnError timing for all registered handlers. Call this
// when the component returns an error. Errors that occur mid-stream (after the
// StreamReader has been returned) are NOT routed through OnError; they surface
// as errors inside Recv.
//
// Handlers run in registration order (same as OnEnd).
//
// OnError 会为所有已注册的处理器触发 OnError 时机。当组件返回错误时调用。流中途发生的错误（StreamReader 返回之后）不会经过 OnError；它们会以 Recv 内部的错误形式暴露。
// 处理器按注册顺序运行（与 OnEnd 相同）。
func OnError(ctx context.Context, err error) context.Context {
	ctx, _ = callbacks.On(ctx, err, callbacks.OnErrorHandle, TimingOnError, false)

	return ctx
}

// EnsureRunInfo ensures the context carries a [RunInfo] for the given type and
// component kind. If the context already has a matching RunInfo, it is
// returned unchanged. Otherwise, a new callback manager is created that
// inherits the global handlers plus any handlers already in ctx.
//
// Component implementations that set IsCallbacksEnabled() = true should call
// this at the start of every public method (Generate, Stream, etc.) before
// calling [OnStart], so that the RunInfo is never missing from callbacks.
//
// EnsureRunInfo 确保 context 携带给定类型和组件类别的 [RunInfo]。如果 context 已有匹配的 RunInfo，则原样返回。否则会创建新的回调管理器，继承全局处理器以及 ctx 中已有的处理器。
// 设置 IsCallbacksEnabled() = true 的组件实现，应在每个公开方法（Generate、Stream 等）开头、调用 [OnStart] 之前调用它，以确保回调中不会缺少 RunInfo。
func EnsureRunInfo(ctx context.Context, typ string, comp components.Component) context.Context {
	return callbacks.EnsureRunInfo(ctx, typ, comp)
}

// ReuseHandlers creates a new context that inherits all handlers already
// present in ctx and sets a new RunInfo. Global handlers are added if ctx
// carries none yet.
//
// Use this when a component calls another component internally and wants the
// inner component's callbacks to share the same set of handlers as the outer
// component, but with the inner component's own identity in RunInfo:
//
//	innerCtx := callbacks.ReuseHandlers(ctx, &callbacks.RunInfo{
//	    Type:      "InnerChatModel",
//	    Component: components.ComponentOfChatModel,
//	    Name:      "inner-cm",
//	})
//
// ReuseHandlers 创建一个新的 context，继承 ctx 中已有的所有处理器，并设置新的 RunInfo。如果 ctx 尚未携带任何处理器，则会添加全局处理器。
// 当组件在内部调用另一个组件，并希望内部组件的回调与外部组件共享同一组处理器，但在 RunInfo 中使用内部组件自身身份时使用：
// innerCtx := callbacks.ReuseHandlers(ctx, &callbacks.RunInfo{
// Type:      "InnerChatModel",
// Component: components.ComponentOfChatModel,
// Name:      "inner-cm",
// })
func ReuseHandlers(ctx context.Context, info *RunInfo) context.Context {
	return callbacks.ReuseHandlers(ctx, info)
}

// InitCallbacks creates a new context with the given RunInfo and handlers,
// completely replacing any RunInfo and handlers already in ctx.
//
// Use this when running a component standalone outside a Graph — the Graph
// normally manages RunInfo injection automatically, but standalone callers must
// set it up themselves:
//
//	ctx = callbacks.InitCallbacks(ctx, &callbacks.RunInfo{
//	    Type:      myModel.GetType(),
//	    Component: components.ComponentOfChatModel,
//	    Name:      "my-model",
//	}, myHandler)
//
// InitCallbacks 使用给定的 RunInfo 和处理器创建新的 context，完全替换 ctx 中已有的任何 RunInfo 和处理器。
// 在 Graph 之外独立运行组件时使用 —— Graph 通常会自动管理 RunInfo 注入，但独立调用方必须自行设置：
// ctx = callbacks.InitCallbacks(ctx, &callbacks.RunInfo{
// Type:      myModel.GetType(),
// Component: components.ComponentOfChatModel,
// Name:      "my-model",
// }, myHandler)
func InitCallbacks(ctx context.Context, info *RunInfo, handlers ...Handler) context.Context {
	return callbacks.InitCallbacks(ctx, info, handlers...)
}
