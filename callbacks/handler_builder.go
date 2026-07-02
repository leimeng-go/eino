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

	"github.com/cloudwego/eino/schema"
)

// HandlerBuilder constructs a [Handler] by registering callback functions for
// individual timings. Only set the timings you care about; the built handler
// implements [TimingChecker] and returns false for unregistered timings, so
// the framework skips those timings with no overhead.
//
// The input/output values are untyped (CallbackInput / CallbackOutput). To
// work with a specific component's payload, use the component package's
// ConvCallbackInput / ConvCallbackOutput helpers inside your function. For a
// higher-level API that dispatches by component type automatically, see
// utils/callbacks.NewHandlerHelper.
//
// Example:
//
//	handler := callbacks.NewHandlerBuilder().
//	    OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
//	        mi := model.ConvCallbackInput(input)
//	        if mi != nil {
//	            log.Printf("[%s] model start: %d messages", info.Name, len(mi.Messages))
//	        }
//	        return ctx
//	    }).
//	    OnEndFn(func(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
//	        mo := model.ConvCallbackOutput(output)
//	        if mo != nil && mo.Message.ResponseMeta != nil {
//	            log.Printf("[%s] tokens: %d", info.Name, mo.Message.ResponseMeta.Usage.TotalTokens)
//	        }
//	        return ctx
//	    }).
//	    Build()
//
// HandlerBuilder 通过为各个时机注册回调函数来构造 [Handler]。只需设置你关心的时机；构建出的处理器实现 [TimingChecker]，并对未注册的时机返回 false，因此框架会零开销地跳过这些时机。
// 输入/输出值无类型（CallbackInput / CallbackOutput）。若要处理特定组件的载荷，请在函数内使用组件包的 ConvCallbackInput / ConvCallbackOutput helper。若需要按组件类型自动分发的更高层 API，请参见 utils/callbacks.NewHandlerHelper。
// 示例：
// handler := callbacks.NewHandlerBuilder().
// OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
// mi := model.ConvCallbackInput(input)
// if mi != nil {
// log.Printf("[%s] model start: %d messages", info.Name, len(mi.Messages))
// }
// return ctx
// }).
// OnEndFn(func(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
// mo := model.ConvCallbackOutput(output)
// if mo != nil && mo.Message.ResponseMeta != nil {
// log.Printf("[%s] tokens: %d", info.Name, mo.Message.ResponseMeta.Usage.TotalTokens)
// }
// return ctx
// }).
// Build()
type HandlerBuilder struct {
	onStartFn                func(ctx context.Context, info *RunInfo, input CallbackInput) context.Context
	onEndFn                  func(ctx context.Context, info *RunInfo, output CallbackOutput) context.Context
	onErrorFn                func(ctx context.Context, info *RunInfo, err error) context.Context
	onStartWithStreamInputFn func(ctx context.Context, info *RunInfo, input *schema.StreamReader[CallbackInput]) context.Context
	onEndWithStreamOutputFn  func(ctx context.Context, info *RunInfo, output *schema.StreamReader[CallbackOutput]) context.Context
}

type handlerImpl struct {
	HandlerBuilder
}

func (hb *handlerImpl) OnStart(ctx context.Context, info *RunInfo, input CallbackInput) context.Context {
	return hb.onStartFn(ctx, info, input)
}

func (hb *handlerImpl) OnEnd(ctx context.Context, info *RunInfo, output CallbackOutput) context.Context {
	return hb.onEndFn(ctx, info, output)
}

func (hb *handlerImpl) OnError(ctx context.Context, info *RunInfo, err error) context.Context {
	return hb.onErrorFn(ctx, info, err)
}

func (hb *handlerImpl) OnStartWithStreamInput(ctx context.Context, info *RunInfo,
	input *schema.StreamReader[CallbackInput]) context.Context {

	return hb.onStartWithStreamInputFn(ctx, info, input)
}

func (hb *handlerImpl) OnEndWithStreamOutput(ctx context.Context, info *RunInfo,
	output *schema.StreamReader[CallbackOutput]) context.Context {

	return hb.onEndWithStreamOutputFn(ctx, info, output)
}

func (hb *handlerImpl) Needed(_ context.Context, _ *RunInfo, timing CallbackTiming) bool {
	switch timing {
	case TimingOnStart:
		return hb.onStartFn != nil
	case TimingOnEnd:
		return hb.onEndFn != nil
	case TimingOnError:
		return hb.onErrorFn != nil
	case TimingOnStartWithStreamInput:
		return hb.onStartWithStreamInputFn != nil
	case TimingOnEndWithStreamOutput:
		return hb.onEndWithStreamOutputFn != nil
	default:
		return false
	}
}

// NewHandlerBuilder creates and returns a new HandlerBuilder instance.
// HandlerBuilder is used to construct a Handler with custom callback functions
//
// NewHandlerBuilder 创建并返回新的 HandlerBuilder 实例。
// HandlerBuilder 用于构造带自定义回调函数的 Handler
func NewHandlerBuilder() *HandlerBuilder {
	return &HandlerBuilder{}
}

// OnStartFn sets the handler for the start timing.
// OnStartFn 设置开始时机的处理器。
func (hb *HandlerBuilder) OnStartFn(
	fn func(ctx context.Context, info *RunInfo, input CallbackInput) context.Context) *HandlerBuilder {

	hb.onStartFn = fn
	return hb
}

// OnEndFn sets the handler for the end timing.
// OnEndFn 设置结束时机的处理器。
func (hb *HandlerBuilder) OnEndFn(
	fn func(ctx context.Context, info *RunInfo, output CallbackOutput) context.Context) *HandlerBuilder {

	hb.onEndFn = fn
	return hb
}

// OnErrorFn sets the handler for the error timing.
// OnErrorFn 设置错误时机的处理器。
func (hb *HandlerBuilder) OnErrorFn(
	fn func(ctx context.Context, info *RunInfo, err error) context.Context) *HandlerBuilder {

	hb.onErrorFn = fn
	return hb
}

// OnStartWithStreamInputFn sets the callback invoked when a component receives
// streaming input. The handler receives a [*schema.StreamReader] that is a
// private copy; it MUST close the reader after consuming it to avoid goroutine
// and memory leaks.
//
// OnStartWithStreamInputFn 设置组件接收流式输入时调用的回调。处理器会收到一个私有副本 [*schema.StreamReader]；消费完成后 MUST 关闭该读取器，以避免 goroutine 和内存泄漏。
func (hb *HandlerBuilder) OnStartWithStreamInputFn(
	fn func(ctx context.Context, info *RunInfo, input *schema.StreamReader[CallbackInput]) context.Context) *HandlerBuilder {

	hb.onStartWithStreamInputFn = fn
	return hb
}

// OnEndWithStreamOutputFn sets the callback invoked when a component produces
// streaming output. Like OnStartWithStreamInputFn, the handler receives a
// private copy of the stream and MUST close it after reading to prevent
// goroutine and memory leaks. This is the right place to implement streaming
// token-usage accounting or streaming log capture.
//
// OnEndWithStreamOutputFn 设置组件产生流式输出时调用的回调。与 OnStartWithStreamInputFn 一样，处理器会收到流的私有副本，读取完成后 MUST 关闭它，以避免 goroutine 和内存泄漏。这里适合实现流式 token 用量统计或流式日志采集。
func (hb *HandlerBuilder) OnEndWithStreamOutputFn(
	fn func(ctx context.Context, info *RunInfo, output *schema.StreamReader[CallbackOutput]) context.Context) *HandlerBuilder {

	hb.onEndWithStreamOutputFn = fn
	return hb
}

// Build returns a Handler with the functions set in the builder.
// Build 返回包含 builder 中已设置函数的 Handler。
func (hb *HandlerBuilder) Build() Handler {
	return &handlerImpl{*hb}
}
