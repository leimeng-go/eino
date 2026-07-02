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

// Package callbacks provides observability hooks for component execution in Eino.
//
// Callbacks fire at five lifecycle timings around every component invocation:
//   - [TimingOnStart] / [TimingOnEnd]: non-streaming input and output.
//   - [TimingOnStartWithStreamInput] / [TimingOnEndWithStreamOutput]: streaming
//     variants — handlers receive a copy of the stream and MUST close it.
//   - [TimingOnError]: component returned a non-nil error (stream-internal
//     errors are NOT reported here).
//
// # Attaching Handlers
//
// Global handlers (observe every node in every graph):
//
//	callbacks.AppendGlobalHandlers(myHandler) // call once, at startup — NOT thread-safe
//
// Per-invocation handlers (observe one graph run):
//
//	runnable.Invoke(ctx, input, compose.WithCallbacks(myHandler))
//
// Target a specific node:
//
//	compose.WithCallbacks(myHandler).DesignateNode("nodeName")
//
// Handler inheritance: if the context passed to a graph run already carries
// handlers (e.g. from a parent graph), those handlers are inherited by the
// entire child run automatically.
//
// # Building Handlers
//
// Option 1 — [NewHandlerBuilder]: register raw functions for the timings you
// need. Input/output are untyped; use the component package's ConvCallbackInput
// helper to cast to a concrete type:
//
//	handler := callbacks.NewHandlerBuilder().
//		OnStartFn(func(ctx context.Context, info *RunInfo, input CallbackInput) context.Context {
//			// Handle component start
//			return ctx
//		}).
//		OnEndFn(func(ctx context.Context, info *RunInfo, output CallbackOutput) context.Context {
//			// Handle component end
//			return ctx
//		}).
//		OnErrorFn(func(ctx context.Context, info *RunInfo, err error) context.Context {
//			// Handle component error
//			return ctx
//		}).
//		OnStartWithStreamInputFn(func(ctx context.Context, info *RunInfo, input *schema.StreamReader[CallbackInput]) context.Context {
//			defer input.Close() // MUST close — failure causes pipeline goroutine leak
//			return ctx
//		}).
//		OnEndWithStreamOutputFn(func(ctx context.Context, info *RunInfo, output *schema.StreamReader[CallbackOutput]) context.Context {
//			defer output.Close() // MUST close
//			return ctx
//		}).
//		Build()
//
// Option 2 — utils/callbacks.NewHandlerHelper: dispatches by component type, so
// each handler function receives the concrete typed input/output directly:
//
//	handler := callbacks.NewHandlerHelper().
//		ChatModel(&model.CallbackHandler{
//			OnStart: func(ctx context.Context, info *RunInfo, input *model.CallbackInput) context.Context {
//				log.Printf("Model started: %s, messages: %d", info.Name, len(input.Messages))
//				return ctx
//			},
//		}).
//		Prompt(&prompt.CallbackHandler{
//			OnEnd: func(ctx context.Context, info *RunInfo, output *prompt.CallbackOutput) context.Context {
//				log.Printf("Prompt completed")
//				return ctx
//			},
//		}).
//		Handler()
//
// # Passing State Within a Handler
//
// The ctx returned by one timing is passed to the next timing of the SAME
// handler, enabling OnStart→OnEnd state transfer via context.WithValue:
//
//	NewHandlerBuilder().
//		OnStartFn(func(ctx context.Context, info *RunInfo, _ CallbackInput) context.Context {
//			return context.WithValue(ctx, startTimeKey{}, time.Now())
//		}).
//		OnEndFn(func(ctx context.Context, info *RunInfo, _ CallbackOutput) context.Context {
//			start := ctx.Value(startTimeKey{}).(time.Time)
//			log.Printf("duration: %v", time.Since(start))
//			return ctx
//		}).Build()
//
// Between DIFFERENT handlers there is no guaranteed execution order and no
// context chain. To share state between handlers, store it in a
// concurrency-safe variable in the outermost context instead.
//
// # Common Pitfalls
//
//   - Stream copies must be closed: when N handlers register for a streaming
//     timing, the stream is copied N+1 times (one per handler + one for
//     downstream). If any handler's copy is not closed, the original stream
//     cannot be freed and the entire pipeline leaks.
//
//   - Do NOT mutate Input/Output: all downstream nodes and handlers share the
//     same pointer. Mutations cause data races in concurrent graph execution.
//
//   - AppendGlobalHandlers is NOT thread-safe: call only during initialization,
//     never concurrently with graph execution.
//
//   - Stream errors are invisible to OnError: errors that occur while a
//     consumer reads from a StreamReader are not routed through OnError.
//
//   - RunInfo may be nil: always nil-check before dereferencing in handlers,
//     especially when a component is used standalone outside a graph without
//     InitCallbacks being called.
//
// Package callbacks 为 Eino 中的组件执行提供可观测性钩子。
// 回调会在每次组件调用前后的五个生命周期时机触发：
// - [TimingOnStart] / [TimingOnEnd]：非流式输入和输出。
// - [TimingOnStartWithStreamInput] / [TimingOnEndWithStreamOutput]：流式变体 —— 处理器会收到流的副本，并且 MUST 关闭它。
// - [TimingOnError]：组件返回非 nil 错误（流内部错误不会在这里上报）。
// # 挂载处理器
// 全局处理器（观察每个 graph 中的每个 node）：
// callbacks.AppendGlobalHandlers(myHandler) // 仅在启动时调用一次 —— NOT thread-safe
// 单次调用处理器（观察一次 graph 运行）：
// runnable.Invoke(ctx, input, compose.WithCallbacks(myHandler))
// 指定特定 node：
// compose.WithCallbacks(myHandler).DesignateNode("nodeName")
// 处理器继承：如果传给 graph 运行的 context 已携带处理器（例如来自父 graph），这些处理器会自动被整个子运行继承。
// # 构建处理器
// 选项 1 —— [NewHandlerBuilder]：为所需时机注册原始函数。输入/输出无类型；使用组件包的 ConvCallbackInput helper 转换为具体类型：
// handler := callbacks.NewHandlerBuilder().
// OnStartFn(func(ctx context.Context, info *RunInfo, input CallbackInput) context.Context {
// 处理组件开始
// return ctx
// }).
// OnEndFn(func(ctx context.Context, info *RunInfo, output CallbackOutput) context.Context {
// 处理组件结束
// return ctx
// }).
// OnErrorFn(func(ctx context.Context, info *RunInfo, err error) context.Context {
// 处理组件错误
// return ctx
// }).
// OnStartWithStreamInputFn(func(ctx context.Context, info *RunInfo, input *schema.StreamReader[CallbackInput]) context.Context {
// defer input.Close() // MUST 关闭 —— 否则会导致 pipeline goroutine 泄漏
// return ctx
// }).
// OnEndWithStreamOutputFn(func(ctx context.Context, info *RunInfo, output *schema.StreamReader[CallbackOutput]) context.Context {
// defer output.Close() // MUST 关闭
// return ctx
// }).
// Build()
// 选项 2 —— utils/callbacks.NewHandlerHelper：按组件类型分发，因此每个处理器函数会直接收到具体类型的输入/输出：
// handler := callbacks.NewHandlerHelper().
// ChatModel(&model.CallbackHandler{
// OnStart: func(ctx context.Context, info *RunInfo, input *model.CallbackInput) context.Context {
// log.Printf("Model started: %s, messages: %d", info.Name, len(input.Messages))
// return ctx
// },
// }).
// Prompt(&prompt.CallbackHandler{
// OnEnd: func(ctx context.Context, info *RunInfo, output *prompt.CallbackOutput) context.Context {
// log.Printf("Prompt completed")
// return ctx
// },
// }).
// Handler()
// # 在处理器内传递状态
// 某个时机返回的 ctx 会传给同一个处理器的下一个时机，从而可通过 context.WithValue 实现 OnStart→OnEnd 状态传递：
// NewHandlerBuilder().
// OnStartFn(func(ctx context.Context, info *RunInfo, _ CallbackInput) context.Context {
// return context.WithValue(ctx, startTimeKey{}, time.Now())
// }).
// OnEndFn(func(ctx context.Context, info *RunInfo, _ CallbackOutput) context.Context {
// start := ctx.Value(startTimeKey{}).(time.Time)
// log.Printf("duration: %v", time.Since(start))
// return ctx
// }).Build()
// 不同处理器之间没有保证的执行顺序，也没有 context 链。若要在处理器之间共享状态，请将其存放在最外层 context 中的并发安全变量里。
// # 常见陷阱
// - 流副本必须关闭：当 N 个处理器注册某个流式时机时，流会被复制 N+1 份（每个处理器一份 + 下游一份）。如果任何处理器的副本未关闭，原始流就无法释放，整个 pipeline 会泄漏。
// - 不要修改 Input/Output：所有下游节点和处理器共享同一个指针。修改会在并发 graph 执行中造成数据竞争。
// - AppendGlobalHandlers 不是线程安全的：只在初始化期间调用，绝不要与 graph 执行并发调用。
// - OnError 看不到流错误：消费者读取 StreamReader 时发生的错误不会经过 OnError。
// - RunInfo 可能为 nil：处理器中解引用前务必先检查 nil，尤其是组件在 graph 外独立使用且未调用 InitCallbacks 时。
package callbacks
