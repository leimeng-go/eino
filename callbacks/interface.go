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
	"github.com/cloudwego/eino/internal/callbacks"
)

// RunInfo describes the entity that triggered a callback. Always nil-check
// before dereferencing — a component that calls OnStart without first calling
// EnsureRunInfo or InitCallbacks will leave RunInfo absent in the context.
//
// Fields:
//   - Name: business-meaningful name specified by the user. For nodes in a
//     graph this is the node name (compose.WithNodeName). For standalone
//     components it must be set explicitly via [InitCallbacks] or
//     [ReuseHandlers]; it is empty string if not set.
//   - Type: implementation identity, e.g. "OpenAI". Set by the component via
//     [components.Typer]; falls back to reflection (struct/func name) if the
//     interface is not implemented. Empty for Graph itself.
//   - Component: category constant, e.g. components.ComponentOfChatModel.
//     Fixed value "Lambda" for lambdas, "Graph"/"Chain"/"Workflow" for graphs.
//     Use this to branch on component kind without caring about implementation.
//
// Handlers should filter using RunInfo rather than assuming a fixed execution
// order — there is no guaranteed ordering between different Handlers.
//
// RunInfo 描述触发回调的实体。解引用前务必先检查 nil —— 如果组件调用 OnStart 前未先调用 EnsureRunInfo 或 InitCallbacks，context 中将缺少 RunInfo。
// 字段：
// - Name：用户指定的业务含义名称。对于 graph 中的 node，这是 node 名称（compose.WithNodeName）。对于独立组件，必须通过 [InitCallbacks] 或 [ReuseHandlers] 显式设置；未设置时为空字符串。
// - Type：实现身份，例如 "OpenAI"。由组件通过 [components.Typer] 设置；如果未实现该接口，则回退到反射（struct/func 名称）。Graph 本身为空。
// - Component：类别常量，例如 components.ComponentOfChatModel。lambda 固定值为 "Lambda"，graph 为 "Graph"/"Chain"/"Workflow"。用于按组件类别分支，而无需关心具体实现。
// 处理器应使用 RunInfo 进行过滤，而不是假设固定执行顺序 —— 不同 Handler 之间没有保证的顺序。
type RunInfo = callbacks.RunInfo

// CallbackInput is the value passed to OnStart and OnStartWithStreamInput
// handlers. The concrete type is defined by the component — for example,
// ChatModel callbacks carry *model.CallbackInput. Use the component package's
// ConvCallbackInput helper (e.g. model.ConvCallbackInput) to cast safely; it
// returns nil if the type does not match, so you can ignore irrelevant
// component types:
//
//	modelInput := model.ConvCallbackInput(in)
//	if modelInput == nil {
//	    return ctx // not a model invocation, skip
//	}
//	log.Printf("prompt: %v", modelInput.Messages)
//
// CallbackInput 是传给 OnStart 和 OnStartWithStreamInput 处理器的值。具体类型由组件定义 —— 例如 ChatModel 回调携带 *model.CallbackInput。使用组件包的 ConvCallbackInput helper（如 model.ConvCallbackInput）安全转换；类型不匹配时返回 nil，因此可忽略无关组件类型：
// modelInput := model.ConvCallbackInput(in)
// if modelInput == nil {
// return ctx // 非 model 调用，跳过
// }
// log.Printf("prompt: %v", modelInput.Messages)
type CallbackInput = callbacks.CallbackInput

// CallbackOutput is the value passed to OnEnd and OnEndWithStreamOutput
// handlers. Like CallbackInput, the concrete type is component-defined.
// Use the component package's ConvCallbackOutput helper to cast safely.
//
// CallbackOutput 是传给 OnEnd 和 OnEndWithStreamOutput 处理器的值。与 CallbackInput 一样，具体类型由组件定义。
// 使用组件包的 ConvCallbackOutput helper 安全转换。
type CallbackOutput = callbacks.CallbackOutput

// Handler is the unified callback handler interface. Implement all five
// methods (OnStart, OnEnd, OnError, OnStartWithStreamInput,
// OnEndWithStreamOutput) or use [NewHandlerBuilder] to set only the timings
// you care about.
//
// Each method receives the context returned by the previous timing of the
// SAME handler, which lets a single handler pass state between its OnStart
// and OnEnd calls via context.WithValue. There is NO guaranteed execution
// order between DIFFERENT handlers, and the context chain does not flow
// from one handler to the next — do not rely on handler ordering.
//
// Implement [TimingChecker] (the Needed method) on your handler so the
// framework can skip timings you have not registered; this avoids unnecessary
// stream copies and goroutine allocations on every component invocation.
//
// Stream handlers (OnStartWithStreamInput, OnEndWithStreamOutput) receive a
// [*schema.StreamReader] that has already been copied; they MUST close their
// copy after reading. If any handler's copy is not closed, the original stream
// cannot be freed, causing a goroutine/memory leak for the entire pipeline.
//
// Important: do NOT mutate the Input or Output values. All downstream nodes
// and handlers share the same pointer (direct assignment, not a deep copy).
// Mutations cause data races in concurrent graph execution.
//
// Handler 是统一的回调处理器接口。实现全部五个方法（OnStart、OnEnd、OnError、OnStartWithStreamInput、OnEndWithStreamOutput），或使用 [NewHandlerBuilder] 只设置你关心的时机。
// 每个方法都会接收同一处理器上一个时机返回的 context，使单个处理器可通过 context.WithValue 在 OnStart 和 OnEnd 之间传递状态。不同处理器之间没有保证的执行顺序，context 链也不会从一个处理器流向下一个处理器——不要依赖处理器顺序。
// 在你的处理器上实现 [TimingChecker]（Needed 方法），以便框架跳过未注册的时机；这可避免每次组件调用时不必要的流复制和 goroutine 分配。
// 流处理器（OnStartWithStreamInput、OnEndWithStreamOutput）会收到已复制的 [*schema.StreamReader]；读取后必须关闭自己的副本。若任一处理器的副本未关闭，原始流就无法释放，导致整个 pipeline 的 goroutine/内存泄漏。
// 重要：不要修改 Input 或 Output 值。所有下游节点和处理器共享同一个指针（直接赋值，不是深拷贝）。修改会在并发图执行中造成数据竞争。
type Handler = callbacks.Handler

// InitCallbackHandlers sets the global callback handlers.
// It should be called BEFORE any callback handler by user.
// It's useful when you want to inject some basic callbacks to all nodes.
// Deprecated: Use AppendGlobalHandlers instead.
//
// InitCallbackHandlers 设置全局回调处理器。
// 应在用户使用任何回调处理器之前调用。
// 当你想向所有节点注入一些基础回调时很有用。
// Deprecated: Use AppendGlobalHandlers instead.
func InitCallbackHandlers(handlers []Handler) {
	callbacks.GlobalHandlers = handlers
}

// AppendGlobalHandlers appends handlers to the process-wide list of callback
// handlers. Global handlers run before per-invocation handlers provided via
// compose.WithCallbacks, giving them higher priority for instrumentation that
// must observe every component invocation (e.g. distributed tracing, metrics).
//
// This function is NOT thread-safe. Call it once during program initialization
// (e.g. in main or TestMain), before any graph executions begin.
// Calling it concurrently with ongoing graph executions leads to data races.
//
// AppendGlobalHandlers 将处理器追加到进程级回调处理器列表。全局处理器会先于通过 compose.WithCallbacks 提供的单次调用处理器运行，因此对必须观测每次组件调用的 instrumentation（如分布式追踪、指标）具有更高优先级。
// 此函数不是线程安全的。请在程序初始化期间调用一次（如 main 或 TestMain 中），并在任何图执行开始之前完成。
// 若与正在进行的图执行并发调用，会导致数据竞争。
func AppendGlobalHandlers(handlers ...Handler) {
	callbacks.GlobalHandlers = append(callbacks.GlobalHandlers, handlers...)
}

// CallbackTiming enumerates the lifecycle moments at which a callback handler
// is invoked. Implement [TimingChecker] on your handler and return false for
// timings you do not handle, so the framework skips the overhead of stream
// copying and goroutine spawning for those timings.
//
// CallbackTiming 枚举回调处理器被调用的生命周期时刻。在你的处理器上实现 [TimingChecker]，并对不处理的时机返回 false，以便框架跳过这些时机的流复制和 goroutine 启动开销。
type CallbackTiming = callbacks.CallbackTiming

// Callback timing constants.
// Callback 时机常量。
const (
	// TimingOnStart fires just before the component begins processing.
	// Receives a fully-formed input value (non-streaming).
	//
	// TimingOnStart 在组件开始处理前触发。
	// 接收完整构造的输入值（非流式）。
	TimingOnStart CallbackTiming = iota
	// TimingOnEnd fires after the component returns a result successfully.
	// Receives the output value. Only fires on success — not on error.
	//
	// TimingOnEnd 在组件成功返回结果后触发。
	// 接收输出值。仅在成功时触发——错误时不触发。
	TimingOnEnd
	// TimingOnError fires when the component returns a non-nil error.
	// Stream errors (mid-stream panics) are NOT reported here; they surface
	// as errors inside the stream reader.
	//
	// TimingOnError 在组件返回非 nil 错误时触发。
	// 流错误（流中途 panic）不会在这里报告；它们会作为流读取器中的错误暴露。
	TimingOnError
	// TimingOnStartWithStreamInput fires when the component receives a
	// streaming input (Collect / Transform paradigms). The handler receives a
	// copy of the input stream and must close it after reading.
	//
	// TimingOnStartWithStreamInput 在组件接收流式输入时触发（Collect / Transform 范式）。处理器会收到输入流的副本，读取后必须关闭。
	TimingOnStartWithStreamInput
	// TimingOnEndWithStreamOutput fires after the component returns a
	// streaming output (Stream / Transform paradigms). The handler receives a
	// copy of the output stream and must close it after reading. This is
	// typically where you implement streaming metrics or logging.
	//
	// TimingOnEndWithStreamOutput 在组件返回流式输出后触发（Stream / Transform 范式）。处理器会收到输出流的副本，读取后必须关闭。通常在这里实现流式指标或日志。
	TimingOnEndWithStreamOutput
)

// TimingChecker is an optional interface for [Handler] implementations.
// When a handler implements Needed, the framework calls it before each
// component invocation to decide whether to set up callback infrastructure
// (stream copying, goroutine allocation) for that timing. Returning false
// avoids unnecessary overhead.
//
// Handlers built with [NewHandlerBuilder] or
// utils/callbacks.NewHandlerHelper automatically implement TimingChecker
// based on which callback functions were set.
//
// TimingChecker 是 [Handler] 实现可选的接口。
// 当处理器实现 Needed 时，框架会在每次组件调用前调用它，以决定是否为该时机设置回调基础设施（流复制、goroutine 分配）。返回 false 可避免不必要的开销。
// 使用 [NewHandlerBuilder] 或 utils/callbacks.NewHandlerHelper 构建的处理器，会根据已设置的回调函数自动实现 TimingChecker。
type TimingChecker = callbacks.TimingChecker
