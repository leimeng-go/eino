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

// Package schema defines the core data structures and utilities shared across
// all Eino components.
//
// # Key Types
//
// [Message] is the universal unit of communication between users, models, and
// tools. It carries role, text content, multimodal media, tool calls, and
// response metadata. Helper constructors — [UserMessage], [SystemMessage],
// [AssistantMessage], [ToolMessage] — cover the most common cases.
//
// [Document] represents a piece of text with a metadata map. Typed accessors
// (Score, SubIndexes, DenseVector, SparseVector, DSLInfo, ExtraInfo) read and
// write well-known metadata keys so pipeline stages can pass structured data
// without coupling to specific struct types.
//
// [ToolInfo] describes a tool's name, description, and parameter schema.
// Parameters can be declared either as a [ParameterInfo] map (simple, struct-
// like) or as a raw [jsonschema.Schema] (full JSON Schema 2020-12 expressiveness).
// [ToolChoice] controls whether the model must, may, or must not call tools.
//
// # Streaming
//
// [StreamReader] and [StreamWriter] are the building blocks for streaming data
// through Eino pipelines. Create a linked pair with [Pipe]:
//
//	sr, sw := schema.Pipe[*schema.Message](10)
//	go func() {
//		defer sw.Close()
//		sw.Send(chunk, nil)
//	}()
//	defer sr.Close()
//	for {
//		chunk, err := sr.Recv()
//		if errors.Is(err, io.EOF) { break }
//	}
//
// Important constraints:
//   - A StreamReader is read-once: only one goroutine may call Recv.
//   - Always call Close, even when the loop ends on io.EOF, to release resources.
//   - To give the same stream to multiple consumers, call [StreamReader.Copy].
//
// # Four Streaming Paradigms
//
// Eino components and Lambda functions are classified by their input/output
// streaming shape. The framework automatically bridges mismatches:
//
//   - Invoke: non-streaming in, non-streaming out (ping-pong).
//   - Stream: non-streaming in, StreamReader out (server-streaming). ChatModel
//     and Tool support this.
//   - Collect: StreamReader in, non-streaming out (client-streaming). Useful
//     for branch conditions that decide after the first chunk.
//   - Transform: StreamReader in, StreamReader out (bidirectional).
//
// When an upstream node outputs T but a downstream node only accepts
// StreamReader[T], the framework wraps T in a single-chunk StreamReader —
// this is called a "fake stream". It satisfies the interface but does NOT
// reduce time-to-first-chunk. Conversely, when a downstream node only accepts
// T but the upstream outputs StreamReader[T], the framework automatically
// concatenates the stream into a complete T.
//
// Utility functions:
//   - [StreamReaderFromArray] wraps a slice as a stream (useful in tests).
//   - [MergeStreamReaders] fans-in multiple streams into one.
//   - [MergeNamedStreamReaders] like MergeStreamReaders but emits [SourceEOF]
//     when each named source ends, useful for tracking per-source completion.
//   - [StreamReaderWithConvert] transforms element types; return [ErrNoValue]
//     from the convert function to skip an element.
//
// See https://www.cloudwego.io/docs/eino/core_modules/chain_and_graph_orchestration/stream_programming_essentials/
//
// Package schema 定义所有 Eino 组件共享的核心数据结构和工具。
// # Key Types
// [Message] 是用户、模型和工具之间通信的通用单元。它承载 role、文本内容、多模态媒体、工具调用和响应元数据。辅助构造函数 — [UserMessage]、[SystemMessage]、[AssistantMessage]、[ToolMessage] — 覆盖最常见的场景。
// [Document] 表示一段文本及其元数据 map。类型化访问器（Score、SubIndexes、DenseVector、SparseVector、DSLInfo、ExtraInfo）读写约定的元数据键，使流水线阶段无需耦合到特定结构体类型即可传递结构化数据。
// [ToolInfo] 描述工具的名称、描述和参数 schema。参数既可以声明为 [ParameterInfo] map（简单、类似 struct），也可以声明为原始 [jsonschema.Schema]（完整 JSON Schema 2020-12 表达能力）。[ToolChoice] 控制模型必须、可以或不得调用工具。
// # Streaming
// [StreamReader] 和 [StreamWriter] 是在 Eino 流水线中传递流式数据的构建块。使用 [Pipe] 创建一对关联对象：
// sr, sw := schema.Pipe[*schema.Message](10)
// go func() {
// defer sw.Close()
// sw.Send(chunk, nil)
// }()
// defer sr.Close()
// for {
// chunk, err := sr.Recv()
// if errors.Is(err, io.EOF) { break }
// }
// 重要约束：
// - StreamReader 只能读取一次：只能有一个 goroutine 调用 Recv。
// - 即使循环因 io.EOF 结束，也始终调用 Close 以释放资源。
// - 要将同一个流提供给多个消费者，请调用 [StreamReader.Copy]。
// # Four Streaming Paradigms
// Eino 组件和 Lambda 函数按其输入/输出流式形态分类。框架会自动桥接不匹配的形态：
// - Invoke：非流式输入，非流式输出（ping-pong）。
// - Stream：非流式输入，StreamReader 输出（server-streaming）。ChatModel 和 Tool 支持此模式。
// - Collect：StreamReader 输入，非流式输出（client-streaming）。适用于读取首个 chunk 后再决定的分支条件。
// - Transform：StreamReader 输入，StreamReader 输出（双向）。
// 当上游节点输出 T，而下游节点只接受 StreamReader[T] 时，框架会将 T 包装成单 chunk 的 StreamReader —— 这称为 "fake stream"。它满足接口，但不会减少 time-to-first-chunk。反之，当下游节点只接受 T，而上游输出 StreamReader[T] 时，框架会自动将流拼接为完整的 T。
// 工具函数：
// - [StreamReaderFromArray] 将 slice 包装为流（测试中很有用）。
// - [MergeStreamReaders] 将多个流 fan-in 为一个。
// - [MergeNamedStreamReaders] 类似 MergeStreamReaders，但会在每个具名 source 结束时发出 [SourceEOF]，便于跟踪每个 source 的完成情况。
// - [StreamReaderWithConvert] 转换元素类型；从 convert 函数返回 [ErrNoValue] 可跳过某个元素。
// 参见 https://www.cloudwego.io/docs/eino/core_modules/chain_and_graph_orchestration/stream_programming_essentials/
package schema
