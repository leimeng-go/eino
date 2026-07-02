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

package tool

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

// BaseTool provides the metadata that a ChatModel uses to decide whether and
// how to call a tool. Info returns a [schema.ToolInfo] containing the tool
// name, description, and parameter JSON schema.
//
// BaseTool alone is sufficient when passing tool definitions to a ChatModel
// via WithTools — the model only needs the schema to generate tool calls.
// To also execute the tool, implement [InvokableTool] or [StreamableTool].
//
// BaseTool 提供 ChatModel 用来判断是否以及如何调用工具的元数据。Info 返回包含工具名称、描述和参数 JSON schema 的 [schema.ToolInfo]。
// 通过 WithTools 将工具定义传给 ChatModel 时，仅 BaseTool 就足够了 —— 模型只需要 schema 来生成工具调用。若还要执行工具，请实现 [InvokableTool] 或 [StreamableTool]。
type BaseTool interface {
	Info(ctx context.Context) (*schema.ToolInfo, error)
}

// InvokableTool is a tool that can be executed by ToolsNode.
//
// InvokableRun receives the model's tool call arguments as a JSON-encoded
// string and returns a plain string result that is sent back to the model as
// a tool message. The framework handles JSON decoding automatically when using
// the [utils.InferTool] or [utils.NewTool] constructors.
//
// InvokableTool 是可由 ToolsNode 执行的工具。
// InvokableRun 以 JSON 编码字符串接收模型的工具调用参数，并返回纯字符串结果，该结果会作为工具消息发送回模型。使用 [utils.InferTool] 或 [utils.NewTool] 构造器时，框架会自动处理 JSON 解码。
type InvokableTool interface {
	BaseTool

	// InvokableRun executes the tool with arguments encoded as a JSON string.
	// InvokableRun 使用编码为 JSON 字符串的参数执行工具。
	InvokableRun(ctx context.Context, argumentsInJSON string, opts ...Option) (string, error)
}

// StreamableTool is a streaming variant of [InvokableTool].
//
// StreamableRun returns a [schema.StreamReader] that yields string chunks
// incrementally. The caller (ToolsNode) is responsible for closing the reader.
//
// StreamableTool 是 [InvokableTool] 的流式变体。
// StreamableRun 返回一个 [schema.StreamReader]，用于逐步产出 string 分片。调用方（ToolsNode）负责关闭该 reader。
type StreamableTool interface {
	BaseTool

	StreamableRun(ctx context.Context, argumentsInJSON string, opts ...Option) (*schema.StreamReader[string], error)
}

// EnhancedInvokableTool is a tool that returns structured multimodal results.
//
// Unlike [InvokableTool], arguments arrive as a [schema.ToolArgument] (not a
// raw JSON string) and the result is a [schema.ToolResult] which can carry
// text, images, audio, video, and file content.
//
// When a tool implements both a standard and an enhanced interface, ToolsNode
// prioritises the enhanced interface.
//
// EnhancedInvokableTool 是返回结构化多模态结果的工具。
// 不同于 [InvokableTool]，参数以 [schema.ToolArgument]（不是原始 JSON 字符串）传入，结果为 [schema.ToolResult]，可携带文本、图片、音频、视频和文件内容。
// 当工具同时实现标准接口和增强接口时，ToolsNode 会优先使用增强接口。
type EnhancedInvokableTool interface {
	BaseTool
	InvokableRun(ctx context.Context, toolArgument *schema.ToolArgument, opts ...Option) (*schema.ToolResult, error)
}

// EnhancedStreamableTool is the streaming variant of [EnhancedInvokableTool].
//
// It streams [schema.ToolResult] chunks, enabling incremental multimodal
// output. The caller is responsible for closing the returned [schema.StreamReader].
//
// EnhancedStreamableTool 是 [EnhancedInvokableTool] 的流式变体。
// 它流式输出 [schema.ToolResult] 分片，支持增量多模态输出。调用方负责关闭返回的 [schema.StreamReader]。
type EnhancedStreamableTool interface {
	BaseTool
	StreamableRun(ctx context.Context, toolArgument *schema.ToolArgument, opts ...Option) (*schema.StreamReader[*schema.ToolResult], error)
}
