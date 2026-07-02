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

// Package tool defines the tool component interfaces that allow language models
// to invoke external capabilities, and helpers for interrupt/resume within tools.
//
// # Interface Hierarchy
//
//	BaseTool                  — Info() only; for passing tool metadata to a ChatModel
//	├── InvokableTool         — standard: args as JSON string, returns string
//	├── StreamableTool        — standard streaming: args as JSON string, returns StreamReader[string]
//	├── EnhancedInvokableTool — multimodal: args as *schema.ToolArgument, returns *schema.ToolResult
//	└── EnhancedStreamableTool— multimodal streaming
//
// # Choosing an Interface
//
// Implement [InvokableTool] for most tools — arguments arrive as a JSON string
// automatically decoded from the model's tool call, and the result is a string
// sent back to the model.
//
// Implement [EnhancedInvokableTool] when the tool needs to return structured
// multimodal content (images, audio, files) rather than plain text. When a
// tool implements both a standard and an enhanced interface, ToolsNode
// prioritises the enhanced interface.
//
// # Creating Tools
//
// The [utils] sub-package provides constructors that eliminate boilerplate:
//   - [utils.InferTool] / [utils.InferStreamTool] — infer parameter schema from Go struct tags
//   - [utils.NewTool] / [utils.NewStreamTool] — manual ToolInfo + typed function
//
// # Interrupt / Resume
//
// Tools can pause execution and wait for external input using [Interrupt],
// [StatefulInterrupt], and [CompositeInterrupt]. Use [GetInterruptState] and
// [GetResumeContext] inside the tool to distinguish first-run from resumed-run.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/tools_node_guide/
// See https://www.cloudwego.io/docs/eino/core_modules/components/tools_node_guide/how_to_create_a_tool/
//
// Package tool 定义工具组件接口，使语言模型能够调用外部能力，并提供工具内 Interrupt/Resume 的辅助函数。
// # 接口层级
// BaseTool                  — 仅 Info()；用于向 ChatModel 传递工具元数据
// ├── InvokableTool         — 标准：参数为 JSON 字符串，返回 string
// ├── StreamableTool        — 标准流式：参数为 JSON 字符串，返回 StreamReader[string]
// ├── EnhancedInvokableTool — 多模态：参数为 *schema.ToolArgument，返回 *schema.ToolResult
// └── EnhancedStreamableTool— 多模态流式
// # 选择接口
// 多数工具实现 [InvokableTool] 即可 —— 参数以 JSON 字符串传入，由模型的工具调用自动解码，结果作为 string 返回给模型。
// 当工具需要返回结构化多模态内容（图片、音频、文件）而不是纯文本时，实现 [EnhancedInvokableTool]。当工具同时实现标准接口和增强接口时，ToolsNode 会优先使用增强接口。
// # 创建工具
// [utils] 子包提供构造器以消除样板代码：
// - [utils.InferTool] / [utils.InferStreamTool] — 从 Go struct tags 推断参数 schema
// - [utils.NewTool] / [utils.NewStreamTool] — 手动 ToolInfo + 类型化函数
// # Interrupt / Resume
// 工具可以使用 [Interrupt]、[StatefulInterrupt] 和 [CompositeInterrupt] 暂停执行并等待外部输入。在工具内部使用 [GetInterruptState] 和 [GetResumeContext] 区分首次运行与恢复运行。
// See https://www.cloudwego.io/docs/eino/core_modules/components/tools_node_guide/
// See https://www.cloudwego.io/docs/eino/core_modules/components/tools_node_guide/how_to_create_a_tool/
package tool
