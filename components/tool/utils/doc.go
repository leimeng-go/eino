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

// Package utils provides constructors for building tool implementations without
// writing boilerplate JSON serialization code.
//
// # Choosing a Constructor
//
// There are two main strategies:
//
//  1. Infer from struct tags (recommended): [InferTool], [InferStreamTool],
//     [InferEnhancedTool], [InferEnhancedStreamTool].
//     The parameter JSON schema is derived automatically from the input struct's
//     field names and tags. Requires a typed input struct.
//
//  2. Manual ToolInfo: [NewTool], [NewStreamTool], [NewEnhancedTool],
//     [NewEnhancedStreamTool].
//     You supply a [schema.ToolInfo] directly. Useful when the schema cannot
//     be expressed as a Go struct, or must be dynamically constructed.
//
// # Struct Tag Convention
//
// InferTool and friends use the following tags on the input struct fields:
//
//	type Input struct {
//	    Query    string `json:"query"     jsonschema:"required"             jsonschema_description:"The search query"`
//	    MaxItems int    `json:"max_items"                                   jsonschema_description:"Maximum results to return"`
//	}
//
// Key rules:
//   - Use a separate jsonschema_description tag for field descriptions —
//     embedding descriptions inside the jsonschema tag causes comma-parsing
//     issues.
//   - Use jsonschema:"required" to mark mandatory parameters.
//   - The json tag controls the parameter name visible to the model.
//
// # Schema Utilities
//
// [GoStruct2ToolInfo] and [GoStruct2ParamsOneOf] convert a Go struct to schema
// types without creating a tool — useful for ChatModel structured output via
// ResponseFormat or BindTools.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/tools_node_guide/how_to_create_a_tool/
//
// Package utils 提供用于构建工具实现的构造器，无需编写样板 JSON 序列化代码。
// # 选择构造器
// 主要有两种策略：
// 1. 从 struct tag 推断（推荐）：[InferTool]、[InferStreamTool]、[InferEnhancedTool]、[InferEnhancedStreamTool]。
// 参数 JSON schema 会根据输入 struct 的字段名和标签自动生成。需要类型化的输入 struct。
// 2. 手动 ToolInfo：[NewTool]、[NewStreamTool]、[NewEnhancedTool]、[NewEnhancedStreamTool]。
// 你需要直接提供 [schema.ToolInfo]。适用于 schema 无法表示为 Go struct，或必须动态构造的场景。
// # Struct Tag 约定
// InferTool 及相关函数在输入 struct 字段上使用以下标签：
// type Input struct {
// Query    string `json:"query"     jsonschema:"required"             jsonschema_description:"The search query"`
// MaxItems int    `json:"max_items"                                   jsonschema_description:"Maximum results to return"`
// }
// 关键规则：
// - 字段描述请使用单独的 jsonschema_description 标签 ——
// 将描述嵌入 jsonschema 标签会导致逗号解析问题。
// - 使用 jsonschema:"required" 标记必填参数。
// - json 标签控制模型可见的参数名。
// # Schema 工具
// [GoStruct2ToolInfo] 和 [GoStruct2ParamsOneOf] 可将 Go struct 转换为 schema 类型，
// 而不创建工具 —— 适用于通过 ResponseFormat 或 BindTools 实现 ChatModel 结构化输出。
// 参见 https://www.cloudwego.io/docs/eino/core_modules/components/tools_node_guide/how_to_create_a_tool/
package utils
