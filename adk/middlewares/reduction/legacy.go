/*
 * Copyright 2026 CloudWeGo Authors
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

package reduction

import "github.com/cloudwego/eino/adk/middlewares/reduction/internal"

// Package reduction provides historical compatibility exports for reduction middleware APIs.
//
// DEPRECATED: All top-level exports in this file are maintained exclusively for backward compatibility.
// New reduction middleware implementations are now developed and maintained in this package.
// It is STRONGLY RECOMMENDED that new code directly use the New instead.
//
// Existing code relying on these exports will continue to work indefinitely,
// but no new features or bug fixes will be backported to this compatibility layer.
//
// Package reduction 为 reduction 中间件 API 提供历史兼容导出。
// DEPRECATED: 此文件中的所有顶层导出仅为向后兼容而保留。
// 新的 reduction 中间件实现现在在此 package 中开发和维护。
// 强烈建议新代码直接使用 New。
// 依赖这些导出的现有代码将无限期继续可用，
// 但不会向该兼容层回移植新功能或 bug 修复。

type (
	ClearToolResultConfig = internal.ClearToolResultConfig
	ToolResultConfig      = internal.ToolResultConfig
	Backend               = internal.Backend
)

var (
	// NewClearToolResult creates a new middleware that clears old tool results
	// based on token thresholds while protecting recent messages.
	//
	// Deprecated: Use New instead, which provides a more comprehensive tool result reduction
	// middleware with both truncation and clearing strategies. New returns a ChatModelAgentMiddleware
	// for better context propagation through wrapper methods.
	//
	// NewClearToolResult 创建一个新中间件，基于 token 阈值清除旧工具结果，同时保护近期消息。
	// Deprecated: 请使用 New，它提供更完整的工具结果缩减中间件，包含截断和清除两种策略。New 返回 ChatModelAgentMiddleware，以便通过包装方法更好地传播 context。
	NewClearToolResult = internal.NewClearToolResult

	// NewToolResultMiddleware creates a tool result reduction middleware.
	// This middleware combines two strategies to manage tool result tokens:
	//
	//  1. Clearing: Replaces old tool results with a placeholder when the total
	//     tool result tokens exceed the threshold, while protecting recent messages.
	//
	//  2. Offloading: Writes large individual tool results to the filesystem and
	//     returns a summary message guiding the LLM to read the full content.
	//
	// NOTE: If you are using the filesystem middleware (github.com/cloudwego/eino/adk/middlewares/filesystem),
	// this functionality is already included by default. Set Config.WithoutLargeToolResultOffloading = true
	// in the filesystem middleware if you want to use this middleware separately instead.
	//
	// NOTE: This middleware only handles offloading results to the filesystem.
	// You MUST also provide a read_file tool to your agent, otherwise the agent
	// will not be able to read the offloaded content. You can either:
	//   - Use the filesystem middleware (github.com/cloudwego/eino/adk/middlewares/filesystem)
	//     which provides the read_file tool automatically, OR
	//   - Implement your own read_file tool that reads from the same Backend
	//
	// Deprecated: Use New instead, which provides a more comprehensive tool result reduction
	// middleware with both truncation and clearing strategies, per-tool configuration support,
	// and returns a ChatModelAgentMiddleware for better context propagation through wrapper methods.
	//
	// NewToolResultMiddleware 创建工具结果缩减中间件。
	// 该中间件结合两种策略来管理工具结果 token：
	// 1. Clearing：当工具结果总 token 数超过阈值时，将旧工具结果替换为占位符，同时保护近期消息。
	// 2. Offloading：将较大的单个工具结果写入文件系统，并返回一条摘要消息，引导 LLM 读取完整内容。
	// NOTE: 如果你使用 filesystem 中间件（github.com/cloudwego/eino/adk/middlewares/filesystem），该功能已默认包含。若想改为单独使用此中间件，请在 filesystem 中间件中设置 Config.WithoutLargeToolResultOffloading = true。
	// NOTE: 此中间件只负责将结果卸载到文件系统。
	// 你还 MUST 为智能体提供 read_file 工具，否则智能体无法读取已卸载的内容。可以选择：
	// - 使用 filesystem 中间件（github.com/cloudwego/eino/adk/middlewares/filesystem），它会自动提供 read_file 工具，OR
	// - 实现你自己的 read_file 工具，从同一个 Backend 读取
	// Deprecated: 请使用 New，它提供更完整的工具结果缩减中间件，包含截断和清除两种策略、按工具配置支持，并返回 ChatModelAgentMiddleware，以便通过包装方法更好地传播 context。
	NewToolResultMiddleware = internal.NewToolResultMiddleware
)
