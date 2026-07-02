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

package internal

import (
	"context"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// Backend defines the interface provided by the user to implement file storage.
// It is used to save the content of large tool results to a persistent storage.
//
// Backend 定义用户提供的用于实现文件存储的接口。
// 它用于将大型工具结果的内容保存到持久化存储。
type Backend interface {
	Write(context.Context, *filesystem.WriteRequest) error
}

// ToolResultConfig configures the tool result reduction middleware.
// ToolResultConfig 用于配置工具结果缩减中间件。
type ToolResultConfig struct {
	// ClearingTokenThreshold is the threshold for the total token count of all tool results.
	// When the sum of all tool result tokens exceeds this threshold, old tool results
	// (outside the KeepRecentTokens range) will be replaced with a placeholder.
	// Token estimation uses a simple heuristic: character count / 4.
	// optional, 20000 by default
	//
	// ClearingTokenThreshold 是所有工具结果总 token 数的阈值。
	// 当所有工具结果 token 之和超过该阈值时，旧工具结果（不在 KeepRecentTokens 范围内）将替换为占位符。
	// Token 估算使用简单启发式：字符数 / 4。
	// 可选，默认 20000
	ClearingTokenThreshold int

	// KeepRecentTokens is the token budget for recent messages to keep intact.
	// Messages within this token budget from the end will not have their tool results cleared,
	// even if the total tool result tokens exceed the threshold.
	// optional, 40000 by default
	//
	// KeepRecentTokens 是用于保留近期消息完整性的 token 预算。
	// 从末尾开始处于该 token 预算内的消息，其工具结果不会被清除，即使工具结果总 token 数超过阈值。
	// 可选，默认 40000
	KeepRecentTokens int

	// ClearToolResultPlaceholder is the text to replace old tool results with.
	// optional, "[Old tool result content cleared]" by default
	//
	// ClearToolResultPlaceholder 是用于替换旧工具结果的文本。
	// 可选，默认 "[Old tool result content cleared]"
	ClearToolResultPlaceholder string

	// TokenCounter is a custom function to estimate token count for a message.
	// optional, uses the default counter (character count / 4) if nil
	//
	// TokenCounter 是用于估算消息 token 数的自定义函数。
	// 可选，若为 nil 则使用默认计数器（字符数 / 4）
	TokenCounter func(msg *schema.Message) int

	// ExcludeTools is a list of tool names whose results should never be cleared.
	// optional
	//
	// ExcludeTools 是一组工具名称列表，这些工具的结果永远不会被清除。
	// 可选
	ExcludeTools []string

	// Backend is the storage backend for offloaded tool results.
	// required
	//
	// Backend 是卸载后工具结果的存储 backend。
	// 必填
	Backend Backend

	// OffloadingTokenLimit is the token threshold for a single tool result to trigger offloading.
	// When a single tool result exceeds OffloadingTokenLimit * 4 characters, it will be
	// offloaded to the filesystem.
	// optional, 20000 by default
	//
	// OffloadingTokenLimit 是触发单个工具结果卸载的 token 阈值。
	// 当单个工具结果超过 OffloadingTokenLimit * 4 个字符时，会被卸载到文件系统。
	// 可选，默认 20000
	OffloadingTokenLimit int

	// ReadFileToolName is the name of the tool that LLM should use to read offloaded content.
	// This name will be included in the summary message sent to the LLM.
	// optional, "read_file" by default
	//
	// NOTE: If you are using the filesystem middleware, the read_file tool name
	// is exactly "read_file", which matches the default value.
	//
	// ReadFileToolName 是 LLM 用来读取已卸载内容的工具名称。
	// 该名称会包含在发送给 LLM 的摘要消息中。
	// 可选，默认 "read_file"
	// NOTE: 如果你使用 filesystem 中间件，read_file 工具名称正是 "read_file"，与默认值匹配。
	ReadFileToolName string

	// PathGenerator generates the write path for offloaded results.
	// optional, "/large_tool_result/{ToolCallID}" by default
	//
	// PathGenerator 生成已卸载结果的写入路径。
	// 可选，默认 "/large_tool_result/{ToolCallID}"
	PathGenerator func(ctx context.Context, input *compose.ToolInput) (string, error)
}

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
// NewToolResultMiddleware 创建工具结果缩减中间件。
// 该中间件结合两种策略来管理工具结果 token：
// 1. Clearing：当工具结果总 token 数超过阈值时，将旧工具结果替换为占位符，同时保护近期消息。
// 2. Offloading：将较大的单个工具结果写入文件系统，并返回一条摘要消息，引导 LLM 读取完整内容。
// NOTE: 如果你使用 filesystem 中间件（github.com/cloudwego/eino/adk/middlewares/filesystem），该功能已默认包含。若想改为单独使用此中间件，请在 filesystem 中间件中设置 Config.WithoutLargeToolResultOffloading = true。
// NOTE: 此中间件只负责将结果卸载到文件系统。
// 你还 MUST 为智能体提供 read_file 工具，否则智能体无法读取已卸载的内容。可以选择：
// - 使用 filesystem 中间件（github.com/cloudwego/eino/adk/middlewares/filesystem），它会自动提供 read_file 工具，OR
// - 实现你自己的 read_file 工具，从同一个 Backend 读取
func NewToolResultMiddleware(ctx context.Context, cfg *ToolResultConfig) (adk.AgentMiddleware, error) {
	bc := newClearToolResult(ctx, &ClearToolResultConfig{
		ToolResultTokenThreshold:   cfg.ClearingTokenThreshold,
		KeepRecentTokens:           cfg.KeepRecentTokens,
		ClearToolResultPlaceholder: cfg.ClearToolResultPlaceholder,
		TokenCounter:               cfg.TokenCounter,
		ExcludeTools:               cfg.ExcludeTools,
	})
	tm := newToolResultOffloading(ctx, &toolResultOffloadingConfig{
		Backend:          cfg.Backend,
		ReadFileToolName: cfg.ReadFileToolName,
		TokenLimit:       cfg.OffloadingTokenLimit,
		PathGenerator:    cfg.PathGenerator,
	})
	return adk.AgentMiddleware{
		BeforeChatModel: bc,
		WrapToolCall:    tm,
	}, nil
}
