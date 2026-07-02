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

// Package internal provides middlewares to trim context and clear tool results.
// Package internal 提供用于裁剪上下文和清理工具结果的中间件。
package internal

import (
	"context"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

// ClearToolResultConfig configures the tool result clearing middleware.
// This middleware clears old tool results when their total token count exceeds a threshold,
// while protecting recent messages within a token budget.
//
// ClearToolResultConfig 配置工具结果清理中间件。
// 当工具结果总 token 数超过阈值时，该中间件会清理旧工具结果，同时在 token 预算内保护最近的消息。
type ClearToolResultConfig struct {
	// ToolResultTokenThreshold is the threshold for total tool result tokens.
	// When the sum of all tool result tokens exceeds this threshold, old tool results
	// (outside the KeepRecentTokens range) will be replaced with a placeholder.
	// Token estimation uses a simple heuristic: character count / 4.
	// If 0, defaults to 20000.
	//
	// ToolResultTokenThreshold 是工具结果总 token 数的阈值。
	// 当所有工具结果的 token 总和超过该阈值时，旧工具结果（KeepRecentTokens 范围之外）会被替换为占位符。
	// token 估算使用简单启发式：字符数 / 4。
	// 如果为 0，默认值为 20000。
	ToolResultTokenThreshold int

	// KeepRecentTokens is the token budget for recent messages to keep intact.
	// Messages within this token budget from the end will not have their tool results cleared,
	// even if the total tool result tokens exceed the threshold.
	// If 0, defaults to 40000.
	//
	// KeepRecentTokens 是保持最近消息完整的 token 预算。
	// 从末尾开始处于该 token 预算内的消息，其工具结果不会被清理，即使工具结果总 token 数超过阈值。
	// 如果为 0，默认值为 40000。
	KeepRecentTokens int

	// ClearToolResultPlaceholder is the text to replace old tool results with.
	// If empty, defaults to "[Old tool result content cleared]".
	//
	// ClearToolResultPlaceholder 是替换旧工具结果的文本。
	// 如果为空，默认值为 "[Old tool result content cleared]"。
	ClearToolResultPlaceholder string

	// TokenCounter is a custom function to estimate token count for a message.
	// If nil, uses the default counter (character count / 4).
	//
	// TokenCounter 是用于估算消息 token 数的自定义函数。
	// 如果为 nil，则使用默认计数器（字符数 / 4）。
	TokenCounter func(msg *schema.Message) int

	// ExcludeTools is a list of tool names whose results should never be cleared.
	// ExcludeTools 是工具名称列表，这些工具的结果永远不会被清理。
	ExcludeTools []string
}

// NewClearToolResult creates a new middleware that clears old tool results
// based on token thresholds while protecting recent messages.
//
// NewClearToolResult 创建一个新的中间件，用于基于 token 阈值清理旧工具结果，同时保护最近的消息。
func NewClearToolResult(ctx context.Context, config *ClearToolResultConfig) (adk.AgentMiddleware, error) {
	return adk.AgentMiddleware{
		BeforeChatModel: newClearToolResult(ctx, config),
	}, nil
}

func newClearToolResult(ctx context.Context, config *ClearToolResultConfig) func(ctx context.Context, state *adk.ChatModelAgentState) error {
	if config == nil {
		config = &ClearToolResultConfig{}
	}

	// Set defaults
	// 设置默认值
	toolResultTokenThreshold := config.ToolResultTokenThreshold
	if toolResultTokenThreshold == 0 {
		toolResultTokenThreshold = 20000
	}

	keepRecentTokens := config.KeepRecentTokens
	if keepRecentTokens == 0 {
		keepRecentTokens = 40000
	}

	placeholder := config.ClearToolResultPlaceholder
	if placeholder == "" {
		placeholder = "[Old tool result content cleared]"
	}

	// Set token estimator
	// 设置 token 估算器
	counter := config.TokenCounter
	if counter == nil {
		counter = defaultTokenCounter
	}
	return func(ctx context.Context, state *adk.ChatModelAgentState) error {
		return reduceByTokens(state, toolResultTokenThreshold, keepRecentTokens, placeholder, counter, config.ExcludeTools)
	}
}

// defaultTokenCounter estimates token count using character count / 4
// This is a simple heuristic that works reasonably well for most languages
//
// defaultTokenCounter 使用字符数 / 4 估算 token 数。
// 这是一个简单启发式方法，适用于大多数语言。
func defaultTokenCounter(msg *schema.Message) int {
	count := len(msg.Content)

	// Also count tool call arguments if present
	// 如果存在工具调用参数，也计入其中
	for _, tc := range msg.ToolCalls {
		count += len(tc.Function.Arguments)
	}

	// Estimate: roughly 4 characters per token
	// 估算：约每 4 个字符一个 token
	return (count + 3) / 4
}

// reduceByTokens reduces context based on tool result token threshold and recent message protection.
// It clears old tool results when:
// 1. The total tokens of all tool results exceed toolResultTokenThreshold
// 2. Only tool results outside the keepRecentTokens range (from the end) are cleared
//
// reduceByTokens 基于工具结果 token 阈值和最近消息保护来缩减上下文。
// 它会在以下情况下清理旧工具结果：
// 1. 所有工具结果的 token 总数超过 toolResultTokenThreshold
// 2. 只清理 keepRecentTokens 范围之外（从末尾开始计算）的工具结果
func reduceByTokens(state *adk.ChatModelAgentState, toolResultTokenThreshold, keepRecentTokens int, placeholder string, counter func(*schema.Message) int, excludedTools []string) error {
	if len(state.Messages) == 0 {
		return nil
	}

	// Step 1: Calculate total tool result tokens
	// 步骤 1：计算工具结果 token 总数
	totalToolResultTokens := 0
	for _, msg := range state.Messages {
		if msg.Role == schema.Tool && msg.Content != placeholder {
			totalToolResultTokens += counter(msg)
		}
	}

	// If total tool result tokens are under the threshold, no reduction needed
	// 如果工具结果总 token 数低于阈值，则无需缩减
	if totalToolResultTokens <= toolResultTokenThreshold {
		return nil
	}

	// Step 2: Calculate the index from which to protect recent messages
	// We need to find the starting index where cumulative tokens from the end <= keepRecentTokens
	//
	// 步骤 2：计算从哪个索引开始保护最近消息
	// 需要找到起始索引，使得从末尾累计的 token 数 <= keepRecentTokens
	recentStartIdx := len(state.Messages)
	cumulativeTokens := 0

	for i := len(state.Messages) - 1; i >= 0; i-- {
		msgTokens := counter(state.Messages[i])
		if cumulativeTokens+msgTokens > keepRecentTokens {
			// Adding this message would exceed the budget, so stop here
			// 加入这条消息会超出预算，因此在此停止
			recentStartIdx = i
			break
		}
		cumulativeTokens += msgTokens
		recentStartIdx = i
	}

	// Step 3: Clear tool results outside the protected range (before recentStartIdx)
	// 步骤 3：清理受保护范围之外（recentStartIdx 之前）的工具结果
	for i := 0; i < recentStartIdx; i++ {
		msg := state.Messages[i]
		if msg.Role == schema.Tool && msg.Content != placeholder && !excluded(msg.ToolName, excludedTools) {
			msg.Content = placeholder
		}
	}

	return nil
}

func excluded(name string, exclude []string) bool {
	for _, ex := range exclude {
		if name == ex {
			return true
		}
	}
	return false
}
