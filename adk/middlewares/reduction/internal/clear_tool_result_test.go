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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

func Test_reduceByTokens(t *testing.T) {
	type args struct {
		state                    *adk.ChatModelAgentState
		toolResultTokenThreshold int
		keepRecentTokens         int
		placeholder              string
		estimator                func(*schema.Message) int
	}
	tests := []struct {
		name          string
		args          args
		wantErr       assert.ErrorAssertionFunc
		validateState func(*testing.T, *adk.ChatModelAgentState)
	}{
		{
			name: "no reduction when tool result tokens under threshold",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("hello"),
						schema.AssistantMessage("hi", nil),
						schema.ToolMessage("short tool result", "call-1", schema.WithToolName("tool1")),
					},
				},
				toolResultTokenThreshold: 100,
				keepRecentTokens:         500,
				placeholder:              "[Old tool result content cleared]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				assert.Equal(t, "short tool result", state.Messages[2].Content)
			},
		},
		{
			name: "clear old tool results when total exceeds threshold",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("msg1"),
						schema.ToolMessage(strings.Repeat("a", 40), "call-1", schema.WithToolName("tool1")), // ~10 tokens (old)
						// 约 10 个 token（旧）
						schema.UserMessage("msg2"),
						schema.ToolMessage(strings.Repeat("b", 40), "call-2", schema.WithToolName("tool2")), // ~10 tokens (old)
						// 约 10 个 token（旧）
						schema.UserMessage("msg3"),
						schema.ToolMessage(strings.Repeat("c", 40), "call-3", schema.WithToolName("tool3")), // ~10 tokens (recent, protected)
						// 约 10 个 token（最近，受保护）
					},
				},
				toolResultTokenThreshold: 20,
				keepRecentTokens:         10,
				placeholder:              "[Old tool result content cleared]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				assert.Equal(t, "[Old tool result content cleared]", state.Messages[1].Content)
				assert.Equal(t, "[Old tool result content cleared]", state.Messages[3].Content)
				assert.Equal(t, strings.Repeat("c", 40), state.Messages[5].Content)
			},
		},
		{
			name: "protect recent messages even when tool results exceed threshold",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("old msg"),
						schema.ToolMessage(strings.Repeat("x", 100), "call-1", schema.WithToolName("tool1")), // ~25 tokens (old)
						// 约 25 个 token（旧）
						schema.UserMessage("recent msg"),
						schema.ToolMessage(strings.Repeat("x", 100), "call-2", schema.WithToolName("tool2")), // ~25 tokens (recent, protected)
						// 约 25 个 token（最近，受保护）
					},
				},
				toolResultTokenThreshold: 10,
				keepRecentTokens:         20,
				placeholder:              "[Old tool result content cleared]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				// Total tool result tokens = 50, exceeds threshold of 10
				// But last 200 tokens are protected (includes last 2 messages)
				// So only the first tool result should be cleared
				//
				// 工具结果 token 总数 = 50，超过阈值 10
				// 但最后 200 个 token 受保护（包括最后 2 条消息）
				// 因此只应清除第一个工具结果
				assert.Equal(t, "[Old tool result content cleared]", state.Messages[1].Content)
				assert.Equal(t, strings.Repeat("x", 100), state.Messages[3].Content)
			},
		},
		{
			name: "custom placeholder text",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("msg"),
						schema.ToolMessage(strings.Repeat("x", 100), "call-1", schema.WithToolName("tool1")),
						schema.UserMessage(strings.Repeat("x", 100)),
					},
				},
				toolResultTokenThreshold: 10,
				keepRecentTokens:         20,
				placeholder:              "[历史工具结果已清除]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				assert.Equal(t, "[历史工具结果已清除]", state.Messages[1].Content)
			},
		},
		{
			name: "no tool messages",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("msg 1"),
						schema.AssistantMessage("response 1", nil),
						schema.UserMessage("msg 2"),
						schema.AssistantMessage("response 2", nil),
					},
				},
				toolResultTokenThreshold: 10,
				keepRecentTokens:         10,
				placeholder:              "[Old tool result content cleared]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				// All messages should remain unchanged
				// 所有消息都应保持不变
				assert.Equal(t, "msg 1", state.Messages[0].Content)
				assert.Equal(t, "response 1", state.Messages[1].Content)
				assert.Equal(t, "msg 2", state.Messages[2].Content)
				assert.Equal(t, "response 2", state.Messages[3].Content)
			},
		},
		{
			name: "empty messages",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{},
				},
				toolResultTokenThreshold: 100,
				keepRecentTokens:         500,
				placeholder:              "[Old tool result content cleared]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				assert.Empty(t, state.Messages)
			},
		},
		{
			name: "custom token estimator - word count",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("hello world"),
						schema.ToolMessage("this is a long tool result", "call-1", schema.WithToolName("tool1")), // 6 words (old)
						// 6 个词（旧）
						schema.UserMessage("another message"),
						schema.ToolMessage("recent tool result here", "call-2", schema.WithToolName("tool2")), // 4 words (recent)
						// 4 个词（最近）
					},
				},
				toolResultTokenThreshold: 9, // 10 words total threshold
				// 总阈值为 10 个词
				keepRecentTokens: 5, // 15 words protection budget
				// 保护预算为 15 个词
				placeholder: "[Old tool result content cleared]",
				estimator: func(msg *schema.Message) int {
					if msg.Content == "" {
						return 0
					}
					words := 1
					for _, ch := range msg.Content {
						if ch == ' ' {
							words++
						}
					}
					return words
				},
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				assert.Equal(t, "[Old tool result content cleared]", state.Messages[1].Content)
				assert.Equal(t, "recent tool result here", state.Messages[3].Content)
			},
		},
		{
			name: "already cleared results are not counted",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("msg1"),
						schema.ToolMessage("[Old tool result content cleared]", "call-1", schema.WithToolName("tool1")), // Already cleared
						// 已清除
						schema.UserMessage("msg2"),
						schema.ToolMessage(strings.Repeat("a", 100), "call-2", schema.WithToolName("tool2")), // New long result
						// 新的长结果
					},
				},
				toolResultTokenThreshold: 10,
				keepRecentTokens:         20,
				placeholder:              "[Old tool result content cleared]",
				estimator:                defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				// Only the new long result counts toward the threshold
				// Both should have placeholder
				//
				// 只有新的长结果计入阈值
				// 两者都应有占位符
				assert.Equal(t, "[Old tool result content cleared]", state.Messages[1].Content)
				assert.Equal(t, strings.Repeat("a", 100), state.Messages[3].Content)
			},
		},
		{
			name: "all tool results within protected range",
			args: args{
				state: &adk.ChatModelAgentState{
					Messages: []adk.Message{
						schema.UserMessage("msg1"),
						schema.ToolMessage(strings.Repeat("a", 40), "call-1", schema.WithToolName("tool1")), // ~10 tokens
						schema.UserMessage("msg2"),
						schema.ToolMessage(strings.Repeat("b", 40), "call-2", schema.WithToolName("tool2")), // ~10 tokens
					},
				},
				toolResultTokenThreshold: 10, // Low threshold (will exceed)
				// 低阈值（会超出）
				keepRecentTokens: 1000, // Very high protection (protects all)
				// 很高的保护值（保护全部）
				placeholder: "[Old tool result content cleared]",
				estimator:   defaultTokenCounter,
			},
			wantErr: assert.NoError,
			validateState: func(t *testing.T, state *adk.ChatModelAgentState) {
				// All messages are within protected range, nothing should be cleared
				// 所有消息都在受保护范围内，不应清除任何内容
				assert.Equal(t, strings.Repeat("a", 40), state.Messages[1].Content)
				assert.Equal(t, strings.Repeat("b", 40), state.Messages[3].Content)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := reduceByTokens(tt.args.state, tt.args.toolResultTokenThreshold, tt.args.keepRecentTokens, tt.args.placeholder, tt.args.estimator, []string{})
			tt.wantErr(t, err, fmt.Sprintf("reduceByTokens(%v, %v, %v, %v)", tt.args.state, tt.args.toolResultTokenThreshold, tt.args.keepRecentTokens, tt.args.placeholder))
			if tt.validateState != nil {
				tt.validateState(t, tt.args.state)
			}
		})
	}
}

func Test_newClearToolResult(t *testing.T) {
	ctx := context.Background()

	t.Run("nil config uses defaults", func(t *testing.T) {
		fn := newClearToolResult(ctx, nil)
		assert.NotNil(t, fn)

		// Test that function works with nil config (uses defaults)
		// 测试函数在 nil config 下可工作（使用默认值）
		state := &adk.ChatModelAgentState{
			Messages: []adk.Message{
				schema.UserMessage("hello"),
				schema.ToolMessage("short result", "call-1", schema.WithToolName("tool1")),
			},
		}
		err := fn(ctx, state)
		assert.NoError(t, err)
		// Default threshold is 20000, so short result should not be cleared
		// 默认阈值为 20000，因此短结果不应被清除
		assert.Equal(t, "short result", state.Messages[1].Content)
	})

	t.Run("empty config uses defaults", func(t *testing.T) {
		fn := newClearToolResult(ctx, &ClearToolResultConfig{})
		assert.NotNil(t, fn)

		state := &adk.ChatModelAgentState{
			Messages: []adk.Message{
				schema.UserMessage("hello"),
				schema.ToolMessage("short result", "call-1", schema.WithToolName("tool1")),
			},
		}
		err := fn(ctx, state)
		assert.NoError(t, err)
		assert.Equal(t, "short result", state.Messages[1].Content)
	})
}
