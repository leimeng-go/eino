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

package adk

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cloudwego/eino/schema"
)

func TestSessionValues(t *testing.T) {
	// Test Case 1: Basic AddSessionValues and GetSessionValues
	// 测试用例 1：基本的 AddSessionValues 和 GetSessionValues
	t.Run("BasicSessionValues", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add values to the session
		// 向 session 添加值
		values := map[string]any{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		}
		AddSessionValues(ctx, values)

		// Get all values from the session
		// 从 session 获取所有值
		retrievedValues := GetSessionValues(ctx)

		// Verify the values were added correctly
		// 验证值已正确添加
		assert.Equal(t, "value1", retrievedValues["key1"])
		assert.Equal(t, 42, retrievedValues["key2"])
		assert.Equal(t, true, retrievedValues["key3"])
		assert.Len(t, retrievedValues, 3)
	})

	// Test Case 2: AddSessionValues with empty context
	// 测试用例 2：对空 context 调用 AddSessionValues
	t.Run("AddSessionValuesEmptyContext", func(t *testing.T) {
		ctx := context.Background()

		// Add values to a context without a run session
		// 向没有 run session 的 context 添加值
		values := map[string]any{
			"key1": "value1",
		}
		AddSessionValues(ctx, values)

		// Get values should return empty map
		// 获取值应返回空 map
		retrievedValues := GetSessionValues(ctx)
		assert.Empty(t, retrievedValues)
	})

	// Test Case 3: GetSessionValues with empty context
	// 测试用例 3：GetSessionValues 使用空 context
	t.Run("GetSessionValuesEmptyContext", func(t *testing.T) {
		ctx := context.Background()

		// Get values from a context without a run session
		// 从没有 run session 的 context 获取值
		retrievedValues := GetSessionValues(ctx)
		assert.Empty(t, retrievedValues)
	})

	// Test Case 4: AddSessionValues with nil values
	// 测试用例 4：AddSessionValues 使用 nil values
	t.Run("AddSessionValuesNilValues", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add nil values map
		// 添加 nil values map
		AddSessionValues(ctx, nil)

		// Get values should still be empty
		// 获取到的值仍应为空
		retrievedValues := GetSessionValues(ctx)
		assert.Empty(t, retrievedValues)
	})

	// Test Case 5: AddSessionValues with empty values
	// 测试用例 5：AddSessionValues 使用空 values
	t.Run("AddSessionValuesEmptyValues", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add empty values map
		// 添加空 values map
		AddSessionValues(ctx, map[string]any{})

		// Get values should be empty
		// 获取到的值应为空
		retrievedValues := GetSessionValues(ctx)
		assert.Empty(t, retrievedValues)
	})

	// Test Case 6: AddSessionValues with complex data types
	// 测试用例 6：AddSessionValues 使用复杂数据类型
	t.Run("AddSessionValuesComplexTypes", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add complex values to the session
		// 向 session 添加复杂值
		values := map[string]any{
			"string": "hello world",
			"int":    123,
			"float":  45.67,
			"bool":   true,
			"slice":  []string{"a", "b", "c"},
			"map":    map[string]int{"x": 1, "y": 2},
			"struct": struct{ Name string }{Name: "test"},
		}
		AddSessionValues(ctx, values)

		// Get all values from the session
		// 从 session 获取所有值
		retrievedValues := GetSessionValues(ctx)

		// Verify the complex values were added correctly
		// 验证复杂值已正确添加
		assert.Equal(t, "hello world", retrievedValues["string"])
		assert.Equal(t, 123, retrievedValues["int"])
		assert.Equal(t, 45.67, retrievedValues["float"])
		assert.Equal(t, true, retrievedValues["bool"])
		assert.Equal(t, []string{"a", "b", "c"}, retrievedValues["slice"])
		assert.Equal(t, map[string]int{"x": 1, "y": 2}, retrievedValues["map"])
		assert.Equal(t, struct{ Name string }{Name: "test"}, retrievedValues["struct"])
		assert.Len(t, retrievedValues, 7)
	})

	// Test Case 7: AddSessionValues overwrites existing values
	// 测试用例 7：AddSessionValues 覆盖已有值
	t.Run("AddSessionValuesOverwrite", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add initial values
		// 添加初始值
		initialValues := map[string]any{
			"key1": "initial1",
			"key2": "initial2",
		}
		AddSessionValues(ctx, initialValues)

		// Add values that overwrite some keys
		// 添加会覆盖部分 key 的值
		overwriteValues := map[string]any{
			"key1": "overwritten1",
			"key3": "new3",
		}
		AddSessionValues(ctx, overwriteValues)

		// Get all values from the session
		// 从 session 获取所有值
		retrievedValues := GetSessionValues(ctx)

		// Verify the values were overwritten correctly
		// 验证值是否已正确覆盖
		assert.Equal(t, "overwritten1", retrievedValues["key1"]) // overwritten
		assert.Equal(t, "initial2", retrievedValues["key2"])     // unchanged
		assert.Equal(t, "new3", retrievedValues["key3"])         // new
		assert.Len(t, retrievedValues, 3)
	})

	// Test Case 8: Concurrent access to session values
	// 测试用例 8：并发访问 session 值
	t.Run("ConcurrentSessionValues", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add initial values
		// 添加初始值
		initialValues := map[string]any{
			"counter": 0,
		}
		AddSessionValues(ctx, initialValues)

		// Simulate concurrent access
		// 模拟并发访问
		done := make(chan bool)

		// Goroutine 1: Add values
		// Goroutine 1：添加值
		go func() {
			for i := 0; i < 100; i++ {
				values := map[string]any{
					"goroutine1": i,
				}
				AddSessionValues(ctx, values)
			}
			done <- true
		}()

		// Goroutine 2: Add different values
		// Goroutine 2：添加不同的值
		go func() {
			for i := 0; i < 100; i++ {
				values := map[string]any{
					"goroutine2": i,
				}
				AddSessionValues(ctx, values)
			}
			done <- true
		}()

		// Wait for both goroutines to complete
		// 等待两个 goroutine 完成
		<-done
		<-done

		// Verify that both values were set (last write wins)
		// 验证两个值均已设置（最后写入生效）
		retrievedValues := GetSessionValues(ctx)
		assert.Equal(t, 0, retrievedValues["counter"])
		assert.Equal(t, 99, retrievedValues["goroutine1"])
		assert.Equal(t, 99, retrievedValues["goroutine2"])
	})

	// Test Case 9: GetSessionValue individual value
	// 测试用例 9：GetSessionValue 单个值
	t.Run("GetSessionValueIndividual", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add values to the session
		// 向 session 添加值
		values := map[string]any{
			"key1": "value1",
			"key2": 42,
		}
		AddSessionValues(ctx, values)

		// Get individual values
		// 获取单个值
		value1, exists1 := GetSessionValue(ctx, "key1")
		value2, exists2 := GetSessionValue(ctx, "key2")
		value3, exists3 := GetSessionValue(ctx, "nonexistent")

		// Verify individual values
		// 验证单个值
		assert.True(t, exists1)
		assert.Equal(t, "value1", value1)

		assert.True(t, exists2)
		assert.Equal(t, 42, value2)

		assert.False(t, exists3)
		assert.Nil(t, value3)
	})

	// Test Case 10: AddSessionValue individual value
	// 测试用例 10：AddSessionValue 单个值
	t.Run("AddSessionValueIndividual", func(t *testing.T) {
		ctx := context.Background()

		// Create a context with a run session
		// 创建带有 run session 的 context
		session := newRunSession()
		runCtx := &runContext{Session: session}
		ctx = setRunCtx(ctx, runCtx)

		// Add individual values
		// 添加单个值
		AddSessionValue(ctx, "key1", "value1")
		AddSessionValue(ctx, "key2", 42)

		// Get all values
		// 获取所有值
		retrievedValues := GetSessionValues(ctx)

		// Verify the values were added correctly
		// 验证值是否已正确添加
		assert.Equal(t, "value1", retrievedValues["key1"])
		assert.Equal(t, 42, retrievedValues["key2"])
		assert.Len(t, retrievedValues, 2)
	})

	// Test Case 11: AddSessionValue with empty context
	// 测试用例 11：AddSessionValue 使用空 context
	t.Run("AddSessionValueEmptyContext", func(t *testing.T) {
		ctx := context.Background()

		// Add individual value to a context without a run session
		// 向没有运行会话的 context 添加单个值
		AddSessionValue(ctx, "key1", "value1")

		// Get individual value should return false
		// 获取单个值应返回 false
		value, exists := GetSessionValue(ctx, "key1")
		assert.False(t, exists)
		assert.Nil(t, value)

		// Get all values should return empty map
		// 获取所有值应返回空 map
		retrievedValues := GetSessionValues(ctx)
		assert.Empty(t, retrievedValues)
	})

	// Test Case 12: Integration with run context initialization
	// 测试用例 12：与运行 context 初始化集成
	t.Run("IntegrationWithRunContext", func(t *testing.T) {
		ctx := context.Background()

		// Initialize a run context with an agent
		// 使用 agent 初始化运行 context
		input := &AgentInput{
			Messages: []Message{
				schema.UserMessage("test input"),
			},
		}
		ctx, runCtx := initRunCtx(ctx, "test-agent", input)

		// Verify the run context was created
		// 验证运行 context 已创建
		assert.NotNil(t, runCtx)
		assert.NotNil(t, runCtx.Session)

		// Add values to the session
		// 向 session 添加值
		values := map[string]any{
			"integration_key": "integration_value",
		}
		AddSessionValues(ctx, values)

		// Get values from the session
		// 从 session 获取值
		retrievedValues := GetSessionValues(ctx)
		assert.Equal(t, "integration_value", retrievedValues["integration_key"])

		// Verify the run path was set correctly
		// 验证 run path 设置正确
		assert.Len(t, runCtx.RunPath, 1)
		assert.Equal(t, "test-agent", runCtx.RunPath[0].agentName)
	})
}

func TestForkJoinRunCtx(t *testing.T) {
	// Helper to create a named event
	// 创建命名事件的辅助函数
	newEvent := func(name string) *AgentEvent {
		// Add a small sleep to ensure timestamps are distinct
		// 短暂休眠以确保时间戳不同
		time.Sleep(1 * time.Millisecond)
		return &AgentEvent{AgentName: name}
	}

	// Helper to get event names from a slice of wrappers
	// 从 wrapper 切片获取事件名称的辅助函数
	getEventNames := func(wrappers []*agentEventWrapper) []string {
		names := make([]string, len(wrappers))
		for i, w := range wrappers {
			names[i] = w.AgentName
		}
		return names
	}

	// 1. Setup: Create an initial runContext for the main execution path.
	// 1. 准备：为主执行路径创建初始 runContext。
	mainCtx, mainRunCtx := initRunCtx(context.Background(), "Main", nil)

	// 2. Run Agent A
	// 2. 运行 Agent A
	eventA := newEvent("A")
	mainRunCtx.Session.addEvent(eventA)
	assert.Equal(t, []string{"A"}, getEventNames(mainRunCtx.Session.getEvents()), "After A")

	// 3. Fork for Par(B, C)
	// 3. 为 Par(B, C) Fork
	ctxB := forkRunCtx(mainCtx)
	ctxC := forkRunCtx(mainCtx)

	// Assertions for Fork
	// Fork 断言
	runCtxB := getRunCtx(ctxB)
	runCtxC := getRunCtx(ctxC)
	assert.NotSame(t, mainRunCtx.Session, runCtxB.Session, "Session B should be a new struct")
	assert.NotSame(t, mainRunCtx.Session, runCtxC.Session, "Session C should be a new struct")
	assert.NotSame(t, runCtxB.Session, runCtxC.Session, "Sessions B and C should be different")
	assert.Nil(t, mainRunCtx.Session.LaneEvents, "Main session should have no lane events yet")
	assert.NotNil(t, runCtxB.Session.LaneEvents, "Session B should have lane events")
	assert.NotNil(t, runCtxC.Session.LaneEvents, "Session C should have lane events")
	assert.Nil(t, runCtxB.Session.LaneEvents.Parent, "Lane B's parent should be the main (nil) lane")
	assert.Nil(t, runCtxC.Session.LaneEvents.Parent, "Lane C's parent should be the main (nil) lane")

	// 4. Run Agent B
	// 4. 运行 Agent B
	eventB := newEvent("B")
	runCtxB.Session.addEvent(eventB)
	assert.Equal(t, []string{"A", "B"}, getEventNames(runCtxB.Session.getEvents()), "After B")

	// 5. Run Agent C (and Nested Fork for Par(D, E))
	// 5. 运行 Agent C（并为 Par(D, E) 执行嵌套 Fork）
	eventC1 := newEvent("C1")
	runCtxC.Session.addEvent(eventC1)
	assert.Equal(t, []string{"A", "C1"}, getEventNames(runCtxC.Session.getEvents()), "After C1")

	ctxD := forkRunCtx(ctxC)
	ctxE := forkRunCtx(ctxC)

	// Assertions for Nested Fork
	// 嵌套 Fork 断言
	runCtxD := getRunCtx(ctxD)
	runCtxE := getRunCtx(ctxE)
	assert.NotNil(t, runCtxD.Session.LaneEvents.Parent, "Lane D's parent should be Lane C")
	assert.Same(t, runCtxC.Session.LaneEvents, runCtxD.Session.LaneEvents.Parent, "Lane D's parent must be Lane C's node")
	assert.Same(t, runCtxC.Session.LaneEvents, runCtxE.Session.LaneEvents.Parent, "Lane E's parent must be Lane C's node")

	// 6. Run Agents D and E
	// 6. 运行 Agent D 和 E
	eventD := newEvent("D")
	runCtxD.Session.addEvent(eventD)
	eventE := newEvent("E")
	runCtxE.Session.addEvent(eventE)

	assert.Equal(t, []string{"A", "C1", "D"}, getEventNames(runCtxD.Session.getEvents()), "After D")
	assert.Equal(t, []string{"A", "C1", "E"}, getEventNames(runCtxE.Session.getEvents()), "After E")

	// 7. Join Par(D, E)
	// 7. Join Par(D, E)
	joinRunCtxs(ctxC, ctxD, ctxE)

	// Assertions for Nested Join
	// The events should now be committed to Lane C's event slice.
	//
	// 嵌套 Join 的断言
	// 事件现在应提交到 Lane C 的事件切片。
	assert.Equal(t, []string{"A", "C1", "D", "E"}, getEventNames(runCtxC.Session.getEvents()), "After joining D and E")

	// 8. Join Par(B, C)
	// 8. Join Par(B, C)
	joinRunCtxs(mainCtx, ctxB, ctxC)

	// Assertions for Top-Level Join
	// The events should now be committed to the main session's Events slice.
	//
	// 顶层 Join 的断言
	// 事件现在应提交到主 session 的 Events 切片。
	assert.Equal(t, []string{"A", "B", "C1", "D", "E"}, getEventNames(mainRunCtx.Session.getEvents()), "After joining B and C")

	// 9. Run Agent F
	// 9. 运行 Agent F
	eventF := newEvent("F")
	mainRunCtx.Session.addEvent(eventF)
	assert.Equal(t, []string{"A", "B", "C1", "D", "E", "F"}, getEventNames(mainRunCtx.Session.getEvents()), "After F")
}

// makeStreamingEventWrapper creates an agentEventWrapper with a streaming MessageOutput
// whose stream yields the given message then terminates with streamErr (or io.EOF if nil).
//
// makeStreamingEventWrapper 创建带有流式 MessageOutput 的 agentEventWrapper
// 其流会产出给定消息，然后以 streamErr 结束（若为 nil，则以 io.EOF 结束）。
func makeStreamingEventWrapper(msg Message, streamErr error) *agentEventWrapper {
	r, w := schema.Pipe[Message](2)
	w.Send(msg, nil)
	if streamErr != nil {
		w.Send(nil, streamErr)
	}
	w.Close()

	return &agentEventWrapper{
		AgentEvent: &AgentEvent{
			AgentName: "test-agent",
			Output: &AgentOutput{
				MessageOutput: &MessageVariant{
					IsStreaming:   true,
					MessageStream: r,
					Role:          schema.Assistant,
				},
			},
		},
	}
}

func TestGobEncodeStreamErrors(t *testing.T) {
	t.Run("WillRetryError_unconsumed_stream_fails_GobEncode", func(t *testing.T) {
		// An agentEventWrapper whose stream yields a message then WillRetryError.
		// Without pre-consuming (no getMessageFromWrappedEvent call), GobEncode
		// reaches MessageVariant.GobEncode which treats non-EOF errors as fatal.
		//
		// 一个 agentEventWrapper，其流会产出一条消息，然后返回 WillRetryError。
		// 未预先消费时（未调用 getMessageFromWrappedEvent），GobEncode
		// 会进入 MessageVariant.GobEncode，并将非 EOF 错误视为致命错误。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("partial", nil),
			&WillRetryError{ErrStr: "model error", RetryAttempt: 1},
		)

		_, err := wrapper.GobEncode()
		assert.NoError(t, err, "GobEncode should handle WillRetryError streams gracefully")
	})

	t.Run("ErrStreamCanceled_unconsumed_stream_fails_GobEncode", func(t *testing.T) {
		// Same scenario but with ErrStreamCanceled (*errors.errorString).
		// 相同场景，但使用 ErrStreamCanceled (*errors.errorString)。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("partial", nil),
			ErrStreamCanceled,
		)

		_, err := wrapper.GobEncode()
		assert.NoError(t, err, "GobEncode should handle ErrStreamCanceled streams gracefully")
	})

	t.Run("successful_stream_GobEncode_succeeds", func(t *testing.T) {
		// Control: a clean stream (no error) should encode fine.
		// 对照：干净的流（无错误）应能正常编码。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("hello", nil),
			nil, // no stream error
			// 无流错误
		)

		data, err := wrapper.GobEncode()
		assert.NoError(t, err)
		assert.NotEmpty(t, data)

		// Verify round-trip decode works.
		// 验证往返解码正常。
		decoded := &agentEventWrapper{AgentEvent: &AgentEvent{}}
		err = decoded.GobDecode(data)
		assert.NoError(t, err)
		assert.Equal(t, "test-agent", decoded.AgentName)
	})

	t.Run("preconsumed_WillRetryError_GobEncode_succeeds", func(t *testing.T) {
		// When getMessageFromWrappedEvent is called first, WillRetryError is
		// cached in StreamErr and the stream is replaced with an error-free array.
		//
		// 先调用 getMessageFromWrappedEvent 时，WillRetryError 会
		// 缓存到 StreamErr，且流会替换为无错误数组。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("partial", nil),
			&WillRetryError{ErrStr: "model error", RetryAttempt: 1},
		)

		_, consumeErr := getMessageFromWrappedEvent(wrapper)
		assert.Error(t, consumeErr)

		data, err := wrapper.GobEncode()
		assert.NoError(t, err, "GobEncode should succeed after pre-consuming WillRetryError stream")
		assert.NotEmpty(t, data)
	})

	t.Run("preconsumed_ErrStreamCanceled_GobEncode_succeeds", func(t *testing.T) {
		// ErrStreamCanceled is a *StreamCanceledError which IS gob-registered.
		// After getMessageFromWrappedEvent, StreamErr = ErrStreamCanceled.
		// Since it's registered, gob encoding succeeds.
		//
		// ErrStreamCanceled 是已 gob 注册的 *StreamCanceledError。
		// 调用 getMessageFromWrappedEvent 后，StreamErr = ErrStreamCanceled。
		// 由于它已注册，gob 编码会成功。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("partial", nil),
			ErrStreamCanceled,
		)

		_, consumeErr := getMessageFromWrappedEvent(wrapper)
		assert.Error(t, consumeErr)

		data, err := wrapper.GobEncode()
		assert.NoError(t, err, "GobEncode should succeed; ErrStreamCanceled is gob-registered")
		assert.NotEmpty(t, data)
	})

	t.Run("GobEncode_roundtrip_preserves_content", func(t *testing.T) {
		// Verify that after GobEncode with a WillRetryError stream,
		// the decoded wrapper has the partial message content and StreamErr intact.
		//
		// 验证对带 WillRetryError 的流执行 GobEncode 后，
		// 解码出的 wrapper 保留部分消息内容且 StreamErr 完好。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("partial response", nil),
			&WillRetryError{ErrStr: "err", RetryAttempt: 1},
		)

		data, err := wrapper.GobEncode()
		assert.NoError(t, err)

		decoded := &agentEventWrapper{AgentEvent: &AgentEvent{}}
		err = decoded.GobDecode(data)
		assert.NoError(t, err)
		assert.Equal(t, "test-agent", decoded.AgentName)
		assert.True(t, decoded.Output.MessageOutput.IsStreaming)
		// The stream should be consumable and yield the partial message.
		// 该流应可被消费，并产出部分消息。
		msg, recvErr := decoded.Output.MessageOutput.MessageStream.Recv()
		assert.NoError(t, recvErr)
		assert.Contains(t, msg.Content, "partial response")
		// StreamErr should be preserved for end-user visibility.
		// StreamErr 应保留，以便最终用户可见。
		var willRetryErr *WillRetryError
		assert.True(t, errors.As(decoded.StreamErr, &willRetryErr))
		assert.Equal(t, "err", willRetryErr.ErrStr)
	})

	t.Run("GobEncode_roundtrip_preserves_ErrStreamCanceled", func(t *testing.T) {
		// ErrStreamCanceled (*StreamCanceledError) is gob-registered, so
		// StreamErr should survive encoding/decoding.
		//
		// ErrStreamCanceled (*StreamCanceledError) 已 gob 注册，因此
		// StreamErr 应在编码/解码后保留。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("partial", nil),
			ErrStreamCanceled,
		)

		data, err := wrapper.GobEncode()
		assert.NoError(t, err)

		decoded := &agentEventWrapper{AgentEvent: &AgentEvent{}}
		err = decoded.GobDecode(data)
		assert.NoError(t, err)
		var streamCanceledErr *StreamCanceledError
		assert.ErrorAs(t, decoded.StreamErr, &streamCanceledErr)
	})

	t.Run("GobEncode_idempotent", func(t *testing.T) {
		// Calling GobEncode twice should succeed both times (stream replaced on first call).
		// 连续调用 GobEncode 两次都应成功（第一次调用会替换流）。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("hello", nil),
			&WillRetryError{ErrStr: "err", RetryAttempt: 1},
		)

		data1, err := wrapper.GobEncode()
		assert.NoError(t, err)

		data2, err := wrapper.GobEncode()
		assert.NoError(t, err)

		// Both should decode to equivalent content.
		// 两者解码后应得到等价内容。
		d1, d2 := &agentEventWrapper{AgentEvent: &AgentEvent{}}, &agentEventWrapper{AgentEvent: &AgentEvent{}}
		assert.NoError(t, d1.GobDecode(data1))
		assert.NoError(t, d2.GobDecode(data2))
		assert.Equal(t, d1.AgentName, d2.AgentName)
	})

	t.Run("GobEncode_non_streaming_unaffected", func(t *testing.T) {
		// Non-streaming events should encode/decode as before.
		// 非流式事件应像以前一样编码/解码。
		wrapper := &agentEventWrapper{
			AgentEvent: &AgentEvent{
				AgentName: "non-stream-agent",
				Output: &AgentOutput{
					MessageOutput: &MessageVariant{
						IsStreaming: false,
						Message:     schema.AssistantMessage("direct", nil),
						Role:        schema.Assistant,
					},
				},
			},
		}

		data, err := wrapper.GobEncode()
		assert.NoError(t, err)

		decoded := &agentEventWrapper{AgentEvent: &AgentEvent{}}
		assert.NoError(t, decoded.GobDecode(data))
		assert.Equal(t, "non-stream-agent", decoded.AgentName)
		assert.False(t, decoded.Output.MessageOutput.IsStreaming)
	})

	t.Run("GobEncode_within_runSession", func(t *testing.T) {
		// Simulate the real scenario: a runSession with a streaming event containing
		// WillRetryError is gob-encoded (as happens during checkpoint save).
		//
		// 模拟真实场景：包含 WillRetryError 的流式事件所在的 runSession 被 gob 编码（如保存检查点时所发生的那样）。
		wrapper := makeStreamingEventWrapper(
			schema.AssistantMessage("checkpoint content", nil),
			&WillRetryError{ErrStr: "retry", RetryAttempt: 1},
		)

		session := newRunSession()
		session.Events = []*agentEventWrapper{wrapper}

		// Encode the entire session (the checkpoint path).
		// 编码整个 session（检查点路径）。
		var buf bytes.Buffer
		err := gob.NewEncoder(&buf).Encode(session)
		assert.NoError(t, err, "encoding runSession with WillRetryError stream should succeed")
	})
}
