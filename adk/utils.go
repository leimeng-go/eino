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
	"context"
	"errors"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/cloudwego/eino/internal"
	"github.com/cloudwego/eino/schema"
)

type AsyncIterator[T any] struct {
	ch *internal.UnboundedChan[T]
}

func (ai *AsyncIterator[T]) Next() (T, bool) {
	return ai.ch.Receive()
}

type AsyncGenerator[T any] struct {
	ch *internal.UnboundedChan[T]
}

func (ag *AsyncGenerator[T]) Send(v T) {
	ag.ch.Send(v)
}

func (ag *AsyncGenerator[T]) trySend(v T) bool {
	return ag.ch.TrySend(v)
}

func (ag *AsyncGenerator[T]) Close() {
	ag.ch.Close()
}

// NewAsyncIteratorPair returns a paired async iterator and generator
// that share the same underlying channel.
//
// NewAsyncIteratorPair 返回一对异步迭代器和生成器
// 它们共享同一个底层 channel。
func NewAsyncIteratorPair[T any]() (*AsyncIterator[T], *AsyncGenerator[T]) {
	ch := internal.NewUnboundedChan[T]()
	return &AsyncIterator[T]{ch}, &AsyncGenerator[T]{ch}
}

func copyMap[K comparable, V any](m map[K]V) map[K]V {
	res := make(map[K]V, len(m))
	for k, v := range m {
		res[k] = v
	}
	return res
}

func cloneSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	res := make([]T, len(s))
	copy(res, s)
	return res
}

func concatInstructions(instructions ...string) string {
	var sb strings.Builder
	sb.WriteString(instructions[0])
	for i := 1; i < len(instructions); i++ {
		sb.WriteString("\n\n")
		sb.WriteString(instructions[i])
	}

	return sb.String()
}

// GenTransferMessages generates assistant and tool messages to instruct a
// transfer-to-agent tool call targeting the destination agent.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// GenTransferMessages 生成 assistant 和 tool 消息，用于指示
// 面向目标智能体的 transfer-to-agent 工具调用。
// 不推荐：智能体之间共享完整上下文的智能体转交在实证上并未证明更有效。多数多智能体场景建议改用 ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
func GenTransferMessages(_ context.Context, destAgentName string) (Message, Message) {
	toolCallID := uuid.NewString()
	tooCall := schema.ToolCall{ID: toolCallID, Function: schema.FunctionCall{Name: TransferToAgentToolName, Arguments: destAgentName}}
	assistantMessage := schema.AssistantMessage("", []schema.ToolCall{tooCall})
	msg := transferToAgentToolOutput(destAgentName)
	toolMessage := schema.ToolMessage(msg, toolCallID, schema.WithToolName(TransferToAgentToolName))
	return assistantMessage, toolMessage
}

func typedSetAutomaticClose[M MessageType](e *TypedAgentEvent[M]) {
	if e.Output == nil || e.Output.MessageOutput == nil || !e.Output.MessageOutput.IsStreaming {
		return
	}

	e.Output.MessageOutput.MessageStream.SetAutomaticClose()
}

// set automatic close for event's message stream
// 为 event 的消息流设置自动关闭
func setAutomaticClose(e *AgentEvent) {
	typedSetAutomaticClose(e)
}

// getMessageFromWrappedEvent extracts the message from an AgentEvent.
// If the stream contains an error chunk, this function returns (nil, err) and
// sets StreamErr to prevent re-consumption. The nil message ensures that
// failed stream responses are not included in subsequent agents' context windows.
//
// getMessageFromWrappedEvent 从 AgentEvent 中提取消息。
// 如果流中包含错误 chunk，此函数返回 (nil, err)，并
// 设置 StreamErr 以防止再次消费。nil 消息可确保
// 失败的流响应不会被包含在后续智能体的上下文窗口中。
func getMessageFromTypedWrappedEvent[M MessageType](e *typedAgentEventWrapper[M]) (M, error) {
	var zero M
	if e.event.Output == nil || e.event.Output.MessageOutput == nil {
		return zero, nil
	}

	if !e.event.Output.MessageOutput.IsStreaming {
		return e.event.Output.MessageOutput.Message, nil
	}

	if e.StreamErr != nil {
		return zero, e.StreamErr
	}

	if !isNilMessage(e.concatenatedMessage) {
		return e.concatenatedMessage, nil
	}

	e.consumeStream()

	if e.StreamErr != nil {
		return zero, e.StreamErr
	}
	return e.concatenatedMessage, nil
}

func getMessageFromWrappedEvent(e *agentEventWrapper) (Message, error) {
	if e.AgentEvent.Output == nil || e.AgentEvent.Output.MessageOutput == nil {
		return nil, nil
	}

	if !e.AgentEvent.Output.MessageOutput.IsStreaming {
		return e.AgentEvent.Output.MessageOutput.Message, nil
	}

	if e.StreamErr != nil {
		return nil, e.StreamErr
	}

	if e.concatenatedMessage != nil {
		return e.concatenatedMessage, nil
	}

	e.consumeStream()

	if e.StreamErr != nil {
		return nil, e.StreamErr
	}
	return e.concatenatedMessage, nil
}

// consumeStream drains the message stream, setting concatenatedMessage on
// success or StreamErr on failure. The stream is always replaced with an
// error-free, materialized version safe for gob encoding.
// Must be called at most once (guarded by callers checking concatenatedMessage/StreamErr).
//
// consumeStream 会耗尽消息流，成功时设置 concatenatedMessage，
// 失败时设置 StreamErr。该流总会被替换为一个
// 无错误、已物化且可安全进行 gob 编码的版本。
// 最多只能调用一次（由调用方检查 concatenatedMessage/StreamErr 来保证）。
func (e *agentEventWrapper) consumeStream() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.concatenatedMessage != nil {
		return
	}

	s := e.AgentEvent.Output.MessageOutput.MessageStream
	var msgs []Message

	defer s.Close()
	for {
		msg, err := s.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			e.StreamErr = err
			e.AgentEvent.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray(msgs)
			return
		}
		msgs = append(msgs, msg)
	}

	if len(msgs) == 0 {
		e.StreamErr = errors.New("no messages in MessageVariant.MessageStream")
		// Defensively replace the stream. The defer s.Close() above already
		// ensures subsequent Recv() returns io.EOF, but we replace it anyway
		// to make the invariant explicit: after consumeStream, MessageStream
		// is always safe for MessageVariant.GobEncode to consume.
		//
		// 防御性地替换流。上面的 defer s.Close() 已经
		// 保证后续 Recv() 返回 io.EOF，但这里仍然替换它，
		// 以明确这个不变式：consumeStream 之后，MessageStream
		// 始终可被 MessageVariant.GobEncode 安全消费。
		e.AgentEvent.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray(msgs)
		return
	}

	if len(msgs) == 1 {
		e.concatenatedMessage = msgs[0]
	} else {
		var err error
		e.concatenatedMessage, err = schema.ConcatMessages(msgs)
		if err != nil {
			e.StreamErr = err
			e.AgentEvent.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray(msgs)
			return
		}
	}

	e.AgentEvent.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray([]Message{e.concatenatedMessage})
}

// copyTypedAgentEvent copies a TypedAgentEvent.
// If the MessageVariant is streaming, the MessageStream will be copied.
// RunPath will be deep copied.
// The result of Copy will be a new TypedAgentEvent that is:
// - safe to set fields of TypedAgentEvent
// - safe to extend RunPath
// - safe to receive from MessageStream
// NOTE: even if the event is copied, it's still not recommended to modify
// the Message itself or Chunks of the MessageStream, as they are not copied.
// NOTE: if you have CustomizedOutput or CustomizedAction, they are NOT copied.
//
// copyTypedAgentEvent 复制 TypedAgentEvent。
// 如果 MessageVariant 是流式的，会复制 MessageStream。
// RunPath 会被深拷贝。
// Copy 的结果是一个新的 TypedAgentEvent，满足：
// - 可以安全设置 TypedAgentEvent 的字段
// - 可以安全扩展 RunPath
// - 可以安全地从 MessageStream 接收
// 注意：即使 event 已复制，仍不建议修改
// Message 本身或 MessageStream 的 Chunks，因为它们不会被复制。
// 注意：如果有 CustomizedOutput 或 CustomizedAction，它们不会被复制。
func copyTypedAgentEvent[M MessageType](ae *TypedAgentEvent[M]) *TypedAgentEvent[M] {
	rp := make([]RunStep, len(ae.RunPath))
	copy(rp, ae.RunPath)

	copied := &TypedAgentEvent[M]{
		AgentName: ae.AgentName,
		RunPath:   rp,
		Action:    ae.Action,
		Err:       ae.Err,
	}

	if ae.Output == nil {
		return copied
	}

	copied.Output = &TypedAgentOutput[M]{
		CustomizedOutput: ae.Output.CustomizedOutput,
	}

	mv := ae.Output.MessageOutput
	if mv == nil {
		return copied
	}

	copied.Output.MessageOutput = &TypedMessageVariant[M]{
		IsStreaming: mv.IsStreaming,
		Role:        mv.Role,
		AgenticRole: mv.AgenticRole,
		ToolName:    mv.ToolName,
	}
	if mv.IsStreaming {
		sts := ae.Output.MessageOutput.MessageStream.Copy(2)
		mv.MessageStream = sts[0]
		copied.Output.MessageOutput.MessageStream = sts[1]
	} else {
		copied.Output.MessageOutput.Message = mv.Message
	}

	return copied
}

// TypedGetMessage extracts the message from a TypedAgentEvent, concatenating a stream if present.
// TypedGetMessage 从 TypedAgentEvent 中提取消息；如果存在流，则会将其拼接。
func TypedGetMessage[M MessageType](e *TypedAgentEvent[M]) (M, *TypedAgentEvent[M], error) {
	var zero M
	if e.Output == nil || e.Output.MessageOutput == nil {
		return zero, e, nil
	}

	msgOutput := e.Output.MessageOutput
	if msgOutput.IsStreaming {
		ss := msgOutput.MessageStream.Copy(2)
		e.Output.MessageOutput.MessageStream = ss[0]

		msg, err := concatMessageStream(ss[1])

		return msg, e, err
	}

	return msgOutput.Message, e, nil
}

// GetMessage extracts the Message from an AgentEvent. For streaming output,
// it duplicates the stream and concatenates it into a single Message.
//
// GetMessage 从 AgentEvent 中提取 Message。对于流式输出，
// 它会复制该流并将其拼接成单个 Message。
func GetMessage(e *AgentEvent) (Message, *AgentEvent, error) {
	return TypedGetMessage(e)
}

func typedErrorIter[M MessageType](err error) *AsyncIterator[*TypedAgentEvent[M]] {
	iterator, generator := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	generator.Send(&TypedAgentEvent[M]{Err: err})
	generator.Close()
	return iterator
}

func genErrorIter(err error) *AsyncIterator[*AgentEvent] {
	return typedErrorIter[*schema.Message](err)
}
