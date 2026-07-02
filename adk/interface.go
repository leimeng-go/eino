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
	"fmt"
	"io"

	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/internal/core"
	"github.com/cloudwego/eino/schema"
)

// ComponentOfAgent is the component type identifier for ADK agents in callbacks.
// Use this to filter callback events to only agent-related events.
//
// ComponentOfAgent 是回调中 ADK agents 的组件类型标识符。
// 用它将回调事件过滤为仅 agent 相关事件。
const ComponentOfAgent components.Component = "Agent"

// ComponentOfAgenticAgent is the component type identifier for ADK agents
// that use *schema.AgenticMessage in callbacks.
//
// ComponentOfAgenticAgent 是使用 *schema.AgenticMessage 的 ADK agents 在回调中的组件类型标识符。
const ComponentOfAgenticAgent components.Component = "AgenticAgent"

// MessageType is the sealed type constraint for message types used in ADK.
// Only *schema.Message and *schema.AgenticMessage satisfy this constraint.
// External packages cannot add new types to this union; all generic functions
// in ADK use exhaustive type switches on these two types.
//
// MessageType 是 ADK 中用于消息类型的封闭类型约束。
// 只有 *schema.Message 和 *schema.AgenticMessage 满足此约束。
// 外部包不能向该 union 添加新类型；ADK 中所有泛型函数都会对这两种类型使用穷尽的 type switch。
type MessageType interface {
	*schema.Message | *schema.AgenticMessage
}

type Message = *schema.Message
type MessageStream = *schema.StreamReader[Message]

type AgenticMessage = *schema.AgenticMessage
type AgenticMessageStream = *schema.StreamReader[AgenticMessage]

// isNilMessage checks whether a generic message value is nil.
// Direct `msg == nil` does not compile for generic pointer types in Go;
// the canonical workaround is to compare through the `any` interface.
//
// isNilMessage 检查泛型消息值是否为 nil。
// 在 Go 中，泛型指针类型不能直接 `msg == nil`；
// 规范的变通方法是通过 `any` 接口比较。
func isNilMessage[M MessageType](msg M) bool {
	var zero M
	return any(msg) == any(zero)
}

// TypedMessageVariant represents a message output from an agent event.
// It carries either a complete message or a streaming reader, along with
// metadata describing the event's origin.
//
// Role and ToolName are only meaningful for *schema.Message events. For
// *schema.AgenticMessage events (created via EventFromAgenticMessage), these
// fields are always zero-valued because AgenticMessage carries tool results as
// ContentBlocks within the message itself and does not support agent transfer.
//
// For *schema.Message events, Role and ToolName exist independently of the inner
// Message because in streaming mode (IsStreaming=true, Message=nil), the message
// has not materialized yet and the consumer needs metadata without consuming the stream.
//
// TypedMessageVariant 表示来自智能体事件的消息输出。
// 它携带完整消息或流式读取器，并附带描述事件来源的元数据。
// Role 和 ToolName 仅对 *schema.Message 事件有意义。对于通过 EventFromAgenticMessage 创建的 *schema.AgenticMessage 事件，这些字段始终为零值，因为 AgenticMessage 将工具结果作为消息内部的 ContentBlocks 携带，且不支持 agent transfer。
// 对于 *schema.Message 事件，Role 和 ToolName 独立于内部 Message 存在，因为在流式模式下（IsStreaming=true, Message=nil），消息尚未生成，消费者需要在不消费 stream 的情况下获取元数据。
type TypedMessageVariant[M MessageType] struct {
	IsStreaming bool

	Message       M
	MessageStream *schema.StreamReader[M]

	// Role indicates the origin of this event within the agent's ReAct loop.
	// Only meaningful for *schema.Message events:
	//   - schema.Assistant: the event carries model output (generation or stream).
	//   - schema.Tool: the event carries a tool execution result.
	// Always zero-valued for *schema.AgenticMessage events; use AgenticRole instead.
	//
	// Role 表示此事件在智能体 ReAct 循环中的来源。
	// 仅对 *schema.Message 事件有意义：
	// - schema.Assistant：事件携带 model 输出（生成或 stream）。
	// - schema.Tool：事件携带工具执行结果。
	// 对于 *schema.AgenticMessage 事件始终为零值；请改用 AgenticRole。
	Role schema.RoleType

	// AgenticRole indicates the role of the agentic message (assistant, user, system).
	// Only meaningful for *schema.AgenticMessage events.
	// In streaming mode, this is available before consuming the stream.
	// Always zero-valued for *schema.Message events; use Role instead.
	//
	// AgenticRole 表示 agentic message 的角色（assistant、user、system）。
	// 仅对 *schema.AgenticMessage 事件有意义。
	// 在流式模式下，可在消费 stream 前获取。
	// 对于 *schema.Message 事件始终为零值；请改用 Role。
	AgenticRole schema.AgenticRoleType

	// ToolName is the name of the tool that produced this event.
	// Only meaningful for *schema.Message events: non-empty when Role == schema.Tool.
	// In streaming mode, this is the only way to identify the source tool before
	// the stream is consumed.
	// Always empty for *schema.AgenticMessage events.
	//
	// ToolName 是产生此事件的工具名称。
	// 仅对 *schema.Message 事件有意义：当 Role == schema.Tool 时非空。
	// 在流式模式下，这是消费 stream 前识别源工具的唯一方式。
	// 对于 *schema.AgenticMessage 事件始终为空。
	ToolName string
}

func (mv *TypedMessageVariant[M]) GetMessage() (M, error) {
	if mv.IsStreaming {
		return concatMessageStream(mv.MessageStream)
	}
	return mv.Message, nil
}

type MessageVariant = TypedMessageVariant[*schema.Message]

type messageVariantSerialization struct {
	IsStreaming   bool
	Message       Message
	MessageStream Message
	Role          schema.RoleType
	ToolName      string
}

type agenticMessageVariantSerialization struct {
	IsStreaming   bool
	Message       *schema.AgenticMessage
	MessageStream *schema.AgenticMessage
	Role          schema.RoleType
	AgenticRole   schema.AgenticRoleType
	ToolName      string
}

func (mv *TypedMessageVariant[M]) GobEncode() ([]byte, error) {
	if mvMsg, ok := any(mv).(*TypedMessageVariant[*schema.Message]); ok {
		return gobEncodeMessageVariant(mvMsg)
	}
	if mvAgentic, ok := any(mv).(*TypedMessageVariant[*schema.AgenticMessage]); ok {
		return gobEncodeAgenticMessageVariant(mvAgentic)
	}
	return nil, fmt.Errorf("gob encoding not supported for this message type")
}

func (mv *TypedMessageVariant[M]) GobDecode(b []byte) error {
	if mvMsg, ok := any(mv).(*TypedMessageVariant[*schema.Message]); ok {
		return gobDecodeMessageVariant(mvMsg, b)
	}
	if mvAgentic, ok := any(mv).(*TypedMessageVariant[*schema.AgenticMessage]); ok {
		return gobDecodeAgenticMessageVariant(mvAgentic, b)
	}
	return fmt.Errorf("gob decoding not supported for this message type")
}

func gobEncodeMessageVariant(mv *TypedMessageVariant[*schema.Message]) ([]byte, error) {
	s := &messageVariantSerialization{
		IsStreaming: mv.IsStreaming,
		Message:     mv.Message,
		Role:        mv.Role,
		ToolName:    mv.ToolName,
	}
	if mv.IsStreaming {
		var messages []Message
		for {
			frame, err := mv.MessageStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("error receiving message stream: %w", err)
			}
			messages = append(messages, frame)
		}
		m, err := schema.ConcatMessages(messages)
		if err != nil {
			return nil, fmt.Errorf("failed to encode message: cannot concat message stream: %w", err)
		}
		s.MessageStream = m
	}
	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(s)
	if err != nil {
		return nil, fmt.Errorf("failed to gob encode message variant: %w", err)
	}
	return buf.Bytes(), nil
}

func gobDecodeMessageVariant(mv *TypedMessageVariant[*schema.Message], b []byte) error {
	s := &messageVariantSerialization{}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(s)
	if err != nil {
		return fmt.Errorf("failed to decoding message variant: %w", err)
	}
	mv.IsStreaming = s.IsStreaming
	mv.Message = s.Message
	mv.Role = s.Role
	mv.ToolName = s.ToolName
	if s.MessageStream != nil {
		mv.MessageStream = schema.StreamReaderFromArray([]*schema.Message{s.MessageStream})
	}
	return nil
}

func gobEncodeAgenticMessageVariant(mv *TypedMessageVariant[*schema.AgenticMessage]) ([]byte, error) {
	s := &agenticMessageVariantSerialization{
		IsStreaming: mv.IsStreaming,
		Message:     mv.Message,
		Role:        mv.Role,
		AgenticRole: mv.AgenticRole,
		ToolName:    mv.ToolName,
	}
	if mv.IsStreaming {
		var messages []*schema.AgenticMessage
		for {
			frame, err := mv.MessageStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("error receiving agentic message stream: %w", err)
			}
			messages = append(messages, frame)
		}
		m, err := schema.ConcatAgenticMessages(messages)
		if err != nil {
			return nil, fmt.Errorf("failed to encode agentic message: cannot concat message stream: %w", err)
		}
		s.MessageStream = m
	}
	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(s)
	if err != nil {
		return nil, fmt.Errorf("failed to gob encode agentic message variant: %w", err)
	}
	return buf.Bytes(), nil
}

func gobDecodeAgenticMessageVariant(mv *TypedMessageVariant[*schema.AgenticMessage], b []byte) error {
	s := &agenticMessageVariantSerialization{}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(s)
	if err != nil {
		return fmt.Errorf("failed to decode agentic message variant: %w", err)
	}
	mv.IsStreaming = s.IsStreaming
	mv.Message = s.Message
	mv.Role = s.Role
	mv.AgenticRole = s.AgenticRole
	mv.ToolName = s.ToolName
	if s.MessageStream != nil {
		mv.MessageStream = schema.StreamReaderFromArray([]*schema.AgenticMessage{s.MessageStream})
	}
	return nil
}

// typedEventFromMessage creates a TypedAgentEvent containing the given message and optional stream.
// typedEventFromMessage 创建一个包含给定消息和可选流的 TypedAgentEvent。
func typedEventFromMessage[M MessageType](msg M, msgStream *schema.StreamReader[M],
	role schema.RoleType, toolName string) *TypedAgentEvent[M] {
	return &TypedAgentEvent[M]{
		Output: &TypedAgentOutput[M]{
			MessageOutput: &TypedMessageVariant[M]{
				IsStreaming:   msgStream != nil,
				Message:       msg,
				MessageStream: msgStream,
				Role:          role,
				ToolName:      toolName,
			},
		},
	}
}

// typedModelOutputEvent creates a model-output event for the generic path.
// For *schema.Message, Role is set to schema.Assistant.
// For *schema.AgenticMessage, AgenticRole is set to schema.AgenticRoleTypeAssistant.
//
// typedModelOutputEvent 为泛型路径创建一个模型输出事件。
// 对于 *schema.Message，Role 会设置为 schema.Assistant。
// 对于 *schema.AgenticMessage，AgenticRole 会设置为 schema.AgenticRoleTypeAssistant。
func typedModelOutputEvent[M MessageType](msg M, msgStream *schema.StreamReader[M]) *TypedAgentEvent[M] {
	var role schema.RoleType
	var agenticRole schema.AgenticRoleType
	var zero M
	if _, ok := any(zero).(*schema.Message); ok {
		role = schema.Assistant
	} else {
		agenticRole = schema.AgenticRoleTypeAssistant
	}
	event := typedEventFromMessage(msg, msgStream, role, "")
	event.Output.MessageOutput.AgenticRole = agenticRole
	return event
}

// EventFromMessage creates an AgentEvent containing the given message and optional stream.
//
// role identifies the origin of this event:
//   - schema.Assistant: model output (generation or stream).
//   - schema.Tool: tool execution result; toolName must be non-empty.
//
// For *schema.AgenticMessage events, use EventFromAgenticMessage instead.
//
// EventFromMessage 创建一个包含给定消息和可选流的 AgentEvent。
// role 标识该事件的来源：
// - schema.Assistant：模型输出（生成或流）。
// - schema.Tool：工具执行结果；toolName 必须非空。
// 对于 *schema.AgenticMessage 事件，请使用 EventFromAgenticMessage。
func EventFromMessage(msg Message, msgStream *schema.StreamReader[Message],
	role schema.RoleType, toolName string) *AgentEvent {
	return typedEventFromMessage(msg, msgStream, role, toolName)
}

// EventFromAgenticMessage creates a TypedAgentEvent for the AgenticMessage path.
// Unlike EventFromMessage, it does not require role or toolName parameters because
// AgenticMessage carries tool results as ContentBlocks within the message itself,
// and does not support agent transfer.
//
// agenticRole identifies the role of the message (e.g. schema.AgenticRoleTypeAssistant).
// In streaming mode, the role is available on the event before consuming the stream.
//
// EventFromAgenticMessage 为 AgenticMessage 路径创建一个 TypedAgentEvent。
// 不同于 EventFromMessage，它不需要 role 或 toolName 参数，因为
// AgenticMessage 会在消息自身的 ContentBlocks 中携带工具结果，
// 且不支持智能体转交。
// agenticRole 标识消息角色（例如 schema.AgenticRoleTypeAssistant）。
// 在流式模式下，消费流之前即可从事件中获取该角色。
func EventFromAgenticMessage(msg AgenticMessage, msgStream AgenticMessageStream, agenticRole schema.AgenticRoleType) *TypedAgentEvent[AgenticMessage] {
	return &TypedAgentEvent[AgenticMessage]{
		Output: &TypedAgentOutput[AgenticMessage]{
			MessageOutput: &TypedMessageVariant[AgenticMessage]{
				IsStreaming:   msgStream != nil,
				Message:       msg,
				MessageStream: msgStream,
				AgenticRole:   agenticRole,
			},
		},
	}
}

// TransferToAgentAction represents a transfer-to-agent action.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// TransferToAgentAction 表示转交给智能体的动作。
// 不推荐：智能体之间共享完整上下文的智能体转交在实证上并未证明更有效。
// 对于大多数多智能体场景，建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
type TransferToAgentAction struct {
	DestAgentName string
}

type TypedAgentOutput[M MessageType] struct {
	MessageOutput *TypedMessageVariant[M]

	CustomizedOutput any
}

type AgentOutput = TypedAgentOutput[*schema.Message]

// NewTransferToAgentAction creates an action to transfer to the specified agent.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// NewTransferToAgentAction 创建一个转交到指定智能体的动作。
// 不推荐：智能体之间共享完整上下文的智能体转交在实证上并未证明更有效。
// 对于大多数多智能体场景，建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
func NewTransferToAgentAction(destAgentName string) *AgentAction {
	return &AgentAction{TransferToAgent: &TransferToAgentAction{DestAgentName: destAgentName}}
}

// NewExitAction creates an action that signals the agent to exit.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// NewExitAction 创建一个通知智能体退出的动作。
// 不推荐：智能体之间共享完整上下文的智能体转交在实证上并未证明更有效。
// 对于大多数多智能体场景，建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
func NewExitAction() *AgentAction {
	return &AgentAction{Exit: true}
}

// AgentAction represents actions that an agent can emit during execution.
//
// Action Scoping in Agent Tools:
// When an agent is wrapped as an agent tool (via NewAgentTool), actions emitted by the inner agent
// are scoped to the tool boundary:
//   - Interrupted: Propagated via CompositeInterrupt to allow proper interrupt/resume across boundaries
//   - Exit, TransferToAgent, BreakLoop: Ignored outside the agent tool; these actions only affect
//     the inner agent's execution and do not propagate to the parent agent
//
// This scoping ensures that nested agents cannot unexpectedly terminate or transfer control
// of their parent agent's execution flow.
//
// AgentAction 表示智能体在执行期间可发出的动作。
// 智能体工具中的动作作用域：
// 当智能体被包装为智能体工具（通过 NewAgentTool）时，内部智能体发出的动作
// 会限定在工具边界内：
// - Interrupted：通过 CompositeInterrupt 传播，以支持跨边界的正确中断/恢复
// - Exit、TransferToAgent、BreakLoop：在智能体工具外会被忽略；这些动作仅影响
// 内部智能体的执行，不会传播到父智能体
// 该作用域机制确保嵌套智能体不会意外终止或转移其父智能体的执行流控制权。
type AgentAction struct {
	Exit bool

	Interrupted *InterruptInfo

	TransferToAgent *TransferToAgentAction

	BreakLoop *BreakLoopAction

	CustomizedAction any

	internalInterrupted *core.InterruptSignal
}

// RunStep represents a step in the agent execution path.
// CheckpointSchema: persisted via serialization.RunCtx (gob).
//
// NOT RECOMMENDED: RunStep is mainly relevant for agent transfer and workflow agents,
// which have not proven to be more effective empirically. Consider using ChatModelAgent
// with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// RunStep 表示智能体执行路径中的一个步骤。
// CheckpointSchema：通过 serialization.RunCtx (gob) 持久化。
// 不推荐：RunStep 主要用于智能体转交和工作流智能体，
// 这些方式在实证上并未证明更有效。对于大多数多智能体场景，
// 建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
type RunStep struct {
	agentName string
}

func init() {
	schema.RegisterName[[]RunStep]("eino_run_step_list")
}

func (r *RunStep) String() string {
	return r.agentName
}

func (r *RunStep) Equals(r1 RunStep) bool {
	return r.agentName == r1.agentName
}

func (r *RunStep) GobEncode() ([]byte, error) {
	s := &runStepSerialization{AgentName: r.agentName}
	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(s)
	if err != nil {
		return nil, fmt.Errorf("failed to gob encode RunStep: %w", err)
	}
	return buf.Bytes(), nil
}

func (r *RunStep) GobDecode(b []byte) error {
	s := &runStepSerialization{}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(s)
	if err != nil {
		return fmt.Errorf("failed to gob decode RunStep: %w", err)
	}
	r.agentName = s.AgentName
	return nil
}

type runStepSerialization struct {
	AgentName string
}

// TypedAgentEvent represents a single event emitted during agent execution.
// CheckpointSchema: persisted via serialization.RunCtx (gob).
//
// TypedAgentEvent 表示智能体执行期间发出的单个事件。
// CheckpointSchema：通过 serialization.RunCtx (gob) 持久化。
type TypedAgentEvent[M MessageType] struct {
	AgentName string

	// RunPath represents the execution path from root agent to the current event source.
	// This field is managed entirely by the framework and cannot be set by end-users.
	//
	// NOT RECOMMENDED: RunPath is mainly relevant for agent transfer and workflow agents,
	// which have not proven to be more effective empirically. For ChatModelAgent with
	// AgentTool or DeepAgent, RunPath is trivial. Consider those patterns instead.
	//
	// RunPath 表示从根智能体到当前事件源的执行路径。
	// 该字段完全由框架管理，最终用户无法设置。
	// 不推荐：RunPath 主要用于智能体转交和工作流智能体，
	// 这些方式在实证上并未证明更有效。对于带 AgentTool 的 ChatModelAgent
	// 或 DeepAgent，RunPath 很简单。建议改用这些模式。
	RunPath []RunStep

	Output *TypedAgentOutput[M]

	Action *AgentAction

	Err error
}

// AgentEvent is the default event type using *schema.Message.
// AgentEvent 是使用 *schema.Message 的默认事件类型。
type AgentEvent = TypedAgentEvent[*schema.Message]

type TypedAgentInput[M MessageType] struct {
	Messages        []M
	EnableStreaming bool
}

type AgentInput = TypedAgentInput[*schema.Message]

// TypedAgent is the base agent interface parameterized by message type.
//
// For M = *schema.Message, the full ADK feature set is supported (multi-agent
// orchestration, cancel monitoring, retry, flowAgent).
// For M = *schema.AgenticMessage, single-agent execution works but cancel
// monitoring on the model stream and retry are not yet wired.
//
// TypedAgent 是按消息类型参数化的基础智能体接口。
// 当 M = *schema.Message 时，支持完整的 ADK 功能集（多智能体
// 编排、取消监控、重试、flowAgent）。
// 当 M = *schema.AgenticMessage 时，支持单智能体执行，但模型流上的取消
// 监控和重试尚未接入。
type TypedAgent[M MessageType] interface {
	Name(ctx context.Context) string
	Description(ctx context.Context) string

	// Run runs the agent.
	// The returned AgentEvent within the AsyncIterator must be safe to modify.
	// If the returned AgentEvent within the AsyncIterator contains MessageStream,
	// the MessageStream MUST be exclusive and safe to be received directly.
	// NOTE: it's recommended to use SetAutomaticClose() on the MessageStream of AgentEvents emitted by AsyncIterator,
	// so that even the events are not processed, the MessageStream can still be closed.
	//
	// Run 运行智能体。
	// AsyncIterator 中返回的 AgentEvent 必须可安全修改。
	// 如果 AsyncIterator 中返回的 AgentEvent 包含 MessageStream，
	// 则该 MessageStream 必须是独占的，并可安全地直接接收。
	// 注意：建议对 AsyncIterator 发出的 AgentEvents 的 MessageStream 使用 SetAutomaticClose()，
	// 这样即使事件未被处理，MessageStream 也仍可关闭。
	Run(ctx context.Context, input *TypedAgentInput[M], options ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]]
}

//go:generate  mockgen -destination ../internal/mock/adk/Agent_mock.go --package adk github.com/cloudwego/eino/adk Agent,ResumableAgent
type Agent = TypedAgent[*schema.Message]

// OnSubAgents is the interface for agents that support sub-agent registration and transfer.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// OnSubAgents 是支持子智能体注册和转交的智能体接口。
// 不推荐：智能体之间共享完整上下文的智能体转交在实证上并未证明更有效。
// 对于大多数多智能体场景，建议改用带 AgentTool 的 ChatModelAgent 或 DeepAgent。
type OnSubAgents interface {
	OnSetSubAgents(ctx context.Context, subAgents []Agent) error
	OnSetAsSubAgent(ctx context.Context, parent Agent) error

	OnDisallowTransferToParent(ctx context.Context) error
}

type TypedResumableAgent[M MessageType] interface {
	TypedAgent[M]

	Resume(ctx context.Context, info *ResumeInfo, opts ...AgentRunOption) *AsyncIterator[*TypedAgentEvent[M]]
}

type ResumableAgent = TypedResumableAgent[*schema.Message]

func concatMessageStream[M MessageType](stream *schema.StreamReader[M]) (M, error) {
	var zero M
	switch s := any(stream).(type) {
	case *schema.StreamReader[*schema.Message]:
		result, err := schema.ConcatMessageStream(s)
		if err != nil {
			return zero, err
		}
		return any(result).(M), nil
	case *schema.StreamReader[*schema.AgenticMessage]:
		defer s.Close()
		var msgs []*schema.AgenticMessage
		for {
			frame, err := s.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return zero, err
			}
			msgs = append(msgs, frame)
		}
		result, err := schema.ConcatAgenticMessages(msgs)
		if err != nil {
			return zero, err
		}
		return any(result).(M), nil
	default:
		panic("unreachable: unknown MessageType")
	}
}
