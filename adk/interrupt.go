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
	"fmt"
	"sync"

	"github.com/cloudwego/eino/internal/core"
	"github.com/cloudwego/eino/schema"
)

// ResumeInfo holds all the information necessary to resume an interrupted agent execution.
// It is created by the framework and passed to an agent's Resume method.
//
// ResumeInfo 保存恢复被中断智能体执行所需的全部信息。
// 它由框架创建，并传给智能体的 Resume 方法。
type ResumeInfo struct {
	// EnableStreaming indicates whether the original execution was in streaming mode.
	// EnableStreaming 表示原始执行是否处于流式模式。
	EnableStreaming bool

	// Deprecated: use InterruptContexts from the embedded InterruptInfo for user-facing details,
	// and GetInterruptState for internal state retrieval.
	//
	// Deprecated：面向用户的详情请使用嵌入的 InterruptInfo 中的 InterruptContexts，内部状态获取请使用 GetInterruptState。
	*InterruptInfo

	WasInterrupted bool
	InterruptState any
	IsResumeTarget bool
	ResumeData     any
}

// InterruptInfo contains all the information about an interruption event.
// It is created by the framework when an agent returns an interrupt action.
//
// InterruptInfo 包含中断事件的全部信息。
// 当智能体返回中断动作时，由框架创建。
type InterruptInfo struct {
	Data any

	// InterruptContexts provides a structured, user-facing view of the interrupt chain.
	// Each context represents a step in the agent hierarchy that was interrupted.
	//
	// InterruptContexts 提供中断链的结构化、面向用户的视图。
	// 每个 context 表示智能体层级中被中断的一个步骤。
	InterruptContexts []*InterruptCtx
}

// TypedInterrupt creates a typed interrupt event that pauses execution to request external input.
// It is the generic counterpart of Interrupt; see Interrupt for full documentation.
//
// TypedInterrupt 创建一个类型化中断事件，暂停执行以请求外部输入。
// 它是 Interrupt 的泛型对应版本；完整文档见 Interrupt。
func TypedInterrupt[M MessageType](ctx context.Context, info any) *TypedAgentEvent[M] {
	var rp []RunStep
	rCtx := getRunCtx(ctx)
	if rCtx != nil {
		rp = rCtx.RunPath
	}

	is, err := core.Interrupt(ctx, info, nil, nil,
		core.WithLayerPayload(rp))
	if err != nil {
		return &TypedAgentEvent[M]{Err: err}
	}

	contexts := core.ToInterruptContexts(is, allowedAddressSegmentTypes)

	return &TypedAgentEvent[M]{
		Action: &AgentAction{
			Interrupted: &InterruptInfo{
				InterruptContexts: contexts,
			},
			internalInterrupted: is,
		},
	}
}

// Interrupt creates a basic interrupt action.
// This is used when an agent needs to pause its execution to request external input or intervention,
// but does not need to save any internal state to be restored upon resumption.
// The `info` parameter is user-facing data that describes the reason for the interrupt.
//
// Interrupt 创建一个基本中断动作。
// 当智能体需要暂停执行以请求外部输入或干预，但不需要保存任何内部状态以便恢复时使用。
// `info` 参数是面向用户的数据，用于描述中断原因。
func Interrupt(ctx context.Context, info any) *AgentEvent {
	return TypedInterrupt[*schema.Message](ctx, info)
}

// TypedStatefulInterrupt creates a typed interrupt event that also saves the agent's internal state.
// It is the generic counterpart of StatefulInterrupt; see StatefulInterrupt for full documentation.
//
// TypedStatefulInterrupt 创建一个类型化中断事件，同时保存智能体的内部状态。
// 它是 StatefulInterrupt 的泛型对应版本；完整文档见 StatefulInterrupt。
func TypedStatefulInterrupt[M MessageType](ctx context.Context, info any, state any) *TypedAgentEvent[M] {
	var rp []RunStep
	rCtx := getRunCtx(ctx)
	if rCtx != nil {
		rp = rCtx.RunPath
	}

	is, err := core.Interrupt(ctx, info, state, nil,
		core.WithLayerPayload(rp))
	if err != nil {
		return &TypedAgentEvent[M]{Err: err}
	}

	contexts := core.ToInterruptContexts(is, allowedAddressSegmentTypes)

	return &TypedAgentEvent[M]{
		Action: &AgentAction{
			Interrupted: &InterruptInfo{
				InterruptContexts: contexts,
			},
			internalInterrupted: is,
		},
	}
}

// StatefulInterrupt creates an interrupt action that also saves the agent's internal state.
// This is used when an agent has internal state that must be restored for it to continue correctly.
// The `info` parameter is user-facing data describing the interrupt.
// The `state` parameter is the agent's internal state object, which will be serialized and stored.
//
// StatefulInterrupt 创建一个中断动作，同时保存智能体的内部状态。
// 当智能体有必须恢复才能正确继续的内部状态时使用。
// `info` 参数是描述中断的面向用户数据。
// `state` 参数是智能体的内部状态对象，将被序列化并存储。
func StatefulInterrupt(ctx context.Context, info any, state any) *AgentEvent {
	return TypedStatefulInterrupt[*schema.Message](ctx, info, state)
}

// TypedCompositeInterrupt creates a typed interrupt event that aggregates sub-interrupt signals.
// It is the generic counterpart of CompositeInterrupt; see CompositeInterrupt for full documentation.
//
// TypedCompositeInterrupt 创建一个类型化中断事件，用于聚合子中断信号。
// 它是 CompositeInterrupt 的泛型对应版本；完整文档见 CompositeInterrupt。
func TypedCompositeInterrupt[M MessageType](ctx context.Context, info any, state any,
	subInterruptSignals ...*InterruptSignal) *TypedAgentEvent[M] {
	var rp []RunStep
	rCtx := getRunCtx(ctx)
	if rCtx != nil {
		rp = rCtx.RunPath
	}

	is, err := core.Interrupt(ctx, info, state, subInterruptSignals,
		core.WithLayerPayload(rp))
	if err != nil {
		return &TypedAgentEvent[M]{Err: err}
	}

	contexts := core.ToInterruptContexts(is, allowedAddressSegmentTypes)

	return &TypedAgentEvent[M]{
		Action: &AgentAction{
			Interrupted: &InterruptInfo{
				InterruptContexts: contexts,
			},
			internalInterrupted: is,
		},
	}
}

// CompositeInterrupt creates an interrupt event that aggregates sub-interrupt signals.
// CompositeInterrupt 创建一个用于聚合子中断信号的中断事件。
func CompositeInterrupt(ctx context.Context, info any, state any,
	subInterruptSignals ...*InterruptSignal) *AgentEvent {
	return TypedCompositeInterrupt[*schema.Message](ctx, info, state, subInterruptSignals...)
}

// Address represents the unique, hierarchical address of a component within an execution.
// It is a slice of AddressSegments, where each segment represents one level of nesting.
// This is a type alias for core.Address. See the core package for more details.
//
// Address 表示执行中组件的唯一层级地址。
// 它是 AddressSegments 的切片，其中每个 segment 表示一层嵌套。
// 这是 core.Address 的类型别名。更多详情请参见 core package。
type Address = core.Address
type AddressSegment = core.AddressSegment
type AddressSegmentType = core.AddressSegmentType

const (
	AddressSegmentAgent AddressSegmentType = "agent"
	AddressSegmentTool  AddressSegmentType = "tool"
)

var allowedAddressSegmentTypes = []AddressSegmentType{AddressSegmentAgent, AddressSegmentTool}

// AppendAddressSegment adds an address segment for the current execution context.
// AppendAddressSegment 为当前执行上下文添加一个地址段。
func AppendAddressSegment(ctx context.Context, segType AddressSegmentType, segID string) context.Context {
	return core.AppendAddressSegment(ctx, segType, segID, "")
}

// InterruptCtx provides a structured, user-facing view of a single point of interruption.
// It contains the ID and Address of the interrupted component, as well as user-defined info.
// This is a type alias for core.InterruptCtx. See the core package for more details.
//
// InterruptCtx 提供单个中断点的结构化、面向用户的视图。
// 它包含被中断组件的 ID 和 Address，以及用户自定义信息。
// 这是 core.InterruptCtx 的类型别名。更多细节请参见 core 包。
type InterruptCtx = core.InterruptCtx
type InterruptSignal = core.InterruptSignal

// FromInterruptContexts converts user-facing interrupt contexts to an interrupt signal.
// FromInterruptContexts 将面向用户的中断上下文转换为中断信号。
func FromInterruptContexts(contexts []*InterruptCtx) *InterruptSignal {
	return core.FromInterruptContexts(contexts)
}

// WithCheckPointID sets the checkpoint ID used for interruption persistence.
// WithCheckPointID 设置用于中断持久化的检查点 ID。
func WithCheckPointID(id string) AgentRunOption {
	return WrapImplSpecificOptFn(func(t *options) {
		t.checkPointID = &id
	})
}

func init() {
	schema.RegisterName[*serialization]("_eino_adk_serialization")
	schema.RegisterName[*WorkflowInterruptInfo]("_eino_adk_workflow_interrupt_info")
	// Register []byte for gob: the cancel refactor routes bridge store checkpoint
	// bytes ([]byte) through InterruptState.State (type any) inside the outer
	// serialization struct. Gob requires concrete types behind interface fields
	// to be registered.
	//
	// 为 gob 注册 []byte：cancel 重构会通过外层序列化结构内的 InterruptState.State（类型为 any）传递 bridge store 检查点字节（[]byte）。
	// gob 要求注册 interface 字段背后的具体类型。
	gob.Register([]byte{})
}

// serialization CheckpointSchema: root checkpoint payload (gob).
// Any type tagged with `CheckpointSchema:` is persisted and must remain backward compatible.
//
// serialization CheckpointSchema：根检查点负载（gob）。
// 任何标记为 `CheckpointSchema:` 的类型都会被持久化，并且必须保持向后兼容。
type serialization struct {
	RunCtx *runContext
	// deprecated: still keep it here for backward compatibility
	// deprecated：为向后兼容仍保留在这里
	Info                *InterruptInfo
	EnableStreaming     bool
	InterruptID2Address map[string]Address
	InterruptID2State   map[string]core.InterruptState
}

func runnerLoadCheckPointImpl(store CheckPointStore, ctx context.Context, checkpointID string) (
	context.Context, *runContext, *ResumeInfo, error) {
	data, existed, err := store.Get(ctx, checkpointID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get checkpoint from store: %w", err)
	}
	if !existed {
		return nil, nil, nil, fmt.Errorf("checkpoint[%s] not exist", checkpointID)
	}

	data = preprocessADKCheckpoint(data)

	s := &serialization{}
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(s)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to decode checkpoint: %w", err)
	}
	ctx = core.PopulateInterruptState(ctx, s.InterruptID2Address, s.InterruptID2State)

	return ctx, s.RunCtx, &ResumeInfo{
		EnableStreaming: s.EnableStreaming,
		InterruptInfo:   s.Info,
	}, nil
}

// preprocessADKCheckpoint fixes a gob incompatibility when resuming old ChatModelAgent/DeepAgents checkpoints.
//
// Background
//   - ADK checkpoints are gob-encoded.
//   - Some values inside checkpoints are stored as `any`, so gob includes a concrete type name
//     string in the wire format and uses that name to pick the local Go type to decode into.
//
// Problem (v0.8.0-v0.8.3 checkpoints)
//   - In v0.8.0-v0.8.3, *State was registered under the name "_eino_adk_react_state" AND
//     implemented GobEncode/GobDecode, so the wire format for that name is "GobEncoder payload"
//     (opaque bytes).
//   - In v0.7.*, the same name "_eino_adk_react_state" was used but encoded as a normal struct
//     (no GobEncode). Gob treats these two wire formats as incompatible.
//   - Gob only allows one local Go type per name. Today we register "_eino_adk_react_state" to
//     a v0.7-compatible struct decoder (stateV07). If we try to decode a v0.8.0-v0.8.3
//     checkpoint under that same name, gob fails with a "want struct; got non-struct" mismatch.
//
// Solution
//   - We keep "_eino_adk_react_state" mapped to the v0.7 decoder.
//   - For v0.8.0-v0.8.3 checkpoints only, we rewrite the on-wire name to a same-length alias
//     "_eino_adk_state_v080_", which is registered to a GobDecoder-compatible type (stateV080).
//   - The alias is the same length as the original, so we can safely replace the length-prefixed
//     bytes without re-encoding the whole stream.
//
// preprocessADKCheckpoint 修复恢复旧 ChatModelAgent/DeepAgents 检查点时的 gob 不兼容问题。
// 背景
// - ADK 检查点使用 gob 编码。
// - 检查点中的某些值以 `any` 存储，因此 gob 会在线上格式中包含具体类型名字符串，并用该名称选择本地 Go 类型进行解码。
// 问题（v0.8.0-v0.8.3 检查点）
// - 在 v0.8.0-v0.8.3 中，*State 以名称 "_eino_adk_react_state" 注册，并实现了 GobEncode/GobDecode，因此该名称的线上格式是 "GobEncoder payload"（不透明字节）。
// - 在 v0.7.* 中，同一名称 "_eino_adk_react_state" 被使用，但编码为普通 struct（没有 GobEncode）。gob 认为这两种线上格式不兼容。
// - gob 对每个名称只允许一个本地 Go 类型。现在我们将 "_eino_adk_react_state" 注册到兼容 v0.7 的 struct 解码器（stateV07）。如果尝试用同一名称解码 v0.8.0-v0.8.3 检查点，gob 会因 "want struct; got non-struct" 不匹配而失败。
// 解决方案
// - 保持 "_eino_adk_react_state" 映射到 v0.7 解码器。
// - 仅对 v0.8.0-v0.8.3 检查点，将线上名称改写为等长别名 "_eino_adk_state_v080_"，该别名注册到兼容 GobDecoder 的类型（stateV080）。
// - 该别名与原名称长度相同，因此可以安全替换带长度前缀的字节，而无需重新编码整个流。
func preprocessADKCheckpoint(data []byte) []byte {
	const (
		lenPrefixedReactStateName         = "\x15" + stateGobNameV07
		lenPrefixedCompatName             = "\x15" + stateGobNameV080
		lenPrefixedStateSerializationName = "\x12stateSerialization"
	)

	// the following line checks whether the checkpoint is persisted through v0.8.0-v0.8.3
	// 下面这行检查检查点是否通过 v0.8.0-v0.8.3 持久化
	if !bytes.Contains(data, []byte(lenPrefixedReactStateName)) || !bytes.Contains(data, []byte(lenPrefixedStateSerializationName)) {
		return data
	}
	return bytes.ReplaceAll(data,
		[]byte(lenPrefixedReactStateName),
		[]byte(lenPrefixedCompatName))
}

func runnerSaveCheckPointImpl(
	enableStreaming bool,
	store CheckPointStore,
	ctx context.Context,
	key string,
	info *InterruptInfo,
	is *core.InterruptSignal,
) error {
	if store == nil {
		return nil
	}

	runCtx := getRunCtx(ctx)

	id2Addr, id2State := core.SignalToPersistenceMaps(is)

	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(&serialization{
		RunCtx:              runCtx,
		Info:                info,
		InterruptID2Address: id2Addr,
		InterruptID2State:   id2State,
		EnableStreaming:     enableStreaming,
	})
	if err != nil {
		return fmt.Errorf("failed to encode checkpoint: %w", err)
	}
	return store.Set(ctx, key, buf.Bytes())
}

const bridgeCheckpointID = "adk_react_mock_key"

func newBridgeStore() *bridgeStore {
	return &bridgeStore{data: make(map[string][]byte)}
}

func newResumeBridgeStore(checkPointID string, data []byte) *bridgeStore {
	return &bridgeStore{
		data: map[string][]byte{checkPointID: data},
	}
}

type bridgeStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *bridgeStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[key]; ok {
		return v, true, nil
	}
	return nil, false, nil
}

func (m *bridgeStore) Set(_ context.Context, key string, checkPoint []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[key] = checkPoint
	return nil
}

func getNextResumeAgent(ctx context.Context, _ *ResumeInfo) (string, error) {
	nextAgents, err := core.GetNextResumptionPoints(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get next agent leading to interruption: %w", err)
	}

	if len(nextAgents) == 0 {
		return "", errors.New("no child agents leading to interrupted agent were found")
	}

	if len(nextAgents) > 1 {
		return "", errors.New("agent has multiple child agents leading to interruption, " +
			"but concurrent transfer is not supported")
	}

	// get the single next agent to delegate to.
	// 获取要委托给的唯一下一个智能体。
	var nextAgentID string
	for id := range nextAgents {
		nextAgentID = id
		break
	}

	return nextAgentID, nil
}

func getNextResumeAgents(ctx context.Context, _ *ResumeInfo) (map[string]bool, error) {
	nextAgents, err := core.GetNextResumptionPoints(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get next agents leading to interruption: %w", err)
	}

	if len(nextAgents) == 0 {
		return nil, errors.New("no child agents leading to interrupted agent were found")
	}

	return nextAgents, nil
}

func buildResumeInfo(ctx context.Context, nextAgentID string, info *ResumeInfo) (
	context.Context, *ResumeInfo) {
	ctx = AppendAddressSegment(ctx, AddressSegmentAgent, nextAgentID)
	nextResumeInfo := &ResumeInfo{
		EnableStreaming: info.EnableStreaming,
		InterruptInfo:   info.InterruptInfo,
	}

	wasInterrupted, hasState, state := core.GetInterruptState[any](ctx)
	nextResumeInfo.WasInterrupted = wasInterrupted
	if hasState {
		nextResumeInfo.InterruptState = state
	}

	if wasInterrupted {
		isResumeTarget, hasData, data := core.GetResumeContext[any](ctx)
		nextResumeInfo.IsResumeTarget = isResumeTarget
		if hasData {
			nextResumeInfo.ResumeData = data
		}
	}

	ctx = updateRunPathOnly(ctx, nextAgentID)

	return ctx, nextResumeInfo
}
