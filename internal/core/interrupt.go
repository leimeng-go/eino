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

package core

import (
	"context"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

type CheckPointStore interface {
	Get(ctx context.Context, checkPointID string) ([]byte, bool, error)
	Set(ctx context.Context, checkPointID string, checkPoint []byte) error
}

// CheckPointDeleter is an optional interface that CheckPointStore implementations
// can implement to support explicit checkpoint deletion.
//
// If the Store does not implement this interface, stale checkpoints will NOT be
// automatically cleaned up. The store owner is responsible for managing checkpoint
// lifecycle in that case (e.g., via TTL, external cleanup, or implementing this
// interface).
//
// CheckPointDeleter 是可选接口，CheckPointStore 实现可通过它支持显式删除检查点。
// 如果 Store 未实现此接口，过期检查点不会被自动清理。此时 store 所有者负责管理检查点生命周期（例如通过 TTL、外部清理，或实现此接口）。
type CheckPointDeleter interface {
	Delete(ctx context.Context, checkPointID string) error
}

type InterruptSignal struct {
	ID string
	Address
	InterruptInfo
	InterruptState
	Subs []*InterruptSignal
}

func (is *InterruptSignal) Error() string {
	return fmt.Sprintf("interrupt signal: ID=%s, Addr=%s, Info=%s, State=%s, SubsLen=%d",
		is.ID, is.Address.String(), is.InterruptInfo.String(), is.InterruptState.String(), len(is.Subs))
}

type InterruptState struct {
	State                any
	LayerSpecificPayload any
}

func (is *InterruptState) String() string {
	if is == nil {
		return ""
	}
	return fmt.Sprintf("interrupt state: State=%v, LayerSpecificPayload=%v", is.State, is.LayerSpecificPayload)
}

// InterruptConfig holds optional parameters for creating an interrupt.
// InterruptConfig 保存创建中断时的可选参数。
type InterruptConfig struct {
	LayerPayload any
}

// InterruptOption is a function that configures an InterruptConfig.
// InterruptOption 是用于配置 InterruptConfig 的函数。
type InterruptOption func(*InterruptConfig)

// WithLayerPayload creates an option to attach layer-specific metadata
// to the interrupt's state.
//
// WithLayerPayload 创建一个 option，用于将层级专用元数据附加到中断状态。
func WithLayerPayload(payload any) InterruptOption {
	return func(c *InterruptConfig) {
		c.LayerPayload = payload
	}
}

func Interrupt(ctx context.Context, info any, state any, subContexts []*InterruptSignal, opts ...InterruptOption) (
	*InterruptSignal, error) {
	addr := GetCurrentAddress(ctx)

	// Apply options to get config
	// 应用 options 以获取配置
	config := &InterruptConfig{}
	for _, opt := range opts {
		opt(config)
	}

	myPoint := InterruptInfo{
		Info: info,
	}

	if len(subContexts) == 0 {
		myPoint.IsRootCause = true
		return &InterruptSignal{
			ID:            uuid.NewString(),
			Address:       addr,
			InterruptInfo: myPoint,
			InterruptState: InterruptState{
				State:                state,
				LayerSpecificPayload: config.LayerPayload,
			},
		}, nil
	}

	return &InterruptSignal{
		ID:            uuid.NewString(),
		Address:       addr,
		InterruptInfo: myPoint,
		InterruptState: InterruptState{
			State:                state,
			LayerSpecificPayload: config.LayerPayload,
		},
		Subs: subContexts,
	}, nil
}

// InterruptCtx provides a complete, user-facing context for a single, resumable interrupt point.
// InterruptCtx 为单个可恢复中断点提供完整的面向用户上下文。
type InterruptCtx struct {
	// ID is the unique, fully-qualified address of the interrupt point.
	// It is constructed by joining the individual Address segments, e.g., "agent:A;node:graph_a;tool:tool_call_123".
	// This ID should be used when providing resume data via ResumeWithData.
	//
	// ID 是中断点唯一的完全限定地址。
	// 它通过连接各个 Address 段构造，例如 "agent:A;node:graph_a;tool:tool_call_123"。
	// 通过 ResumeWithData 提供恢复数据时应使用此 ID。
	ID string
	// Address is the structured sequence of AddressSegment segments that leads to the interrupt point.
	// Address 是通向中断点的 AddressSegment 段的结构化序列。
	Address Address
	// Info is the user-facing information associated with the interrupt, provided by the component that triggered it.
	// Info 是与中断关联的面向用户信息，由触发中断的组件提供。
	Info any
	// IsRootCause indicates whether the interrupt point is the exact root cause for an interruption.
	// IsRootCause 表示该中断点是否为中断的确切根因。
	IsRootCause bool
	// Parent points to the context of the parent component in the interrupt chain.
	// It is nil for the top-level interrupt.
	//
	// Parent 指向中断链中父组件的上下文。
	// 对于顶层中断，它为 nil。
	Parent *InterruptCtx
}

func (ic *InterruptCtx) EqualsWithoutID(other *InterruptCtx) bool {
	if ic == nil && other == nil {
		return true
	}

	if ic == nil || other == nil {
		return false
	}

	if !ic.Address.Equals(other.Address) {
		return false
	}

	if ic.IsRootCause != other.IsRootCause {
		return false
	}

	if ic.Info != nil || other.Info != nil {
		if ic.Info == nil || other.Info == nil {
			return false
		}

		if !reflect.DeepEqual(ic.Info, other.Info) {
			return false
		}
	}

	if ic.Parent != nil || other.Parent != nil {
		if ic.Parent == nil || other.Parent == nil {
			return false
		}

		if !ic.Parent.EqualsWithoutID(other.Parent) {
			return false
		}
	}

	return true
}

// InterruptContextsProvider is an interface for errors that contain interrupt contexts.
// This allows different packages to check for and extract interrupt contexts from errors
// without needing to know the concrete error type.
//
// InterruptContextsProvider 是包含中断上下文的错误所实现的接口。
// 它允许不同包从错误中检查并提取中断上下文，而无需知道具体错误类型。
type InterruptContextsProvider interface {
	GetInterruptContexts() []*InterruptCtx
}

// FromInterruptContexts converts a list of user-facing InterruptCtx objects into an
// internal InterruptSignal tree. It correctly handles common ancestors and ensures
// that the resulting tree is consistent with the original interrupt chain.
//
// This method is primarily used by components that bridge different execution environments.
// For example, an `adk.AgentTool` might catch an `adk.InterruptInfo`, extract the
// `adk.InterruptCtx` objects from it, and then call this method on each one. The resulting
// error signals are then typically aggregated into a single error using `compose.CompositeInterrupt`
// to be returned from the tool's `InvokableRun` method.
// FromInterruptContexts reconstructs a single InterruptSignal tree from a list of
// user-facing InterruptCtx objects. It correctly merges common ancestors.
//
// FromInterruptContexts 将一组面向用户的 InterruptCtx 对象转换为内部 InterruptSignal 树。它能正确处理共同祖先，并确保生成的树与原始中断链一致。
// 此方法主要供桥接不同执行环境的组件使用。例如，`adk.AgentTool` 可能会捕获 `adk.InterruptInfo`，从中提取 `adk.InterruptCtx` 对象，然后对每个对象调用此方法。生成的错误信号通常会再通过 `compose.CompositeInterrupt` 聚合为单个错误，并从工具的 `InvokableRun` 方法返回。
// FromInterruptContexts 会从一组面向用户的 InterruptCtx 对象重建单个 InterruptSignal 树。它会正确合并共同祖先。
func FromInterruptContexts(contexts []*InterruptCtx) *InterruptSignal {
	if len(contexts) == 0 {
		return nil
	}

	signalMap := make(map[string]*InterruptSignal)
	var rootSignal *InterruptSignal

	// getOrCreateSignal is a recursive helper that builds the tree bottom-up.
	// getOrCreateSignal 是一个递归辅助函数，用于自底向上构建树。
	var getOrCreateSignal func(*InterruptCtx) *InterruptSignal
	getOrCreateSignal = func(ctx *InterruptCtx) *InterruptSignal {
		if ctx == nil {
			return nil
		}
		// If we've already created a signal for this context, return it.
		// 如果已为此 context 创建过信号，则直接返回。
		if signal, exists := signalMap[ctx.ID]; exists {
			return signal
		}

		// Create the signal for the current context.
		// 为当前 context 创建信号。
		newSignal := &InterruptSignal{
			ID:      ctx.ID,
			Address: ctx.Address,
			InterruptInfo: InterruptInfo{
				Info:        ctx.Info,
				IsRootCause: ctx.IsRootCause,
			},
		}
		signalMap[ctx.ID] = newSignal // Cache it immediately.
		// 立即缓存它。

		// Recursively ensure the parent exists. If it doesn't, this is the root.
		// 递归确保父级存在。若不存在，则当前为根。
		if parentSignal := getOrCreateSignal(ctx.Parent); parentSignal != nil {
			parentSignal.Subs = append(parentSignal.Subs, newSignal)
		} else {
			rootSignal = newSignal
		}
		return newSignal
	}

	// Process all contexts to ensure all branches of the tree are built.
	// 处理所有 context，确保构建出树的所有分支。
	for _, ctx := range contexts {
		_ = getOrCreateSignal(ctx)
	}

	return rootSignal
}

// ToInterruptContexts converts the internal InterruptSignal tree into a list of
// user-facing InterruptCtx objects for the root causes of the interruption.
// Each returned context has its Parent field populated (if it has a parent),
// allowing traversal up the interrupt chain.
//
// If allowedSegmentTypes is nil, all segment types are kept and addresses are unchanged.
// If allowedSegmentTypes is provided, it:
//  1. Filters the parent chain to only keep contexts whose leaf segment type is allowed
//  2. Strips non-allowed segment types from all addresses
//
// ToInterruptContexts 将内部 InterruptSignal 树转换为面向用户的 InterruptCtx 对象列表，表示中断的根因。
// 每个返回的 context 都会填充其 Parent 字段（如果有父级），以便沿中断链向上遍历。
// 如果 allowedSegmentTypes 为 nil，则保留所有 segment type，地址保持不变。
// 如果提供 allowedSegmentTypes，则会：
// 1. 过滤父链，只保留叶子 segment type 被允许的 context
// 2. 从所有地址中剥离未允许的 segment type
func ToInterruptContexts(is *InterruptSignal, allowedSegmentTypes []AddressSegmentType) []*InterruptCtx {
	if is == nil {
		return nil
	}
	var rootCauseContexts []*InterruptCtx

	var buildContexts func(*InterruptSignal, *InterruptCtx)
	buildContexts = func(signal *InterruptSignal, parentCtx *InterruptCtx) {
		currentCtx := &InterruptCtx{
			ID:          signal.ID,
			Address:     signal.Address,
			Info:        signal.InterruptInfo.Info,
			IsRootCause: signal.InterruptInfo.IsRootCause,
			Parent:      parentCtx,
		}

		if currentCtx.IsRootCause {
			rootCauseContexts = append(rootCauseContexts, currentCtx)
		}

		for _, subSignal := range signal.Subs {
			buildContexts(subSignal, currentCtx)
		}
	}

	buildContexts(is, nil)

	if len(allowedSegmentTypes) > 0 {
		allowedSet := make(map[AddressSegmentType]bool, len(allowedSegmentTypes))
		for _, t := range allowedSegmentTypes {
			allowedSet[t] = true
		}

		for _, ctx := range rootCauseContexts {
			filterParentChain(ctx, allowedSet)
			encapsulateContextAddresses(ctx, allowedSet)
		}
	}

	return rootCauseContexts
}

func filterParentChain(ctx *InterruptCtx, allowedSet map[AddressSegmentType]bool) {
	if ctx == nil {
		return
	}

	parent := ctx.Parent
	for parent != nil {
		if len(parent.Address) > 0 && allowedSet[parent.Address[len(parent.Address)-1].Type] {
			break
		}
		parent = parent.Parent
	}

	ctx.Parent = parent

	filterParentChain(parent, allowedSet)
}

func encapsulateContextAddresses(ctx *InterruptCtx, allowedSet map[AddressSegmentType]bool) {
	for c := ctx; c != nil; c = c.Parent {
		newAddr := make(Address, 0, len(c.Address))
		for _, seg := range c.Address {
			if allowedSet[seg.Type] {
				newAddr = append(newAddr, seg)
			}
		}
		c.Address = newAddr
	}
}

// SignalToPersistenceMaps flattens an InterruptSignal tree into two maps suitable for persistence in a checkpoint.
// SignalToPersistenceMaps 将 InterruptSignal 树展平为两个适合持久化到检查点的 map。
func SignalToPersistenceMaps(is *InterruptSignal) (map[string]Address, map[string]InterruptState) {
	id2addr := make(map[string]Address)
	id2state := make(map[string]InterruptState)

	if is == nil {
		return id2addr, id2state
	}

	var traverse func(*InterruptSignal)
	traverse = func(signal *InterruptSignal) {
		// Add current signal's data to the maps.
		// 将当前信号的数据添加到 map。
		id2addr[signal.ID] = signal.Address
		id2state[signal.ID] = signal.InterruptState // The embedded struct
		// 嵌入的 struct

		// Recurse into children.
		// 递归处理子节点。
		for _, sub := range signal.Subs {
			traverse(sub)
		}
	}

	traverse(is)
	return id2addr, id2state
}
