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

package compose

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/cloudwego/eino/internal/core"
	"github.com/cloudwego/eino/schema"
)

// WithInterruptBeforeNodes instructs to interrupt before the given nodes.
// WithInterruptBeforeNodes 指示在给定节点之前中断。
func WithInterruptBeforeNodes(nodes []string) GraphCompileOption {
	return func(options *graphCompileOptions) {
		options.interruptBeforeNodes = nodes
	}
}

// WithInterruptAfterNodes instructs to interrupt after the given nodes.
// WithInterruptAfterNodes 指示在给定节点之后中断。
func WithInterruptAfterNodes(nodes []string) GraphCompileOption {
	return func(options *graphCompileOptions) {
		options.interruptAfterNodes = nodes
	}
}

// Deprecated: prefer Interrupt/StatefulInterrupt and CompositeInterrupt.
// If you need to pass the legacy error into CompositeInterrupt, wrap it using WrapInterruptAndRerunIfNeeded first.
//
// 已弃用：请优先使用 Interrupt/StatefulInterrupt 和 CompositeInterrupt。
// 如果需要将旧版 error 传入 CompositeInterrupt，请先用 WrapInterruptAndRerunIfNeeded 包装。
var InterruptAndRerun = deprecatedInterruptAndRerun
var deprecatedInterruptAndRerun = errors.New("interrupt and rerun")

// NewInterruptAndRerunErr creates a legacy interrupt-and-rerun error.
// Deprecated: prefer Interrupt(ctx, info) or StatefulInterrupt(ctx, info, state).
// If passing into CompositeInterrupt, wrap using WrapInterruptAndRerunIfNeeded first.
//
// NewInterruptAndRerunErr 创建旧版 interrupt-and-rerun error。
// 已弃用：请优先使用 Interrupt(ctx, info) 或 StatefulInterrupt(ctx, info, state)。
// 如果要传入 CompositeInterrupt，请先用 WrapInterruptAndRerunIfNeeded 包装。
func NewInterruptAndRerunErr(extra any) error {
	return deprecatedInterruptAndRerunErr(extra)
}
func deprecatedInterruptAndRerunErr(extra any) error {
	return &core.InterruptSignal{InterruptInfo: core.InterruptInfo{
		Info:        extra,
		IsRootCause: true,
	}}
}

type wrappedInterruptAndRerun struct {
	ps    Address
	inner error
}

func (w *wrappedInterruptAndRerun) Error() string {
	return fmt.Sprintf("interrupt and rerun at address %s: %s", w.ps.String(), w.inner.Error())
}

func (w *wrappedInterruptAndRerun) Unwrap() error {
	return w.inner
}

// WrapInterruptAndRerunIfNeeded wraps the deprecated old interrupt errors, with the current execution address.
// If the error is returned by either Interrupt, StatefulInterrupt or CompositeInterrupt,
// it will be returned as-is without wrapping
//
// WrapInterruptAndRerunIfNeeded 会用当前执行地址包装已弃用的旧版中断 error。
// 如果 error 由 Interrupt、StatefulInterrupt 或 CompositeInterrupt 返回，
// 则会原样返回，不再包装
func WrapInterruptAndRerunIfNeeded(ctx context.Context, step AddressSegment, err error) error {
	addr := GetCurrentAddress(ctx)
	newAddr := append(append([]AddressSegment{}, addr...), step)
	if errors.Is(err, deprecatedInterruptAndRerun) {
		return &wrappedInterruptAndRerun{
			ps:    newAddr,
			inner: err,
		}
	}

	ire := &core.InterruptSignal{}
	if errors.As(err, &ire) {
		if ire.Address == nil {
			return &wrappedInterruptAndRerun{
				ps:    newAddr,
				inner: err,
			}
		}
		return ire
	}

	return fmt.Errorf("failed to wrap error as addressed InterruptAndRerun: %w", err)
}

// Interrupt creates a special error that signals the execution engine to interrupt
// the current run at the component's specific address and save a checkpoint.
//
// This is the standard way for a single, non-composite component to signal a resumable interruption.
//
//   - ctx: The context of the running component, used to retrieve the current execution address.
//   - info: User-facing information about the interrupt. This is not persisted but is exposed to the
//     calling application via the InterruptCtx to provide context (e.g., a reason for the pause).
//
// Interrupt 创建一个特殊 error，用于通知执行引擎在组件的特定地址中断当前运行并保存检查点。
// 这是单个非复合组件发出可恢复中断的标准方式。
// - ctx：运行中组件的 context，用于获取当前执行地址。
// - info：面向用户的中断信息。它不会被持久化，但会通过 InterruptCtx 暴露给调用方应用以提供上下文（例如暂停原因）。
func Interrupt(ctx context.Context, info any) error {
	is, err := core.Interrupt(ctx, info, nil, nil)
	if err != nil {
		return err
	}

	return is
}

// StatefulInterrupt creates a special error that signals the execution engine to interrupt
// the current run at the component's specific address and save a checkpoint.
//
// This is the standard way for a single, non-composite component to signal a resumable interruption.
//
//   - ctx: The context of the running component, used to retrieve the current execution address.
//   - info: User-facing information about the interrupt. This is not persisted but is exposed to the
//     calling application via the InterruptCtx to provide context (e.g., a reason for the pause).
//   - state: The internal state that the interrupting component needs to persist to be able to resume
//     its work later. This state is saved in the checkpoint and will be provided back to the component
//     upon resumption via GetInterruptState.
//
// StatefulInterrupt 创建一个特殊 error，用于通知执行引擎在组件的特定地址中断当前运行并保存检查点。
// 这是单个非复合组件发出可恢复中断的标准方式。
// - ctx：运行中组件的 context，用于获取当前执行地址。
// - info：面向用户的中断信息。它不会被持久化，但会通过 InterruptCtx 暴露给调用方应用以提供上下文（例如暂停原因）。
// - state：中断组件为稍后恢复工作而需要持久化的内部状态。该状态会保存在检查点中，并在恢复时通过 GetInterruptState 提供回组件。
func StatefulInterrupt(ctx context.Context, info any, state any) error {
	is, err := core.Interrupt(ctx, info, state, nil)
	if err != nil {
		return err
	}

	return is
}

// CompositeInterrupt creates a special error that signals a composite interruption.
// It is designed for "composite" nodes (like ToolsNode) that manage multiple, independent,
// interruptible sub-processes. It bundles multiple sub-interrupt errors into a single error
// that the engine can deconstruct into a flat list of resumable points.
//
// This function is robust and can handle several types of errors from sub-processes:
//
//   - A `Interrupt` or `StatefulInterrupt` error from a simple component.
//
//   - A nested `CompositeInterrupt` error from another composite component.
//
//   - An error containing `InterruptInfo` returned by a `Runnable` (e.g., a Graph within a lambda node).
//
//   - An error returned by \'WrapInterruptAndRerunIfNeeded\' for the legacy old interrupt and rerun error,
//     and for the error returned by the deprecated old interrupt errors.
//
// Parameters:
//
//   - ctx: The context of the running composite node.
//
//   - info: User-facing information for the composite node itself. Can be nil.
//     This info will be attached to InterruptInfo.RerunNodeExtra.
//     Provided mainly for compatibility purpose as the composite node itself
//     is not an interrupt point with interrupt ID,
//     which means it lacks enough reason to give a user-facing info.
//
//   - state: The state for the composite node itself. Can be nil.
//     This could be useful when the composite node needs to restore state,
//     such as its input (e.g. ToolsNode).
//
//   - errs: a list of errors emitted by sub-processes.
//
// NOTE: if the error you passed in is the deprecated old interrupt and rerun err, or an error returned by
// the deprecated old interrupt function, you must wrap it using WrapInterruptAndRerunIfNeeded first
// before passing them into this function.
//
// CompositeInterrupt 创建一个特殊 error，用于表示复合中断。
// 它面向“复合”节点（如 ToolsNode）设计，这类节点管理多个独立且可中断的子进程。它会将多个子中断 error 打包为单个 error，供引擎拆解成扁平的可恢复点列表。
// 此函数很健壮，可处理来自子进程的多种 error：
// - 来自简单组件的 `Interrupt` 或 `StatefulInterrupt` error。
// - 来自其他复合组件的嵌套 `CompositeInterrupt` error。
// - 由 `Runnable` 返回且包含 `InterruptInfo` 的 error（例如 lambda 节点内的 Graph）。
// - 由 'WrapInterruptAndRerunIfNeeded' 为旧版 interrupt and rerun error 返回的 error，以及已弃用旧中断 error 返回的 error。
// 参数：
// - ctx：运行中复合节点的 context。
// - info：复合节点自身面向用户的信息。可为 nil。
// 该信息会附加到 InterruptInfo.RerunNodeExtra。
// 主要用于兼容，因为复合节点本身不是带 interrupt ID 的中断点，
// 因此没有足够理由提供面向用户的信息。
// - state：复合节点自身的状态。可为 nil。
// 当复合节点需要恢复状态时会有用，例如其输入（如 ToolsNode）。
// - errs：子进程发出的 error 列表。
// 注意：如果传入的 error 是已弃用的旧版 interrupt and rerun err，或是已弃用旧中断函数返回的 error，必须先用 WrapInterruptAndRerunIfNeeded 包装，再传入此函数。
func CompositeInterrupt(ctx context.Context, info any, state any, errs ...error) error {
	if len(errs) == 0 {
		return StatefulInterrupt(ctx, info, state)
	}

	var cErrs []*core.InterruptSignal
	for _, err := range errs {
		wrapped := &wrappedInterruptAndRerun{}
		if errors.As(err, &wrapped) {
			inner := wrapped.Unwrap()
			if errors.Is(inner, deprecatedInterruptAndRerun) {
				id := uuid.NewString()
				cErrs = append(cErrs, &core.InterruptSignal{
					ID:      id,
					Address: wrapped.ps,
					InterruptInfo: core.InterruptInfo{
						Info:        nil,
						IsRootCause: true,
					},
				})
				continue
			}

			ire := &core.InterruptSignal{}
			if errors.As(err, &ire) {
				id := uuid.NewString()
				cErrs = append(cErrs, &core.InterruptSignal{
					ID:      id,
					Address: wrapped.ps,
					InterruptInfo: core.InterruptInfo{
						Info:        ire.InterruptInfo.Info,
						IsRootCause: ire.InterruptInfo.IsRootCause,
					},
					InterruptState: core.InterruptState{
						State: ire.InterruptState.State,
					},
				})
			}

			continue
		}

		ire := &core.InterruptSignal{}
		if errors.As(err, &ire) {
			cErrs = append(cErrs, ire)
			continue
		}

		ie := &interruptError{}
		if errors.As(err, &ie) {
			is := core.FromInterruptContexts(ie.Info.InterruptContexts)
			cErrs = append(cErrs, is)
			continue
		}

		return fmt.Errorf("composite interrupt but one of the sub error is not interrupt and rerun error: %w", err)
	}

	is, err := core.Interrupt(ctx, info, state, cErrs)
	if err != nil {
		return err
	}
	return is
}

// IsInterruptRerunError reports whether the error represents an interrupt-and-rerun
// and returns any attached info.
//
// IsInterruptRerunError 报告该 error 是否表示 interrupt-and-rerun，
// 并返回任何附加信息。
func IsInterruptRerunError(err error) (any, bool) {
	info, _, ok := isInterruptRerunError(err)
	return info, ok
}

func isInterruptRerunError(err error) (info any, state any, ok bool) {
	if errors.Is(err, deprecatedInterruptAndRerun) {
		return nil, nil, true
	}
	ire := &core.InterruptSignal{}
	if errors.As(err, &ire) {
		return ire.Info, ire.State, true
	}
	return nil, nil, false
}

// InterruptInfo aggregates interrupt metadata for composite or nested runs.
// InterruptInfo 聚合复合或嵌套运行的中断元数据。
type InterruptInfo struct {
	State             any
	BeforeNodes       []string
	AfterNodes        []string
	RerunNodes        []string
	RerunNodesExtra   map[string]any
	SubGraphs         map[string]*InterruptInfo
	InterruptContexts []*InterruptCtx
}

func init() {
	schema.RegisterName[*InterruptInfo]("_eino_compose_interrupt_info")
}

// AddressSegmentType defines the type of a segment in an execution address.
// AddressSegmentType 定义执行地址中一个段的类型。
type AddressSegmentType = core.AddressSegmentType

const (
	// AddressSegmentNode represents a segment of an address that corresponds to a graph node.
	// AddressSegmentNode 表示地址中对应图节点的一个段。
	AddressSegmentNode AddressSegmentType = "node"
	// AddressSegmentTool represents a segment of an address that corresponds to a specific tool call within a ToolsNode.
	// AddressSegmentTool 表示地址中对应 ToolsNode 内特定工具调用的一个段。
	AddressSegmentTool AddressSegmentType = "tool"
	// AddressSegmentRunnable represents a segment of an address that corresponds to an instance of the Runnable interface.
	// Currently the possible Runnable types are: Graph, Workflow and Chain.
	// Note that for sub-graphs added through AddGraphNode to another graph is not a Runnable.
	// So a AddressSegmentRunnable indicates a standalone Root level Graph,
	// or a Root level Graph inside a node such as Lambda node.
	//
	// AddressSegmentRunnable 表示地址中对应 Runnable interface 实例的一个段。
	// 当前可能的 Runnable 类型有：Graph、Workflow 和 Chain。
	// 注意，通过 AddGraphNode 添加到另一个 graph 的 sub-graph 不是 Runnable。
	// 因此 AddressSegmentRunnable 表示独立的根级 Graph，
	// 或 Lambda node 等节点内的根级 Graph。
	AddressSegmentRunnable AddressSegmentType = "runnable"
)

// Address represents a full, hierarchical address to a point in the execution structure.
// Address 表示执行结构中某一点的完整层级地址。
type Address = core.Address

// AddressSegment represents a single segment in the hierarchical address of an execution point.
// A sequence of AddressSegments uniquely identifies a location within a potentially nested structure.
//
// AddressSegment 表示执行点层级地址中的单个段。
// 一组 AddressSegments 可唯一标识潜在嵌套结构中的某个位置。
type AddressSegment = core.AddressSegment

// InterruptCtx provides a complete, user-facing context for a single, resumable interrupt point.
// InterruptCtx 为单个可恢复中断点提供完整的、面向用户的上下文。
type InterruptCtx = core.InterruptCtx

// ExtractInterruptInfo extracts InterruptInfo from an error if present.
// ExtractInterruptInfo 在存在时从 error 中提取 InterruptInfo。
func ExtractInterruptInfo(err error) (info *InterruptInfo, existed bool) {
	if err == nil {
		return nil, false
	}
	var iE *interruptError
	if errors.As(err, &iE) {
		return iE.Info, true
	}
	var sIE *subGraphInterruptError
	if errors.As(err, &sIE) {
		return sIE.Info, true
	}
	return nil, false
}

type interruptError struct {
	Info *InterruptInfo
}

func (e *interruptError) Error() string {
	return fmt.Sprintf("interrupt happened, info: %+v", e.Info)
}

func (e *interruptError) GetInterruptContexts() []*InterruptCtx {
	if e.Info == nil {
		return nil
	}
	return e.Info.InterruptContexts
}

func isSubGraphInterrupt(err error) *subGraphInterruptError {
	if err == nil {
		return nil
	}
	var iE *subGraphInterruptError
	if errors.As(err, &iE) {
		return iE
	}
	return nil
}

type subGraphInterruptError struct {
	Info       *InterruptInfo
	CheckPoint *checkpoint

	signal *core.InterruptSignal
}

func (e *subGraphInterruptError) Error() string {
	return fmt.Sprintf("interrupt happened, info: %+v", e.Info)
}

func isInterruptError(err error) bool {
	if _, ok := ExtractInterruptInfo(err); ok {
		return true
	}
	if info := isSubGraphInterrupt(err); info != nil {
		return true
	}
	if _, ok := IsInterruptRerunError(err); ok {
		return true
	}

	return false
}
