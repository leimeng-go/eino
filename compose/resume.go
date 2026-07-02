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

package compose

import (
	"context"

	"github.com/cloudwego/eino/internal/core"
)

// GetInterruptState provides a type-safe way to check for and retrieve the persisted state from a previous interruption.
// It is the primary function a component should use to understand its past state.
//
// It returns three values:
//   - wasInterrupted (bool): True if the node was part of a previous interruption, regardless of whether state was provided.
//   - state (T): The typed state object, if it was provided and matches type `T`.
//   - hasState (bool): True if state was provided during the original interrupt and successfully cast to type `T`.
//
// GetInterruptState 提供类型安全的方式，用于检查并获取上一次 interrupt 持久化的 state。
// 它是组件了解其历史 state 时应使用的主要函数。
// 它返回三个值：
// - wasInterrupted (bool)：如果该 node 属于上一次 interrupt，则为 True，无论是否提供了 state。
// - state (T)：如果提供了 state 且类型匹配 `T`，则为该类型化 state 对象。
// - hasState (bool)：如果原始 interrupt 时提供了 state，并成功转换为 `T`，则为 True。
func GetInterruptState[T any](ctx context.Context) (wasInterrupted bool, hasState bool, state T) {
	return core.GetInterruptState[T](ctx)
}

// GetResumeContext checks if the current component is the target of a resume operation
// and retrieves any data provided by the user for that resumption.
//
// This function is typically called *after* a component has already determined it is in a
// resumed state by calling GetInterruptState.
//
// It returns three values:
//   - isResumeFlow: A boolean that is true if the current component's address was explicitly targeted
//     by a call to Resume() or ResumeWithData().
//   - hasData: A boolean that is true if data was provided for this component (i.e., not nil).
//   - data: The typed data provided by the user.
//
// ### How to Use This Function: A Decision Framework
//
// The correct usage pattern depends on the application's desired resume strategy.
//
// #### Strategy 1: Implicit "Resume All"
// In some use cases, any resume operation implies that *all* interrupted points should proceed.
// For example, if an application's UI only provides a single "Continue" button for a set of
// interruptions. In this model, a component can often just use `GetInterruptState` to see if
// `wasInterrupted` is true and then proceed with its logic, as it can assume it is an intended target.
// It may still call `GetResumeContext` to check for optional data, but the `isResumeFlow` flag is less critical.
//
// #### Strategy 2: Explicit "Targeted Resume" (Most Common)
// For applications with multiple, distinct interrupt points that must be resumed independently, it is
// crucial to differentiate which point is being resumed. This is the primary use case for the `isResumeFlow` flag.
//   - If `isResumeFlow` is `true`: Your component is the explicit target. You should consume
//     the `data` (if any) and complete your work.
//   - If `isResumeFlow` is `false`: Another component is the target. You MUST re-interrupt
//     (e.g., by returning `StatefulInterrupt(...)`) to preserve your state and allow the
//     resume signal to propagate.
//
// ### Guidance for Composite Components
//
// Composite components (like `Graph` or other `Runnable`s that contain sub-processes) have a dual role:
//  1. Check for Self-Targeting: A composite component can itself be the target of a resume
//     operation, for instance, to modify its internal state. It may call `GetResumeContext`
//     to check for data targeted at its own address.
//  2. Act as a Conduit: After checking for itself, its primary role is to re-execute its children,
//     allowing the resume context to flow down to them. It must not consume a resume signal
//     intended for one of its descendants.
//
// GetResumeContext 检查当前组件是否是 resume 操作的目标，并获取用户为此次 resume 提供的任何数据。
// 此函数通常在组件已通过调用 GetInterruptState 确认自己处于 resumed state 之后调用。
// 它返回三个值：
// - isResumeFlow：如果当前组件的地址被 Resume() 或 ResumeWithData() 显式指定为目标，则为 true。
// - hasData：如果为该组件提供了数据（即非 nil），则为 true。
// - data：用户提供的类型化数据。
// ### 如何使用此函数：决策框架
// 正确的使用模式取决于应用期望的 resume 策略。
// #### 策略 1：隐式 "Resume All"
// 在某些用例中，任何 resume 操作都意味着所有 interrupted points 都应继续执行。
// 例如，应用的 UI 只为一组 interruptions 提供一个 "Continue" 按钮。
// 在此模型中，组件通常只需使用 `GetInterruptState` 查看 `wasInterrupted` 是否为 true，然后继续执行自身逻辑，因为它可以假定自己是预期目标。
// 它仍可调用 `GetResumeContext` 检查可选数据，但 `isResumeFlow` 标志没那么关键。
// #### 策略 2：显式 "Targeted Resume"（最常见）
// 对于有多个不同 interrupt points 且必须独立 resume 的应用，区分正在 resume 的具体点至关重要。
// 这是 `isResumeFlow` 标志的主要用例。
// - 如果 `isResumeFlow` 为 `true`：你的组件就是显式目标。应消费 `data`（如有）并完成工作。
// - 如果 `isResumeFlow` 为 `false`：目标是另一个组件。你必须重新 interrupt（例如返回 `StatefulInterrupt(...)`），以保留 state 并允许 resume signal 继续传播。
// ### 复合组件指南
// 复合组件（如 `Graph` 或其他包含子流程的 `Runnable`）具有双重角色：
// 1. 检查是否针对自身：复合组件自身也可以是 resume 操作的目标，例如用于修改其内部 state。它可以调用 `GetResumeContext` 检查是否有指向自身地址的数据。
// 2. 充当通道：检查自身之后，其主要职责是重新执行子组件，让 resume context 向下流动。它不能消费本应发送给其后代的 resume signal。
func GetResumeContext[T any](ctx context.Context) (isResumeFlow bool, hasData bool, data T) {
	return core.GetResumeContext[T](ctx)
}

// GetCurrentAddress returns the hierarchical address of the currently executing component.
// The address is a sequence of segments, each identifying a structural part of the execution
// like an agent, a graph node, or a tool call. This can be useful for logging or debugging.
//
// GetCurrentAddress 返回当前执行组件的层级地址。
// 该地址是一组 segment，每个 segment 标识执行结构中的一部分，例如 agent、graph node 或 tool call。
// 这对日志记录或调试很有用。
func GetCurrentAddress(ctx context.Context) Address {
	return core.GetCurrentAddress(ctx)
}

// Resume prepares a context for an "Explicit Targeted Resume" operation by targeting one or more
// components without providing data. It is a convenience wrapper around BatchResumeWithData.
//
// This is useful when the act of resuming is itself the signal, and no extra data is needed.
// The components at the provided addresses (interrupt IDs) will receive `isResumeFlow = true`
// when they call `GetResumeContext`.
//
// Resume 通过指定一个或多个组件且不提供数据，为 "Explicit Targeted Resume" 操作准备 context。
// 它是 BatchResumeWithData 的便捷包装。
// 当 resume 行为本身就是信号且不需要额外数据时，这很有用。
// 所提供地址（interrupt IDs）上的组件在调用 `GetResumeContext` 时会收到 `isResumeFlow = true`。
func Resume(ctx context.Context, interruptIDs ...string) context.Context {
	resumeData := make(map[string]any, len(interruptIDs))
	for _, addr := range interruptIDs {
		resumeData[addr] = nil
	}
	return BatchResumeWithData(ctx, resumeData)
}

// ResumeWithData prepares a context to resume a single, specific component with data.
// It is the primary function for the "Explicit Targeted Resume" strategy when data is required.
// It is a convenience wrapper around BatchResumeWithData.
// The `interruptID` parameter is the unique interrupt ID of the target component.
//
// ResumeWithData 准备用于携带数据 resume 单个特定组件的 context。
// 当需要数据时，它是 "Explicit Targeted Resume" 策略的主要函数。
// 它是 BatchResumeWithData 的便捷包装。
// `interruptID` 参数是目标组件的唯一 interrupt ID。
func ResumeWithData(ctx context.Context, interruptID string, data any) context.Context {
	return BatchResumeWithData(ctx, map[string]any{interruptID: data})
}

// BatchResumeWithData is the core function for preparing a resume context. It injects a map
// of resume targets and their corresponding data into the context.
//
// The `resumeData` map should contain the interrupt IDs (which are the string form of addresses) of the
// components to be resumed as keys. The value can be the resume data for that component, or `nil`
// if no data is needed (equivalent to using `Resume`).
//
// This function is the foundation for the "Explicit Targeted Resume" strategy. Components whose interrupt IDs
// are present as keys in the map will receive `isResumeFlow = true` when they call `GetResumeContext`.
//
// BatchResumeWithData 是准备 resume context 的核心函数。它将 resume 目标及其对应数据的 map 注入 context。
// `resumeData` map 应以要 resume 的组件的 interrupt IDs（即地址的字符串形式）为 key。value 可以是该组件的 resume data，或在不需要数据时为 `nil`（等同于使用 `Resume`）。
// 此函数是 "Explicit Targeted Resume" 策略的基础。interrupt IDs 作为 key 存在于 map 中的组件，在调用 `GetResumeContext` 时会收到 `isResumeFlow = true`。
func BatchResumeWithData(ctx context.Context, resumeData map[string]any) context.Context {
	return core.BatchResumeWithData(ctx, resumeData)
}

func getNodePath(ctx context.Context) (*NodePath, bool) {
	currentAddress := GetCurrentAddress(ctx)
	if len(currentAddress) == 0 {
		return nil, false
	}

	nodePath := make([]string, 0, len(currentAddress))
	for _, p := range currentAddress {
		if p.Type == AddressSegmentRunnable {
			nodePath = []string{}
			continue
		}

		nodePath = append(nodePath, p.ID)
	}

	return NewNodePath(nodePath...), len(nodePath) > 0
}

// AppendAddressSegment creates a new execution context for a sub-component (e.g., a graph node or a tool call).
//
// It extends the current context's address with a new segment and populates the new context with the
// appropriate interrupt state and resume data for that specific sub-address.
//
//   - ctx: The parent context, typically the one passed into the component's Invoke/Stream method.
//   - segType: The type of the new address segment (e.g., "node", "tool").
//   - segID: The unique ID for the new address segment.
//
// AppendAddressSegment 为子组件（例如 graph node 或 tool call）创建新的执行 context。
// 它用新的 segment 扩展当前 context 的地址，并为该特定子地址在新 context 中填充适当的 interrupt state 和 resume data。
// - ctx：父 context，通常是传入组件 Invoke/Stream 方法的 context。
// - segType：新地址 segment 的类型（例如 "node"、"tool"）。
// - segID：新地址 segment 的唯一 ID。
func AppendAddressSegment(ctx context.Context, segType AddressSegmentType, segID string) context.Context {
	return core.AppendAddressSegment(ctx, segType, segID, "")
}

func appendToolAddressSegment(ctx context.Context, segID string, subID string) context.Context {
	return core.AppendAddressSegment(ctx, AddressSegmentTool, segID, subID)
}
