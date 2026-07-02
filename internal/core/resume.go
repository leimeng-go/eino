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

import "context"

// GetInterruptState provides a type-safe way to check for and retrieve the persisted state from a previous interruption.
// It is the primary function a component should use to understand its past state.
//
// It returns three values:
//   - wasInterrupted (bool): True if the node was part of a previous interruption, regardless of whether state was provided.
//   - state (T): The typed state object, if it was provided and matches type `T`.
//   - hasState (bool): True if state was provided during the original interrupt and successfully cast to type `T`.
//
// GetInterruptState 提供一种类型安全的方式，用于检查并获取上一次中断持久化的状态。
// 它是组件理解自身历史状态时应使用的主要函数。
// 它返回三个值：
// - wasInterrupted (bool)：如果该节点曾属于上一次中断的一部分，则为 true，无论是否提供了状态。
// - state (T)：已提供且类型匹配 `T` 时的类型化状态对象。
// - hasState (bool)：如果原始中断时提供了状态，并成功转换为类型 `T`，则为 true。
func GetInterruptState[T any](ctx context.Context) (wasInterrupted bool, hasState bool, state T) {
	rCtx, ok := getRunCtx(ctx)
	if !ok || rCtx.interruptState == nil {
		return
	}

	wasInterrupted = true
	if rCtx.interruptState.State == nil {
		return
	}

	state, hasState = rCtx.interruptState.State.(T)
	return
}

// GetResumeContext checks if the current component is the target of a resume operation
// and retrieves any data provided by the user for that resumption.
//
// This function is typically called *after* a component has already determined it is in a
// resumed state by calling GetInterruptState.
//
// It returns three values:
//   - isResumeTarget: A boolean that is true if the current component's address OR any of its
//     descendant addresses was explicitly targeted by a call to Resume() or ResumeWithData().
//     This allows composite components (like tools containing nested graphs) to know they should
//     execute their children to reach the actual resume target.
//   - hasData: A boolean that is true if data was provided for this specific component (i.e., not nil).
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
// crucial to differentiate which point is being resumed. This is the primary use case for the `isResumeTarget` flag.
//   - If `isResumeTarget` is `true`: Your component (or one of its descendants) is the target.
//     If `hasData` is true, you are the direct target and should consume the data.
//     If `hasData` is false, a descendant is the target—execute your children to reach it.
//   - If `isResumeTarget` is `false`: Neither you nor your descendants are the target. You MUST
//     re-interrupt (e.g., by returning `StatefulInterrupt(...)`) to preserve your state.
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
// GetResumeContext 检查当前组件是否为恢复操作的目标，
// 并获取用户为此次恢复提供的任何数据。
// 此函数通常在组件已通过调用 GetInterruptState 确认自身处于恢复状态之后调用。
// 它返回三个值：
// - isResumeTarget：布尔值；如果当前组件的地址或其任一后代地址被 Resume() 或 ResumeWithData() 显式指定为目标，则为 true。
// 这允许复合组件（例如包含嵌套图的工具）知道自己应执行子组件以到达实际恢复目标。
// - hasData：布尔值；如果为此特定组件提供了数据（即非 nil），则为 true。
// - data：用户提供的类型化数据。
// ### 如何使用此函数：决策框架
// 正确的使用模式取决于应用期望的恢复策略。
// #### 策略 1：隐式 "Resume All"
// 在某些用例中，任何恢复操作都意味着所有中断点都应继续执行。
// 例如，应用的 UI 只为一组中断提供一个 "Continue" 按钮。
// 在此模型中，组件通常只需使用 `GetInterruptState` 查看 `wasInterrupted` 是否为 true，然后继续其逻辑，因为它可以假定自己是预期目标。
// 它仍可调用 `GetResumeContext` 检查可选数据，但 `isResumeFlow` 标记不那么关键。
// #### 策略 2：显式 "Targeted Resume"（最常见）
// 对于存在多个独立中断点且必须分别恢复的应用，区分正在恢复的是哪个点至关重要。
// 这是 `isResumeTarget` 标记的主要用例。
// - 如果 `isResumeTarget` 为 `true`：你的组件（或其某个后代）是目标。
// 如果 `hasData` 为 true，你就是直接目标，应消费该数据。
// 如果 `hasData` 为 false，则某个后代是目标——执行你的子组件以到达它。
// - 如果 `isResumeTarget` 为 `false`：你及你的后代都不是目标。你必须
// 重新中断（例如返回 `StatefulInterrupt(...)`）以保留你的状态。
// ### 复合组件指南
// 复合组件（如 `Graph` 或其他包含子流程的 `Runnable`）具有双重角色：
// 1. 检查自身是否为目标：复合组件本身也可以是恢复操作的目标，
// 例如用于修改其内部状态。它可以调用 `GetResumeContext`
// 检查是否有发往自身地址的数据。
// 2. 作为通道：完成自身检查后，它的主要职责是重新执行子组件，
// 让恢复 context 向下流动。它不得消费本应发往某个后代的恢复信号。
func GetResumeContext[T any](ctx context.Context) (isResumeTarget bool, hasData bool, data T) {
	rCtx, ok := getRunCtx(ctx)
	if !ok {
		return
	}

	isResumeTarget = rCtx.isResumeTarget
	if !isResumeTarget {
		return
	}

	// It is a resume flow, now check for data
	// 这是一个恢复流，现在检查数据
	if rCtx.resumeData == nil {
		return // hasData is false
		// hasData 为 false
	}

	data, hasData = rCtx.resumeData.(T)
	return
}

func getRunCtx(ctx context.Context) (*addrCtx, bool) {
	rCtx, ok := ctx.Value(addrCtxKey{}).(*addrCtx)
	return rCtx, ok
}
