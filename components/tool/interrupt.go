/*
 * Copyright 2026 CloudWeGo Authors
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

package tool

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudwego/eino/internal/core"
)

// Interrupt pauses tool execution and signals the orchestration layer to checkpoint.
// The tool can be resumed later with optional data.
//
// Parameters:
//   - ctx: The context passed to InvokableRun/StreamableRun
//   - info: User-facing information about why the tool is interrupting (e.g., "needs user confirmation")
//
// Returns an error that should be returned from InvokableRun/StreamableRun.
//
// Example:
//
//	func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
//	    if needsConfirmation(args) {
//	        return "", tool.Interrupt(ctx, "Please confirm this action")
//	    }
//	    return doWork(args), nil
//	}
//
// Interrupt 暂停工具执行，并通知编排层进行 checkpoint。工具之后可以携带可选数据恢复。
// 参数：
// - ctx: 传给 InvokableRun/StreamableRun 的 context
// - info: 面向用户的中断原因信息（例如 "needs user confirmation"）
// 返回一个应从 InvokableRun/StreamableRun 返回的 error。
// 示例：
// func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
// if needsConfirmation(args) {
// return "", tool.Interrupt(ctx, "Please confirm this action")
// }
// return doWork(args), nil
// }
func Interrupt(ctx context.Context, info any) error {
	is, err := core.Interrupt(ctx, info, nil, nil)
	if err != nil {
		return err
	}
	return is
}

// StatefulInterrupt pauses tool execution with state preservation.
// Use this when the tool has internal state that must be restored on resume.
//
// Parameters:
//   - ctx: The context passed to InvokableRun/StreamableRun
//   - info: User-facing information about the interrupt
//   - state: Internal state to persist (must be gob-serializable)
//
// Example:
//
//	func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
//	    wasInterrupted, hasState, state := tool.GetInterruptState[MyState](ctx)
//	    if !wasInterrupted {
//	        // First run - interrupt with state
//	        return "", tool.StatefulInterrupt(ctx, "processing", MyState{Step: 1})
//	    }
//	    // Resumed - continue from saved state
//	    return continueFrom(state), nil
//	}
//
// StatefulInterrupt 在保留状态的情况下暂停工具执行。当工具有必须在 resume 时恢复的内部状态时使用。
// 参数：
// - ctx: 传给 InvokableRun/StreamableRun 的 context
// - info: 面向用户的中断信息
// - state: 要持久化的内部状态（必须可被 gob 序列化）
// 示例：
// func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
// wasInterrupted, hasState, state := tool.GetInterruptState[MyState](ctx)
// if !wasInterrupted {
// 首次运行 - 带状态中断
// return "", tool.StatefulInterrupt(ctx, "processing", MyState{Step: 1})
// }
// 已恢复 - 从保存的状态继续
// return continueFrom(state), nil
// }
func StatefulInterrupt(ctx context.Context, info any, state any) error {
	is, err := core.Interrupt(ctx, info, state, nil)
	if err != nil {
		return err
	}
	return is
}

// CompositeInterrupt creates an interrupt that aggregates multiple sub-interrupts.
// Use this when a tool internally executes a graph or other interruptible components.
//
// Parameters:
//   - ctx: The context passed to InvokableRun/StreamableRun
//   - info: User-facing information for this tool's interrupt
//   - state: Internal state to persist for this tool
//   - errs: Interrupt errors from sub-components (graphs, other tools, etc.)
//
// Example:
//
//	func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
//	    result, err := t.internalGraph.Invoke(ctx, input)
//	    if err != nil {
//	        if _, ok := tool.IsInterruptError(err); ok {
//	            return "", tool.CompositeInterrupt(ctx, "graph interrupted", myState, err)
//	        }
//	        return "", err
//	    }
//	    return result, nil
//	}
//
// CompositeInterrupt 创建聚合多个子中断的 interrupt。当工具内部执行 graph 或其他可中断组件时使用。
// 参数：
// - ctx: 传给 InvokableRun/StreamableRun 的 context
// - info: 此工具 interrupt 的面向用户信息
// - state: 要为此工具持久化的内部状态
// - errs: 来自子组件（graphs、其他工具等）的 interrupt errors
// 示例：
// func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
// result, err := t.internalGraph.Invoke(ctx, input)
// if err != nil {
// if _, ok := tool.IsInterruptError(err); ok {
// return "", tool.CompositeInterrupt(ctx, "graph interrupted", myState, err)
// }
// return "", err
// }
// return result, nil
// }
func CompositeInterrupt(ctx context.Context, info any, state any, errs ...error) error {
	if len(errs) == 0 {
		return StatefulInterrupt(ctx, info, state)
	}

	var cErrs []*core.InterruptSignal
	for _, err := range errs {
		ire := &core.InterruptSignal{}
		if errors.As(err, &ire) {
			cErrs = append(cErrs, ire)
			continue
		}

		var provider core.InterruptContextsProvider
		if errors.As(err, &provider) {
			is := core.FromInterruptContexts(provider.GetInterruptContexts())
			if is != nil {
				cErrs = append(cErrs, is)
			}
			continue
		}

		return fmt.Errorf("composite interrupt but one of the sub error is not interrupt error: %w", err)
	}

	is, err := core.Interrupt(ctx, info, state, cErrs)
	if err != nil {
		return err
	}
	return is
}

// GetInterruptState checks if the tool was previously interrupted and retrieves saved state.
//
// Returns:
//   - wasInterrupted: true if this tool was part of a previous interruption
//   - hasState: true if state was saved and successfully cast to type T
//   - state: the saved state (zero value if hasState is false)
//
// Example:
//
//	func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
//	    wasInterrupted, hasState, state := tool.GetInterruptState[MyState](ctx)
//	    if wasInterrupted && hasState {
//	        // Continue from saved state
//	        return continueFrom(state), nil
//	    }
//	    // First run
//	    return "", tool.StatefulInterrupt(ctx, "need input", MyState{Step: 1})
//	}
//
// GetInterruptState 检查工具之前是否被中断，并取回保存的状态。
// 返回：
// - wasInterrupted: 如果此工具属于上一次中断的一部分，则为 true
// - hasState: 如果已保存状态且成功转换为类型 T，则为 true
// - state: 保存的状态（hasState 为 false 时为零值）
// 示例：
// func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
// wasInterrupted, hasState, state := tool.GetInterruptState[MyState](ctx)
// if wasInterrupted && hasState {
// 从保存的状态继续
// return continueFrom(state), nil
// }
// 首次运行
// return "", tool.StatefulInterrupt(ctx, "need input", MyState{Step: 1})
// }
func GetInterruptState[T any](ctx context.Context) (wasInterrupted bool, hasState bool, state T) {
	return core.GetInterruptState[T](ctx)
}

// GetResumeContext checks if this tool is the explicit target of a resume operation.
//
// Returns:
//   - isResumeTarget: true if this tool was explicitly targeted for resume
//   - hasData: true if resume data was provided
//   - data: the resume data (zero value if hasData is false)
//
// Use this to differentiate between:
//   - Being resumed as the target (should proceed with work)
//   - Being re-executed because a sibling was resumed (should re-interrupt)
//
// Example:
//
//	func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
//	    wasInterrupted, _, _ := tool.GetInterruptState[any](ctx)
//	    if !wasInterrupted {
//	        return "", tool.Interrupt(ctx, "need confirmation")
//	    }
//
//	    isTarget, hasData, data := tool.GetResumeContext[string](ctx)
//	    if !isTarget {
//	        // Not our turn - re-interrupt
//	        return "", tool.Interrupt(ctx, nil)
//	    }
//	    if hasData {
//	        return data, nil
//	    }
//	    return "default result", nil
//	}
//
// GetResumeContext 检查此工具是否为 resume 操作的明确目标。
// 返回：
// - isResumeTarget: 如果此工具被明确指定为 resume 目标，则为 true
// - hasData: 如果提供了 resume 数据，则为 true
// - data: resume 数据（hasData 为 false 时为零值）
// 用于区分：
// - 作为目标被恢复（应继续工作）
// - 因兄弟节点被恢复而重新执行（应再次 interrupt）
// 示例：
// func (t *MyTool) InvokableRun(ctx context.Context, args string, opts ...Option) (string, error) {
// wasInterrupted, _, _ := tool.GetInterruptState[any](ctx)
// if !wasInterrupted {
// return "", tool.Interrupt(ctx, "need confirmation")
// }
// isTarget, hasData, data := tool.GetResumeContext[string](ctx)
// if !isTarget {
// 还没轮到我们 - 再次 interrupt
// return "", tool.Interrupt(ctx, nil)
// }
// if hasData {
// return data, nil
// }
// return "default result", nil
// }
func GetResumeContext[T any](ctx context.Context) (isResumeTarget bool, hasData bool, data T) {
	return core.GetResumeContext[T](ctx)
}
