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
	"fmt"
	"reflect"
	"sync"

	"github.com/cloudwego/eino/internal/generic"
	"github.com/cloudwego/eino/schema"
)

// GenLocalState is a function that generates the state.
// GenLocalState 是生成状态的函数。
type GenLocalState[S any] func(ctx context.Context) (state S)

type stateKey struct{}

type internalState struct {
	state  any
	mu     sync.Mutex
	parent *internalState
}

// StatePreHandler is a function called before the node is executed.
// Notice: if user called Stream but with StatePreHandler, the StatePreHandler will read all stream chunks and merge them into a single object.
//
// StatePreHandler 是节点执行前调用的函数。
// 注意：如果用户调用 Stream 但使用了 StatePreHandler，StatePreHandler 会读取所有流 chunk 并将其合并为单个对象。
type StatePreHandler[I, S any] func(ctx context.Context, in I, state S) (I, error)

// StatePostHandler is a function called after the node is executed.
// Notice: if user called Stream but with StatePostHandler, the StatePostHandler will read all stream chunks and merge them into a single object.
//
// StatePostHandler 是节点执行后调用的函数。
// 注意：如果用户调用 Stream 但使用了 StatePostHandler，StatePostHandler 会读取所有流 chunk 并将其合并为单个对象。
type StatePostHandler[O, S any] func(ctx context.Context, out O, state S) (O, error)

// StreamStatePreHandler is a function that is called before the node is executed with stream input and output.
// StreamStatePreHandler 是在节点以流式输入和输出执行前调用的函数。
type StreamStatePreHandler[I, S any] func(ctx context.Context, in *schema.StreamReader[I], state S) (*schema.StreamReader[I], error)

// StreamStatePostHandler is a function that is called after the node is executed with stream input and output.
// StreamStatePostHandler 是在节点以流式输入和输出执行后调用的函数。
type StreamStatePostHandler[O, S any] func(ctx context.Context, out *schema.StreamReader[O], state S) (*schema.StreamReader[O], error)

func convertPreHandler[I, S any](handler StatePreHandler[I, S]) *composableRunnable {
	rf := func(ctx context.Context, in I, opts ...any) (I, error) {
		cState, pMu, err := getState[S](ctx)
		if err != nil {
			return in, err
		}
		pMu.Lock()
		defer pMu.Unlock()

		return handler(ctx, in, cState)
	}

	return runnableLambda[I, I](rf, nil, nil, nil, false)
}

func convertPostHandler[O, S any](handler StatePostHandler[O, S]) *composableRunnable {
	rf := func(ctx context.Context, out O, opts ...any) (O, error) {
		cState, pMu, err := getState[S](ctx)
		if err != nil {
			return out, err
		}
		pMu.Lock()
		defer pMu.Unlock()

		return handler(ctx, out, cState)
	}

	return runnableLambda[O, O](rf, nil, nil, nil, false)
}

func streamConvertPreHandler[I, S any](handler StreamStatePreHandler[I, S]) *composableRunnable {
	rf := func(ctx context.Context, in *schema.StreamReader[I], opts ...any) (*schema.StreamReader[I], error) {
		cState, pMu, err := getState[S](ctx)
		if err != nil {
			return in, err
		}
		pMu.Lock()
		defer pMu.Unlock()

		return handler(ctx, in, cState)
	}

	return runnableLambda[I, I](nil, nil, nil, rf, false)
}

func streamConvertPostHandler[O, S any](handler StreamStatePostHandler[O, S]) *composableRunnable {
	rf := func(ctx context.Context, out *schema.StreamReader[O], opts ...any) (*schema.StreamReader[O], error) {
		cState, pMu, err := getState[S](ctx)
		if err != nil {
			return out, err
		}
		pMu.Lock()
		defer pMu.Unlock()

		return handler(ctx, out, cState)
	}

	return runnableLambda[O, O](nil, nil, nil, rf, false)
}

// ProcessState processes the state from the context in a concurrency-safe way.
// This is the recommended way to access and modify state in custom nodes.
// The provided function handler will be executed with exclusive access to the state (protected by mutex).
//
// State Lookup Behavior:
// - If the requested state type exists in the current graph, it will be returned
// - If not found in current graph, ProcessState will search in parent graph states (for nested graphs)
// - This enables nested graphs to access state from their parent graphs
// - Follows lexical scoping: inner state of the same type shadows outer state
//
// Concurrency Safety:
// - ProcessState automatically locks the mutex of the state being accessed (current or parent level)
// - Each state level has its own mutex, allowing concurrent access to different levels
// - The lock is held for the entire duration of the handler function
//
// Note: This method will report an error if the state type doesn't match or state is not found in the context chain.
//
// Example - Basic usage in a single graph:
//
//	lambdaFunc := func(ctx context.Context, in string, opts ...any) (string, error) {
//		err := compose.ProcessState[*MyState](ctx, func(ctx context.Context, state *MyState) error {
//			// Safely modify state
//			state.Count++
//			return nil
//		})
//		if err != nil {
//			return "", err
//		}
//		return in, nil
//	}
//
// Example - Nested graph accessing parent state:
//
//	// In an inner graph node
//	innerNode := func(ctx context.Context, input string) (string, error) {
//		// Access parent graph's state
//		err := compose.ProcessState[*OuterState](ctx, func(ctx context.Context, s *OuterState) error {
//			s.Counter++  // Safely modify parent state
//			return nil
//		})
//		if err != nil {
//			return "", err
//		}
//
//		// Also access inner graph's own state
//		err = compose.ProcessState[*InnerState](ctx, func(ctx context.Context, s *InnerState) error {
//			s.Data = "processed"
//			return nil
//		})
//		return input, nil
//	}
//
// ProcessState 以并发安全的方式处理 context 中的状态。
// 这是在自定义节点中访问和修改状态的推荐方式。
// 提供的函数 handler 会在独占访问状态的情况下执行（受 mutex 保护）。
// 状态查找行为：
// - 如果当前 graph 中存在请求的状态类型，则返回该状态
// - 如果当前 graph 中未找到，ProcessState 会在父 graph 状态中查找（用于嵌套 graph）
// - 这使嵌套 graph 能够访问其父 graph 的状态
// - 遵循词法作用域：相同类型的内部状态会遮蔽外部状态
// 并发安全：
// - ProcessState 会自动锁定被访问状态的 mutex（当前层级或父层级）
// - 每个状态层级都有自己的 mutex，允许并发访问不同层级
// - 锁会在整个 handler 函数执行期间保持
// 注意：如果状态类型不匹配，或在 context 链中找不到状态，此方法会报告错误。
// 示例 - 单个 graph 中的基本用法：
// lambdaFunc := func(ctx context.Context, in string, opts ...any) (string, error) {
// err := compose.ProcessState[*MyState](ctx, func(ctx context.Context, state *MyState) error {
// 安全地修改状态
// state.Count++
// return nil
// })
// if err != nil {
// return "", err
// }
// return in, nil
// }
// 示例 - 嵌套 graph 访问父状态：
// 在内部 graph 节点中
// innerNode := func(ctx context.Context, input string) (string, error) {
// 访问父 graph 的状态
// err := compose.ProcessState[*OuterState](ctx, func(ctx context.Context, s *OuterState) error {
// s.Counter++  // 安全地修改父状态
// return nil
// })
// if err != nil {
// return "", err
// }
// 同时访问内部 graph 自己的状态
// err = compose.ProcessState[*InnerState](ctx, func(ctx context.Context, s *InnerState) error {
// s.Data = "processed"
// return nil
// })
// return input, nil
// }
func ProcessState[S any](ctx context.Context, handler func(context.Context, S) error) error {
	s, pMu, err := getState[S](ctx)
	if err != nil {
		return fmt.Errorf("get state from context fail: %w", err)
	}
	pMu.Lock()
	defer pMu.Unlock()
	return handler(ctx, s)
}

func getState[S any](ctx context.Context) (S, *sync.Mutex, error) {
	state := ctx.Value(stateKey{})

	if state == nil {
		var s S
		return s, nil, fmt.Errorf("have not set state")
	}

	interState := state.(*internalState)

	for interState != nil {
		if cState, ok := interState.state.(S); ok {
			return cState, &interState.mu, nil
		}
		interState = interState.parent
	}

	var s S
	return s, nil, fmt.Errorf("cannot find state with type: %v in states chain, "+
		"current state type: %v",
		generic.TypeOf[S](), reflect.TypeOf(state.(*internalState).state))
}
