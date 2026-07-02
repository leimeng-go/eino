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
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/cloudwego/eino/schema"
)

type midStr string

func TestStateGraphWithEdge(t *testing.T) {

	ctx := context.Background()

	const (
		nodeOfL1 = "invokable"
		nodeOfL2 = "streamable"
		nodeOfL3 = "transformable"
	)

	type testState struct {
		ms []string
	}

	gen := func(ctx context.Context) *testState {
		return &testState{}
	}

	sg := NewGraph[string, string](WithGenLocalState(gen))

	l1 := InvokableLambda(func(ctx context.Context, in string) (out midStr, err error) {
		return midStr("InvokableLambda: " + in), nil
	})

	l1StateToInput := func(ctx context.Context, in string, state *testState) (string, error) {
		state.ms = append(state.ms, in)
		return in, nil
	}

	l1StateToOutput := func(ctx context.Context, out midStr, state *testState) (midStr, error) {
		state.ms = append(state.ms, string(out))
		return out, nil
	}

	err := sg.AddLambdaNode(nodeOfL1, l1,
		WithStatePreHandler(l1StateToInput), WithStatePostHandler(l1StateToOutput))
	assert.NoError(t, err)

	l2 := StreamableLambda(func(ctx context.Context, input midStr) (output *schema.StreamReader[string], err error) {
		outStr := "StreamableLambda: " + string(input)

		sr, sw := schema.Pipe[string](utf8.RuneCountInString(outStr))

		go func() {
			for _, field := range strings.Fields(outStr) {
				sw.Send(field+" ", nil)
			}
			sw.Close()
		}()

		return sr, nil
	})

	l2StateToOutput := func(ctx context.Context, out string, state *testState) (string, error) {
		state.ms = append(state.ms, out)
		return out, nil
	}

	err = sg.AddLambdaNode(nodeOfL2, l2, WithStatePostHandler(l2StateToOutput))
	assert.NoError(t, err)

	l3 := TransformableLambda(func(ctx context.Context, input *schema.StreamReader[string]) (
		output *schema.StreamReader[string], err error) {

		prefix := "TransformableLambda: "
		sr, sw := schema.Pipe[string](20)

		go func() {
			for _, field := range strings.Fields(prefix) {
				sw.Send(field+" ", nil)
			}
			defer input.Close()

			for {
				chunk, err := input.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					// TODO: how to trace this kind of error in the goroutine of processing stream
					// TODO: 如何追踪处理流的 goroutine 中这类错误
					sw.Send(chunk, err)
					break
				}

				sw.Send(chunk, nil)

			}
			sw.Close()
		}()

		return sr, nil
	})

	l3StateToOutput := func(ctx context.Context, out string, state *testState) (string, error) {
		state.ms = append(state.ms, out)
		assert.Len(t, state.ms, 4)
		return out, nil
	}

	err = sg.AddLambdaNode(nodeOfL3, l3, WithStatePostHandler(l3StateToOutput))
	assert.NoError(t, err)

	err = sg.AddEdge(START, nodeOfL1)
	assert.NoError(t, err)

	err = sg.AddEdge(nodeOfL1, nodeOfL2)
	assert.NoError(t, err)

	err = sg.AddEdge(nodeOfL2, nodeOfL3)
	assert.NoError(t, err)

	err = sg.AddEdge(nodeOfL3, END)
	assert.NoError(t, err)

	run, err := sg.Compile(ctx)
	assert.NoError(t, err)

	out, err := run.Invoke(ctx, "how are you")
	assert.NoError(t, err)
	assert.Equal(t, "TransformableLambda: StreamableLambda: InvokableLambda: how are you ", out)

	stream, err := run.Stream(ctx, "how are you")
	assert.NoError(t, err)
	out, err = concatStreamReader(stream)
	assert.NoError(t, err)
	assert.Equal(t, "TransformableLambda: StreamableLambda: InvokableLambda: how are you ", out)

	sr, sw := schema.Pipe[string](1)
	sw.Send("how are you", nil)
	sw.Close()

	stream, err = run.Transform(ctx, sr)
	assert.NoError(t, err)
	out, err = concatStreamReader(stream)
	assert.NoError(t, err)
	assert.Equal(t, "TransformableLambda: StreamableLambda: InvokableLambda: how are you ", out)
}

func TestStateGraphUtils(t *testing.T) {
	t.Run("getState_success", func(t *testing.T) {
		type testStruct struct {
			UserID int64
		}

		ctx := context.Background()

		ctx = context.WithValue(ctx, stateKey{}, &internalState{
			state: &testStruct{UserID: 10},
		})

		var userID int64
		err := ProcessState[*testStruct](ctx, func(_ context.Context, state *testStruct) error {
			userID = state.UserID
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(10), userID)
	})

	t.Run("getState_nil", func(t *testing.T) {
		type testStruct struct {
			UserID int64
		}

		ctx := context.Background()
		ctx = context.WithValue(ctx, stateKey{}, &internalState{})

		err := ProcessState[*testStruct](ctx, func(_ context.Context, state *testStruct) error {
			return nil
		})
		assert.ErrorContains(t, err, "cannot find state with type: *compose.testStruct in states chain, "+
			"current state type: <nil>")
	})

	t.Run("getState_type_error", func(t *testing.T) {
		type testStruct struct {
			UserID int64
		}

		ctx := context.Background()
		ctx = context.WithValue(ctx, stateKey{}, &internalState{
			state: &testStruct{UserID: 10},
		})

		err := ProcessState[string](ctx, func(_ context.Context, state string) error {
			return nil
		})
		assert.ErrorContains(t, err, "cannot find state with type: string in states chain, "+
			"current state type: *compose.testStruct")

	})
}

func TestStateChain(t *testing.T) {
	ctx := context.Background()
	type testState struct {
		Field1 string
		Field2 string
	}
	sc := NewChain[string, string](WithGenLocalState(func(ctx context.Context) (state *testState) {
		return &testState{}
	}))

	r, err := sc.AppendLambda(InvokableLambda(func(ctx context.Context, input string) (output string, err error) {
		err = ProcessState[*testState](ctx, func(_ context.Context, state *testState) error {
			state.Field1 = "node1"
			return nil
		})
		if err != nil {
			return "", err
		}
		return input, nil
	}), WithStatePostHandler(func(ctx context.Context, out string, state *testState) (string, error) {
		state.Field2 = "node2"
		return out, nil
	})).
		AppendLambda(InvokableLambda(func(ctx context.Context, input string) (output string, err error) {
			return input, nil
		}), WithStatePreHandler(func(ctx context.Context, in string, state *testState) (string, error) {
			return in + state.Field1 + state.Field2, nil
		})).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Invoke(ctx, "start")
	if err != nil {
		t.Fatal(err)
	}
	if result != "startnode1node2" {
		t.Fatal("result is unexpected")
	}
}

func TestStreamState(t *testing.T) {
	type testState struct {
		Field1 string
	}
	ctx := context.Background()
	s := &testState{Field1: "1"}
	g := NewGraph[string, string](WithGenLocalState(func(ctx context.Context) (state *testState) { return s }))
	err := g.AddLambdaNode("1", TransformableLambda(func(ctx context.Context, input *schema.StreamReader[string]) (output *schema.StreamReader[string], err error) {
		return input, nil
	}), WithStreamStatePreHandler(func(ctx context.Context, in *schema.StreamReader[string], state *testState) (*schema.StreamReader[string], error) {
		sr, sw := schema.Pipe[string](5)
		for i := 0; i < 5; i++ {
			sw.Send(state.Field1, nil)
		}
		sw.Close()
		return sr, nil
	}), WithStreamStatePostHandler(func(ctx context.Context, in *schema.StreamReader[string], state *testState) (*schema.StreamReader[string], error) {
		ss := in.Copy(2)
		for {
			chunk, err := ss[0].Recv()
			if err == io.EOF {
				return ss[1], nil
			}
			if err != nil {
				return nil, err
			}
			state.Field1 += chunk
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = g.AddEdge(START, "1")
	if err != nil {
		t.Fatal(err)
	}
	err = g.AddEdge("1", END)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sr, _ := schema.Pipe[string](1)
	streamResult, err := r.Transform(ctx, sr)
	if err != nil {
		t.Fatal(err)
	}
	if s.Field1 != "111111" {
		t.Fatal("state is unexpected")
	}
	for i := 0; i < 5; i++ {
		chunk, err := streamResult.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if chunk != "1" {
			t.Fatal("result is unexpected")
		}
	}
	_, err = streamResult.Recv()
	if err != io.EOF {
		t.Fatal("result is unexpected")
	}
}

// Nested Graph State Tests
// 嵌套 Graph 状态测试

type NestedOuterState struct {
	Value   string
	Counter int
}

type NestedInnerState struct {
	Value string
}

func init() {
	schema.RegisterName[*NestedOuterState]("NestedOuterState")
	schema.RegisterName[*NestedInnerState]("NestedInnerState")
}

func TestNestedGraphStateAccess(t *testing.T) {
	// Test that inner graph can access outer graph's state
	// 测试内部 graph 可以访问外部 graph 的状态
	genOuterState := func(ctx context.Context) *NestedOuterState {
		return &NestedOuterState{Value: "outer", Counter: 0}
	}

	genInnerState := func(ctx context.Context) *NestedInnerState {
		return &NestedInnerState{Value: "inner"}
	}

	innerNode := func(ctx context.Context, input string) (string, error) {
		// Access both inner and outer state
		// 同时访问内部和外部状态
		var outerValue string
		err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
			outerValue = s.Value
			return nil
		})
		if err != nil {
			return "", err
		}

		var innerValue string
		err = ProcessState(ctx, func(ctx context.Context, s *NestedInnerState) error {
			innerValue = s.Value
			return nil
		})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s_inner=%s_outer=%s", input, innerValue, outerValue), nil
	}

	innerGraph := NewGraph[string, string](WithGenLocalState(genInnerState))
	_ = innerGraph.AddLambdaNode("inner_node", InvokableLambda(innerNode))
	_ = innerGraph.AddEdge(START, "inner_node")
	_ = innerGraph.AddEdge("inner_node", END)

	outerGraph := NewGraph[string, string](WithGenLocalState(genOuterState))
	_ = outerGraph.AddGraphNode("inner_graph", innerGraph)
	_ = outerGraph.AddEdge(START, "inner_graph")
	_ = outerGraph.AddEdge("inner_graph", END)

	r, err := outerGraph.Compile(context.Background())
	assert.NoError(t, err)

	out, err := r.Invoke(context.Background(), "start")
	assert.NoError(t, err)
	assert.Equal(t, "start_inner=inner_outer=outer", out)
}

func TestNestedGraphStateShadowing(t *testing.T) {
	// Test that inner state shadows outer state of the same type (lexical scoping)
	// 测试相同类型的内部状态会遮蔽外部状态（词法作用域）
	type CommonState struct {
		Value string
	}

	genOuterState := func(ctx context.Context) *CommonState {
		return &CommonState{Value: "outer"}
	}

	genInnerState := func(ctx context.Context) *CommonState {
		return &CommonState{Value: "inner"}
	}

	innerNode := func(ctx context.Context, input string) (string, error) {
		var value string
		err := ProcessState(ctx, func(ctx context.Context, s *CommonState) error {
			// Should see "inner" because inner state shadows outer state
			// 应看到 "inner"，因为内部状态会遮蔽外部状态
			value = s.Value
			return nil
		})
		if err != nil {
			return "", err
		}
		return input + "_" + value, nil
	}

	innerGraph := NewGraph[string, string](WithGenLocalState(genInnerState))
	_ = innerGraph.AddLambdaNode("inner_node", InvokableLambda(innerNode))
	_ = innerGraph.AddEdge(START, "inner_node")
	_ = innerGraph.AddEdge("inner_node", END)

	outerGraph := NewGraph[string, string](WithGenLocalState(genOuterState))
	_ = outerGraph.AddGraphNode("inner_graph", innerGraph)
	_ = outerGraph.AddEdge(START, "inner_graph")
	_ = outerGraph.AddEdge("inner_graph", END)

	r, err := outerGraph.Compile(context.Background())
	assert.NoError(t, err)

	out, err := r.Invoke(context.Background(), "start")
	assert.NoError(t, err)
	assert.Equal(t, "start_inner", out)
}

func TestNestedGraphStateAfterResume(t *testing.T) {
	// Test that state parent linking works correctly after resume
	// when the outer state is restored from checkpoint (new instance)
	//
	// 测试 resume 后状态父级链接是否正常工作
	// 此时外部状态从 checkpoint 恢复（新实例）
	genOuterState := func(ctx context.Context) *NestedOuterState {
		return &NestedOuterState{Value: "outer", Counter: 0}
	}

	genInnerState := func(ctx context.Context) *NestedInnerState {
		return &NestedInnerState{Value: "inner"}
	}

	// Node that modifies outer state
	// 修改外部状态的节点
	outerNode := func(ctx context.Context, input string) (string, error) {
		err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
			s.Counter = 42
			return nil
		})
		if err != nil {
			return "", err
		}
		return input, nil
	}

	// Inner node that reads outer state
	// 读取外部状态的内部节点
	innerNode := func(ctx context.Context, input string) (string, error) {
		var outerCounter int
		var outerValue string
		err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
			// Should see the modified counter value from the restored state
			// 应看到恢复后状态中的已修改 counter 值
			outerCounter = s.Counter
			outerValue = s.Value
			return nil
		})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s_counter=%d_value=%s", input, outerCounter, outerValue), nil
	}

	innerGraph := NewGraph[string, string](WithGenLocalState(genInnerState))
	_ = innerGraph.AddLambdaNode("inner_node", InvokableLambda(innerNode))
	_ = innerGraph.AddEdge(START, "inner_node")
	_ = innerGraph.AddEdge("inner_node", END)

	outerGraph := NewGraph[string, string](WithGenLocalState(genOuterState))
	_ = outerGraph.AddLambdaNode("outer_node", InvokableLambda(outerNode))
	_ = outerGraph.AddGraphNode("inner_graph", innerGraph, WithGraphCompileOptions(WithInterruptBeforeNodes([]string{"inner_node"})))
	_ = outerGraph.AddEdge(START, "outer_node")
	_ = outerGraph.AddEdge("outer_node", "inner_graph")
	_ = outerGraph.AddEdge("inner_graph", END)

	store := newInMemoryStore()
	r, err := outerGraph.Compile(context.Background(), WithCheckPointStore(store))
	assert.NoError(t, err)

	// First run - should interrupt after modifying outer state
	// 第一次运行 - 修改外部状态后应 interrupt
	_, err = r.Invoke(context.Background(), "start", WithCheckPointID("state_resume_test"))
	assert.Error(t, err)

	// Resume - outer state should be restored with Counter=42
	// Inner graph should link to this restored outer state
	//
	// Resume - 外部状态应以 Counter=42 恢复
	// 内部 graph 应链接到这个恢复后的外部状态
	out, err := r.Invoke(context.Background(), "start", WithCheckPointID("state_resume_test"))
	assert.NoError(t, err)
	assert.Equal(t, "start_counter=42_value=outer", out)
}

func TestLambdaNestedGraphStateAccess(t *testing.T) {
	// Test that inner graph invoked from a lambda can access outer graph's state
	// This tests the case: outer graph -> lambda node -> inner graph (using CompositeInterrupt)
	//
	// 测试从 lambda 调用的内部 graph 可以访问外部 graph 的状态
	// 这测试的是：outer graph -> lambda node -> inner graph（使用 CompositeInterrupt）
	genOuterState := func(ctx context.Context) *NestedOuterState {
		return &NestedOuterState{Value: "outer", Counter: 100}
	}

	genInnerState := func(ctx context.Context) *NestedInnerState {
		return &NestedInnerState{Value: "inner"}
	}

	// Inner node that accesses outer state
	// 访问外部状态的内部节点
	innerNode := func(ctx context.Context, input string) (string, error) {
		var outerValue string
		var outerCounter int
		err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
			outerValue = s.Value
			outerCounter = s.Counter
			return nil
		})
		if err != nil {
			return "", err
		}

		var innerValue string
		err = ProcessState(ctx, func(ctx context.Context, s *NestedInnerState) error {
			innerValue = s.Value
			return nil
		})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s_inner=%s_outer=%s_%d", input, innerValue, outerValue, outerCounter), nil
	}

	// Build inner graph
	// 构建内部 graph
	innerGraph := NewGraph[string, string](WithGenLocalState(genInnerState))
	_ = innerGraph.AddLambdaNode("inner_node", InvokableLambda(innerNode))
	_ = innerGraph.AddEdge(START, "inner_node")
	_ = innerGraph.AddEdge("inner_node", END)

	// Compile inner graph as a standalone runnable
	// 将内部图编译为独立的可运行对象（Runnable）
	innerRunnable, err := innerGraph.Compile(context.Background())
	assert.NoError(t, err)

	// Lambda that invokes the inner graph
	// 调用内部图的 Lambda
	lambdaNode := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		// Simply invoke the inner graph - state context is passed through
		// 直接调用内部图，state context 会透传
		return innerRunnable.Invoke(ctx, input)
	})

	// Build outer graph
	// 构建外部图
	outerGraph := NewGraph[string, string](WithGenLocalState(genOuterState))
	_ = outerGraph.AddLambdaNode("lambda_with_graph", lambdaNode)
	_ = outerGraph.AddEdge(START, "lambda_with_graph")
	_ = outerGraph.AddEdge("lambda_with_graph", END)

	r, err := outerGraph.Compile(context.Background())
	assert.NoError(t, err)

	out, err := r.Invoke(context.Background(), "start")
	assert.NoError(t, err)
	assert.Equal(t, "start_inner=inner_outer=outer_100", out)
}

func TestLambdaNestedGraphStateAfterResume(t *testing.T) {
	// Test that state parent linking works correctly after resume
	// in the lambda-nested case (outer graph -> lambda -> inner graph)
	//
	// 测试恢复后 state 父级链接是否正常工作
	// 适用于 lambda 嵌套场景（外部图 -> lambda -> 内部图）
	genOuterState := func(ctx context.Context) *NestedOuterState {
		return &NestedOuterState{Value: "outer", Counter: 0}
	}

	genInnerState := func(ctx context.Context) *NestedInnerState {
		return &NestedInnerState{Value: "inner"}
	}

	// Outer node that modifies state
	// 修改 state 的外部节点
	outerNode := func(ctx context.Context, input string) (string, error) {
		err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
			s.Counter = 99
			return nil
		})
		if err != nil {
			return "", err
		}
		return input, nil
	}

	// Inner lambda that interrupts on first run, reads outer state on resume
	// 内部 lambda 首次运行时中断，恢复时读取外部 state
	innerLambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		wasInterrupted, _, _ := GetInterruptState[*NestedInnerState](ctx)
		if !wasInterrupted {
			// First run: interrupt
			// 首次运行：中断
			return "", StatefulInterrupt(ctx, "inner interrupt", &NestedInnerState{Value: "inner"})
		}

		// Resumed: read outer state
		// 已恢复：读取外部 state
		var outerCounter int
		var outerValue string
		err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
			// Should see the modified counter from the restored state
			// 应看到已恢复 state 中修改后的 counter
			outerCounter = s.Counter
			outerValue = s.Value
			return nil
		})
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s_counter=%d_value=%s", input, outerCounter, outerValue), nil
	})

	// Build inner graph
	// 构建内部图
	innerGraph := NewGraph[string, string](WithGenLocalState(genInnerState))
	_ = innerGraph.AddLambdaNode("inner_lambda", innerLambda)
	_ = innerGraph.AddEdge(START, "inner_lambda")
	_ = innerGraph.AddEdge("inner_lambda", END)

	// Compile inner graph as standalone runnable with checkpoint support
	// 将内部图编译为支持检查点的独立可运行对象（Runnable）
	innerRunnable, err := innerGraph.Compile(context.Background(),
		WithGraphName("inner"),
		WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	// Composite lambda that invokes the inner graph and handles interrupts
	// 调用内部图并处理中断的复合 lambda
	compositeLambda := InvokableLambda(func(ctx context.Context, input string) (string, error) {
		output, err := innerRunnable.Invoke(ctx, input, WithCheckPointID("inner-cp"))
		if err != nil {
			_, isInterrupt := ExtractInterruptInfo(err)
			if !isInterrupt {
				return "", err
			}
			// Wrap the interrupt using CompositeInterrupt
			// 使用 CompositeInterrupt 包装中断
			return "", CompositeInterrupt(ctx, "composite interrupt", nil, err)
		}
		return output, nil
	})

	// Build outer graph
	// 构建外部图
	outerGraph := NewGraph[string, string](WithGenLocalState(genOuterState))
	_ = outerGraph.AddLambdaNode("outer_node", InvokableLambda(outerNode))
	_ = outerGraph.AddLambdaNode("composite_lambda", compositeLambda)
	_ = outerGraph.AddEdge(START, "outer_node")
	_ = outerGraph.AddEdge("outer_node", "composite_lambda")
	_ = outerGraph.AddEdge("composite_lambda", END)

	// Compile outer graph
	// 编译外部图
	outerRunnable, err := outerGraph.Compile(context.Background(),
		WithGraphName("root"),
		WithCheckPointStore(newInMemoryStore()))
	assert.NoError(t, err)

	// First run - should interrupt after modifying outer state
	// 首次运行：应在修改外部 state 后中断
	checkPointID := "lambda_state_resume_test"
	_, err = outerRunnable.Invoke(context.Background(), "start", WithCheckPointID(checkPointID))
	assert.Error(t, err)

	interruptInfo, isInterrupt := ExtractInterruptInfo(err)
	assert.True(t, isInterrupt)

	// Resume - outer state should be restored with Counter=99
	// Inner lambda should link to this restored outer state
	//
	// 恢复：外部 state 应以 Counter=99 恢复
	// 内部 lambda 应链接到这个已恢复的外部 state
	ctx := ResumeWithData(context.Background(), interruptInfo.InterruptContexts[0].ID, nil)
	out, err := outerRunnable.Invoke(ctx, "start", WithCheckPointID(checkPointID))
	assert.NoError(t, err)

	// Verify the inner lambda saw the modified counter from the restored outer state
	// 验证内部 lambda 看到了已恢复外部 state 中修改后的 counter
	assert.Contains(t, out, "counter=99")
	assert.Contains(t, out, "value=outer")
}

func TestNestedGraphStateConcurrency(t *testing.T) {
	// Test that concurrent access to parent and child states uses correct locks
	// This verifies that ProcessState properly locks the parent state's mutex when accessing it
	//
	// 测试并发访问父子 state 时使用正确的锁
	// 这会验证 ProcessState 访问父 state 时会正确锁定其 mutex
	genOuterState := func(ctx context.Context) *NestedOuterState {
		return &NestedOuterState{Value: "outer", Counter: 0}
	}

	genInnerState := func(ctx context.Context) *NestedInnerState {
		return &NestedInnerState{Value: "inner"}
	}

	// Inner node that concurrently modifies both outer and inner state
	// 同时修改外层和内层 state 的内层节点
	innerNode := func(ctx context.Context, input string) (string, error) {
		var wg sync.WaitGroup
		errors := make(chan error, 20)

		// Launch 10 goroutines that modify outer state
		// If locks don't work correctly, we'll see race conditions
		//
		// 启动 10 个 goroutine 修改外层 state
		// 如果锁工作不正确，就会出现竞态条件
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ProcessState(ctx, func(ctx context.Context, s *NestedOuterState) error {
					// ProcessState should hold the parent's lock during this entire function
					// ProcessState 应在整个函数执行期间持有父级的锁
					current := s.Counter
					time.Sleep(time.Millisecond) // Simulate work
					// 模拟工作
					s.Counter = current + 1
					return nil
				})
				if err != nil {
					errors <- err
				}
			}()
		}

		// Launch 10 goroutines that modify inner state
		// 启动 10 个 goroutine 修改内层 state
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := ProcessState(ctx, func(ctx context.Context, s *NestedInnerState) error {
					// This uses the inner state's own lock
					// 这里使用内层 state 自己的锁
					return nil
				})
				if err != nil {
					errors <- err
				}
			}()
		}

		wg.Wait()
		close(errors)

		// Check for errors
		// 检查错误
		for err := range errors {
			return "", err
		}

		return input, nil
	}

	innerGraph := NewGraph[string, string](WithGenLocalState(genInnerState))
	_ = innerGraph.AddLambdaNode("inner_node", InvokableLambda(innerNode))
	_ = innerGraph.AddEdge(START, "inner_node")
	_ = innerGraph.AddEdge("inner_node", END)

	outerGraph := NewGraph[string, string](WithGenLocalState(genOuterState))
	_ = outerGraph.AddGraphNode("inner_graph", innerGraph)
	_ = outerGraph.AddEdge(START, "inner_graph")
	_ = outerGraph.AddEdge("inner_graph", END)

	r, err := outerGraph.Compile(context.Background())
	assert.NoError(t, err)

	_, err = r.Invoke(context.Background(), "start")
	assert.NoError(t, err)

	// Note: This test is primarily validated by running with -race flag
	// If locks don't work correctly, the race detector will catch it
	//
	// 注意：此测试主要通过 -race flag 运行来验证
	// 如果锁工作不正确，race detector 会捕获到
}
