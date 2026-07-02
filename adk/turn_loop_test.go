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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudwego/eino/schema"
)

type turnLoopMockAgent struct {
	name    string
	events  []*AgentEvent
	runFunc func(ctx context.Context, input *AgentInput) (*AgentOutput, error)
}

func (a *turnLoopMockAgent) Name(_ context.Context) string        { return a.name }
func (a *turnLoopMockAgent) Description(_ context.Context) string { return "mock agent" }
func (a *turnLoopMockAgent) Run(ctx context.Context, input *AgentInput, _ ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iter, gen := NewAsyncIteratorPair[*AgentEvent]()

	if a.runFunc != nil {
		go func() {
			defer gen.Close()
			output, err := a.runFunc(ctx, input)
			if err != nil {
				gen.Send(&AgentEvent{Err: err})
				return
			}
			gen.Send(&AgentEvent{Output: output})
		}()
		return iter
	}

	go func() {
		defer gen.Close()
		for _, e := range a.events {
			gen.Send(e)
		}
	}()
	return iter
}

type turnLoopCheckpointStore struct {
	m  map[string][]byte
	mu sync.Mutex
}

func (s *turnLoopCheckpointStore) Set(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

func (s *turnLoopCheckpointStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	return v, ok, nil
}

type turnLoopCancellableMockAgent struct {
	name     string
	runFunc  func(ctx context.Context, input *AgentInput) (*AgentOutput, error)
	onCancel func(cc *cancelContext)
	cancel   context.CancelFunc
	mu       sync.Mutex
}

func (a *turnLoopCancellableMockAgent) Name(_ context.Context) string        { return a.name }
func (a *turnLoopCancellableMockAgent) Description(_ context.Context) string { return "mock agent" }

func (a *turnLoopCancellableMockAgent) Run(ctx context.Context, input *AgentInput, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iter, gen := NewAsyncIteratorPair[*AgentEvent]()

	o := getCommonOptions(nil, opts...)
	cc := o.cancelCtx

	a.mu.Lock()
	var cancelCtx context.Context
	cancelCtx, a.cancel = context.WithCancel(ctx)
	a.mu.Unlock()

	go func() {
		defer gen.Close()
		if cc != nil {
			go func() {
				<-cc.cancelChan
				// CRITICAL: call onCancel BEFORE cancel() to avoid race condition.
				// If cancel() fires first, the runFunc returns immediately,
				// flowAgent's defer calls markDone(), and doneChan closes
				// before onCancel can read cc.config.
				//
				// 关键：在 cancel() 之前调用 onCancel，以避免竞态条件。
				// 如果 cancel() 先触发，runFunc 会立即返回，flowAgent 的 defer 会调用 markDone()，doneChan 会在 onCancel 读取 cc.config 前关闭。
				if a.onCancel != nil {
					a.onCancel(cc)
				}
				a.mu.Lock()
				if a.cancel != nil {
					a.cancel()
				}
				a.mu.Unlock()
			}()
		}

		output, err := a.runFunc(cancelCtx, input)
		if err != nil {
			gen.Send(&AgentEvent{Err: err})
			return
		}
		gen.Send(&AgentEvent{Output: output})
	}()
	return iter
}

type turnLoopStopModeProbeAgent struct {
	ccCh chan *cancelContext
}

func (a *turnLoopStopModeProbeAgent) Name(_ context.Context) string        { return "probe" }
func (a *turnLoopStopModeProbeAgent) Description(_ context.Context) string { return "probe" }
func (a *turnLoopStopModeProbeAgent) Run(_ context.Context, _ *AgentInput, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iter, gen := NewAsyncIteratorPair[*AgentEvent]()
	o := getCommonOptions(nil, opts...)
	cc := o.cancelCtx
	a.ccCh <- cc
	go func() {
		defer gen.Close()
		<-cc.cancelChan
		for {
			if cc.getMode() == CancelImmediate {
				gen.Send(&AgentEvent{Err: cc.createCancelError()})
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
	return iter
}

func newAndRunTurnLoop[T any, M MessageType](ctx context.Context, cfg TurnLoopConfig[T, M]) *TurnLoop[T, M] {
	l := NewTurnLoop(cfg)
	l.Run(ctx)
	return l
}

func genInputConsumeAll(_ context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
	return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
}

func genInputConsumeAllWithMsg(_ context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
	return &GenInputResult[string, *schema.Message]{
		Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
		Consumed: items,
	}, nil
}

func genInputConsumeFirst(_ context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
	return &GenInputResult[string, *schema.Message]{
		Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
		Consumed:  []string{items[0]},
		Remaining: items[1:],
	}, nil
}

var prepareTestAgent = func(_ context.Context, _ *TurnLoop[string, *schema.Message], _ []string) (Agent, error) {
	return &turnLoopMockAgent{name: "test"}, nil
}

func prepareAgent(a Agent) func(context.Context, *TurnLoop[string, *schema.Message], []string) (Agent, error) {
	return func(_ context.Context, _ *TurnLoop[string, *schema.Message], _ []string) (Agent, error) {
		return a, nil
	}
}

func waitOrFail(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

func newTestStore() *turnLoopCheckpointStore {
	return &turnLoopCheckpointStore{m: make(map[string][]byte)}
}

func newPreemptTestLoop(t *testing.T, agent *turnLoopCancellableMockAgent) *TurnLoop[string, *schema.Message] {
	t.Helper()

	agentStarted := make(chan struct{})
	agentStartedOnce := sync.Once{}

	originalRunFunc := agent.runFunc
	agent.runFunc = func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
		agentStartedOnce.Do(func() { close(agentStarted) })
		return originalRunFunc(ctx, input)
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput:     genInputConsumeFirst,
	})

	loop.Push("first")

	waitOrFail(t, agentStarted, "agent did not start")

	return loop
}

func TestTurnLoop_StopIsIdempotent(t *testing.T) {
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Stop()
	loop.Stop()
	loop.Stop()

	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_WaitMultipleGoroutines(t *testing.T) {
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Stop()

	var wg sync.WaitGroup
	results := make([]*TurnLoopExitState[string, *schema.Message], 3)

	for i := 0; i < 3; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = loop.Wait()
		}()
	}

	wg.Wait()

	assert.Equal(t, results[0], results[1])
	assert.Equal(t, results[1], results[2])
}

func TestTurnLoop_UnhandledItemsOnStop(t *testing.T) {
	started := make(chan struct{})
	blocked := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			close(started)
			<-blocked
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")
	loop.Push("msg2")
	loop.Push("msg3")

	<-started

	loop.Stop()
	close(blocked)

	result := loop.Wait()
	assert.NotEmpty(t, result.UnhandledItems, "should return unhandled items")
}

func TestTurnLoop_GenInputError(t *testing.T) {
	genErr := errors.New("gen input error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return nil, genErr
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, genErr)
}

func TestTurnLoop_GetAgentError(t *testing.T) {
	agentErr := errors.New("get agent error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAll,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return nil, agentErr
		},
	})

	loop.Push("msg1")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, agentErr)
}

func TestTurnLoop_BatchProcessing(t *testing.T) {
	var batches [][]string
	var mu sync.Mutex

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			mu.Lock()
			batches = append(batches, items)
			mu.Unlock()

			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")
	loop.Push("msg2")
	loop.Push("msg3")

	time.Sleep(200 * time.Millisecond)

	loop.Stop()
	loop.Wait()

	mu.Lock()
	defer mu.Unlock()

	assert.NotEmpty(t, batches, "should have processed at least one batch")
}

func TestTurnLoop_StopWithMode(t *testing.T) {
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Stop(WithGraceful())

	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_Preempt_CancelsCurrentAgent(t *testing.T) {
	agentStarted := make(chan struct{})
	agentCancelled := make(chan struct{})
	agentStartedOnce := sync.Once{}
	agentCancelledOnce := sync.Once{}

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			agentStartedOnce.Do(func() {
				close(agentStarted)
			})
			<-ctx.Done()
			agentCancelledOnce.Do(func() {
				close(agentCancelled)
			})
			return &AgentOutput{}, nil
		},
	}

	genInputCalls := int32(0)
	secondGenInputCalled := make(chan struct{})
	secondGenInputOnce := sync.Once{}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			count := atomic.AddInt32(&genInputCalls, 1)
			if count >= 2 {
				secondGenInputOnce.Do(func() {
					close(secondGenInputCalled)
				})
			}
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  []string{items[0]},
				Remaining: items[1:],
			}, nil
		},
	})

	loop.Push("first")

	waitOrFail(t, agentStarted, "agent did not start")

	loop.Push("urgent", WithPreempt[string, *schema.Message](AnySafePoint))

	waitOrFail(t, agentCancelled, "agent was not cancelled by preempt")

	waitOrFail(t, secondGenInputCalled, "second GenInput was not called after preempt")

	loop.Stop(WithImmediate())
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&genInputCalls), int32(2))
}

func TestTurnLoop_Preempt_DiscardsConsumedItems(t *testing.T) {
	agentStarted := make(chan struct{})
	agentDone := make(chan struct{})
	agentStartedOnce := sync.Once{}
	agentDoneOnce := sync.Once{}
	firstAgentRun := true
	var firstRunMu sync.Mutex

	genInputResults := make([][]string, 0)
	var mu sync.Mutex

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			firstRunMu.Lock()
			isFirst := firstAgentRun
			firstAgentRun = false
			firstRunMu.Unlock()

			if isFirst {
				agentStartedOnce.Do(func() {
					close(agentStarted)
				})
				<-ctx.Done()
			} else {
				agentDoneOnce.Do(func() {
					close(agentDone)
				})
			}
			return &AgentOutput{}, nil
		},
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			mu.Lock()
			genInputResults = append(genInputResults, items)
			mu.Unlock()

			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  []string{items[0]},
				Remaining: items[1:],
			}, nil
		},
	})

	loop.Push("first")

	waitOrFail(t, agentStarted, "agent did not start")

	loop.Push("urgent", WithPreempt[string, *schema.Message](AnySafePoint))

	waitOrFail(t, agentDone, "second agent run did not complete")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(genInputResults), 2)
	assert.NotContains(t, genInputResults[1], "first")
	assert.Contains(t, genInputResults[1], "urgent")
}

func TestTurnLoop_Preempt_WithAgentCancelMode(t *testing.T) {
	cancelFuncCalled := make(chan struct{})
	cancelFuncCalledOnce := sync.Once{}
	firstCancelModeUsed := CancelImmediate
	var cancelModeMu sync.Mutex

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			<-ctx.Done()
			return &AgentOutput{}, nil
		},
		onCancel: func(cc *cancelContext) {
			cancelModeMu.Lock()
			cancelFuncCalledOnce.Do(func() {
				firstCancelModeUsed = cc.getMode()
				close(cancelFuncCalled)
			})
			cancelModeMu.Unlock()
		},
	}

	loop := newPreemptTestLoop(t, agent)

	loop.Push("urgent", WithPreempt[string, *schema.Message](AfterToolCalls))

	waitOrFail(t, cancelFuncCalled, "cancelFunc was not called by preempt")

	loop.Stop(WithImmediate())
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	cancelModeMu.Lock()
	actualMode := firstCancelModeUsed
	cancelModeMu.Unlock()
	assert.Equal(t, CancelAfterToolCalls, actualMode)
}

func TestTurnLoop_PreemptAck_ClosesAfterCancelIsInitiated(t *testing.T) {
	cancelObserved := make(chan struct{})
	agentFinishGate := make(chan struct{})
	cancelObservedOnce := sync.Once{}

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			<-ctx.Done()
			<-agentFinishGate
			return &AgentOutput{}, nil
		},
		onCancel: func(cc *cancelContext) {
			cancelObservedOnce.Do(func() { close(cancelObserved) })
		},
	}

	loop := newPreemptTestLoop(t, agent)

	ok, ack := loop.Push("urgent", WithPreempt[string, *schema.Message](AfterToolCalls))
	assert.True(t, ok)
	assert.NotNil(t, ack)

	waitOrFail(t, ack, "preempt ack was not closed")

	waitOrFail(t, cancelObserved, "cancel was not initiated")

	close(agentFinishGate)

	loop.Stop(WithImmediate())
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_PreemptAck_ClosesImmediatelyIfLoopNotStarted(t *testing.T) {
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	ok, ack := loop.Push("urgent", WithPreempt[string, *schema.Message](AnySafePoint))
	assert.True(t, ok)
	assert.NotNil(t, ack)

	waitOrFail(t, ack, "preempt ack was not closed")
}

func TestTurnLoop_Preempt_EscalatesOnSecondPreempt(t *testing.T) {
	firstCancelSeen := make(chan struct{})
	agentFinishGate := make(chan struct{})
	firstCancelOnce := sync.Once{}

	var ccPtr atomic.Value

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			<-ctx.Done()
			<-agentFinishGate
			return &AgentOutput{}, nil
		},
		onCancel: func(cc *cancelContext) {
			ccPtr.Store(cc)
			firstCancelOnce.Do(func() { close(firstCancelSeen) })
		},
	}

	loop := newPreemptTestLoop(t, agent)

	loop.Push("urgent1", WithPreempt[string, *schema.Message](AfterChatModel))
	waitOrFail(t, firstCancelSeen, "first preempt did not trigger cancel")

	loop.Push("urgent2", WithPreemptTimeout[string, *schema.Message](AnySafePoint, time.Millisecond))

	wantMode := CancelAfterChatModel | CancelAfterToolCalls
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		v := ccPtr.Load()
		if v == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		cc := v.(*cancelContext)
		if cc.getMode() == wantMode && atomic.LoadInt32(&cc.escalated) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	v := ccPtr.Load()
	if v == nil {
		t.Fatal("cancel context was not captured")
	}
	cc := v.(*cancelContext)
	assert.Equal(t, wantMode, cc.getMode())
	assert.Equal(t, int32(1), atomic.LoadInt32(&cc.escalated))

	close(agentFinishGate)

	loop.Stop(WithImmediate())
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_Preempt_JoinsSafePointModesOnSecondPreempt(t *testing.T) {
	firstCancelSeen := make(chan struct{})
	agentFinishGate := make(chan struct{})
	firstCancelOnce := sync.Once{}

	var ccPtr atomic.Value

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			<-ctx.Done()
			<-agentFinishGate
			return &AgentOutput{}, nil
		},
		onCancel: func(cc *cancelContext) {
			ccPtr.Store(cc)
			firstCancelOnce.Do(func() { close(firstCancelSeen) })
		},
	}

	loop := newPreemptTestLoop(t, agent)

	loop.Push("urgent1", WithPreempt[string, *schema.Message](AfterChatModel))
	waitOrFail(t, firstCancelSeen, "first preempt did not trigger cancel")

	loop.Push("urgent2", WithPreempt[string, *schema.Message](AfterToolCalls))

	want := CancelAfterChatModel | CancelAfterToolCalls
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		v := ccPtr.Load()
		if v == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		cc := v.(*cancelContext)
		if cc.getMode() == want {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	v := ccPtr.Load()
	if v == nil {
		t.Fatal("cancel context was not captured")
	}
	cc := v.(*cancelContext)
	assert.Equal(t, want, cc.getMode())

	close(agentFinishGate)

	loop.Stop(WithImmediate())
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_Push_WithoutPreempt_DoesNotCancel(t *testing.T) {
	agentRunCount := 0
	agentDone := make(chan struct{})

	agent := &turnLoopMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			agentRunCount++
			if agentRunCount == 1 {
				time.Sleep(100 * time.Millisecond)
			}
			if agentRunCount == 2 {
				close(agentDone)
			}
			return &AgentOutput{}, nil
		},
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput:     genInputConsumeFirst,
	})

	loop.Push("first")
	time.Sleep(20 * time.Millisecond)
	loop.Push("second")

	waitOrFail(t, agentDone, "second agent run did not complete")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.Equal(t, 2, agentRunCount)
}

func TestTurnLoop_PreemptDelay_NoMispreemptOnNaturalCompletion(t *testing.T) {
	agent1Started := make(chan struct{})
	allowAgent1Done := make(chan struct{})
	agent1Done := make(chan struct{})
	agent2Started := make(chan struct{})
	allowAgent2Done := make(chan struct{})
	agent2Done := make(chan struct{})
	agent1StartedOnce := sync.Once{}
	agent1DoneOnce := sync.Once{}
	agent2StartedOnce := sync.Once{}
	agent2DoneOnce := sync.Once{}

	var agentRunCount int32

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			count := atomic.AddInt32(&agentRunCount, 1)
			switch count {
			case 1:
				agent1StartedOnce.Do(func() { close(agent1Started) })
				<-allowAgent1Done
				agent1DoneOnce.Do(func() { close(agent1Done) })
			case 2:
				agent2StartedOnce.Do(func() { close(agent2Started) })
				select {
				case <-allowAgent2Done:
				case <-ctx.Done():
					t.Error("Agent2 was unexpectedly cancelled")
					return nil, ctx.Err()
				}
				agent2DoneOnce.Do(func() { close(agent2Done) })
			}
			return &AgentOutput{}, nil
		},
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput:     genInputConsumeFirst,
	})

	loop.Push("first")

	waitOrFail(t, agent1Started, "agent1 did not start")

	_, ack := loop.Push("second", WithPreempt[string, *schema.Message](AnySafePoint), WithPreemptDelay[string, *schema.Message](500*time.Millisecond))
	require.NotNil(t, ack)
	close(allowAgent1Done)

	waitOrFail(t, agent1Done, "agent1 did not complete naturally")

	waitOrFail(t, agent2Started, "agent2 did not start")

	requireAckClosed(t, ack)
	close(allowAgent2Done)

	waitOrFail(t, agent2Done, "agent2 did not complete - may have been incorrectly preempted")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.Equal(t, int32(2), atomic.LoadInt32(&agentRunCount))
}

func TestTurnLoop_PreemptDuringPlanningCancelsUpcomingAgent(t *testing.T) {
	genInputEntered := make(chan struct{})
	allowGenInput := make(chan struct{})
	cancelSubmitted := make(chan struct{})
	agentCancelled := make(chan struct{})
	genInputOnce := sync.Once{}
	cancelOnce := sync.Once{}
	agentCancelledOnce := sync.Once{}

	var agentRunCount int32
	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			count := atomic.AddInt32(&agentRunCount, 1)
			if count == 1 {
				<-ctx.Done()
				agentCancelledOnce.Do(func() { close(agentCancelled) })
				return &AgentOutput{}, nil
			}
			return &AgentOutput{}, nil
		},
		onCancel: func(cc *cancelContext) {
			cancelOnce.Do(func() { close(cancelSubmitted) })
		},
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			genInputOnce.Do(func() {
				close(genInputEntered)
				<-allowGenInput
			})
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  []string{items[0]},
				Remaining: items[1:],
			}, nil
		},
	})

	loop.Push("first")

	waitOrFail(t, genInputEntered, "GenInput did not enter planning phase")

	ok, ack := loop.Push("urgent", WithPreempt[string, *schema.Message](AnySafePoint))
	require.True(t, ok)
	require.NotNil(t, ack)
	requireAckOpen(t, ack)

	close(allowGenInput)

	waitOrFail(t, cancelSubmitted, "planning-phase preempt did not submit cancel")
	requireAckClosed(t, ack)

	waitOrFail(t, agentCancelled, "agent was not cancelled by planning-phase preempt")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&agentRunCount), int32(1))
}

func TestTurnLoop_ConcurrentPush(t *testing.T) {
	var count int32

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			atomic.AddInt32(&count, int32(len(items)))
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = loop.Push(fmt.Sprintf("msg-%d-%d", i, j))
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	loop.Stop()
	result := loop.Wait()

	processed := atomic.LoadInt32(&count)
	unhandled := len(result.UnhandledItems)

	assert.True(t, processed > 0, "should have processed some items")
	assert.True(t, int(processed)+unhandled <= 100, "total should not exceed pushed amount")
}

func TestTurnLoop_StopAfterReceive_RecoverItem(t *testing.T) {
	receiveStarted := make(chan struct{})
	cancelDone := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			close(receiveStarted)
			<-cancelDone
			time.Sleep(50 * time.Millisecond)
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")
	<-receiveStarted

	loop.Stop()
	close(cancelDone)

	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_StopAfterGenInput_RecoverConsumed(t *testing.T) {
	genInputDone := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			close(genInputDone)
			time.Sleep(50 * time.Millisecond)
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			time.Sleep(100 * time.Millisecond)
			return &turnLoopMockAgent{name: "test"}, nil
		},
	})

	loop.Push("msg1")
	loop.Push("msg2")

	<-genInputDone

	time.Sleep(60 * time.Millisecond)
	loop.Stop()

	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_GetAgentError_RecoverConsumed(t *testing.T) {
	agentErr := errors.New("get agent error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], c []string) (Agent, error) {
			return nil, agentErr
		},
	})

	loop.Push("msg1")
	loop.Push("msg2")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, agentErr)
	assert.NotEmpty(t, result.UnhandledItems, "should recover at least the consumed item and remaining")
}

func TestTurnLoop_GenInputError_RecoverItems(t *testing.T) {
	genErr := errors.New("gen input error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return nil, genErr
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")
	loop.Push("msg2")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, genErr)
	assert.Len(t, result.UnhandledItems, 2, "should recover all items when GenInput fails")
	assert.Contains(t, result.UnhandledItems, "msg1")
	assert.Contains(t, result.UnhandledItems, "msg2")
}

func TestTurnLoop_PrepareAgentError_RecoverItemsInOrder(t *testing.T) {
	agentErr := errors.New("prepare agent error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			var urgent string
			remaining := make([]string, 0, len(items))
			for _, item := range items {
				if item == "urgent" {
					urgent = item
				} else {
					remaining = append(remaining, item)
				}
			}
			if urgent != "" {
				return &GenInputResult[string, *schema.Message]{
					Input:     &AgentInput{},
					Consumed:  []string{urgent},
					Remaining: remaining,
				}, nil
			}
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return nil, agentErr
		},
	})

	loop.Push("msg1")
	loop.Push("urgent")
	loop.Push("msg2")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, agentErr)
	assert.Len(t, result.UnhandledItems, 3, "should recover all items")
	assert.Equal(t, []string{"msg1", "urgent", "msg2"}, result.UnhandledItems,
		"should preserve original push order even when GenInput selects non-prefix items")
}

// Context cancel tests: the TurnLoop monitors context cancellation by closing
// the internal buffer when ctx.Done() fires, which unblocks the blocking
// Receive() call. The loop then checks ctx.Err() and exits with the context error.
//
// 上下文取消测试：TurnLoop 通过在 ctx.Done() 触发时关闭内部缓冲区来监控 context 取消，从而解除阻塞中的 Receive() 调用。
// 随后循环检查 ctx.Err()，并带着 context 错误退出。

func TestTurnLoop_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	genInputStarted := make(chan struct{})
	genInputDone := make(chan struct{})

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			close(genInputStarted)
			<-genInputDone
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")

	<-genInputStarted
	cancel()
	close(genInputDone)

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, context.Canceled)
}

func TestTurnLoop_ContextDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			select {
			case <-time.After(100 * time.Millisecond):
				return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, context.DeadlineExceeded)
}

func TestTurnLoop_ContextCancelBeforeReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	// Push before Run to guarantee the item is buffered before the
	// context-monitoring goroutine can close the buffer.
	//
	// 在 Run 前 Push，确保该项在 context 监控 goroutine 关闭缓冲区之前已进入缓冲区。
	_, _ = loop.Push("msg1")
	loop.Run(ctx)

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, context.Canceled)
	assert.Len(t, result.UnhandledItems, 1)
}

func TestTurnLoop_ContextCancelDuringBlockingReceive(t *testing.T) {
	// When context is cancelled while Receive() is blocking (no items in buffer),
	// the context monitoring goroutine closes the buffer, which unblocks Receive().
	//
	// 当 context 在 Receive() 阻塞时（缓冲区无项）被取消，context 监控 goroutine 会关闭缓冲区，从而解除 Receive() 阻塞。
	ctx, cancel := context.WithCancel(context.Background())

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	// Don't push any items — let Receive() block
	// 不要 Push 任何项 —— 让 Receive() 阻塞
	time.Sleep(50 * time.Millisecond)
	cancel()

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, context.Canceled)
}

func TestTurnLoop_ContextCancelAfterGenInput_RecoverItems(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	genInputCount := 0
	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			genInputCount++
			if genInputCount == 1 {
				cancel()
			}
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], c []string) (Agent, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &turnLoopMockAgent{name: "test"}, nil
		},
	})

	loop.Push("msg1")
	loop.Push("msg2")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, context.Canceled)
	assert.NotEmpty(t, result.UnhandledItems, "should recover consumed and remaining items")
}

func TestTurnLoop_OnAgentEventsReceivesEvents(t *testing.T) {
	var receivedEvents []*AgentEvent
	var receivedConsumed []string
	var mu sync.Mutex

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			mu.Lock()
			receivedConsumed = append(receivedConsumed, tc.Consumed...)
			mu.Unlock()

			for {
				event, ok := events.Next()
				if !ok {
					break
				}
				mu.Lock()
				receivedEvents = append(receivedEvents, event)
				mu.Unlock()
			}
			return nil
		},
	})

	loop.Push("msg1")

	time.Sleep(100 * time.Millisecond)

	loop.Stop()
	result := loop.Wait()

	assert.NoError(t, result.ExitReason)

	mu.Lock()
	defer mu.Unlock()
	assert.NotEmpty(t, receivedConsumed, "should have received consumed items")
}

// TestTurnLoop_BareStop_AgentRunsToCompletion verifies the core contract of
// bare Stop(): the running agent finishes naturally with an uncanceled context,
// the loop exits cleanly (ExitReason == nil), and no new turn starts even when
// additional items are buffered.
//
// TestTurnLoop_BareStop_AgentRunsToCompletion 验证 bare Stop() 的核心契约：运行中的智能体会在未取消的 context 下自然完成，循环干净退出（ExitReason == nil），即使还有额外项已缓冲，也不会启动新的 turn。
func TestTurnLoop_BareStop_AgentRunsToCompletion(t *testing.T) {
	const agentWorkDuration = 200 * time.Millisecond

	agentStarted := make(chan struct{})
	agentCtxErr := make(chan error, 1)
	agentOutput := make(chan string, 1)

	turnsExecuted := int32(0)

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeFirst,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "worker",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					atomic.AddInt32(&turnsExecuted, 1)
					close(agentStarted)

					// Simulate real work (NOT blocking on <-ctx.Done())
					// 模拟真实工作（不要阻塞在 <-ctx.Done() 上）
					time.Sleep(agentWorkDuration)

					// Record context state AFTER work completes
					// 在工作完成后记录 context 状态
					agentCtxErr <- ctx.Err()
					agentOutput <- "work-done"

					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	// Push two items so the loop has a reason to start a second turn.
	// Push 两个项，让循环有理由启动第二个 turn。
	loop.Push("task1")
	loop.Push("task2")

	// Wait for the agent to start processing task1.
	// 等待智能体开始处理 task1。
	waitOrFail(t, agentStarted, "agent did not start")

	// Call bare Stop() while the agent is doing work.
	// 在智能体工作期间调用 bare Stop()。
	loop.Stop()

	result := loop.Wait()

	// 1. Agent's context was NOT canceled.
	// 1. 智能体的 context 未被取消。
	select {
	case err := <-agentCtxErr:
		assert.NoError(t, err, "bare Stop must not cancel the agent's context")
	default:
		t.Fatal("agent never reported context state")
	}

	// 2. Agent completed its work.
	// 2. 智能体完成了工作。
	select {
	case out := <-agentOutput:
		assert.Equal(t, "work-done", out)
	default:
		t.Fatal("agent never produced output")
	}

	// 3. ExitReason is nil (clean exit, not a CancelError).
	// 3. ExitReason 为 nil（干净退出，不是 CancelError）。
	assert.NoError(t, result.ExitReason)

	// 4. InterruptedItems is empty (agent was not interrupted).
	// 4. InterruptedItems 为空（智能体未被中断）。
	assert.Empty(t, result.InterruptedItems)

	// 5. Only one turn executed; the second item is unhandled.
	// 5. 只执行了一个 turn；第二个项未处理。
	assert.Equal(t, int32(1), atomic.LoadInt32(&turnsExecuted),
		"bare Stop must prevent new turns from starting after the current one completes")
	assert.Equal(t, []string{"task2"}, result.UnhandledItems,
		"the second item should appear in UnhandledItems")
}

func TestTurnLoop_StopCheckPointIDInCancelError(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	checkpointID := "turn-loop-cancel-ckpt-1"
	store := newTestStore()

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: checkpointID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})

	loop.Push("msg1")

	<-modelStarted
	loop.Stop(WithImmediate())

	result := loop.Wait()

	var cancelErr *CancelError
	assert.True(t, errors.As(result.ExitReason, &cancelErr), "ExitReason should be a *CancelError")

	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.m[checkpointID]
	assert.True(t, ok, "checkpoint should be saved under the configured CheckpointID")
}

// TestTurnLoop_CancelError_CapturedIndependentlyOfCallback verifies that the TurnLoop
// correctly reports *CancelError as ExitReason and populates InterruptedItems even when
// the user's custom OnAgentEvents callback swallows the CancelError (returns nil).
// This tests the documented guarantee: "the callback should NEVER propagate CancelError
// — the framework handles it automatically."
//
// TestTurnLoop_CancelError_CapturedIndependentlyOfCallback 验证即使用户自定义的 OnAgentEvents 回调吞掉 CancelError（返回 nil），TurnLoop 也能正确将 *CancelError 报告为 ExitReason，并填充 InterruptedItems。
// 这测试了文档中的保证：“the callback should NEVER propagate CancelError — the framework handles it automatically.”
func TestTurnLoop_CancelError_CapturedIndependentlyOfCallback(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	checkpointID := "cancel-capture-independent-1"
	store := newTestStore()

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: checkpointID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
		// Custom OnAgentEvents that deliberately swallows all errors including CancelError.
		// 自定义 OnAgentEvents，故意吞掉包括 CancelError 在内的所有错误。
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*TypedAgentEvent[*schema.Message]]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
				// Deliberately ignore event.Err — do NOT propagate CancelError.
				// 故意忽略 event.Err —— 不要传播 CancelError。
			}
			return nil // swallow everything
			// 吞掉所有错误
		},
	})

	loop.Push("msg1")

	<-modelStarted
	loop.Stop(WithImmediate())

	result := loop.Wait()

	// The framework should capture CancelError independently of the callback's return value.
	// 框架应独立于回调返回值捕获 CancelError。
	var cancelErr *CancelError
	assert.True(t, errors.As(result.ExitReason, &cancelErr),
		"ExitReason should be *CancelError even when OnAgentEvents swallows it, got: %v", result.ExitReason)

	// InterruptedItems should be populated.
	// InterruptedItems 应被填充。
	assert.Equal(t, []string{"msg1"}, result.InterruptedItems,
		"InterruptedItems should contain the items that were being processed")

	// Checkpoint should be saved.
	// Checkpoint 应被保存。
	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.m[checkpointID]
	assert.True(t, ok, "checkpoint should be saved under the configured CheckpointID")
}

// TestTurnLoop_CancelError_CustomErrorWins_InterruptedItemsStillSet verifies that when
// the user's OnAgentEvents callback returns a custom error during a cancel, the custom
// error becomes ExitReason (not overwritten by CancelError), but InterruptedItems is still
// populated because the items were factually mid-execution when the cancel signal arrived.
//
// TestTurnLoop_CancelError_CustomErrorWins_InterruptedItemsStillSet 验证：当
// 用户的 OnAgentEvents 回调在取消期间返回自定义错误时，该自定义
// 错误会成为 ExitReason（不会被 CancelError 覆盖），但 InterruptedItems 仍会
// 被填充，因为取消信号到达时这些条目事实上正在执行中。
func TestTurnLoop_CancelError_CustomErrorWins_InterruptedItemsStillSet(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	checkpointID := "cancel-custom-error-wins-1"
	store := newTestStore()
	customErr := fmt.Errorf("user callback encountered a problem")

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: checkpointID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
		// Custom OnAgentEvents that returns a custom error instead of the CancelError.
		// 自定义 OnAgentEvents 返回自定义错误，而不是 CancelError。
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*TypedAgentEvent[*schema.Message]]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			return customErr
		},
	})

	loop.Push("msg1")

	<-modelStarted
	loop.Stop(WithImmediate())

	result := loop.Wait()

	// User's custom error should win as ExitReason.
	// 用户的自定义错误应优先作为 ExitReason。
	assert.ErrorIs(t, result.ExitReason, customErr,
		"ExitReason should be the user's custom error, not CancelError")

	// But InterruptedItems should still be populated (items were factually in-flight).
	// 但 InterruptedItems 仍应被填充（条目事实上正在执行中）。
	assert.Equal(t, []string{"msg1"}, result.InterruptedItems,
		"InterruptedItems should contain the items that were being processed")

	// Checkpoint should be saved (cancel was captured, items were in-flight).
	// Checkpoint 应被保存（已捕获取消，条目正在执行中）。
	store.mu.Lock()
	defer store.mu.Unlock()
	_, ok := store.m[checkpointID]
	assert.True(t, ok, "checkpoint should be saved even when user returns custom error")
}

func TestTurnLoop_StopWithoutCheckpointIDDoesNotPersist(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	store := newTestStore()

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})

	loop.Push("msg1")

	<-modelStarted
	loop.Stop(WithImmediate())

	result := loop.Wait()

	var cancelErr *CancelError
	assert.True(t, errors.As(result.ExitReason, &cancelErr), "ExitReason should be a *CancelError")

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Empty(t, store.m, "no checkpoint should be saved when CheckpointID is not configured")
}

func TestTurnLoop_StopWhileIdle_SkipsCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := &deletableCheckpointStore{
		turnLoopCheckpointStore: turnLoopCheckpointStore{m: make(map[string][]byte)},
	}
	cpID := "idle-session"

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Stop()
	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)

	store.mu.Lock()
	defer store.mu.Unlock()
	_, exists := store.m[cpID]
	assert.False(t, exists, "no checkpoint should be saved when TurnLoop is idle")
}

func TestTurnLoop_StopBetweenTurnsAndResume(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "between-turns-session"

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("a")
	loop.Push("b")
	loop.Stop()
	loop.Run(ctx)

	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)

	var seen []string
	var mu sync.Mutex
	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			mu.Lock()
			seen = append([]string{}, items...)
			mu.Unlock()
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items,
			}, nil
		},
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})

	loop2.Push("c")
	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.NoError(t, exit2.ExitReason)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"a", "b", "c"}, seen)
}

// TestTurnLoop_StopCancel_InterruptedItems_PersistAndRestore verifies the full
// lifecycle: Stop(WithImmediate()) cancels a running agent, the exit state
// reports the correct InterruptedItems, the checkpoint persists them, and a new
// TurnLoop restoring from that checkpoint passes the exact same items to
// GenResume's interruptedItems parameter.
//
// TestTurnLoop_StopCancel_InterruptedItems_PersistAndRestore 验证完整
// 生命周期：Stop(WithImmediate()) 取消正在运行的智能体，退出状态
// 报告正确的 InterruptedItems，检查点持久化它们，并且从该检查点恢复的新
// TurnLoop 会将完全相同的条目传给 GenResume 的 interruptedItems 参数。
func TestTurnLoop_StopCancel_InterruptedItems_PersistAndRestore(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	store := newTestStore()
	cpID := "inflight-persist-session"

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	require.NoError(t, err)

	// Phase 1: Run, cancel mid-turn, verify exit state.
	// 阶段 1：运行，在 turn 中途取消，并验证退出状态。
	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})

	loop.Push("msg1")
	<-modelStarted
	loop.Stop(WithImmediate())
	exit := loop.Wait()

	// ExitReason must be CancelError.
	// ExitReason 必须是 CancelError。
	var cancelErr *CancelError
	require.True(t, errors.As(exit.ExitReason, &cancelErr), "expected *CancelError, got: %v", exit.ExitReason)

	// InterruptedItems must contain the consumed items.
	// InterruptedItems 必须包含已消费的条目。
	assert.Equal(t, []string{"msg1"}, exit.InterruptedItems,
		"InterruptedItems in exit state should contain the items that were mid-execution")

	// Checkpoint must be saved.
	// Checkpoint 必须被保存。
	assert.True(t, exit.CheckpointAttempted, "checkpoint should be attempted")
	assert.NoError(t, exit.CheckpointErr, "checkpoint save should succeed")

	store.mu.Lock()
	_, cpExists := store.m[cpID]
	store.mu.Unlock()
	require.True(t, cpExists, "checkpoint should exist in store")

	// Phase 2: Restore from checkpoint, verify GenResume receives the exact InterruptedItems.
	// 阶段 2：从检查点恢复，验证 GenResume 收到完全相同的 InterruptedItems。
	slowModel.setDelay(10 * time.Millisecond)

	var genResumeCalled bool
	var resumeInterruptedItems []string
	var resumeUnhandledItems []string

	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenResume: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], interruptedItems, unhandledItems, newItems []string) (*GenResumeResult[string, *schema.Message], error) {
			genResumeCalled = true
			resumeInterruptedItems = append([]string{}, interruptedItems...)
			resumeUnhandledItems = append([]string{}, unhandledItems...)
			return &GenResumeResult[string, *schema.Message]{
				Consumed:  interruptedItems,
				Remaining: append(append([]string{}, unhandledItems...), newItems...),
			}, nil
		},
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			t.Fatal("GenInput should not be called when resuming from a cancel checkpoint")
			return nil, nil
		},
		PrepareAgent: prepareAgent(agent),
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})

	loop2.Run(ctx)
	exit2 := loop2.Wait()

	assert.NoError(t, exit2.ExitReason)
	assert.True(t, genResumeCalled, "GenResume should be called when restoring from a cancel checkpoint")
	assert.Equal(t, []string{"msg1"}, resumeInterruptedItems,
		"GenResume's interruptedItems should match the original exit state's InterruptedItems")
	assert.Empty(t, resumeUnhandledItems,
		"unhandledItems should be empty (all items were consumed before cancel)")
}

// TestTurnLoop_PreemptThenStop_InterruptedItems_ReflectsStoppedTurn verifies that
// when a preempt interrupts turn 1 and then Stop(WithImmediate()) cancels turn 2,
// the reported InterruptedItems correspond to turn 2's consumed items (not turn 1's).
//
// TestTurnLoop_PreemptThenStop_InterruptedItems_ReflectsStoppedTurn 验证：
// 当 preempt 中断 turn 1，随后 Stop(WithImmediate()) 取消 turn 2 时，
// 报告的 InterruptedItems 对应 turn 2 已消费的条目（而不是 turn 1 的）。
func TestTurnLoop_PreemptThenStop_InterruptedItems_ReflectsStoppedTurn(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "preempt-then-stop-session"

	turnCount := int32(0)
	modelStarted := make(chan struct{}, 10)

	slowModel := &cancelTestChatModel{
		delayNs: int64(5 * time.Second),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 10),
	}

	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	require.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			n := atomic.AddInt32(&turnCount, 1)
			_ = n
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items,
			}, nil
		},
		PrepareAgent: prepareAgent(agent),
	})

	// Turn 1: push "a", wait for agent to start, then preempt with "b".
	// Turn 1：推入 "a"，等待智能体启动，然后用 "b" preempt。
	loop.Push("a")
	<-modelStarted

	_, ack := loop.Push("b", WithPreemptTimeout[string, *schema.Message](AnySafePoint, 10*time.Millisecond))
	// Wait for the preempt to be acknowledged.
	// 等待 preempt 被确认。
	waitOrFail(t, ack, "preempt ack timed out")

	// Turn 2: the loop restarts with all buffered items. Wait for agent to start.
	// Turn 2：循环带着所有缓冲条目重启。等待智能体启动。
	<-modelStarted

	// Now stop with immediate cancel on turn 2.
	// 现在在第 2 轮立即取消并停止。
	loop.Stop(WithImmediate())
	exit := loop.Wait()

	// ExitReason must be CancelError.
	// ExitReason 必须是 CancelError。
	var cancelErr *CancelError
	require.True(t, errors.As(exit.ExitReason, &cancelErr), "expected *CancelError, got: %v", exit.ExitReason)

	// KEY ASSERTION: InterruptedItems must reflect turn 2's consumed items (which
	// includes both "a" and "b" since GenInput consumes all items), NOT turn 1's ["a"].
	// Turn 2 consumed whatever GenInput received (the re-buffered items from preempt + "b").
	//
	// 关键断言：InterruptedItems 必须反映第 2 轮消耗的 items（由于 GenInput 会消耗所有 items，因此包含 "a" 和 "b"），而不是第 1 轮的 ["a"]。
	// 第 2 轮消耗了 GenInput 收到的内容（preempt 重新缓冲的 items 加上 "b"）。
	assert.NotEmpty(t, exit.InterruptedItems, "InterruptedItems should not be empty after Stop cancel")
	assert.Contains(t, exit.InterruptedItems, "b",
		"InterruptedItems should contain 'b' which was part of turn 2's consumed items")
}

func TestTurnLoop_BusinessInterrupt_PersistAndResume(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "interrupt-session"

	// Agent that produces a business interrupt via Interrupt() call.
	// 通过 Interrupt() 调用产生业务中断的智能体。
	interruptAgent := &turnLoopInterruptAgent{interruptInfo: "approval_needed"}

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return interruptAgent, nil
		},
	})

	loop.Push("msg1")
	exit := loop.Wait()

	// 1. ExitReason is an *InterruptError (not nil, not *CancelError).
	// 1. ExitReason 是 *InterruptError（非 nil，且不是 *CancelError）。
	var intErr *InterruptError
	require.True(t, errors.As(exit.ExitReason, &intErr), "expected *InterruptError, got: %v", exit.ExitReason)

	// 2. InterruptContexts is populated.
	// 2. InterruptContexts 已填充。
	require.NotEmpty(t, intErr.InterruptContexts)

	// 3. InterruptedItems contains the items being processed.
	// 3. InterruptedItems 包含正在处理的 items。
	assert.Equal(t, []string{"msg1"}, exit.InterruptedItems)

	// 4. Checkpoint was persisted.
	// 4. Checkpoint 已持久化。
	assert.True(t, exit.CheckpointAttempted)
	assert.NoError(t, exit.CheckpointErr)

	store.mu.Lock()
	_, cpExists := store.m[cpID]
	store.mu.Unlock()
	assert.True(t, cpExists, "checkpoint should exist in store")

	// 5. Resume: new TurnLoop with same CheckpointID gets GenResume called.
	// 5. Resume：使用相同 CheckpointID 的新 TurnLoop 会调用 GenResume。
	var genResumeCalled bool
	var resumeInterruptedItems []string
	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenResume: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], interruptedItems []string, unhandledItems []string, newItems []string) (*GenResumeResult[string, *schema.Message], error) {
			genResumeCalled = true
			resumeInterruptedItems = append([]string{}, interruptedItems...)
			return &GenResumeResult[string, *schema.Message]{
				Consumed:  interruptedItems,
				Remaining: append(append([]string{}, unhandledItems...), newItems...),
			}, nil
		},
		GenInput: genInputConsumeAll,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			// On resume, agent completes normally.
			// 恢复时，智能体正常完成。
			return &turnLoopMockAgent{
				name:   "ResumeAgent",
				events: []*AgentEvent{{Output: &AgentOutput{}}},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})

	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.NoError(t, exit2.ExitReason)
	assert.True(t, genResumeCalled, "GenResume should be called on checkpoint resume")
	assert.Equal(t, []string{"msg1"}, resumeInterruptedItems, "interruptedItems should contain the original items")
}

// turnLoopInterruptAgent is a test agent that produces a business interrupt event.
// turnLoopInterruptAgent 是一个产生业务中断事件的测试智能体。
type turnLoopInterruptAgent struct {
	interruptInfo any
}

func (a *turnLoopInterruptAgent) Name(_ context.Context) string { return "InterruptAgent" }
func (a *turnLoopInterruptAgent) Description(_ context.Context) string {
	return "agent that interrupts"
}
func (a *turnLoopInterruptAgent) Run(ctx context.Context, _ *AgentInput, _ ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iter, gen := NewAsyncIteratorPair[*AgentEvent]()
	go func() {
		defer gen.Close()
		event := Interrupt(ctx, a.interruptInfo)
		gen.Send(event)
	}()
	return iter
}

func TestTurnLoop_CheckpointIDWithoutStore_FreshStart(t *testing.T) {
	ctx := context.Background()
	var genInputCalled bool
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		CheckpointID: "some-id",
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			genInputCalled = true
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})
	loop.Push("a")
	loop.Run(ctx)
	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)
	assert.True(t, genInputCalled)
}

func TestTurnLoop_CheckpointNotFound_FreshStart(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	var genInputCalled bool
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: "nonexistent-id",
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			genInputCalled = true
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})
	loop.Push("a")
	loop.Run(ctx)
	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)
	assert.True(t, genInputCalled)
}

func TestTurnLoop_CheckpointEmptyData_TreatedAsNoCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	store.m["cp-empty"] = nil

	var genInputCalled bool
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: "cp-empty",
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			genInputCalled = true
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})
	loop.Push("a")
	loop.Run(ctx)
	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)
	assert.True(t, genInputCalled)
}

type errorCheckpointStore struct {
	getErr error
	setErr error
}

func (s *errorCheckpointStore) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, s.getErr
}

func (s *errorCheckpointStore) Set(_ context.Context, _ string, _ []byte) error {
	return s.setErr
}

func TestTurnLoop_CheckpointLoadError_ReturnsError(t *testing.T) {
	ctx := context.Background()
	store := &errorCheckpointStore{getErr: fmt.Errorf("store unavailable")}
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: "cp-1",
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Run(ctx)
	exit := loop.Wait()
	assert.Error(t, exit.ExitReason)
	assert.Contains(t, exit.ExitReason.Error(), "store unavailable")
}

func TestTurnLoop_CheckpointCorruptData_ReturnsError(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	store.m["cp-corrupt"] = []byte("not-valid-gob-data")
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: "cp-corrupt",
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Run(ctx)
	exit := loop.Wait()
	assert.Error(t, exit.ExitReason)
	assert.Contains(t, exit.ExitReason.Error(), "failed to unmarshal checkpoint")
}

func TestTurnLoop_CheckpointSaveError_ReturnsError(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	saveStore := &errorCheckpointStore{setErr: fmt.Errorf("write failed")}
	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        saveStore,
		CheckpointID: "cp-1",
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})
	loop.Push("msg1")
	<-modelStarted
	loop.Stop(WithImmediate())
	exit := loop.Wait()
	assert.Error(t, exit.ExitReason)
	assert.True(t, exit.CheckpointAttempted)
	assert.Error(t, exit.CheckpointErr)
	assert.Contains(t, exit.CheckpointErr.Error(), "write failed")
}

func TestTurnLoop_StaleCheckpointDeletion_OnCleanResume(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "stale-session"

	loop1 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop1.Push("a")
	loop1.Stop()
	loop1.Run(ctx)
	loop1.Wait()

	store.mu.Lock()
	_, exists := store.m[cpID]
	store.mu.Unlock()
	assert.True(t, exists, "checkpoint should exist after first loop saves it")

	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})
	loop2.Push("b")
	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.NoError(t, exit2.ExitReason)

	store.mu.Lock()
	_, exists = store.m[cpID]
	store.mu.Unlock()
	assert.True(t, exists, "checkpoint should still exist because loop2 was stopped and saved a new one")
}

type deletableCheckpointStore struct {
	turnLoopCheckpointStore
	deleteCalled bool
	deletedKey   string
}

func (s *deletableCheckpointStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCalled = true
	s.deletedKey = key
	delete(s.m, key)
	return nil
}

func TestTurnLoop_CheckpointDeleter_CalledOnContextCancel(t *testing.T) {
	ctx := context.Background()
	store := &deletableCheckpointStore{
		turnLoopCheckpointStore: turnLoopCheckpointStore{m: make(map[string][]byte)},
	}
	cpID := "deleter-session"

	loop1 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop1.Push("a")
	loop1.Stop()
	loop1.Run(ctx)
	loop1.Wait()

	store.mu.Lock()
	_, exists := store.m[cpID]
	store.mu.Unlock()
	assert.True(t, exists, "checkpoint saved after loop1")

	ctx2, cancel2 := context.WithCancel(ctx)
	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			cancel2()
			return nil
		},
	})
	loop2.Push("b")
	loop2.Run(ctx2)
	exit2 := loop2.Wait()
	assert.ErrorIs(t, exit2.ExitReason, context.Canceled)

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.True(t, store.deleteCalled, "CheckPointDeleter.Delete should be called")
	assert.Equal(t, cpID, store.deletedKey)
	_, exists = store.m[cpID]
	assert.False(t, exists, "checkpoint should be removed from store")
}

func TestTurnLoop_GenResumeNil_Error(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "resume-nil-session"
	modelStarted := make(chan struct{}, 1)

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop1 := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})
	loop1.Push("msg1")
	<-modelStarted
	loop1.Stop(WithImmediate())
	loop1.Wait()

	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.Error(t, exit2.ExitReason)
	assert.Contains(t, exit2.ExitReason.Error(), "GenResume is required")
}

func TestTurnLoop_SameCheckpointID_OverwritePattern(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "overwrite-session"

	loop1 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop1.Push("a")
	loop1.Push("b")
	loop1.Stop()
	loop1.Run(ctx)
	loop1.Wait()

	store.mu.Lock()
	data1 := append([]byte{}, store.m[cpID]...)
	store.mu.Unlock()
	assert.NotEmpty(t, data1)

	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop2.Push("c")
	loop2.Stop()
	loop2.Run(ctx)
	loop2.Wait()

	store.mu.Lock()
	data2 := append([]byte{}, store.m[cpID]...)
	store.mu.Unlock()
	assert.NotEmpty(t, data2)
	assert.NotEqual(t, data1, data2, "checkpoint data should change because items are different")

	var seen []string
	var mu sync.Mutex
	loop3 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			mu.Lock()
			seen = append([]string{}, items...)
			mu.Unlock()
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items,
			}, nil
		},
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})
	loop3.Push("d")
	loop3.Run(ctx)
	exit3 := loop3.Wait()
	assert.NoError(t, exit3.ExitReason)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"a", "b", "c", "d"}, seen, "should see loop2's unhandled items (a,b,c from loop2's checkpoint) plus new d")
}

func TestTurnLoop_CheckpointHasRunnerStateButEmptyBytes(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "empty-runner-bytes"

	cp := &turnLoopCheckpoint[string]{
		HasRunnerState:   true,
		RunnerCheckpoint: nil,
		UnhandledItems:   []string{"x"},
	}
	data, err := marshalTurnLoopCheckpoint(cp)
	assert.NoError(t, err)
	store.m[cpID] = data

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Run(ctx)
	exit := loop.Wait()
	assert.Error(t, exit.ExitReason)
	assert.Contains(t, exit.ExitReason.Error(), "has runner state but bytes are empty")
}

func TestTurnLoop_GenResumeReturnsError(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "resume-err-session"
	modelStarted := make(chan struct{}, 1)

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop1 := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})
	loop1.Push("msg1")
	<-modelStarted
	loop1.Stop(WithImmediate())
	loop1.Wait()

	genResumeErr := fmt.Errorf("resume callback failed")
	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		GenResume: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], canceled, unhandled, newItems []string) (*GenResumeResult[string, *schema.Message], error) {
			return nil, genResumeErr
		},
		PrepareAgent: prepareTestAgent,
	})
	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.Error(t, exit2.ExitReason)
	assert.ErrorIs(t, exit2.ExitReason, genResumeErr)
}

func TestTurnLoop_ResumeWaitsForInFlightPushBeforePlanning(t *testing.T) {
	ctx := context.Background()
	resumeErr := errors.New("stop after observing resume inputs")
	strategyEntered := make(chan struct{})
	allowStrategy := make(chan struct{})
	pushDone := make(chan struct{})
	genResumeCalled := make(chan struct{})

	var resumeNewItems []string

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAll,
		GenResume: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], interruptedItems, unhandledItems, newItems []string) (*GenResumeResult[string, *schema.Message], error) {
			resumeNewItems = append([]string{}, newItems...)
			close(genResumeCalled)
			return nil, resumeErr
		},
		PrepareAgent: prepareTestAgent,
	})
	loop.pendingResume = &turnLoopPendingResume[string]{
		interrupted: []string{"interrupted"},
		newItems:    []string{"pre-existing"},
	}

	go func() {
		defer close(pushDone)
		ok, ack := loop.Push("during-resume", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
			close(strategyEntered)
			<-allowStrategy
			return nil
		}))
		assert.True(t, ok)
		assert.Nil(t, ack)
	}()

	waitOrFail(t, strategyEntered, "strategy did not enter")

	loop.Run(ctx)

	select {
	case <-genResumeCalled:
		t.Fatal("GenResume should wait for in-flight PushStrategy to finish")
	default:
	}

	close(allowStrategy)
	waitOrFail(t, pushDone, "push did not finish")

	exit := loop.Wait()
	assert.ErrorIs(t, exit.ExitReason, resumeErr)
	assert.Equal(t, []string{"pre-existing", "during-resume"}, resumeNewItems)
}

func TestTurnLoop_CheckpointSaveError_MergesWithExistingError(t *testing.T) {
	ctx := context.Background()
	modelStarted := make(chan struct{}, 1)
	saveStore := &errorCheckpointStore{setErr: fmt.Errorf("disk full")}
	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        saveStore,
		CheckpointID: "cp-merge-err",
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})
	loop.Push("msg1")
	<-modelStarted
	loop.Stop(WithImmediate())
	exit := loop.Wait()
	assert.Error(t, exit.ExitReason)
	var ce *CancelError
	assert.True(t, errors.As(exit.ExitReason, &ce), "ExitReason should be CancelError, not merged with checkpoint error")
	assert.True(t, exit.CheckpointAttempted)
	assert.Error(t, exit.CheckpointErr)
	assert.Contains(t, exit.CheckpointErr.Error(), "disk full")
}

func TestTurnLoop_ResumeWithParams(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "resume-params-session"
	modelStarted := make(chan struct{}, 1)

	slowModel := &cancelTestChatModel{
		delayNs: int64(500 * time.Millisecond),
		response: &schema.Message{
			Role:    schema.Assistant,
			Content: "Hello",
		},
		startedChan: modelStarted,
		doneChan:    make(chan struct{}, 1),
	}
	agent, err := NewChatModelAgent(ctx, &ChatModelAgentConfig{
		Name:        "TestAgent",
		Description: "Test agent",
		Instruction: "You are a test assistant",
		Model:       slowModel,
	})
	assert.NoError(t, err)

	loop1 := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareAgent(agent),
	})
	loop1.Push("msg1")
	<-modelStarted
	loop1.Stop(WithImmediate())
	exit1 := loop1.Wait()
	var ce *CancelError
	assert.True(t, errors.As(exit1.ExitReason, &ce))

	var resumeParamsUsed *ResumeParams
	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		GenResume: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], canceled, unhandled, newItems []string) (*GenResumeResult[string, *schema.Message], error) {
			params := &ResumeParams{
				Targets: map[string]any{"some-address": "user-data"},
			}
			resumeParamsUsed = params
			return &GenResumeResult[string, *schema.Message]{
				ResumeParams: params,
				Consumed:     append(append(canceled, unhandled...), newItems...),
			}, nil
		},
		PrepareAgent: prepareAgent(agent),
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	})
	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.NotNil(t, resumeParamsUsed, "GenResume should have been called with ResumeParams")
	assert.Contains(t, resumeParamsUsed.Targets, "some-address")
	_ = exit2
}

func TestTurnLoop_ResumeInterruptAgain_PreservesEnableStreamingCheckpoint(t *testing.T) {
	for _, enableStreaming := range []bool{true, false} {
		t.Run(fmt.Sprintf("enable_streaming_%t", enableStreaming), func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore()
			cpID := fmt.Sprintf("streaming-resume-%t", enableStreaming)
			originalMessage := "msg1"

			firstAgent := &myAgent{
				runFn: func(ctx context.Context, input *AgentInput, _ ...AgentRunOption) *AsyncIterator[*AgentEvent] {
					assert.Equal(t, enableStreaming, input.EnableStreaming)
					assert.Len(t, input.Messages, 1)
					assert.Equal(t, originalMessage, input.Messages[0].Content)

					iter, gen := NewAsyncIteratorPair[*AgentEvent]()
					go func() {
						defer gen.Close()
						gen.Send(Interrupt(ctx, "first_interrupt"))
					}()
					return iter
				},
			}

			loop1 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
				Store:        store,
				CheckpointID: cpID,
				GenInput: func(_ context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
					return &GenInputResult[string, *schema.Message]{
						Input: &AgentInput{
							Messages:        []Message{schema.UserMessage(items[0])},
							EnableStreaming: enableStreaming,
						},
						Consumed: items,
					}, nil
				},
				PrepareAgent: prepareAgent(firstAgent),
			})
			loop1.Push(originalMessage)
			loop1.Run(ctx)
			exit1 := loop1.Wait()
			require.ErrorAs(t, exit1.ExitReason, new(*InterruptError))
			require.NoError(t, exit1.CheckpointErr)

			secondAgent := &myAgent{
				resumeFn: func(ctx context.Context, info *ResumeInfo, _ ...AgentRunOption) *AsyncIterator[*AgentEvent] {
					assert.Equal(t, enableStreaming, info.EnableStreaming)

					iter, gen := NewAsyncIteratorPair[*AgentEvent]()
					go func() {
						defer gen.Close()
						gen.Send(Interrupt(ctx, "second_interrupt"))
					}()
					return iter
				},
			}

			loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
				Store:        store,
				CheckpointID: cpID,
				GenInput:     genInputConsumeAll,
				GenResume: func(_ context.Context, _ *TurnLoop[string, *schema.Message], interrupted, unhandled, newItems []string) (*GenResumeResult[string, *schema.Message], error) {
					return &GenResumeResult[string, *schema.Message]{
						Consumed:  interrupted,
						Remaining: append(append([]string{}, unhandled...), newItems...),
					}, nil
				},
				PrepareAgent: prepareAgent(secondAgent),
			})
			loop2.Run(ctx)
			exit2 := loop2.Wait()
			require.ErrorAs(t, exit2.ExitReason, new(*InterruptError))
			require.NoError(t, exit2.CheckpointErr)

			// Verify the runner-level checkpoint persisted by the second interrupt
			// still encodes the original streaming mode. This is the invariant the PR
			// fixes: even though loop2's TypedRunner was constructed with the
			// resume-path placeholder (false), runner uses resumeInfo.EnableStreaming
			// from the previous checkpoint when re-saving.
			//
			// 验证第二次中断持久化的 runner 级 checkpoint 仍然编码了原始流式模式。
			// 这是该 PR 修复的不变量：即使 loop2 的 TypedRunner 是用 resume 路径占位值（false）构造的，runner 在重新保存时也会使用上一个 checkpoint 中的 resumeInfo.EnableStreaming。
			store.mu.Lock()
			data, ok := store.m[cpID]
			store.mu.Unlock()
			require.True(t, ok)
			cp, err := unmarshalTurnLoopCheckpoint[string](data)
			require.NoError(t, err)
			require.True(t, cp.HasRunnerState)
			_, _, info2, err := runnerLoadCheckPointImpl(newResumeBridgeStore(bridgeCheckpointID, cp.RunnerCheckpoint), context.Background(), bridgeCheckpointID)
			require.NoError(t, err)
			assert.Equal(t, enableStreaming, info2.EnableStreaming)
		})
	}
}

func TestTurnLoop_Stop_EscalatesCancelMode(t *testing.T) {
	ctx := context.Background()
	agentStarted := make(chan *cancelContext, 1)
	probe := &turnLoopStopModeProbeAgent{ccCh: agentStarted}
	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return probe, nil
		},
	})

	loop.Push("msg1")
	cc := <-agentStarted

	loop.Stop(WithGracefulTimeout(10 * time.Second))
	loop.Stop(WithImmediate())

	deadline := time.After(1 * time.Second)
	for {
		if cc.getMode() == CancelImmediate {
			break
		}
		select {
		case <-deadline:
			t.Fatal("cancel mode did not escalate to CancelImmediate")
		default:
		}
		time.Sleep(1 * time.Millisecond)
	}

	exit := loop.Wait()
	var ce *CancelError
	require.True(t, errors.As(exit.ExitReason, &ce))
	assert.Equal(t, CancelImmediate, ce.Info.Mode)
}

func TestTurnLoop_DefaultOnAgentEvents_ErrorPropagation(t *testing.T) {
	agentErr := errors.New("agent execution error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					return nil, agentErr
				},
			}, nil
		},
		// No OnAgentEvents — use default handler
		// 没有 OnAgentEvents —— 使用默认处理器
	})

	loop.Push("msg1")

	result := loop.Wait()
	// The default handler should propagate the agent error as ExitReason
	// 默认处理器应将智能体错误作为 ExitReason 传播
	assert.Error(t, result.ExitReason)
}

func TestTurnLoop_OnAgentEventsError(t *testing.T) {
	handlerErr := errors.New("event handler error")

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			// Drain events then return error
			// 耗尽 events 后返回错误
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			return handlerErr
		},
	})

	loop.Push("msg1")

	result := loop.Wait()
	assert.ErrorIs(t, result.ExitReason, handlerErr)
}

func TestTurnLoop_StopCallFromGenInput(t *testing.T) {
	// Test that calling Stop() from within GenInput works correctly
	// 测试在 GenInput 内调用 Stop() 能正确工作
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			loop.Stop()
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")

	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
}

func TestTurnLoop_PushFromOnAgentEvents(t *testing.T) {
	// Test that calling Push() from within OnAgentEvents works
	// 测试在 OnAgentEvents 内调用 Push() 能工作
	pushCount := int32(0)

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeFirst,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			count := atomic.AddInt32(&pushCount, 1)
			if count == 1 {
				// Push a follow-up item from the callback
				// 从回调中 Push 一个后续 item
				_, _ = tc.Loop.Push("follow-up")
			} else {
				tc.Loop.Stop()
			}
			return nil
		},
	})

	loop.Push("initial")

	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.Equal(t, int32(2), atomic.LoadInt32(&pushCount))
}

// Tests for NewTurnLoop: the permissive API where Push, Stop, and Wait are
// all valid on a not-yet-running loop.
//
// NewTurnLoop 的测试：宽松 API，允许在尚未运行的 loop 上调用 Push、Stop 和 Wait。

func TestNewTurnLoop_PushBeforeRun(t *testing.T) {
	// Items pushed before Run are buffered and processed after Run starts.
	// Run 启动前 Push 的 items 会被缓冲，并在 Run 启动后处理。
	var processedItems []string
	var mu sync.Mutex

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			mu.Lock()
			processedItems = append(processedItems, items...)
			mu.Unlock()
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items,
			}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	// Push before Run — items should be buffered.
	// 在 Run 之前 Push——条目应被缓冲。
	ok, _ := loop.Push("msg1")
	assert.True(t, ok)
	ok, _ = loop.Push("msg2")
	assert.True(t, ok)

	loop.Run(context.Background())

	time.Sleep(100 * time.Millisecond)

	loop.Stop()
	result := loop.Wait()

	mu.Lock()
	defer mu.Unlock()

	assert.NoError(t, result.ExitReason)
	assert.Contains(t, processedItems, "msg1")
	assert.Contains(t, processedItems, "msg2")
}

func TestNewTurnLoop_WaitBeforeRun(t *testing.T) {
	// Wait blocks until Run is called AND the loop exits.
	// Wait 会阻塞，直到调用 Run 且循环退出。
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	waitDone := make(chan *TurnLoopExitState[string, *schema.Message], 1)
	go func() {
		waitDone <- loop.Wait()
	}()

	// Wait should not return yet since Run hasn't been called.
	// 由于尚未调用 Run，Wait 还不应返回。
	select {
	case <-waitDone:
		t.Fatal("Wait returned before Run was called")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	loop.Push("msg1")
	loop.Stop()
	loop.Run(context.Background())

	select {
	case result := <-waitDone:
		assert.NoError(t, result.ExitReason)
		assert.Equal(t, []string{"msg1"}, result.UnhandledItems)
	case <-time.After(1 * time.Second):
		t.Fatal("Wait did not return after Run + Stop")
	}
}

func TestNewTurnLoop_RunIsIdempotent(t *testing.T) {
	var genInputCalls int32

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			atomic.AddInt32(&genInputCalls, 1)
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("msg1")
	loop.Run(context.Background())
	loop.Run(context.Background())
	loop.Run(context.Background())

	time.Sleep(100 * time.Millisecond)

	loop.Stop()
	result := loop.Wait()

	assert.NoError(t, result.ExitReason)
	assert.True(t, atomic.LoadInt32(&genInputCalls) >= 1)
}

func TestNewTurnLoop_ConcurrentPushAndRun(t *testing.T) {
	// Concurrent Push and Run should not race.
	// 并发 Push 和 Run 不应产生竞态。
	for i := 0; i < 100; i++ {
		var count int32

		loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				atomic.AddInt32(&count, int32(len(items)))
				return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopMockAgent{name: "test"}, nil
			},
		})

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			_, _ = loop.Push("item")
		}()

		go func() {
			defer wg.Done()
			loop.Run(context.Background())
		}()

		wg.Wait()

		time.Sleep(50 * time.Millisecond)

		loop.Stop()
		result := loop.Wait()
		assert.NoError(t, result.ExitReason)

		processed := atomic.LoadInt32(&count)
		unhandled := len(result.UnhandledItems)
		assert.True(t, int(processed)+unhandled <= 1,
			"total should not exceed pushed amount")
	}
}

type turnCtxKey struct{}

func TestTurnLoop_RunCtx_Propagation(t *testing.T) {
	// Verify that GenInputResult.RunCtx is propagated to PrepareAgent,
	// the agent run, and OnAgentEvents.
	//
	// 验证 GenInputResult.RunCtx 会传递给 PrepareAgent、智能体运行过程和 OnAgentEvents。

	const traceVal = "trace-123"
	var prepareCtxVal, agentCtxVal, eventsCtxVal string

	cfg := TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			// Derive a new context with per-item trace data
			// 派生一个带有每项 trace 数据的新 context
			runCtx := context.WithValue(ctx, turnCtxKey{}, traceVal)
			return &GenInputResult[string, *schema.Message]{
				RunCtx:   runCtx,
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items,
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			if v, ok := ctx.Value(turnCtxKey{}).(string); ok {
				prepareCtxVal = v
			}
			return &turnLoopMockAgent{
				name: "trace-agent",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					if v, ok := ctx.Value(turnCtxKey{}).(string); ok {
						agentCtxVal = v
					}
					return &AgentOutput{}, nil
				},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			if v, ok := ctx.Value(turnCtxKey{}).(string); ok {
				eventsCtxVal = v
			}
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			tc.Loop.Stop()
			return nil
		},
	}

	loop := NewTurnLoop(cfg)
	loop.Push("hello")
	loop.Run(context.Background())
	result := loop.Wait()

	assert.Nil(t, result.ExitReason)
	assert.Equal(t, traceVal, prepareCtxVal, "PrepareAgent should receive RunCtx")
	assert.Equal(t, traceVal, agentCtxVal, "Agent run should receive RunCtx")
	assert.Equal(t, traceVal, eventsCtxVal, "OnAgentEvents should receive RunCtx")
}

func TestTurnLoop_TurnContext_PreemptedChannel(t *testing.T) {
	preemptedSeen := make(chan struct{})
	agentStarted := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "slow",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			close(agentStarted)
			select {
			case <-tc.Preempted:
				close(preemptedSeen)
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for Preempted channel")
			}
			// Drain events
			// 排空事件
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			return nil
		},
	})

	loop.Push("msg1")
	<-agentStarted
	loop.Push("msg2", WithPreemptTimeout[string, *schema.Message](AnySafePoint, time.Millisecond))

	select {
	case <-preemptedSeen:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("preempted channel was never observed in OnAgentEvents")
	}

	loop.Stop()
	loop.Wait()
}

// =============================================================================
// preemptController unit tests
// =============================================================================
//
// =============================================================================
// preemptController 单元测试
// =============================================================================

func requireAckClosed(t *testing.T, ack <-chan struct{}) {
	t.Helper()
	waitOrFail(t, ack, "ack should be closed")
}

func requireAckOpen(t *testing.T, ack <-chan struct{}) {
	t.Helper()
	select {
	case <-ack:
		t.Fatal("ack should still be open")
	default:
	}
}

func requirePreemptPhase(t *testing.T, c *preemptController, phase preemptTurnPhase) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.turnPhase == phase
	}, time.Second, time.Millisecond)
}

func TestPreemptController_BeginPushSnapshotsPlanningTurn(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()

	snapshot := c.beginPush()
	c.endPush()

	assert.True(t, snapshot.hasTargetTurn)
	assert.NotZero(t, snapshot.turnID)
	assert.Nil(t, snapshot.ctx)
	assert.Nil(t, snapshot.tc)
}

type testContextKey struct{}

func TestPreemptController_BeginPushSnapshotsActiveTurn(t *testing.T) {
	c := newPreemptController()
	ctx := context.WithValue(context.Background(), testContextKey{}, "value")
	tc := "turn-context"

	c.beginPlanningTurn()
	c.beginActiveTurn(ctx, tc)
	snapshot := c.beginPush()
	c.endPush()

	assert.True(t, snapshot.hasTargetTurn)
	assert.NotZero(t, snapshot.turnID)
	assert.Equal(t, ctx, snapshot.ctx)
	assert.Equal(t, tc, snapshot.tc)
}

func TestPreemptController_RequestPreemptIdleTurnAcksImmediately(t *testing.T) {
	c := newPreemptController()
	snapshot := c.beginPush()
	c.endPush()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	requireAckClosed(t, ack)
	_, ok := c.receivePreempt()
	assert.False(t, ok)
}

func TestPreemptController_RejectsInvalidTurnPhaseTransitions(t *testing.T) {
	c := newPreemptController()

	assert.PanicsWithValue(t, "adk: preemptController.beginActiveTurn called while turn phase is idle; expected planning", func() {
		c.beginActiveTurn(context.Background(), "tc")
	})
	assert.PanicsWithValue(t, "adk: preemptController.abortPlanningTurn called while turn phase is idle; expected planning", func() {
		c.abortPlanningTurn()
	})
	assert.PanicsWithValue(t, "adk: preemptController.endActiveTurn called while turn phase is idle; expected active", func() {
		c.endActiveTurn()
	})

	c.beginPlanningTurn()
	assert.PanicsWithValue(t, "adk: preemptController.beginPlanningTurn called while turn phase is planning; expected idle", func() {
		c.beginPlanningTurn()
	})
	c.abortPlanningTurn()

	c.pending = newPreemptRequest(nil, nil, time.Now())
	assert.PanicsWithValue(t, "adk: preemptController.beginPlanningTurn called with stale pending preempt request", func() {
		c.beginPlanningTurn()
	})
}

func TestPreemptController_RequestPreemptForPlanningTurnIsConsumedAfterActivation(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	snapshot := c.beginPush()
	c.endPush()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	_, ok := c.receivePreempt()
	assert.False(t, ok)
	requireAckOpen(t, ack)

	c.beginActiveTurn(context.Background(), "tc")
	req, ok := c.receivePreempt()
	require.True(t, ok)
	requireAckOpen(t, ack)

	req.ack()
	requireAckClosed(t, ack)
}

func TestPreemptController_RequestPreemptForActiveTurnIsConsumedOnce(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	req, ok := c.receivePreempt()
	require.True(t, ok)
	opts := req.cancelOptions(time.Now())
	cfg := parseAgentCancelOptions(opts...)
	assert.Equal(t, CancelAfterChatModel, cfg.Mode)
	requireAckOpen(t, ack)

	_, ok = c.receivePreempt()
	assert.False(t, ok)

	req.ack()
	requireAckClosed(t, ack)
}

func TestPreemptController_AbortPlanningTurnAcksUnconsumedRequest(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	snapshot := c.beginPush()
	c.endPush()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	req := c.abortPlanningTurn()
	require.NotNil(t, req)
	req.ack()
	requireAckClosed(t, ack)

	_, ok := c.receivePreempt()
	assert.False(t, ok)
}

func TestPreemptController_EndActiveTurnAcksUnconsumedRequest(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	req := c.endActiveTurn()
	require.NotNil(t, req)
	req.ack()
	requireAckClosed(t, ack)

	_, ok := c.receivePreempt()
	assert.False(t, ok)
}

func TestPreemptController_EndActiveTurnDoesNotAckConsumedRequest(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))
	req, ok := c.receivePreempt()
	require.True(t, ok)

	assert.Nil(t, c.endActiveTurn())
	requireAckOpen(t, ack)

	req.ack()
	requireAckClosed(t, ack)
}

func TestPreemptController_TargetTurnMismatchAcksImmediately(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	snapshot := c.beginPush()
	c.endPush()
	c.abortPlanningTurn()
	c.beginPlanningTurn()

	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	requireAckClosed(t, ack)
	_, ok := c.receivePreempt()
	assert.False(t, ok)
}

func TestPreemptController_WaitForPushesBlocksUntilPushEnds(t *testing.T) {
	c := newPreemptController()
	c.beginPush()

	waitDone := make(chan struct{})
	go func() {
		c.waitForPushes()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("waitForPushes should block while a push is in flight")
	default:
	}

	c.endPush()
	waitOrFail(t, waitDone, "waitForPushes should unblock after endPush")
}

func TestPreemptController_MultipleRequestsBeforeReceiveMergeAcksAndUseMergedOpts(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	ack1 := make(chan struct{})
	ack2 := make(chan struct{})
	c.requestPreempt(snapshot, ack1, WithAgentCancelMode(CancelAfterChatModel), WithAgentCancelTimeout(time.Minute))
	c.requestPreempt(snapshot, ack2, WithAgentCancelMode(CancelAfterToolCalls), WithRecursive(), WithAgentCancelTimeout(time.Second))

	req, ok := c.receivePreempt()
	require.True(t, ok)
	opts := req.cancelOptions(time.Now())
	cfg := parseAgentCancelOptions(opts...)
	assert.Equal(t, CancelAfterChatModel|CancelAfterToolCalls, cfg.Mode)
	assert.True(t, cfg.Recursive)
	require.NotNil(t, cfg.Timeout)
	assert.LessOrEqual(t, *cfg.Timeout, time.Second)

	requireAckOpen(t, ack1)
	requireAckOpen(t, ack2)
	req.ack()
	requireAckClosed(t, ack1)
	requireAckClosed(t, ack2)
}

func TestPreemptController_ConcurrentPreemptRequestsMergeAndAck(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	const requestCount = 50
	start := make(chan struct{})
	acks := make([]chan struct{}, requestCount)
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		acks[i] = make(chan struct{})
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c.requestPreempt(snapshot, acks[i], WithAgentCancelMode(CancelAfterChatModel))
		}(i)
	}

	close(start)
	wg.Wait()

	req, ok := c.receivePreempt()
	require.True(t, ok)
	req.ack()
	for _, ack := range acks {
		requireAckClosed(t, ack)
	}
}

func TestPreemptController_CloseForLoopExitDuringDelayedPreempt(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")

	// Capture a snapshot while the turn is active.
	// 在 turn 活跃期间捕获快照。
	snapshot := c.beginPush()
	c.endPush()

	// closeForLoopExit tears down controller state during TurnLoop cleanup.
	// closeForLoopExit 会在 TurnLoop 清理期间拆除控制器状态。
	c.closeForLoopExit()

	// A delayed goroutine fires requestPreempt AFTER closeForLoopExit.
	// This must not panic or deadlock; ack should be closed immediately
	// because the controller is now closed.
	//
	// 一个延迟的 goroutine 会在 closeForLoopExit 之后触发 requestPreempt。
	// 这不能 panic 或死锁；ack 应立即关闭，
	// 因为控制器现在已关闭。
	ack := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))
	}()

	waitOrFail(t, done, "requestPreempt after closeForLoopExit must not deadlock")
	requireAckClosed(t, ack)
}

func TestPreemptController_RequestPreemptAfterCloseForLoopExit(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	c.closeForLoopExit()

	// requestPreempt on a closed controller should close ack immediately.
	// 在已关闭的控制器上调用 requestPreempt 应立即关闭 ack。
	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterToolCalls))
	requireAckClosed(t, ack)

	// receivePreempt should return nothing.
	// receivePreempt 不应返回任何内容。
	_, ok := c.receivePreempt()
	assert.False(t, ok)
}

func TestPreemptController_ConcurrentBeginPushAndWaitForPushes(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")

	const pushCount = 100
	var wg sync.WaitGroup

	// Launch many goroutines doing beginPush / endPush concurrently.
	// 启动多个 goroutine 并发执行 beginPush / endPush。
	for i := 0; i < pushCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.beginPush()
			// Simulate some work.
			// 模拟一些工作。
			time.Sleep(time.Microsecond)
			c.endPush()
		}()
	}

	// Meanwhile, waitForPushes should eventually return once all are done.
	// 同时，waitForPushes 应在全部完成后最终返回。
	waitDone := make(chan struct{})
	go func() {
		c.waitForPushes()
		close(waitDone)
	}()

	wg.Wait() // all pushes complete
	// 所有 push 完成

	waitOrFail(t, waitDone, "waitForPushes deadlocked with concurrent beginPush/endPush")
}

func TestPreemptController_RequestPreemptWithNilAck(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	// requestPreempt with nil ack channel must not panic.
	// 使用 nil ack channel 调用 requestPreempt 不得 panic。
	assert.NotPanics(t, func() {
		c.requestPreempt(snapshot, nil, WithAgentCancelMode(CancelAfterChatModel))
	})

	// The request should still be stored and consumable.
	// 该请求仍应被存储并可被消费。
	req, ok := c.receivePreempt()
	require.True(t, ok)
	// ack() with nil channels in the list must not panic.
	// 列表中有 nil channel 时，ack() 不得 panic。
	assert.NotPanics(t, func() {
		req.ack()
	})
}

func TestPreemptController_MergeImmediateOverridesTimeout(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc")
	snapshot := c.beginPush()
	c.endPush()

	// First request: graceful with timeout.
	// 第一次请求：带超时的 graceful。
	ack1 := make(chan struct{})
	c.requestPreempt(snapshot, ack1,
		WithAgentCancelMode(CancelAfterToolCalls),
		WithAgentCancelTimeout(10*time.Second))

	// Second request: CancelImmediate (no timeout).
	// 第二次请求：CancelImmediate（无超时）。
	ack2 := make(chan struct{})
	c.requestPreempt(snapshot, ack2, WithAgentCancelMode(CancelImmediate))

	req, ok := c.receivePreempt()
	require.True(t, ok)

	opts := req.cancelOptions(time.Now())
	cfg := parseAgentCancelOptions(opts...)

	// CancelImmediate should win and timeout should be nil.
	// CancelImmediate 应胜出，且 timeout 应为 nil。
	assert.Equal(t, CancelImmediate, cfg.Mode)
	assert.Nil(t, cfg.Timeout, "CancelImmediate merge should clear timeout")

	req.ack()
	requireAckClosed(t, ack1)
	requireAckClosed(t, ack2)
}

func TestPreemptController_DelayedPreemptTargetGoneBetweenTurns(t *testing.T) {
	c := newPreemptController()

	// Turn 1: planning → active → end
	// Turn 1：planning → active → end
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc1")
	oldSnapshot := c.beginPush()
	c.endPush()
	req := c.endActiveTurn()
	assert.Nil(t, req) // no pending request
	// 无待处理请求

	// Turn 2: start a new turn
	// Turn 2：开始新 turn
	c.beginPlanningTurn()
	c.beginActiveTurn(context.Background(), "tc2")

	// A delayed preempt from Turn 1 fires with stale snapshot.
	// It should resolve as no-op (ack immediately) because turnID doesn't match.
	//
	// 来自 Turn 1 的延迟 preempt 使用过期 snapshot 触发。
	// 它应解析为 no-op（立即 ack），因为 turnID 不匹配。
	ack := make(chan struct{})
	c.requestPreempt(oldSnapshot, ack, WithAgentCancelMode(CancelAfterChatModel))
	requireAckClosed(t, ack)

	// The new turn should have no pending preempt.
	// 新 turn 不应有待处理的 preempt。
	_, ok := c.receivePreempt()
	assert.False(t, ok, "stale preempt must not affect new turn")

	c.endActiveTurn()
}

func TestPreemptController_EndPushWithoutBeginPushPanics(t *testing.T) {
	c := newPreemptController()

	// endPush without a matching beginPush should panic with the new invariant.
	// 没有匹配 beginPush 的 endPush 应按新不变量 panic。
	assert.PanicsWithValue(t,
		"adk: preemptController.endPush called without matching beginPush",
		func() {
			c.endPush()
		},
	)
}

func TestPreemptController_BeginActiveTurnNotifiesExistingPending(t *testing.T) {
	c := newPreemptController()
	c.beginPlanningTurn()
	snapshot := c.beginPush()
	c.endPush()

	// Send a preempt during planning phase.
	// 在 planning 阶段发送 preempt。
	ack := make(chan struct{})
	c.requestPreempt(snapshot, ack, WithAgentCancelMode(CancelAfterChatModel))

	// During planning, receivePreempt returns nothing.
	// planning 期间，receivePreempt 不返回任何内容。
	_, ok := c.receivePreempt()
	assert.False(t, ok)

	// beginActiveTurn should notify the watcher via the notify channel.
	// beginActiveTurn 应通过 notify channel 通知 watcher。
	c.beginActiveTurn(context.Background(), "tc")

	// The notify channel should have a message.
	// notify channel 应有一条消息。
	select {
	case <-c.notify:
		// Expected: watcher notification was sent.
		// 期望：已发送 watcher 通知。
	case <-time.After(1 * time.Second):
		t.Fatal("beginActiveTurn should notify watcher when there is a pending request")
	}

	// Now receivePreempt should return the pending request.
	// 现在 receivePreempt 应返回待处理请求。
	req, ok := c.receivePreempt()
	require.True(t, ok)
	req.ack()
	requireAckClosed(t, ack)
}

// =============================================================================
// Integration tests for race-prone preempt scenarios
// =============================================================================
//
// =============================================================================
// 易发生竞态的 preempt 场景集成测试
// =============================================================================

func TestTurnLoop_ConcurrentPreemptsDuringTurn(t *testing.T) {
	agentStarted := make(chan struct{})
	agentStartedOnce := sync.Once{}

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			agentStartedOnce.Do(func() {
				close(agentStarted)
			})
			<-ctx.Done()
			return &AgentOutput{}, nil
		},
	}

	var genInputCount int32

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			atomic.AddInt32(&genInputCount, 1)
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{},
				Consumed: items,
			}, nil
		},
	})

	loop.Push("seed")

	waitOrFail(t, agentStarted, "agent did not start")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, ack := loop.Push(fmt.Sprintf("urgent-%d", i), WithPreemptTimeout[string, *schema.Message](AnySafePoint, 10*time.Millisecond))
			if ok && ack != nil {
				select {
				case <-ack:
				case <-time.After(5 * time.Second):
					t.Error("ack channel not closed within timeout")
				}
			}
		}(i)
	}

	// Stop the loop concurrently. The run loop may be blocked on
	// buffer.Receive after processing all preempts; Stop unblocks it
	// and triggers closeForLoopExit which closes any orphaned ack channels.
	//
	// 并发停止 loop。处理完所有 preempt 后，run loop 可能阻塞在 buffer.Receive；Stop 会解除阻塞，并触发 closeForLoopExit 关闭所有孤立的 ack channel。
	go func() {
		time.Sleep(500 * time.Millisecond)
		loop.Stop(WithImmediate())
	}()

	wg.Wait()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.True(t, atomic.LoadInt32(&genInputCount) >= 2, "should have had at least the initial turn + one preempted turn")
}

func TestTurnLoop_PreemptBetweenTurnsAcksImmediately(t *testing.T) {
	var cancelCount int32
	var turnCount int32
	firstTurnDone := make(chan struct{})
	secondTurnDone := make(chan struct{})
	firstTurnOnce := sync.Once{}
	secondTurnOnce := sync.Once{}

	agent := &turnLoopCancellableMockAgent{
		name: "fast",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			return &AgentOutput{}, nil
		},
		onCancel: func(cc *cancelContext) {
			atomic.AddInt32(&cancelCount, 1)
		},
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			atomic.AddInt32(&turnCount, 1)
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{},
				Consumed: items,
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			switch atomic.LoadInt32(&turnCount) {
			case 1:
				firstTurnOnce.Do(func() { close(firstTurnDone) })
			case 2:
				secondTurnOnce.Do(func() { close(secondTurnDone) })
			}
			return nil
		},
	})

	loop.Push("first")
	waitOrFail(t, firstTurnDone, "first turn did not complete")
	requirePreemptPhase(t, loop.preemptCtrl, preemptTurnIdle)

	ok, ack := loop.Push("between-turns", WithPreempt[string, *schema.Message](AnySafePoint))
	require.True(t, ok)
	require.NotNil(t, ack)
	requireAckClosed(t, ack)
	assert.Equal(t, int32(0), atomic.LoadInt32(&cancelCount), "between-turn preempt must not submit cancel")

	waitOrFail(t, secondTurnDone, "between-turn item was not processed")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.Equal(t, int32(2), atomic.LoadInt32(&turnCount))
}

func TestTurnLoop_PushStrategy_DuringTurnTransition(t *testing.T) {
	agentStarted := make(chan struct{})
	agentStartedOnce := sync.Once{}
	allowFinish := make(chan struct{})
	strategyEntered := make(chan struct{})

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			agentStartedOnce.Do(func() {
				close(agentStarted)
			})
			select {
			case <-allowFinish:
				return &AgentOutput{}, nil
			case <-ctx.Done():
				return &AgentOutput{}, nil
			}
		},
	}

	var genInputCount int32
	secondTurnDone := make(chan struct{})
	secondTurnOnce := sync.Once{}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			count := atomic.AddInt32(&genInputCount, 1)
			if count >= 2 {
				secondTurnOnce.Do(func() {
					close(secondTurnDone)
				})
			}
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{},
				Consumed: items,
			}, nil
		},
	})

	loop.Push("first")

	waitOrFail(t, agentStarted, "agent did not start")

	strategyBlocker := make(chan struct{})
	var strategyTCNotNil int32

	go func() {
		loop.Push("strategic-item", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
			if tc != nil {
				atomic.StoreInt32(&strategyTCNotNil, 1)
			}
			close(strategyEntered)
			<-strategyBlocker
			return []PushOption[string, *schema.Message]{WithPreempt[string, *schema.Message](AnySafePoint)}
		}))
	}()

	waitOrFail(t, strategyEntered, "strategy did not enter")

	close(allowFinish)

	select {
	case <-secondTurnDone:
		t.Fatal("second turn should not be planned before strategy Push finishes")
	default:
	}

	close(strategyBlocker)

	waitOrFail(t, secondTurnDone, "second turn should eventually run after strategy resolves")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.True(t, atomic.LoadInt32(&genInputCount) >= 2)
	assert.Equal(t, int32(1), atomic.LoadInt32(&strategyTCNotNil))
}

func TestTurnLoop_ConcurrentPreemptAndStop(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		t.Run(fmt.Sprintf("iter_%d", iter), func(t *testing.T) {
			ctx := context.Background()

			agentStarted := make(chan struct{})
			agentStartedOnce := sync.Once{}

			agent := &turnLoopCancellableMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					agentStartedOnce.Do(func() {
						close(agentStarted)
					})
					<-ctx.Done()
					return &AgentOutput{}, nil
				},
			}

			loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
				PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
					return agent, nil
				},
				GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
					return &GenInputResult[string, *schema.Message]{
						Input:    &AgentInput{},
						Consumed: items,
					}, nil
				},
			})

			loop.Push("seed")

			select {
			case <-agentStarted:
			case <-time.After(1 * time.Second):
				t.Fatal("agent did not start")
			}

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				_, ack := loop.Push("preempt-item", WithPreempt[string, *schema.Message](AnySafePoint))
				if ack != nil {
					<-ack
				}
			}()

			go func() {
				defer wg.Done()
				loop.Stop(WithImmediate())
			}()

			wg.Wait()
			loop.Wait()
		})
	}
}

func TestTurnLoop_ConcurrentPushStrategyAndStop(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		t.Run(fmt.Sprintf("iter_%d", iter), func(t *testing.T) {
			ctx := context.Background()

			agentStarted := make(chan struct{})
			agentStartedOnce := sync.Once{}

			agent := &turnLoopCancellableMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					agentStartedOnce.Do(func() {
						close(agentStarted)
					})
					<-ctx.Done()
					return &AgentOutput{}, nil
				},
			}

			loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
				PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
					return agent, nil
				},
				GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
					return &GenInputResult[string, *schema.Message]{
						Input:    &AgentInput{},
						Consumed: items,
					}, nil
				},
			})

			loop.Push("seed")

			select {
			case <-agentStarted:
			case <-time.After(1 * time.Second):
				t.Fatal("agent did not start")
			}

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				_, ack := loop.Push("strategic-item", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
					return []PushOption[string, *schema.Message]{WithPreempt[string, *schema.Message](AnySafePoint)}
				}))
				if ack != nil {
					<-ack
				}
			}()

			go func() {
				defer wg.Done()
				loop.Stop(WithImmediate())
			}()

			wg.Wait()
			loop.Wait()
		})
	}
}

func TestTurnLoop_TurnContext_StoppedChannel(t *testing.T) {
	stoppedSeen := make(chan struct{})
	agentStarted := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "slow",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			close(agentStarted)
			select {
			case <-tc.Stopped:
				close(stoppedSeen)
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for Stopped channel")
			}
			// Drain events
			// 清空 events
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			return nil
		},
	})

	loop.Push("msg1")
	<-agentStarted
	loop.Stop(WithImmediate())

	select {
	case <-stoppedSeen:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("stopped channel was never observed in OnAgentEvents")
	}

	loop.Wait()
}

func TestTurnLoop_TurnContext_BothPreemptedAndStopped(t *testing.T) {
	t.Run("PreemptThenStop_OnlyPreemptContributes", func(t *testing.T) {
		preemptedSeen := make(chan struct{})
		agentStarted := make(chan struct{})

		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopCancellableMockAgent{
					name: "slow",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					},
				}, nil
			},
			OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*TypedAgentEvent[*schema.Message]]) error {
				close(agentStarted)
				select {
				case <-tc.Preempted:
					close(preemptedSeen)
				case <-time.After(5 * time.Second):
					t.Error("timed out waiting for Preempted")
				}
				for {
					if _, ok := events.Next(); !ok {
						break
					}
				}
				return nil
			},
		})

		loop.Push("msg1")
		<-agentStarted
		loop.Push("msg2", WithPreemptTimeout[string, *schema.Message](AnySafePoint, time.Millisecond))

		select {
		case <-preemptedSeen:
		case <-time.After(5 * time.Second):
			t.Fatal("Preempted channel was never closed")
		}

		loop.Stop(WithImmediate())
		loop.Wait()
	})

	t.Run("StopThenPreempt_OnlyStopContributes", func(t *testing.T) {
		stoppedSeen := make(chan struct{})
		agentStarted := make(chan struct{})

		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopCancellableMockAgent{
					name: "slow",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					},
				}, nil
			},
			OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*TypedAgentEvent[*schema.Message]]) error {
				close(agentStarted)
				select {
				case <-tc.Stopped:
					close(stoppedSeen)
				case <-time.After(5 * time.Second):
					t.Error("timed out waiting for Stopped")
				}
				for {
					if _, ok := events.Next(); !ok {
						break
					}
				}
				return nil
			},
		})

		loop.Push("msg1")
		<-agentStarted
		loop.Stop(WithImmediate())

		select {
		case <-stoppedSeen:
		case <-time.After(5 * time.Second):
			t.Fatal("Stopped channel was never closed")
		}

		loop.Push("msg2", WithPreemptTimeout[string, *schema.Message](AnySafePoint, time.Millisecond))
		loop.Wait()
	})
}

func TestTurnLoop_PushStrategy_DuringTurn(t *testing.T) {
	agentStarted := make(chan struct{})
	agentStartedOnce := sync.Once{}
	agentCancelled := make(chan struct{})
	agentCancelledOnce := sync.Once{}

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			agentStartedOnce.Do(func() {
				close(agentStarted)
			})
			<-ctx.Done()
			agentCancelledOnce.Do(func() {
				close(agentCancelled)
			})
			return &AgentOutput{}, nil
		},
	}

	genInputCalls := int32(0)
	secondGenInputCalled := make(chan struct{})
	secondGenInputOnce := sync.Once{}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			count := atomic.AddInt32(&genInputCalls, 1)
			if count >= 2 {
				secondGenInputOnce.Do(func() {
					close(secondGenInputCalled)
				})
			}
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  []string{items[0]},
				Remaining: items[1:],
			}, nil
		},
	})

	loop.Push("first")

	waitOrFail(t, agentStarted, "agent did not start")

	// Strategy inspects TurnContext during a running turn and decides to preempt.
	// Strategy 在运行中的 turn 期间检查 TurnContext，并决定 preempt。
	var strategyCalled int32
	var strategyTC *TurnContext[string, *schema.Message]
	loop.Push("urgent", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
		atomic.AddInt32(&strategyCalled, 1)
		strategyTC = tc
		return []PushOption[string, *schema.Message]{WithPreempt[string, *schema.Message](AnySafePoint)}
	}))

	waitOrFail(t, agentCancelled, "agent was not cancelled by strategy-returned preempt")

	waitOrFail(t, secondGenInputCalled, "second GenInput was not called after preempt")

	loop.Stop(WithImmediate())
	loop.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&strategyCalled))
	assert.NotNil(t, strategyTC, "strategy should receive non-nil TurnContext during a turn")
	assert.Equal(t, []string{"first"}, strategyTC.Consumed)
}

func TestTurnLoop_PushStrategy_BetweenTurns(t *testing.T) {
	// Push with strategy before Run() — TurnContext should be nil.
	// 在 Run() 之前使用 strategy Push —— TurnContext 应为 nil。
	var strategyCalled int32
	var strategyTCWasNil bool

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			return &AgentOutput{}, nil
		},
	}

	agentDone := make(chan struct{})
	agentDoneOnce := sync.Once{}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  items,
				Remaining: nil,
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			agentDoneOnce.Do(func() {
				close(agentDone)
			})
			return nil
		},
	})

	// Push with strategy — no turn is active yet, so tc should be nil.
	// 使用策略推送——此时还没有活跃 turn，因此 tc 应为 nil。
	loop.Push("item", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
		atomic.AddInt32(&strategyCalled, 1)
		strategyTCWasNil = tc == nil
		return nil // plain push, no preempt
		// 普通推送，不 preempt
	}))

	waitOrFail(t, agentDone, "agent did not complete")

	loop.Stop()
	loop.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&strategyCalled))
	assert.True(t, strategyTCWasNil, "strategy should receive nil TurnContext between turns")
}

func TestTurnLoop_PushStrategy_OverridesOtherOptions(t *testing.T) {
	// Push with both WithPreempt and WithPushStrategy — only strategy's result applies.
	// 同时使用 WithPreempt 和 WithPushStrategy 推送——只应用策略的结果。
	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			return &AgentOutput{}, nil
		},
	}

	agentDone := make(chan struct{})
	agentDoneOnce := sync.Once{}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  items,
				Remaining: nil,
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			agentDoneOnce.Do(func() {
				close(agentDone)
			})
			return nil
		},
	})

	// Strategy returns nil (no preempt), even though WithPreempt is also passed.
	// The strategy should override — so the agent should NOT be preempted.
	//
	// 策略返回 nil（不 preempt），即使同时传入了 WithPreempt。
	// 策略应覆盖它——因此 agent 不应被 preempt。
	ok, ack := loop.Push("item", WithPreempt[string, *schema.Message](AnySafePoint), WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
		return nil // no preempt
		// 不 preempt
	}))
	assert.True(t, ok)
	assert.Nil(t, ack, "ack should be nil since strategy returned no preempt")

	waitOrFail(t, agentDone, "agent did not complete normally")

	loop.Stop()
	loop.Wait()
}

func TestTurnLoop_PushStrategy_NestedStrategyStripped(t *testing.T) {
	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			return &AgentOutput{}, nil
		},
	}

	agentDone := make(chan struct{})
	agentDoneOnce := sync.Once{}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  items,
				Remaining: nil,
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				_, ok := events.Next()
				if !ok {
					break
				}
			}
			agentDoneOnce.Do(func() {
				close(agentDone)
			})
			return nil
		},
	})

	// Strategy returns another WithPushStrategy — the nested one should be stripped.
	// 策略返回另一个 WithPushStrategy——嵌套的那个应被剥离。
	innerCalled := int32(0)
	ok, ack := loop.Push("item", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
		return []PushOption[string, *schema.Message]{
			WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
				atomic.AddInt32(&innerCalled, 1)
				return []PushOption[string, *schema.Message]{WithPreempt[string, *schema.Message](AnySafePoint)}
			}),
		}
	}))
	assert.True(t, ok)
	assert.Nil(t, ack, "ack should be nil since nested strategy was stripped (no preempt)")

	waitOrFail(t, agentDone, "agent did not complete normally")

	loop.Stop()
	loop.Wait()

	assert.Equal(t, int32(0), atomic.LoadInt32(&innerCalled), "nested strategy should not be called")
}

func TestTurnLoop_PushStrategy_ConsumedInspection(t *testing.T) {
	// Strategy preempts only when current turn is processing "low-priority" items.
	// 仅当当前 turn 正在处理 "low-priority" 项时，策略才 preempt。
	agentStarted := make(chan struct{})
	agentStartedOnce := sync.Once{}

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			agentStartedOnce.Do(func() {
				close(agentStarted)
			})
			<-ctx.Done()
			return &AgentOutput{}, nil
		},
	}

	genInputCalls := int32(0)
	secondGenInputItems := make(chan []string, 1)

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			count := atomic.AddInt32(&genInputCalls, 1)
			if count >= 2 {
				select {
				case secondGenInputItems <- append([]string{}, items...):
				default:
				}
			}
			return &GenInputResult[string, *schema.Message]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  []string{items[0]},
				Remaining: items[1:],
			}, nil
		},
	})

	loop.Push("low-priority-task")

	waitOrFail(t, agentStarted, "agent did not start")

	// Strategy checks Consumed and preempts because current turn has "low-priority" items.
	// 策略检查 Consumed，并因当前 turn 有 "low-priority" 项而 preempt。
	loop.Push("urgent-task", WithPushStrategy(func(ctx context.Context, tc *TurnContext[string, *schema.Message]) []PushOption[string, *schema.Message] {
		if tc != nil && len(tc.Consumed) > 0 && tc.Consumed[0] == "low-priority-task" {
			return []PushOption[string, *schema.Message]{WithPreempt[string, *schema.Message](AnySafePoint)}
		}
		return nil
	}))

	select {
	case items := <-secondGenInputItems:
		assert.Contains(t, items, "urgent-task")
	case <-time.After(2 * time.Second):
		t.Fatal("second GenInput was not called after strategy-driven preempt")
	}

	loop.Stop(WithImmediate())
	loop.Wait()
}

func TestTurnLoop_PushAfterStop_BufferedAsLateItems(t *testing.T) {
	ctx := context.Background()
	processed := make(chan string, 10)

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			processed <- tc.Consumed[0]
			return nil
		},
	})

	loop.Push("msg1")
	<-processed
	loop.Stop()
	result := loop.Wait()

	// Push after stop — should be buffered as late items
	// Stop 后推送——应作为 late items 缓冲
	ok1, _ := loop.Push("late1")
	ok2, _ := loop.Push("late2")
	ok3, _ := loop.Push("late3")
	assert.False(t, ok1)
	assert.False(t, ok2)
	assert.False(t, ok3)

	late := result.TakeLateItems()
	assert.Equal(t, []string{"late1", "late2", "late3"}, late)
}

func TestTurnLoop_TakeLateItems_Idempotent(t *testing.T) {
	ctx := context.Background()

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Stop()
	loop.Run(ctx)
	result := loop.Wait()

	loop.Push("late1")

	first := result.TakeLateItems()
	second := result.TakeLateItems()
	third := result.TakeLateItems()

	assert.Equal(t, []string{"late1"}, first)
	assert.Equal(t, first, second, "subsequent calls should return the same slice")
	assert.Equal(t, first, third, "subsequent calls should return the same slice")
}

func TestTurnLoop_PushAfterTakeLateItems_Panics(t *testing.T) {
	ctx := context.Background()

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Stop()
	loop.Run(ctx)
	result := loop.Wait()

	result.TakeLateItems()

	assert.PanicsWithValue(t, "TurnLoop: Push called after TakeLateItems", func() {
		loop.Push("too-late")
	})
}

func TestTurnLoop_TakeLateItems_NeverCalled_NoImpact(t *testing.T) {
	ctx := context.Background()

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Push("b")
	loop.Stop()
	loop.Run(ctx)
	result := loop.Wait()

	// Don't call TakeLateItems — verify UnhandledItems works normally
	// 不要调用 TakeLateItems——验证 UnhandledItems 正常工作
	assert.Contains(t, result.UnhandledItems, "b")
	assert.Nil(t, result.ExitReason)
}

func TestTurnLoop_CheckpointErr_SeparateFromExitReason(t *testing.T) {
	ctx := context.Background()
	saveStore := &errorCheckpointStore{setErr: fmt.Errorf("storage unavailable")}

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        saveStore,
		CheckpointID: "cp-separate-err",
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Stop()
	loop.Run(ctx)
	result := loop.Wait()

	// ExitReason should be nil (clean stop), checkpoint error should be separate
	// ExitReason 应为 nil（干净停止），checkpoint 错误应单独处理
	assert.Nil(t, result.ExitReason)
	assert.True(t, result.CheckpointAttempted)
	assert.Error(t, result.CheckpointErr)
	assert.Contains(t, result.CheckpointErr.Error(), "storage unavailable")
}

func TestTurnLoop_CheckpointAttempted_FalseWhenNoStore(t *testing.T) {
	ctx := context.Background()

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop.Push("a")
	loop.Stop()
	loop.Run(ctx)
	result := loop.Wait()

	assert.False(t, result.CheckpointAttempted)
	assert.Nil(t, result.CheckpointErr)
}

func TestTurnLoop_CheckpointAttempted_FalseOnErrorExit(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	genInputErr := errors.New("gen input failed")

	firstTurnDone := make(chan struct{})
	var callCount int32
	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: "cp-err-exit",
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			n := atomic.AddInt32(&callCount, 1)
			if n > 1 {
				return nil, genInputErr
			}
			return &GenInputResult[string, *schema.Message]{Input: &AgentInput{}, Consumed: items}, nil
		},
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			close(firstTurnDone)
			return nil
		},
	})
	loop.Push("msg1")
	<-firstTurnDone
	loop.Push("msg2")
	result := loop.Wait()

	// Loop exited from error, not Stop() — checkpoint should not be saved
	// 循环因错误退出，而不是 Stop()——不应保存 checkpoint
	assert.ErrorIs(t, result.ExitReason, genInputErr)
	assert.False(t, result.CheckpointAttempted)
	assert.Nil(t, result.CheckpointErr)
}

func TestTurnLoop_StopConcurrentWithCallbackError_NoCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "stop-concurrent-err"

	prepareErr := errors.New("prepare agent failed")
	firstTurnDone := make(chan struct{})
	stopCalled := make(chan struct{})
	var prepareCount int32

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			n := atomic.AddInt32(&prepareCount, 1)
			if n > 1 {
				// Wait until Stop() has been called so stopCtrl.isCommitted() is true.
				// 等待 Stop() 已被调用，使 stopCtrl.isCommitted() 为 true。
				<-stopCalled
				return nil, prepareErr
			}
			return &turnLoopMockAgent{name: "test"}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			close(firstTurnDone)
			return nil
		},
	})

	loop.Push("msg1")
	<-firstTurnDone
	loop.Push("msg2")

	// Call Stop() and signal PrepareAgent to proceed with error
	// 调用 Stop() 并通知 PrepareAgent 继续并返回错误
	go func() {
		loop.Stop()
		close(stopCalled)
	}()

	result := loop.Wait()

	// The loop may exit via Stop (clean) or via PrepareAgent error.
	// If it exited via PrepareAgent error with Stop also called:
	// checkpoint should NOT be saved.
	//
	// 循环可能通过 Stop（干净）退出，也可能通过 PrepareAgent 错误退出。
	// 如果它在 Stop 也被调用的情况下通过 PrepareAgent 错误退出：
	// 不应保存 checkpoint。
	if result.ExitReason != nil && !errors.As(result.ExitReason, new(*CancelError)) {
		assert.ErrorIs(t, result.ExitReason, prepareErr)
		assert.False(t, result.CheckpointAttempted, "should not checkpoint when exit is caused by callback error")
	}
	// If Stop won the race, that's fine — checkpoint may or may not be saved
	// depending on idle state. The test is about the error path.
	//
	// 如果 Stop 赢得竞争，也可以——checkpoint 是否保存取决于 idle 状态。
	// 该测试关注错误路径。
}

func TestTurnLoop_DeleteWithoutCheckPointDeleter_NoOp(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "no-deleter"

	// First loop: save a checkpoint
	// 第一次循环：保存一个 checkpoint
	loop1 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop1.Push("a")
	loop1.Stop()
	loop1.Run(ctx)
	loop1.Wait()

	store.mu.Lock()
	_, exists := store.m[cpID]
	store.mu.Unlock()
	assert.True(t, exists, "checkpoint should be saved")

	// Second loop: exit via context cancel — should try to delete but store
	// doesn't implement CheckPointDeleter, so checkpoint persists (no-op)
	//
	// 第二次循环：通过 context cancel 退出——应尝试删除，但 store
	// 未实现 CheckPointDeleter，因此 checkpoint 保留（no-op）
	ctx2, cancel2 := context.WithCancel(ctx)
	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			cancel2()
			return nil
		},
	})
	loop2.Push("b")
	loop2.Run(ctx2)
	loop2.Wait()

	// Without CheckPointDeleter, the stale checkpoint should NOT be deleted
	// 没有 CheckPointDeleter 时，不应删除过期 checkpoint
	store.mu.Lock()
	v, exists := store.m[cpID]
	store.mu.Unlock()
	assert.True(t, exists, "checkpoint should still exist without CheckPointDeleter")
	assert.NotNil(t, v, "checkpoint should not be set to nil")
}

func TestTurnLoop_StopWithSkipCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	cpID := "skip-cp-session"

	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("a")
	loop.Push("b")
	loop.Stop(WithSkipCheckpoint())
	loop.Run(ctx)

	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)
	assert.False(t, exit.CheckpointAttempted, "checkpoint should be skipped when WithSkipCheckpoint is used")

	store.mu.Lock()
	_, exists := store.m[cpID]
	store.mu.Unlock()
	assert.False(t, exists, "no checkpoint should be saved when WithSkipCheckpoint is used")
}

func TestTurnLoop_StopWithSkipCheckpoint_DeletesStaleCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := &deletableCheckpointStore{
		turnLoopCheckpointStore: turnLoopCheckpointStore{m: make(map[string][]byte)},
	}
	cpID := "skip-stale-session"

	loop1 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop1.Push("a")
	loop1.Stop()
	loop1.Run(ctx)
	exit1 := loop1.Wait()
	assert.True(t, exit1.CheckpointAttempted)

	store.mu.Lock()
	_, exists := store.m[cpID]
	store.mu.Unlock()
	assert.True(t, exists, "first loop should save checkpoint")

	loop2 := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})
	loop2.Push("b")
	loop2.Stop(WithSkipCheckpoint())
	loop2.Run(ctx)
	exit2 := loop2.Wait()
	assert.False(t, exit2.CheckpointAttempted, "second loop should skip checkpoint")

	store.mu.Lock()
	deleteCalled := store.deleteCalled
	store.mu.Unlock()
	assert.True(t, deleteCalled, "stale checkpoint should be deleted when SkipCheckpoint is used")
}

func TestTurnLoop_StopWithStopCause(t *testing.T) {
	ctx := context.Background()
	cause := "user session timeout"

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Push("a")
	loop.Stop(WithStopCause(cause))

	exit := loop.Wait()
	assert.Equal(t, cause, exit.StopCause)
}

func TestTurnLoop_StopCause_EmptyWhenNoStop(t *testing.T) {
	ctx := context.Background()

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAll,
		PrepareAgent: prepareTestAgent,
	})

	loop.Stop()
	exit := loop.Wait()
	assert.Empty(t, exit.StopCause)
}

func TestTurnLoop_StopCause_InTurnContext(t *testing.T) {
	cause := "business shutdown"
	gotCause := make(chan string, 1)
	agentStarted := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "slow",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			close(agentStarted)
			select {
			case <-tc.Stopped:
				gotCause <- tc.StopCause()
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for Stopped channel")
			}
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			return nil
		},
	})

	loop.Push("msg1")
	<-agentStarted
	loop.Stop(WithImmediate(), WithStopCause(cause))

	select {
	case c := <-gotCause:
		assert.Equal(t, cause, c)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for StopCause in TurnContext")
	}

	exit := loop.Wait()
	assert.Equal(t, cause, exit.StopCause)
}

func TestTurnLoop_StopCause_FirstNonEmptyWins(t *testing.T) {
	agentStarted := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "slow",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			close(agentStarted)
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			return nil
		},
	})

	loop.Push("msg1")
	<-agentStarted
	loop.Stop(WithGraceful(), WithStopCause("first cause"))
	loop.Stop(WithStopCause("second cause"))

	exit := loop.Wait()
	assert.Equal(t, "first cause", exit.StopCause, "first non-empty StopCause should win")
}

func TestTurnLoop_StopBeforeRun_PushThenStop(t *testing.T) {
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			t.Fatal("GenInput should not be called when Stop is called before Run")
			return nil, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			t.Fatal("PrepareAgent should not be called when Stop is called before Run")
			return nil, nil
		},
	})

	ok, _ := loop.Push("item1")
	assert.True(t, ok)
	ok, _ = loop.Push("item2")
	assert.True(t, ok)

	loop.Stop()
	loop.Run(context.Background())
	result := loop.Wait()

	assert.NoError(t, result.ExitReason)
	assert.Equal(t, []string{"item1", "item2"}, result.UnhandledItems)
	assert.Empty(t, result.InterruptedItems)
	assert.Empty(t, result.TakeLateItems())
}

func TestTurnLoop_StopBeforeRun_StopThenPush(t *testing.T) {
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			t.Fatal("GenInput should not be called when Stop is called before Run")
			return nil, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			t.Fatal("PrepareAgent should not be called when Stop is called before Run")
			return nil, nil
		},
	})

	loop.Stop()

	ok, _ := loop.Push("item1")
	assert.False(t, ok)
	ok, _ = loop.Push("item2")
	assert.False(t, ok)

	loop.Run(context.Background())
	result := loop.Wait()

	assert.NoError(t, result.ExitReason)
	assert.Empty(t, result.UnhandledItems)
	assert.Empty(t, result.InterruptedItems)
	assert.Equal(t, []string{"item1", "item2"}, result.TakeLateItems())
}

func TestTurnLoop_SkipCheckpoint_Sticky(t *testing.T) {
	agentStarted := make(chan struct{})

	store := newTestStore()
	cpID := "sticky-skip-session"

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: cpID,
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "slow",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			close(agentStarted)
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			return nil
		},
	})

	loop.Push("msg1")
	<-agentStarted
	loop.Stop(WithGraceful(), WithSkipCheckpoint())
	loop.Stop()

	exit := loop.Wait()
	assert.False(t, exit.CheckpointAttempted, "SkipCheckpoint should be sticky across multiple Stop calls")

	store.mu.Lock()
	_, exists := store.m[cpID]
	store.mu.Unlock()
	assert.False(t, exists, "no checkpoint should be saved when SkipCheckpoint was set in any Stop call")
}

func TestWithGracefulTimeout_NonPositive_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "adk: WithGracefulTimeout: gracePeriod must be positive",
		func() { WithGracefulTimeout(0) })
	assert.PanicsWithValue(t, "adk: WithGracefulTimeout: gracePeriod must be positive",
		func() { WithGracefulTimeout(-1 * time.Second) })
}

func TestWithPreempt_ZeroSafePoint_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "adk: SafePoint must not be zero; use AfterToolCalls, AfterChatModel, or AnySafePoint",
		func() { WithPreempt[string, *schema.Message](SafePoint(0)) })
}

func TestWithPreemptTimeout_ZeroSafePoint_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "adk: SafePoint must not be zero; use AfterToolCalls, AfterChatModel, or AnySafePoint",
		func() { WithPreemptTimeout[string, *schema.Message](SafePoint(0), time.Second) })
}

func TestSafePoint_ToCancelMode(t *testing.T) {
	assert.Equal(t, CancelAfterToolCalls, AfterToolCalls.toCancelMode())
	assert.Equal(t, CancelAfterChatModel, AfterChatModel.toCancelMode())
	assert.Equal(t, CancelAfterToolCalls|CancelAfterChatModel, AnySafePoint.toCancelMode())
}

func TestNewTurnLoop_NilGenInput_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "adk: NewTurnLoop: GenInput is required", func() {
		NewTurnLoop(TurnLoopConfig[string, *schema.Message]{PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return nil, nil
		}})
	})
}

func TestNewTurnLoop_NilPrepareAgent_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "adk: NewTurnLoop: PrepareAgent is required", func() {
		NewTurnLoop(TurnLoopConfig[string, *schema.Message]{GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return nil, nil
		}})
	})
}

func TestDeriveAgentToolCancelContext_NilParent_ReturnsNil(t *testing.T) {
	var cc *cancelContext
	assert.Nil(t, cc.deriveAgentToolCancelContext(context.Background()))
}

func TestUntilIdleFor(t *testing.T) {
	t.Run("FiresAfterIdleDuration", func(t *testing.T) {
		turnDone := make(chan struct{})
		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopMockAgent{
					name: "test",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						close(turnDone)
						return &AgentOutput{}, nil
					},
				}, nil
			},
		})

		loop.Push("msg1")
		<-turnDone

		loop.Stop(UntilIdleFor(50 * time.Millisecond))

		done := make(chan struct{})
		go func() {
			loop.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after idle timeout")
		}
	})

	t.Run("ResetsOnPush", func(t *testing.T) {
		turnCount := int32(0)
		turnDone := make(chan struct{}, 10)
		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopMockAgent{
					name: "test",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						atomic.AddInt32(&turnCount, 1)
						turnDone <- struct{}{}
						return &AgentOutput{}, nil
					},
				}, nil
			},
		})

		loop.Push("msg1")
		<-turnDone

		loop.Stop(UntilIdleFor(200 * time.Millisecond))

		time.Sleep(100 * time.Millisecond)
		loop.Push("msg2")
		<-turnDone

		done := make(chan struct{})
		go func() {
			loop.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("loop did not exit after idle timeout")
		}

		assert.Equal(t, int32(2), atomic.LoadInt32(&turnCount))
	})

	t.Run("EscalatedByStopWithImmediate", func(t *testing.T) {
		agentStarted := make(chan *cancelContext, 1)
		probe := &turnLoopStopModeProbeAgent{ccCh: agentStarted}
		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return probe, nil
			},
		})

		loop.Push("msg1")
		cc := <-agentStarted

		loop.Stop(UntilIdleFor(10 * time.Minute))
		loop.Stop(WithImmediate())

		deadline := time.After(2 * time.Second)
		for {
			if cc.getMode() == CancelImmediate {
				break
			}
			select {
			case <-deadline:
				t.Fatal("cancel mode did not escalate to CancelImmediate")
			default:
			}
			time.Sleep(1 * time.Millisecond)
		}

		exit := loop.Wait()
		var ce *CancelError
		require.True(t, errors.As(exit.ExitReason, &ce))
		assert.Equal(t, CancelImmediate, ce.Info.Mode)
	})

	t.Run("EscalatedByStopWithGraceful", func(t *testing.T) {
		agentStarted := make(chan struct{})
		agentDone := make(chan struct{})
		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopCancellableMockAgent{
					name: "test",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						close(agentStarted)
						<-ctx.Done()
						close(agentDone)
						return nil, ctx.Err()
					},
				}, nil
			},
		})

		loop.Push("msg1")
		<-agentStarted

		loop.Stop(UntilIdleFor(10 * time.Minute))
		loop.Stop(WithGracefulTimeout(50 * time.Millisecond))

		select {
		case <-agentDone:
		case <-time.After(2 * time.Second):
			t.Fatal("agent was not cancelled")
		}

		exit := loop.Wait()
		assert.Error(t, exit.ExitReason)
	})
}

// TestUntilIdleFor_DoesNotCancelRunningAgent verifies that Stop(UntilIdleFor)
// records an idle stop policy but does NOT create a pending cancel request for
// the running agent.
//
// TestUntilIdleFor_DoesNotCancelRunningAgent 验证 Stop(UntilIdleFor)
// 会记录 idle 停止策略，但不会为运行中的 agent 创建待处理的 cancel 请求。
func TestUntilIdleFor_DoesNotCancelRunningAgent(t *testing.T) {
	t.Run("BeforeRun", func(t *testing.T) {
		agentStarted := make(chan struct{})
		agentCtxCanceled := int32(0)
		agentDone := make(chan struct{})

		loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopCancellableMockAgent{
					name: "test",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						close(agentStarted)
						// Block until context is canceled or a short timeout.
						// 阻塞直到 context 被取消或短暂超时。
						select {
						case <-ctx.Done():
							atomic.StoreInt32(&agentCtxCanceled, 1)
						case <-time.After(200 * time.Millisecond):
						}
						close(agentDone)
						return &AgentOutput{}, nil
					},
				}, nil
			},
		})

		loop.Push("msg1")
		// Call Stop(UntilIdleFor) BEFORE Run.
		// 在 Run 之前调用 Stop(UntilIdleFor)。
		loop.Stop(UntilIdleFor(50 * time.Millisecond))
		loop.Run(context.Background())

		<-agentStarted
		<-agentDone

		exit := loop.Wait()
		assert.Nil(t, exit.ExitReason, "UntilIdleFor should not produce a CancelError")
		assert.Equal(t, int32(0), atomic.LoadInt32(&agentCtxCanceled),
			"agent context should not have been canceled by UntilIdleFor")
	})

	t.Run("DuringRun", func(t *testing.T) {
		agentStarted := make(chan struct{})
		agentCtxCanceled := int32(0)
		agentDone := make(chan struct{})

		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopCancellableMockAgent{
					name: "test",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						close(agentStarted)
						select {
						case <-ctx.Done():
							atomic.StoreInt32(&agentCtxCanceled, 1)
						case <-time.After(200 * time.Millisecond):
						}
						close(agentDone)
						return &AgentOutput{}, nil
					},
				}, nil
			},
		})

		loop.Push("msg1")
		<-agentStarted

		// Call Stop(UntilIdleFor) while the agent is running.
		// 在智能体运行时调用 Stop(UntilIdleFor)。
		loop.Stop(UntilIdleFor(50 * time.Millisecond))
		<-agentDone

		exit := loop.Wait()
		assert.Nil(t, exit.ExitReason, "UntilIdleFor should not produce a CancelError")
		assert.Equal(t, int32(0), atomic.LoadInt32(&agentCtxCanceled),
			"agent context should not have been canceled by UntilIdleFor")
	})

	// Cancel opts paired with UntilIdleFor in the same call are silently
	// dropped. The agent must run to completion even when WithImmediate is
	// combined with UntilIdleFor.
	//
	// 同一次调用中与 UntilIdleFor 配对的取消 opts 会被静默丢弃。
	// 即使 WithImmediate 与 UntilIdleFor 组合使用，智能体也必须运行到完成。
	t.Run("CancelOptsDroppedInSameCall", func(t *testing.T) {
		agentStarted := make(chan struct{})
		agentCtxCanceled := int32(0)
		agentDone := make(chan struct{})

		loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
			GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
				return &GenInputResult[string, *schema.Message]{
					Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
					Consumed: items,
				}, nil
			},
			PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
				return &turnLoopCancellableMockAgent{
					name: "test",
					runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
						close(agentStarted)
						select {
						case <-ctx.Done():
							atomic.StoreInt32(&agentCtxCanceled, 1)
						case <-time.After(200 * time.Millisecond):
						}
						close(agentDone)
						return &AgentOutput{}, nil
					},
				}, nil
			},
		})

		loop.Push("msg1")
		<-agentStarted

		// WithImmediate in the same call as UntilIdleFor must be ignored.
		// 与 UntilIdleFor 在同一次调用中的 WithImmediate 必须被忽略。
		loop.Stop(UntilIdleFor(50*time.Millisecond), WithImmediate())
		<-agentDone

		exit := loop.Wait()
		assert.Nil(t, exit.ExitReason, "cancel opts should be dropped when combined with UntilIdleFor")
		assert.Equal(t, int32(0), atomic.LoadInt32(&agentCtxCanceled),
			"agent context should not have been canceled")
	})
}

func TestUntilIdleFor_ContextCancelDuringIdleWait(t *testing.T) {
	turnDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(turnDone)
					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-turnDone

	// Start idle timer, then cancel the parent context while idle.
	// 启动空闲计时器，然后在空闲时取消父 context。
	loop.Stop(UntilIdleFor(10 * time.Minute))
	time.Sleep(20 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		loop.Wait()
		close(done)
	}()

	waitOrFail(t, done, "loop should exit when context is canceled during idle wait")

	exit := loop.Wait()
	assert.ErrorIs(t, exit.ExitReason, context.Canceled)
}

func TestCancelRequestState_ImmediateDominatesSafePointModes(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{
		WithAgentCancelMode(CancelAfterChatModel),
		WithAgentCancelTimeout(time.Minute),
	}, now)

	state.merge([]AgentCancelOption{WithAgentCancelMode(CancelImmediate)}, now)

	cfg := parseAgentCancelOptions(state.cancelOptions(now)...)
	assert.Equal(t, CancelImmediate, cfg.Mode)
	assert.Nil(t, cfg.Timeout)
}

func TestCancelRequestState_NilMergeDoesNotCreateCancelIntent(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{WithAgentCancelMode(CancelAfterChatModel)}, now)

	state.merge(nil, now)

	cfg := parseAgentCancelOptions(state.cancelOptions(now)...)
	assert.Equal(t, CancelAfterChatModel, cfg.Mode)
}

func TestCancelRequestState_EmptyMergeMeansExplicitImmediate(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{WithAgentCancelMode(CancelAfterChatModel)}, now)

	state.merge([]AgentCancelOption{}, now)

	cfg := parseAgentCancelOptions(state.cancelOptions(now)...)
	assert.Equal(t, CancelImmediate, cfg.Mode)
}

func TestCancelRequestState_SafePointModesJoin(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{WithAgentCancelMode(CancelAfterChatModel)}, now)

	state.merge([]AgentCancelOption{WithAgentCancelMode(CancelAfterToolCalls)}, now)

	cfg := parseAgentCancelOptions(state.cancelOptions(now)...)
	assert.Equal(t, CancelAfterChatModel|CancelAfterToolCalls, cfg.Mode)
}

func TestCancelRequestState_RecursiveIsMonotonic(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{WithAgentCancelMode(CancelAfterChatModel)}, now)

	state.merge([]AgentCancelOption{WithAgentCancelMode(CancelAfterToolCalls), WithRecursive()}, now)
	state.merge([]AgentCancelOption{WithAgentCancelMode(CancelAfterChatModel)}, now)

	cfg := parseAgentCancelOptions(state.cancelOptions(now)...)
	assert.True(t, cfg.Recursive)
}

func TestCancelRequestState_TimeoutUsesEarliestDeadline(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{
		WithAgentCancelMode(CancelAfterChatModel),
		WithAgentCancelTimeout(10 * time.Second),
	}, now)

	state.merge([]AgentCancelOption{
		WithAgentCancelMode(CancelAfterToolCalls),
		WithAgentCancelTimeout(time.Second),
	}, now.Add(100*time.Millisecond))

	cfg := parseAgentCancelOptions(state.cancelOptions(now.Add(100 * time.Millisecond))...)
	require.NotNil(t, cfg.Timeout)
	assert.LessOrEqual(t, *cfg.Timeout, time.Second)
}

func TestCancelRequestState_ExpiredTimeoutConvertsToImmediate(t *testing.T) {
	now := time.Now()
	state := newCancelRequestState([]AgentCancelOption{
		WithAgentCancelMode(CancelAfterChatModel),
		WithAgentCancelTimeout(time.Nanosecond),
	}, now)

	cfg := parseAgentCancelOptions(state.cancelOptions(now.Add(time.Second))...)
	assert.Equal(t, CancelImmediate, cfg.Mode)
	assert.Nil(t, cfg.Timeout)
}

func TestStopController_BareStopCommitsWithoutCancelRequest(t *testing.T) {
	c := newStopController()

	decision := c.requestStop(&stopConfig{})

	assert.True(t, decision.commit)
	assert.True(t, c.isCommitted())
	c.beginActiveTurn()
	_, ok := c.receiveCancel()
	assert.False(t, ok)
}

func TestStopController_UntilIdleForDoesNotCreateCancelRequest(t *testing.T) {
	c := newStopController()

	decision := c.requestStop(&stopConfig{idleFor: time.Second})

	assert.False(t, decision.commit)
	assert.True(t, decision.wakeIdle)
	assert.Equal(t, time.Second, c.idleDuration())
	assert.False(t, c.isCommitted())
	c.beginActiveTurn()
	_, ok := c.receiveCancel()
	assert.False(t, ok)
}

func TestStopController_CancelOptsDroppedWhenCombinedWithUntilIdleFor(t *testing.T) {
	c := newStopController()

	decision := c.requestStop(&stopConfig{
		idleFor:         time.Second,
		agentCancelOpts: []AgentCancelOption{WithRecursive()},
	})

	assert.False(t, decision.commit)
	c.beginActiveTurn()
	_, ok := c.receiveCancel()
	assert.False(t, ok)
}

func TestStopController_ImmediateStopCreatesPendingCancelForActiveTurn(t *testing.T) {
	c := newStopController()
	c.beginActiveTurn()

	decision := c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithRecursive()}})

	assert.True(t, decision.commit)
	req, ok := c.receiveCancel()
	require.True(t, ok)
	cfg := parseAgentCancelOptions(req.cancelOptions(time.Now())...)
	assert.Equal(t, CancelImmediate, cfg.Mode)
	assert.True(t, cfg.Recursive)
}

func TestStopController_StopBeforeWatcherStartsConsumedAfterBeginActiveTurn(t *testing.T) {
	c := newStopController()

	decision := c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithRecursive()}})
	assert.True(t, decision.commit)

	c.beginActiveTurn()
	req, ok := c.receiveCancel()
	require.True(t, ok)
	cfg := parseAgentCancelOptions(req.cancelOptions(time.Now())...)
	assert.Equal(t, CancelImmediate, cfg.Mode)
}

func TestStopController_EndActiveTurnDropsUnconsumedCancel(t *testing.T) {
	c := newStopController()
	c.beginActiveTurn()
	c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithRecursive()}})

	req := c.endActiveTurn()

	require.NotNil(t, req)
	_, ok := c.receiveCancel()
	assert.False(t, ok)
}

func TestStopController_RepeatedStopsMergeWithoutDeescalation(t *testing.T) {
	c := newStopController()
	c.beginActiveTurn()

	c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithRecursive()}})
	c.requestStop(&stopConfig{})
	c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{
		WithAgentCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
		WithRecursive(),
	}})

	req, ok := c.receiveCancel()
	require.True(t, ok)
	cfg := parseAgentCancelOptions(req.cancelOptions(time.Now())...)
	assert.Equal(t, CancelImmediate, cfg.Mode)
	assert.True(t, cfg.Recursive)
}

func TestStopController_RepeatedStopsUseSharedCancelMergeState(t *testing.T) {
	c := newStopController()
	c.beginActiveTurn()

	c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithAgentCancelMode(CancelAfterChatModel)}})
	c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithAgentCancelMode(CancelAfterToolCalls)}})

	req, ok := c.receiveCancel()
	require.True(t, ok)
	cfg := parseAgentCancelOptions(req.cancelOptions(time.Now())...)
	assert.Equal(t, CancelAfterChatModel|CancelAfterToolCalls, cfg.Mode)
}

func TestStopController_StopCauseFirstNonEmptyWins(t *testing.T) {
	c := newStopController()

	c.requestStop(&stopConfig{})
	c.requestStop(&stopConfig{stopCause: "first"})
	c.requestStop(&stopConfig{stopCause: "second"})

	assert.Equal(t, "first", c.cause())
}

func TestStopController_SkipCheckpointSticky(t *testing.T) {
	c := newStopController()

	c.requestStop(&stopConfig{skipCheckpoint: true})
	c.requestStop(&stopConfig{})

	assert.True(t, c.skipCheckpointEnabled())
}

func TestStopController_ConcurrentStopRequestsRaceSafe(t *testing.T) {
	c := newStopController()
	c.beginActiveTurn()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 5 {
			case 0:
				c.requestStop(&stopConfig{})
			case 1:
				c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithRecursive()}})
			case 2:
				c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{
					WithAgentCancelMode(CancelAfterChatModel),
					WithAgentCancelTimeout(time.Second),
					WithRecursive(),
				}})
			case 3:
				c.requestStop(&stopConfig{idleFor: time.Second})
			case 4:
				c.requestStop(&stopConfig{skipCheckpoint: true, stopCause: "cause"})
			}
		}(i)
	}
	wg.Wait()

	assert.True(t, c.isCommitted())
	assert.True(t, c.skipCheckpointEnabled())
}

func TestStopController_CloseForLoopExitClearsPendingCancel(t *testing.T) {
	c := newStopController()
	c.beginActiveTurn()
	c.requestStop(&stopConfig{agentCancelOpts: []AgentCancelOption{WithRecursive()}})

	c.closeForLoopExit()

	_, ok := c.receiveCancel()
	assert.False(t, ok)
}

func TestAttack_UntilIdleFor_ConcurrentPushDuringIdleTimer(t *testing.T) {
	turnCount := int32(0)
	turnDone := make(chan struct{}, 10)

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					atomic.AddInt32(&turnCount, 1)
					turnDone <- struct{}{}
					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-turnDone

	loop.Stop(UntilIdleFor(200 * time.Millisecond))

	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond)
		loop.Push("concurrent-" + string(rune('a'+i)))
		<-turnDone
	}

	done := make(chan struct{})
	go func() {
		loop.Wait()
		close(done)
	}()

	waitOrFail(t, done, "loop did not exit after idle timeout — Push did not reset timer correctly")

	finalCount := atomic.LoadInt32(&turnCount)
	assert.Equal(t, int32(6), finalCount, "all 6 pushes should have been processed")
}

func TestAttack_UntilIdleFor_MultipleStopCallsFirstWins(t *testing.T) {
	turnDone := make(chan struct{})
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(turnDone)
					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-turnDone

	loop.Stop(UntilIdleFor(100 * time.Millisecond))
	loop.Stop(UntilIdleFor(10 * time.Minute))

	done := make(chan struct{})
	go func() {
		loop.Wait()
		close(done)
	}()

	waitOrFail(t, done, "second UntilIdleFor should have been ignored; loop should have exited with 100ms timer")
}

func TestAttack_BareStopOverridesUntilIdleFor(t *testing.T) {
	agentStarted := make(chan struct{})
	agentDone := make(chan struct{})

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(agentStarted)
					<-agentDone
					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-agentStarted

	loop.Stop(UntilIdleFor(10 * time.Minute))

	loop.Stop()
	close(agentDone)

	done := make(chan struct{})
	go func() {
		loop.Wait()
		close(done)
	}()

	waitOrFail(t, done, "bare Stop should override UntilIdleFor and cause immediate shutdown")

	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason, "bare Stop should exit cleanly")
}

func TestAttack_BareStopDoesNotDeescalateExistingCancelIntent(t *testing.T) {
	agentStarted := make(chan *cancelContext, 1)
	probe := &turnLoopStopModeProbeAgent{ccCh: agentStarted}
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return probe, nil
		},
	})

	loop.Push("msg1")
	cc := <-agentStarted

	loop.Stop(WithImmediate())

	time.Sleep(20 * time.Millisecond)

	loop.Stop()

	time.Sleep(20 * time.Millisecond)
	mode := cc.getMode()
	assert.Equal(t, CancelImmediate, mode, "bare Stop after WithImmediate must not de-escalate cancel mode")

	exit := loop.Wait()
	var ce *CancelError
	require.True(t, errors.As(exit.ExitReason, &ce))
	assert.Equal(t, CancelImmediate, ce.Info.Mode)
}

func TestAttack_InterruptedItems_EmptyWhenAgentFinishesNormally(t *testing.T) {
	agentStarted := make(chan struct{})
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(agentStarted)
					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-agentStarted
	time.Sleep(50 * time.Millisecond)
	loop.Stop()

	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)
	assert.Empty(t, exit.InterruptedItems, "InterruptedItems must be empty when agent finished normally")
}

func TestAttack_TurnBuffer_WakeupDoesNotLoseItems(t *testing.T) {
	tb := newTurnBuffer[string]()

	tb.Send("a")
	tb.Send("b")
	tb.Wakeup()
	tb.Send("c")

	var got []string
	for i := 0; i < 3; i++ {
		val, ok := tb.Receive()
		require.True(t, ok)
		got = append(got, val)
	}

	assert.Equal(t, []string{"a", "b", "c"}, got, "Wakeup must not cause items to be lost")
}

func TestAttack_TurnBuffer_ClearWakeupPreventsSpuriousReturn(t *testing.T) {
	tb := newTurnBuffer[string]()

	tb.Wakeup()
	tb.ClearWakeup()

	received := make(chan string, 1)
	go func() {
		val, ok := tb.Receive()
		if ok {
			received <- val
		}
	}()

	time.Sleep(50 * time.Millisecond)
	tb.Send("real")

	select {
	case val := <-received:
		assert.Equal(t, "real", val, "ClearWakeup should prevent spurious empty return")
	case <-time.After(2 * time.Second):
		t.Fatal("Receive blocked forever despite Send")
	}
}

func TestAttack_StopBeforeRun_UntilIdleFor_ExitsImmediately(t *testing.T) {
	loop := NewTurnLoop(TurnLoopConfig[string, *schema.Message]{
		GenInput:     genInputConsumeAllWithMsg,
		PrepareAgent: prepareTestAgent,
	})

	loop.Stop(UntilIdleFor(10 * time.Minute))
	loop.Stop()

	loop.Run(context.Background())

	done := make(chan struct{})
	go func() {
		loop.Wait()
		close(done)
	}()

	waitOrFail(t, done, "loop should exit immediately when Stop() called before Run()")
}

func TestAttack_PushAfterStop_UntilIdleFor_RoutedToLateItems(t *testing.T) {
	turnDone := make(chan struct{})
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(turnDone)
					return &AgentOutput{}, nil
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-turnDone

	loop.Stop(UntilIdleFor(50 * time.Millisecond))
	exit := loop.Wait()
	assert.NoError(t, exit.ExitReason)

	ok, _ := loop.Push("after-stop")
	assert.False(t, ok, "Push after loop exited should return false")

	late := exit.TakeLateItems()
	assert.Equal(t, []string{"after-stop"}, late)
}

func TestAttack_ConcurrentStopEscalation_RaceDetector(t *testing.T) {
	agentStarted := make(chan struct{})
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(agentStarted)
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
	})

	loop.Push("msg1")
	<-agentStarted

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 4 {
			case 0:
				loop.Stop()
			case 1:
				loop.Stop(WithImmediate())
			case 2:
				loop.Stop(WithGracefulTimeout(100 * time.Millisecond))
			case 3:
				loop.Stop(UntilIdleFor(50 * time.Millisecond))
			}
		}(i)
	}

	wg.Wait()
	exit := loop.Wait()
	t.Log("ExitReason:", exit.ExitReason)
}

func TestAttack_SkipCheckpoint_Sticky(t *testing.T) {
	agentStarted := make(chan struct{})
	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return &turnLoopCancellableMockAgent{
				name: "test",
				runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
					close(agentStarted)
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, nil
		},
		Store:        newTestStore(),
		CheckpointID: "test-sticky",
	})

	loop.Push("msg1")
	<-agentStarted

	loop.Stop(WithSkipCheckpoint())
	loop.Stop(WithImmediate())

	exit := loop.Wait()
	assert.False(t, exit.CheckpointAttempted, "SkipCheckpoint is sticky; checkpoint should be skipped")
}

// turnLoopNestedProbeAgent simulates an agent with a nested sub-agent
// by deriving a child cancelContext. This allows tests to verify that
// TurnLoop's Stop/Push options correctly propagate recursive cancellation.
//
// IMPORTANT: child.markDone() is NOT called by the probe. The test MUST
// call it (e.g. via t.Cleanup) after verifying propagation to avoid a
// race between markDone closing child.doneChan and the deriveAgentToolCancelContext
// goroutines propagating the cancel signal.
//
// turnLoopNestedProbeAgent 通过派生子 cancelContext 来模拟带嵌套子智能体的智能体。
// 这允许测试验证 TurnLoop 的 Stop/Push 选项能正确传播递归取消。
// 重要：probe 不会调用 child.markDone()。测试必须在验证传播后调用它（例如通过 t.Cleanup），以避免 markDone 关闭 child.doneChan 与 deriveAgentToolCancelContext goroutines 传播取消信号之间的竞争。
type turnLoopNestedProbeAgent struct {
	parentCCCh chan *cancelContext
	childCCCh  chan *cancelContext
}

func (a *turnLoopNestedProbeAgent) Name(_ context.Context) string        { return "nested-probe" }
func (a *turnLoopNestedProbeAgent) Description(_ context.Context) string { return "nested-probe" }
func (a *turnLoopNestedProbeAgent) Run(ctx context.Context, _ *AgentInput, opts ...AgentRunOption) *AsyncIterator[*AgentEvent] {
	iter, gen := NewAsyncIteratorPair[*AgentEvent]()
	o := getCommonOptions(nil, opts...)
	cc := o.cancelCtx

	child := cc.deriveAgentToolCancelContext(ctx)
	a.parentCCCh <- cc
	a.childCCCh <- child

	go func() {
		defer gen.Close()
		<-cc.cancelChan
		for {
			if cc.getMode() == CancelImmediate {
				gen.Send(&AgentEvent{Err: cc.createCancelError()})
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
	return iter
}

func TestTurnLoop_Stop_WithImmediate_RecursivePropagation(t *testing.T) {
	parentCCCh := make(chan *cancelContext, 1)
	childCCCh := make(chan *cancelContext, 1)
	probe := &turnLoopNestedProbeAgent{parentCCCh: parentCCCh, childCCCh: childCCCh}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return probe, nil
		},
	})

	loop.Push("msg1")
	cc := <-parentCCCh
	child := <-childCCCh
	t.Cleanup(func() { child.markDone() })

	loop.Stop(WithImmediate())

	// Child should receive the cancel signal via recursive propagation.
	// 子级应通过递归传播收到取消信号。
	select {
	case <-child.cancelChan:
	case <-time.After(2 * time.Second):
		t.Fatal("child did not receive cancel via recursive propagation")
	}

	// Child should also receive the immediate cancel signal.
	// 子级也应收到 immediate 取消信号。
	select {
	case <-child.immediateChan:
	case <-time.After(2 * time.Second):
		t.Fatal("child did not receive immediate cancel via recursive propagation")
	}

	assert.True(t, cc.isRecursive(), "WithImmediate should set recursive on parent")
	assert.True(t, child.shouldCancel(), "child should be cancelled")
	assert.True(t, child.isImmediateCancelled(), "child should have received immediate cancel")

	exit := loop.Wait()
	var ce *CancelError
	require.True(t, errors.As(exit.ExitReason, &ce))
	assert.Equal(t, CancelImmediate, ce.Info.Mode)
}

func TestTurnLoop_Push_WithPreemptTimeout_RecursivePropagation(t *testing.T) {
	parentCCCh := make(chan *cancelContext, 2)
	childCCCh := make(chan *cancelContext, 2)
	probe := &turnLoopNestedProbeAgent{parentCCCh: parentCCCh, childCCCh: childCCCh}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return probe, nil
		},
	})

	loop.Push("first")
	cc := <-parentCCCh
	child := <-childCCCh
	t.Cleanup(func() { child.markDone() })

	// Preempt with a very short timeout so it escalates to CancelImmediate quickly.
	// 使用很短的超时时间进行抢占，使其快速升级为 CancelImmediate。
	loop.Push("urgent", WithPreemptTimeout[string, *schema.Message](AfterChatModel, 10*time.Millisecond))

	// After timeout escalation, child should receive the immediate cancel
	// via recursive propagation.
	//
	// 超时升级后，子级应通过递归传播收到 immediate 取消。
	select {
	case <-child.immediateChan:
	case <-time.After(2 * time.Second):
		t.Fatal("child did not receive immediate cancel after preempt timeout escalation")
	}

	assert.True(t, cc.isRecursive(), "WithPreemptTimeout should set recursive on parent")
	assert.True(t, child.isImmediateCancelled(), "child should have received immediate cancel")

	loop.Stop(WithImmediate())
	loop.Wait()
}

func TestUntilIdleFor_NonPositive_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "adk: UntilIdleFor: duration must be positive",
		func() { UntilIdleFor(0) })
	assert.PanicsWithValue(t, "adk: UntilIdleFor: duration must be positive",
		func() { UntilIdleFor(-1 * time.Second) })
}

func TestSaveTurnLoopCheckpoint_NilStore(t *testing.T) {
	l := &TurnLoop[string, *schema.Message]{config: TurnLoopConfig[string, *schema.Message]{Store: nil}}
	err := l.saveTurnLoopCheckpoint(context.Background(), "cp-1", &turnLoopCheckpoint[string]{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint store is nil")
}

func TestSetupBridgeStore_NilStore_Resume(t *testing.T) {
	l := &TurnLoop[string, *schema.Message]{config: TurnLoopConfig[string, *schema.Message]{Store: nil}}
	spec := &turnRunSpec[string, *schema.Message]{isResume: true}
	_, _, err := l.setupBridgeStore(spec, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint store is nil")
}

// TestTurnLoop_Preempt_LoopStalledAfterSecondPreemptPush covers a liveness
// regression where a preempted turn was followed by another preemptive Push and
// the loop stopped making progress before processing the later item.
//
// TestTurnLoop_Preempt_LoopStalledAfterSecondPreemptPush 覆盖一个活性回归：被抢占的轮次之后又跟着一次抢占式 Push，循环在处理后续项之前停止推进。
func TestTurnLoop_Preempt_LoopStalledAfterSecondPreemptPush(t *testing.T) {
	// turnCount tracks how many turns have been fully processed.
	// turnCount 跟踪已完整处理的轮次数。
	var turnCount int32

	// Channels to synchronize the test with each turn's lifecycle.
	// 用于让测试与每个轮次生命周期同步的 channels。
	firstAgentStarted := make(chan struct{})
	secondTurnDone := make(chan struct{})
	thirdTurnDone := make(chan struct{})

	var firstAgentStartedOnce, secondTurnDoneOnce, thirdTurnDoneOnce sync.Once

	agent := &turnLoopCancellableMockAgent{
		name: "test",
		runFunc: func(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
			turn := atomic.AddInt32(&turnCount, 1)
			switch turn {
			case 1:
				// First turn: signal started, then block until preempted.
				// 第一轮：发出已启动信号，然后阻塞直到被抢占。
				firstAgentStartedOnce.Do(func() { close(firstAgentStarted) })
				<-ctx.Done()
			case 2, 3:
				// Subsequent turns: complete immediately.
				// 后续轮次：立即完成。
			}
			return &AgentOutput{}, nil
		},
	}

	loop := newAndRunTurnLoop(context.Background(), TurnLoopConfig[string, *schema.Message]{
		PrepareAgent: prepareAgent(agent),
		GenInput:     genInputConsumeFirst,
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string, *schema.Message], events *AsyncIterator[*AgentEvent]) error {
			for {
				if _, ok := events.Next(); !ok {
					break
				}
			}
			turn := atomic.LoadInt32(&turnCount)
			switch turn {
			case 2:
				secondTurnDoneOnce.Do(func() { close(secondTurnDone) })
			case 3:
				thirdTurnDoneOnce.Do(func() { close(thirdTurnDone) })
			}
			return nil
		},
	})

	// Step 1: Push item A (no preempt). Wait for agent to start.
	// 步骤 1：Push 项 A（不抢占）。等待智能体启动。
	loop.Push("A")
	waitOrFail(t, firstAgentStarted, "agent did not start for item A")

	// Step 2: Push item B with preempt. This cancels the first turn.
	// 步骤 2：Push 项 B 并抢占。这会取消第一轮。
	loop.Push("B", WithPreempt[string, *schema.Message](AnySafePoint))

	// Wait for the second turn (item B) to complete successfully.
	// 等待第二轮（项 B）成功完成。
	waitOrFail(t, secondTurnDone, "second turn (item B) did not complete")

	// Step 3: Push item C with preempt. This is the scenario that triggers
	// the bug — the loop should process item C but instead gets stuck.
	//
	// 步骤 3：Push 项 C 并抢占。这是触发 bug 的场景——循环应处理项 C，但实际会卡住。
	loop.Push("C", WithPreempt[string, *schema.Message](AnySafePoint))

	// The loop should process item C. If the bug is present, this will timeout.
	// 循环应处理 item C。如果存在该 bug，这里会超时。
	waitOrFail(t, thirdTurnDone, "third turn (item C) was never processed — loop is stuck between turns")

	loop.Stop()
	result := loop.Wait()
	assert.NoError(t, result.ExitReason)
	assert.Equal(t, int32(3), atomic.LoadInt32(&turnCount), "expected 3 turns to be processed")
}

func TestAttack_BusinessInterrupt_NoStore_ExitsWithoutPanic(t *testing.T) {
	ctx := context.Background()
	interruptAgent := &turnLoopInterruptAgent{interruptInfo: "no_store_test"}

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		GenInput: genInputConsumeAllWithMsg,
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return interruptAgent, nil
		},
	})

	loop.Push("msg1")
	exit := loop.Wait()

	var intErr *InterruptError
	require.True(t, errors.As(exit.ExitReason, &intErr), "expected *InterruptError, got: %v", exit.ExitReason)
	assert.Equal(t, []string{"msg1"}, exit.InterruptedItems)
	assert.False(t, exit.CheckpointAttempted, "no store → no checkpoint attempt")
}

func TestAttack_BusinessInterrupt_EmptyConsumed_NoCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	interruptAgent := &turnLoopInterruptAgent{interruptInfo: "idle_test"}

	loop := newAndRunTurnLoop(ctx, TurnLoopConfig[string, *schema.Message]{
		Store:        store,
		CheckpointID: "idle-cp",
		GenInput: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], items []string) (*GenInputResult[string, *schema.Message], error) {
			return &GenInputResult[string, *schema.Message]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage("x")}},
				Consumed: []string{},
			}, nil
		},
		PrepareAgent: func(ctx context.Context, _ *TurnLoop[string, *schema.Message], consumed []string) (Agent, error) {
			return interruptAgent, nil
		},
	})

	loop.Push("msg1")
	exit := loop.Wait()

	var intErr *InterruptError
	require.True(t, errors.As(exit.ExitReason, &intErr), "expected *InterruptError, got: %v", exit.ExitReason)
	assert.Empty(t, exit.InterruptedItems, "consumed was empty → InterruptedItems should be empty")
}
