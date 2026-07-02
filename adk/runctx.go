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
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cloudwego/eino/schema"
)

// runSession CheckpointSchema: persisted via serialization.RunCtx (gob).
// runSession CheckpointSchema：通过 serialization.RunCtx (gob) 持久化。
type runSession struct {
	Values    map[string]any
	valuesMtx *sync.Mutex

	Events     []*agentEventWrapper
	LaneEvents *laneEvents
	mtx        sync.Mutex

	// TypedEvents stores *[]*typedAgentEventWrapper[M] for M != *schema.Message.
	// For M = *schema.Message, the existing Events field is used instead.
	// The any type is required because Go does not support generic fields in non-generic structs.
	//
	// TypedEvents 为 M != *schema.Message 存储 *[]*typedAgentEventWrapper[M]。
	// 对于 M = *schema.Message，则改用现有的 Events 字段。
	// 需要 any 类型，因为 Go 不支持在非泛型结构体中使用泛型字段。
	TypedEvents any
}

// laneEvents CheckpointSchema: persisted via serialization.RunCtx (gob).
// laneEvents CheckpointSchema：通过 serialization.RunCtx (gob) 持久化。
type laneEvents struct {
	Events []*agentEventWrapper
	Parent *laneEvents
}

// agentEventWrapper CheckpointSchema: persisted via serialization.RunCtx (gob).
// agentEventWrapper CheckpointSchema：通过 serialization.RunCtx (gob) 持久化。
type agentEventWrapper struct {
	*AgentEvent
	mu                  sync.Mutex
	concatenatedMessage Message
	// TS is the timestamp (in nanoseconds) when this event was created.
	// It is primarily used by the laneEvents mechanism to order events
	// from different agents in a multi-agent flow.
	//
	// TS 是该事件创建时的时间戳（纳秒）。
	// 它主要由 laneEvents 机制用于在多智能体流程中对来自不同智能体的事件排序。
	TS int64
	// StreamErr stores the error message if the MessageStream contained an error.
	// This field guards against multiple calls to getMessageFromWrappedEvent
	// when the stream has already been consumed and errored.
	// Normally when StreamErr happens, the Agent will return with the error,
	// unless retry is configured for the agent generating this stream, in which case
	// this StreamErr will be of type WillRetryError (indicating retry is pending).
	//
	// 如果 MessageStream 包含错误，StreamErr 存储错误消息。
	// 该字段用于防止在流已被消费且出错后多次调用 getMessageFromWrappedEvent。
	// 通常发生 StreamErr 时，Agent 会带着该错误返回；除非为生成此流的智能体配置了重试，此时该 StreamErr 会是 WillRetryError 类型（表示重试待进行）。
	StreamErr error
}

type typedAgentEventWrapper[M MessageType] struct {
	event               *TypedAgentEvent[M]
	mu                  sync.Mutex
	concatenatedMessage M
	TS                  int64
	StreamErr           error
}

// typedAgentEventWrapperForGob is a gob-serializable representation of typedAgentEventWrapper.
// We encode the event and TS separately to avoid the sync.Mutex and non-exported fields.
//
// typedAgentEventWrapperForGob 是 typedAgentEventWrapper 的 gob 可序列化表示。
// 我们将 event 和 TS 分开编码，以避开 sync.Mutex 和非导出字段。
type typedAgentEventWrapperForGob[M MessageType] struct {
	Event *TypedAgentEvent[M]
	TS    int64
}

func (e *typedAgentEventWrapper[M]) GobEncode() ([]byte, error) {
	if e.event != nil && e.event.Output != nil && e.event.Output.MessageOutput != nil && e.event.Output.MessageOutput.IsStreaming {
		// Materialize the stream before encoding.
		// 编码前先将流物化。
		if isNilMessage(e.concatenatedMessage) && e.StreamErr == nil {
			e.consumeStream()
		}
	}

	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(&typedAgentEventWrapperForGob[M]{
		Event: e.event,
		TS:    e.TS,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to gob encode generic agent event wrapper: %w", err)
	}
	return buf.Bytes(), nil
}

func (e *typedAgentEventWrapper[M]) GobDecode(b []byte) error {
	g := &typedAgentEventWrapperForGob[M]{}
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(g); err != nil {
		return fmt.Errorf("failed to gob decode generic agent event wrapper: %w", err)
	}
	e.event = g.Event
	e.TS = g.TS
	return nil
}

// consumeStream drains the typed message stream, setting concatenatedMessage on success
// or StreamErr on failure. The stream is replaced with a materialized version safe for
// gob encoding.
//
// NOTE: This method parallels agentEventWrapper.consumeStream in utils.go. The two
// implementations exist because agentEventWrapper is non-generic (uses *schema.Message
// directly) while typedAgentEventWrapper[M] is generic. They cannot be unified without
// making the non-generic wrapper generic, which would cascade through the entire
// non-generic event storage layer.
//
// consumeStream 会耗尽类型化消息流；成功时设置 concatenatedMessage，失败时设置 StreamErr。该流会被替换为可安全进行 gob 编码的物化版本。
// NOTE: 此方法与 utils.go 中的 agentEventWrapper.consumeStream 类似。两种实现同时存在，是因为 agentEventWrapper 是非泛型的（直接使用 *schema.Message），而 typedAgentEventWrapper[M] 是泛型的。若不把非泛型 wrapper 改成泛型，就无法合并；而这会影响整个非泛型事件存储层。
func (e *typedAgentEventWrapper[M]) consumeStream() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !isNilMessage(e.concatenatedMessage) {
		return
	}

	s := e.event.Output.MessageOutput.MessageStream
	var msgs []M

	defer s.Close()
	for {
		msg, err := s.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			e.StreamErr = err
			e.event.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray(msgs)
			return
		}
		msgs = append(msgs, msg)
	}

	if len(msgs) == 0 {
		e.StreamErr = errors.New("no messages in typedAgentEventWrapper.MessageStream")
		e.event.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray(msgs)
		return
	}

	if len(msgs) == 1 {
		e.concatenatedMessage = msgs[0]
	} else {
		var err error
		e.concatenatedMessage, err = concatMessageStream(schema.StreamReaderFromArray(msgs))
		if err != nil {
			e.StreamErr = err
			e.event.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray(msgs)
			return
		}
	}

	e.event.Output.MessageOutput.MessageStream = schema.StreamReaderFromArray([]M{e.concatenatedMessage})
}

type otherAgentEventWrapperForEncode agentEventWrapper

func (a *agentEventWrapper) GobEncode() ([]byte, error) {
	if a.Output != nil && a.Output.MessageOutput != nil && a.Output.MessageOutput.IsStreaming {
		// Materialize the stream before encoding. An unconsumed stream that
		// ends with a non-EOF error (WillRetryError, ErrStreamCanceled) would
		// cause MessageVariant.GobEncode to fail. consumeStream replaces the
		// stream with an error-free, materialized version.
		//
		// 编码前先将流物化。未消费的流若以非 EOF 错误（WillRetryError、ErrStreamCanceled）结束，会导致 MessageVariant.GobEncode 失败。consumeStream 会将流替换为无错误的物化版本。
		if a.concatenatedMessage == nil && a.StreamErr == nil {
			a.consumeStream()
		}
	}

	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode((*otherAgentEventWrapperForEncode)(a))
	if err != nil {
		return nil, fmt.Errorf("failed to gob encode agent event wrapper: %w", err)
	}
	return buf.Bytes(), nil
}

func (a *agentEventWrapper) GobDecode(b []byte) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode((*otherAgentEventWrapperForEncode)(a))
}

func newRunSession() *runSession {
	return &runSession{
		Values:    make(map[string]any),
		valuesMtx: &sync.Mutex{},
	}
}

// GetSessionValues returns all session key-value pairs for the current run.
// GetSessionValues 返回当前运行的所有 session 键值对。
func GetSessionValues(ctx context.Context) map[string]any {
	session := getSession(ctx)
	if session == nil {
		return map[string]any{}
	}

	return session.getValues()
}

// AddSessionValue sets a single session key-value pair for the current run.
// AddSessionValue 为当前运行设置单个 session 键值对。
func AddSessionValue(ctx context.Context, key string, value any) {
	session := getSession(ctx)
	if session == nil {
		return
	}

	session.addValue(key, value)
}

// AddSessionValues sets multiple session key-value pairs for the current run.
// AddSessionValues 为当前运行设置多个 session 键值对。
func AddSessionValues(ctx context.Context, kvs map[string]any) {
	session := getSession(ctx)
	if session == nil {
		return
	}

	session.addValues(kvs)
}

// GetSessionValue retrieves a session value by key and reports whether it exists.
// GetSessionValue 按 key 获取 session 值，并报告其是否存在。
func GetSessionValue(ctx context.Context, key string) (any, bool) {
	session := getSession(ctx)
	if session == nil {
		return nil, false
	}

	return session.getValue(key)
}

func (rs *runSession) addEvent(event *AgentEvent) {
	wrapper := &agentEventWrapper{AgentEvent: event, TS: time.Now().UnixNano()}
	// If LaneEvents is not nil, we are in a parallel lane.
	// Append to the lane's local event slice (lock-free).
	//
	// 如果 LaneEvents 非 nil，说明我们在并行 lane 中。
	// 追加到该 lane 的本地事件切片（无锁）。
	if rs.LaneEvents != nil {
		rs.LaneEvents.Events = append(rs.LaneEvents.Events, wrapper)
		return
	}

	// Otherwise, we are on the main path. Append to the shared Events slice (with lock).
	// 否则，我们在主路径上。追加到共享的 Events 切片（带锁）。
	rs.mtx.Lock()
	rs.Events = append(rs.Events, wrapper)
	rs.mtx.Unlock()
}

func (rs *runSession) getEvents() []*agentEventWrapper {
	// If there are no in-flight lane events, we can return the main slice directly.
	// 如果没有进行中的 lane 事件，可以直接返回主切片。
	if rs.LaneEvents == nil {
		rs.mtx.Lock()
		events := rs.Events
		rs.mtx.Unlock()
		return events
	}

	// If there are in-flight events, we must construct the full view.
	// First, get the committed history from the main Events slice.
	//
	// 如果存在进行中的事件，必须构造完整视图。
	// 首先，从主 Events 切片获取已提交的历史。
	rs.mtx.Lock()
	committedEvents := make([]*agentEventWrapper, len(rs.Events))
	copy(committedEvents, rs.Events)
	rs.mtx.Unlock()

	// Then, assemble the in-flight events by traversing the linked list.
	// Reading the .Parent pointer is safe without a lock because the parent of a lane is immutable after creation.
	//
	// 然后，通过遍历链表组装进行中的事件。
	// 无需加锁即可安全读取 .Parent 指针，因为 lane 的 parent 在创建后不可变。
	var laneSlices [][]*agentEventWrapper
	totalLaneSize := 0
	for lane := rs.LaneEvents; lane != nil; lane = lane.Parent {
		if len(lane.Events) > 0 {
			laneSlices = append(laneSlices, lane.Events)
			totalLaneSize += len(lane.Events)
		}
	}

	// Combine committed and in-flight history.
	// 合并已提交和进行中的历史。
	finalEvents := make([]*agentEventWrapper, 0, len(committedEvents)+totalLaneSize)
	finalEvents = append(finalEvents, committedEvents...)
	for i := len(laneSlices) - 1; i >= 0; i-- {
		finalEvents = append(finalEvents, laneSlices[i]...)
	}

	return finalEvents
}

func addTypedEvent[M MessageType](session *runSession, event *TypedAgentEvent[M]) {
	var zero M
	if _, ok := any(zero).(*schema.Message); ok {
		session.addEvent(any(event).(*AgentEvent))
		return
	}
	session.mtx.Lock()
	defer session.mtx.Unlock()
	wrapper := &typedAgentEventWrapper[M]{event: event, TS: time.Now().UnixNano()}
	store, _ := session.TypedEvents.(*[]*typedAgentEventWrapper[M])
	if store == nil {
		s := make([]*typedAgentEventWrapper[M], 0)
		store = &s
		session.TypedEvents = store
	}
	*store = append(*store, wrapper)
}

func (rs *runSession) getValues() map[string]any {
	rs.valuesMtx.Lock()
	values := make(map[string]any, len(rs.Values))
	for k, v := range rs.Values {
		values[k] = v
	}
	rs.valuesMtx.Unlock()

	return values
}

func (rs *runSession) addValue(key string, value any) {
	rs.valuesMtx.Lock()
	rs.Values[key] = value
	rs.valuesMtx.Unlock()
}

func (rs *runSession) addValues(kvs map[string]any) {
	rs.valuesMtx.Lock()
	for k, v := range kvs {
		rs.Values[k] = v
	}
	rs.valuesMtx.Unlock()
}

func (rs *runSession) getValue(key string) (any, bool) {
	rs.valuesMtx.Lock()
	value, ok := rs.Values[key]
	rs.valuesMtx.Unlock()

	return value, ok
}

type runContext struct {
	RootInput *AgentInput
	RunPath   []RunStep

	AgenticRootInput any

	Session *runSession
}

func (rc *runContext) isRoot() bool {
	return len(rc.RunPath) == 1
}

func (rc *runContext) deepCopy() *runContext {
	copied := &runContext{
		RootInput:        rc.RootInput,
		AgenticRootInput: rc.AgenticRootInput,
		RunPath:          make([]RunStep, len(rc.RunPath)),
		Session:          rc.Session,
	}

	copy(copied.RunPath, rc.RunPath)

	return copied
}

type runCtxKey struct{}

func getRunCtx(ctx context.Context) *runContext {
	runCtx, ok := ctx.Value(runCtxKey{}).(*runContext)
	if !ok {
		return nil
	}
	return runCtx
}

func setRunCtx(ctx context.Context, runCtx *runContext) context.Context {
	return context.WithValue(ctx, runCtxKey{}, runCtx)
}

func initRunCtx(ctx context.Context, agentName string, input *AgentInput) (context.Context, *runContext) {
	runCtx := getRunCtx(ctx)
	if runCtx != nil {
		runCtx = runCtx.deepCopy()
	} else {
		runCtx = &runContext{Session: newRunSession()}
	}

	runCtx.RunPath = append(runCtx.RunPath, RunStep{agentName: agentName})
	if runCtx.isRoot() && input != nil {
		runCtx.RootInput = input
	}

	return setRunCtx(ctx, runCtx), runCtx
}

func initTypedRunCtx[M MessageType](ctx context.Context, agentName string, input *TypedAgentInput[M]) (context.Context, *runContext) {
	runCtx := getRunCtx(ctx)
	if runCtx != nil {
		runCtx = runCtx.deepCopy()
	} else {
		runCtx = &runContext{Session: newRunSession()}
	}

	runCtx.RunPath = append(runCtx.RunPath, RunStep{agentName: agentName})
	if runCtx.isRoot() && input != nil {
		var zero M
		if _, ok := any(zero).(*schema.Message); ok {
			runCtx.RootInput = any(input).(*AgentInput)
		} else {
			runCtx.AgenticRootInput = input
		}
	}

	return setRunCtx(ctx, runCtx), runCtx
}

func joinRunCtxs(parentCtx context.Context, childCtxs ...context.Context) {
	switch len(childCtxs) {
	case 0:
		return
	case 1:
		// Optimization for the common case of a single branch.
		// 针对单分支常见场景的优化。
		newEvents := unwindLaneEvents(childCtxs...)
		commitEvents(parentCtx, newEvents)
		return
	}

	// 1. Collect all new events from the leaf nodes of each context's lane.
	// 1. 从每个 context 的 lane 叶子节点收集所有新事件。
	newEvents := unwindLaneEvents(childCtxs...)

	// 2. Sort the collected events by their creation timestamp for chronological order.
	// 2. 按创建时间戳对收集到的事件排序，以保持时间顺序。
	sort.Slice(newEvents, func(i, j int) bool {
		return newEvents[i].TS < newEvents[j].TS
	})

	// 3. Commit the sorted events to the parent.
	// 3. 将排序后的事件提交到 parent。
	commitEvents(parentCtx, newEvents)
}

// commitEvents appends a slice of new events to the correct parent lane or main event log.
// commitEvents 将一组新事件追加到正确的 parent lane 或主事件日志。
func commitEvents(ctx context.Context, newEvents []*agentEventWrapper) {
	runCtx := getRunCtx(ctx)
	if runCtx == nil || runCtx.Session == nil {
		// Should not happen, but handle defensively.
		// 不应发生，但做防御性处理。
		return
	}

	// If the context we are committing to is itself a lane, append to its event slice.
	// 如果要提交到的 context 本身就是 lane，则追加到它的事件切片。
	if runCtx.Session.LaneEvents != nil {
		runCtx.Session.LaneEvents.Events = append(runCtx.Session.LaneEvents.Events, newEvents...)
	} else {
		// Otherwise, commit to the main, shared Events slice with a lock.
		// 否则，加锁后提交到主共享 Events 切片。
		runCtx.Session.mtx.Lock()
		runCtx.Session.Events = append(runCtx.Session.Events, newEvents...)
		runCtx.Session.mtx.Unlock()
	}
}

// unwindLaneEvents traverses the LaneEvents of the given contexts and collects
// all events from the leaf nodes.
//
// unwindLaneEvents 遍历给定 context 的 LaneEvents，并收集叶子节点中的所有事件。
func unwindLaneEvents(ctxs ...context.Context) []*agentEventWrapper {
	var allNewEvents []*agentEventWrapper
	for _, ctx := range ctxs {
		runCtx := getRunCtx(ctx)
		if runCtx != nil && runCtx.Session != nil && runCtx.Session.LaneEvents != nil {
			allNewEvents = append(allNewEvents, runCtx.Session.LaneEvents.Events...)
		}
	}
	return allNewEvents
}

func forkRunCtx(ctx context.Context) context.Context {
	parentRunCtx := getRunCtx(ctx)
	if parentRunCtx == nil || parentRunCtx.Session == nil {
		// Should not happen in a parallel workflow, but handle defensively.
		// 在并行工作流中不应发生，但仍做防御性处理。
		return ctx
	}

	// Create a new session for the child lane by manually copying the parent's session fields.
	// This is crucial to ensure a new mutex is created and that the LaneEvents pointer is unique.
	//
	// 通过手动复制父 session 字段，为子 lane 创建新 session。
	// 这很关键，可确保创建新的 mutex，并且 LaneEvents 指针唯一。
	childSession := &runSession{
		Events: parentRunCtx.Session.Events, // Share the committed history
		// 共享已提交的历史
		Values: parentRunCtx.Session.Values, // Share the values map
		// 共享 values map
		valuesMtx: parentRunCtx.Session.valuesMtx,
	}

	// Fork the lane events within the new session struct.
	// 在新的 session 结构中 fork lane events。
	childSession.LaneEvents = &laneEvents{
		Parent: parentRunCtx.Session.LaneEvents,
		Events: make([]*agentEventWrapper, 0),
	}

	// Create a new runContext for the child lane, pointing to the new session.
	// 为子 lane 创建新的 runContext，指向新的 session。
	childRunCtx := &runContext{
		RootInput: parentRunCtx.RootInput,
		RunPath:   make([]RunStep, len(parentRunCtx.RunPath)),
		Session:   childSession,
	}
	copy(childRunCtx.RunPath, parentRunCtx.RunPath)

	return setRunCtx(ctx, childRunCtx)
}

// updateRunPathOnly creates a new context with an updated RunPath, but does NOT modify the Address.
// This is used by sequential workflows to accumulate execution history for LLM context,
// without incorrectly chaining the static addresses of peer agents.
//
// updateRunPathOnly 创建一个更新了 RunPath 的新 context，但不会修改 Address。
// 顺序工作流使用它为 LLM context 累积执行历史，
// 避免错误地串联同级智能体的静态地址。
func updateRunPathOnly(ctx context.Context, agentNames ...string) context.Context {
	runCtx := getRunCtx(ctx)
	if runCtx == nil {
		// This should not happen in a sequential workflow context, but handle defensively.
		// 这在顺序工作流 context 中不应发生，但仍做防御性处理。
		runCtx = &runContext{Session: newRunSession()}
	} else {
		runCtx = runCtx.deepCopy()
	}

	for _, agentName := range agentNames {
		runCtx.RunPath = append(runCtx.RunPath, RunStep{agentName: agentName})
	}

	return setRunCtx(ctx, runCtx)
}

// ClearRunCtx clears the run context of the multi-agents. This is particularly useful
// when a customized agent with a multi-agents inside it is set as a subagent of another
// multi-agents. In such cases, it's not expected to pass the outside run context to the
// inside multi-agents, so this function helps isolate the contexts properly.
//
// ClearRunCtx 清除 multi-agents 的运行 context。
// 当包含 multi-agents 的自定义智能体被设置为另一个 multi-agents 的子智能体时，这尤其有用。
// 在这种情况下，不期望把外部运行 context 传给内部 multi-agents，
// 因此此函数有助于正确隔离 context。
func ClearRunCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, runCtxKey{}, nil)
}

func ctxWithNewTypedRunCtx[M MessageType](ctx context.Context, input *TypedAgentInput[M], sharedParentSession bool) context.Context {
	var session *runSession
	if sharedParentSession {
		if parentSession := getSession(ctx); parentSession != nil {
			session = &runSession{
				Values:    parentSession.Values,
				valuesMtx: parentSession.valuesMtx,
			}
		}
	}
	if session == nil {
		session = newRunSession()
	}
	var zero M
	rc := &runContext{Session: session}
	if _, ok := any(zero).(*schema.Message); ok {
		rc.RootInput = any(input).(*AgentInput)
	} else {
		rc.AgenticRootInput = input
	}
	return setRunCtx(ctx, rc)
}

func getSession(ctx context.Context) *runSession {
	runCtx := getRunCtx(ctx)
	if runCtx != nil {
		return runCtx.Session
	}

	return nil
}
