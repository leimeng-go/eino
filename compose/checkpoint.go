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

	"github.com/cloudwego/eino/internal/core"
	"github.com/cloudwego/eino/internal/serialization"
	"github.com/cloudwego/eino/schema"
)

func init() {
	schema.RegisterName[*checkpoint]("_eino_checkpoint")
	schema.RegisterName[*dagChannel]("_eino_dag_channel")
	schema.RegisterName[*pregelChannel]("_eino_pregel_channel")
	schema.RegisterName[dependencyState]("_eino_dependency_state")
	_ = serialization.GenericRegister[channel]("_eino_channel")
}

// RegisterSerializableType registers a custom type for eino serialization.
// This allows eino to properly serialize and deserialize custom types.
// Both custom interfaces and structs need to be registered using this function.
// Types only need to be registered once - pointers and other references will be handled automatically.
// All built-in eino types are already registered.
// Parameters:
// - name: A unique identifier for the type being registered (should not start with "_eino")
// - T: The generic type parameter representing the type to register
// Returns:
// - error: An error if registration fails (e.g., if the type is already registered)
// Deprecated: RegisterSerializableType is deprecated. Use schema.RegisterName[T](name) instead.
//
// RegisterSerializableType 为 eino 序列化注册自定义类型。
// 这使 eino 能正确序列化和反序列化自定义类型。
// 自定义接口和结构体都需要使用此函数注册。
// 类型只需注册一次，指针和其他引用会自动处理。
// 所有内置 eino 类型均已注册。
// 参数：
// - name：被注册类型的唯一标识符（不应以 "_eino" 开头）
// - T：表示要注册类型的泛型类型参数
// 返回：
// - error：注册失败时的错误（例如类型已注册）
// 已废弃：RegisterSerializableType 已废弃。请改用 schema.RegisterName[T](name)。
func RegisterSerializableType[T any](name string) (err error) {
	return serialization.GenericRegister[T](name)
}

type CheckPointStore = core.CheckPointStore

type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// WithCheckPointStore sets the checkpoint store implementation for a graph.
// WithCheckPointStore 为图设置检查点存储实现。
func WithCheckPointStore(store CheckPointStore) GraphCompileOption {
	return func(o *graphCompileOptions) {
		o.checkPointStore = store
	}
}

// WithSerializer sets the serializer used to persist checkpoint state.
// WithSerializer 设置用于持久化检查点状态的序列化器。
func WithSerializer(serializer Serializer) GraphCompileOption {
	return func(o *graphCompileOptions) {
		o.serializer = serializer
	}
}

// WithCheckPointID sets the checkpoint ID to load from and write to by default.
// WithCheckPointID 设置默认加载和写入的检查点 ID。
func WithCheckPointID(checkPointID string) Option {
	return Option{
		checkPointID: &checkPointID,
	}
}

// WithWriteToCheckPointID specifies a different checkpoint ID to write to.
// If not provided, the checkpoint ID from WithCheckPointID will be used for writing.
// This is useful for scenarios where you want to load from an existed checkpoint
// but save the progress to a new, separate checkpoint.
//
// WithWriteToCheckPointID 指定另一个用于写入的检查点 ID。
// 如果未提供，将使用 WithCheckPointID 中的检查点 ID 进行写入。
// 这适用于想从已有检查点加载，
// 但将进度保存到新的独立检查点的场景。
func WithWriteToCheckPointID(checkPointID string) Option {
	return Option{
		writeToCheckPointID: &checkPointID,
	}
}

// WithForceNewRun forces the graph to run from the beginning, ignoring any checkpoints.
// WithForceNewRun 强制图从头运行，忽略任何检查点。
func WithForceNewRun() Option {
	return Option{
		forceNewRun: true,
	}
}

// StateModifier modifies state during checkpoint operations for a given node path.
// StateModifier 在给定节点路径的检查点操作期间修改状态。
type StateModifier func(ctx context.Context, path NodePath, state any) error

// WithStateModifier installs a state modifier invoked during checkpoint read/write.
// WithStateModifier 安装在检查点读写期间调用的状态修改器。
func WithStateModifier(sm StateModifier) Option {
	return Option{
		stateModifier: sm,
	}
}

type checkpoint struct {
	Channels       map[string]channel
	Inputs         map[string] /*node key*/ any /*input*/
	State          any
	SkipPreHandler map[string]bool
	RerunNodes     []string

	SubGraphs map[string]*checkpoint

	InterruptID2Addr  map[string]Address
	InterruptID2State map[string]core.InterruptState
}

type stateModifierKey struct{}
type checkPointKey struct{} // *checkpoint

func getStateModifier(ctx context.Context) StateModifier {
	if sm, ok := ctx.Value(stateModifierKey{}).(StateModifier); ok {
		return sm
	}
	return nil
}

func setStateModifier(ctx context.Context, modifier StateModifier) context.Context {
	return context.WithValue(ctx, stateModifierKey{}, modifier)
}

func getCheckPointFromStore(ctx context.Context, id string, cpr *checkPointer) (cp *checkpoint, err error) {
	cp, existed, err := cpr.get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !existed {
		return nil, nil
	}

	return cp, nil
}

func setCheckPointToCtx(ctx context.Context, cp *checkpoint) context.Context {
	ctx = core.PopulateInterruptState(ctx, cp.InterruptID2Addr, cp.InterruptID2State)
	return context.WithValue(ctx, checkPointKey{}, cp)
}

func getCheckPointFromCtx(ctx context.Context) *checkpoint {
	if cp, ok := ctx.Value(checkPointKey{}).(*checkpoint); ok {
		return cp
	}
	return nil
}

func forwardCheckPoint(ctx context.Context, nodeKey string) context.Context {
	cp := getCheckPointFromCtx(ctx)
	if cp == nil {
		return ctx
	}

	if subCP, ok := cp.SubGraphs[nodeKey]; ok {
		delete(cp.SubGraphs, nodeKey) // only forward once
		// 只转发一次
		return context.WithValue(ctx, checkPointKey{}, subCP)
	}
	return context.WithValue(ctx, checkPointKey{}, (*checkpoint)(nil))
}

func newCheckPointer(
	inputPairs, outputPairs map[string]streamConvertPair,
	store CheckPointStore,
	serializer Serializer,
) *checkPointer {
	if serializer == nil {
		serializer = &serialization.InternalSerializer{}
	}
	return &checkPointer{
		sc:         newStreamConverter(inputPairs, outputPairs),
		store:      store,
		serializer: serializer,
	}
}

type checkPointer struct {
	sc         *streamConverter
	store      CheckPointStore
	serializer Serializer
}

func (c *checkPointer) get(ctx context.Context, id string) (*checkpoint, bool, error) {
	data, existed, err := c.store.Get(ctx, id)
	if err != nil || existed == false {
		return nil, existed, err
	}

	cp := &checkpoint{}
	err = c.serializer.Unmarshal(data, cp)
	if err != nil {
		return nil, false, err
	}

	return cp, true, nil
}

func (c *checkPointer) set(ctx context.Context, id string, cp *checkpoint) error {
	normalizeCheckpointTypedNilInputs(cp)

	data, err := c.serializer.Marshal(cp)
	if err != nil {
		return err
	}

	return c.store.Set(ctx, id, data)
}

func normalizeCheckpointTypedNilInputs(cp *checkpoint) {
	if cp == nil {
		return
	}
	for key, input := range cp.Inputs {
		if isTypedNil(input) {
			cp.Inputs[key] = nil
		}
	}
	for _, sub := range cp.SubGraphs {
		normalizeCheckpointTypedNilInputs(sub)
	}
}

func isTypedNil(v any) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// MigrateCheckpointState is an advanced compatibility utility for checkpoint upgrades.
//
// It decodes checkpoint bytes using the given serializer, applies migrate to checkpoint.State and
// all nested SubGraphs' states, then re-encodes the checkpoint.
//
// Typical use cases:
//   - Resume-time migration when you changed your graph state type/schema and need to load old
//     checkpoints without discarding them.
//   - Framework-level backward compatibility (e.g. ADK upgrading checkpoints across versions).
//
// Migrate callback contract:
//   - Returns (newState, changed, error).
//   - If changed is false, the state is left as-is.
//   - If error is non-nil, migration stops and the error is returned to the caller.
//
// The original bytes are returned only if no state was changed anywhere in the checkpoint tree.
//
// MigrateCheckpointState 是用于检查点升级的高级兼容性工具。
// 它使用给定的序列化器解码检查点字节，将 migrate 应用于 checkpoint.State 以及所有嵌套 SubGraphs 的状态，然后重新编码检查点。
// 典型用例：
// - 当你更改了图状态类型/schema，需要在 Resume 时迁移以加载旧检查点而不丢弃它们。
// - 框架级向后兼容（例如 ADK 跨版本升级检查点）。
// Migrate 回调约定：
// - 返回 (newState, changed, error)。
// - 如果 changed 为 false，状态保持不变。
// - 如果 error 非 nil，迁移停止并将错误返回给调用方。
// 仅当检查点树中任何位置的状态都未改变时，才返回原始字节。
func MigrateCheckpointState(data []byte, serializer Serializer, migrate func(state any) (any, bool, error)) ([]byte, error) {
	cp := &checkpoint{}
	if err := serializer.Unmarshal(data, cp); err != nil {
		return nil, err
	}
	changed, err := migrateCheckpoint(cp, migrate)
	if err != nil {
		return nil, err
	}
	if !changed {
		return data, nil
	}
	return serializer.Marshal(cp)
}

// migrateCheckpoint recursively applies migrate to cp.State and all SubGraphs.
// migrateCheckpoint 递归地将 migrate 应用于 cp.State 和所有 SubGraphs。
func migrateCheckpoint(cp *checkpoint, migrate func(state any) (any, bool, error)) (bool, error) {
	anyChanged := false
	if cp.State != nil {
		newState, changed, err := migrate(cp.State)
		if err != nil {
			return false, err
		}
		if changed {
			cp.State = newState
			anyChanged = true
		}
	}
	for _, sub := range cp.SubGraphs {
		changed, err := migrateCheckpoint(sub, migrate)
		if err != nil {
			return false, err
		}
		if changed {
			anyChanged = true
		}
	}
	return anyChanged, nil
}

// convertCheckPoint if value in checkpoint is streamReader, convert it to non-stream
// convertCheckPoint 如果检查点中的值是 streamReader，则将其转换为非流
func (c *checkPointer) convertCheckPoint(cp *checkpoint, isStream bool) (err error) {
	for _, ch := range cp.Channels {
		err = ch.convertValues(func(m map[string]any) error {
			return c.sc.convertOutputs(isStream, m)
		})
		if err != nil {
			return err
		}
	}

	err = c.sc.convertInputs(isStream, cp.Inputs)
	if err != nil {
		return err
	}

	return nil
}

// convertCheckPoint convert values in checkpoint to streamReader if needed
// convertCheckPoint 在需要时将检查点中的值转换为 streamReader
func (c *checkPointer) restoreCheckPoint(cp *checkpoint, isStream bool) (err error) {
	for _, ch := range cp.Channels {
		err = ch.convertValues(func(m map[string]any) error {
			return c.sc.restoreOutputs(isStream, m)
		})
		if err != nil {
			return err
		}
	}

	err = c.sc.restoreInputs(isStream, cp.Inputs)
	if err != nil {
		return err
	}

	return nil
}

func newStreamConverter(inputPairs, outputPairs map[string]streamConvertPair) *streamConverter {
	return &streamConverter{
		inputPairs:  inputPairs,
		outputPairs: outputPairs,
	}
}

type streamConverter struct {
	inputPairs, outputPairs map[string]streamConvertPair
}

func (s *streamConverter) convertInputs(isStream bool, values map[string]any) error {
	return convert(values, s.inputPairs, isStream)
}

func (s *streamConverter) restoreInputs(isStream bool, values map[string]any) error {
	return restore(values, s.inputPairs, isStream)
}

func (s *streamConverter) convertOutputs(isStream bool, values map[string]any) error {
	return convert(values, s.outputPairs, isStream)
}

func (s *streamConverter) restoreOutputs(isStream bool, values map[string]any) error {
	return restore(values, s.outputPairs, isStream)
}

func convert(values map[string]any, convPairs map[string]streamConvertPair, isStream bool) error {
	if !isStream {
		return nil
	}
	for key, v := range values {
		convPair, ok := convPairs[key]
		if !ok {
			return fmt.Errorf("checkpoint conv stream fail, node[%s] have not been registered", key)
		}
		sr, ok := v.(streamReader)
		if !ok {
			return fmt.Errorf("checkpoint conv stream fail, value of [%s] isn't stream", key)
		}
		nValue, err := convPair.concatStream(sr)
		if err != nil {
			return err
		}
		values[key] = nValue
	}
	return nil
}

func restore(values map[string]any, convPairs map[string]streamConvertPair, isStream bool) error {
	if !isStream {
		return nil
	}
	for key, v := range values {
		convPair, ok := convPairs[key]
		if !ok {
			return fmt.Errorf("checkpoint restore stream fail, node[%s] have not been registered", key)
		}
		sr, err := convPair.restoreStream(v)
		if err != nil {
			return err
		}
		values[key] = sr
	}
	return nil
}
