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

package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/eino-contrib/jsonschema"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/internal/generic"
	"github.com/cloudwego/eino/schema"
)

// InvokeFunc is the function type for the tool.
// InvokeFunc 是工具的函数类型。
type InvokeFunc[T, D any] func(ctx context.Context, input T) (output D, err error)

// OptionableInvokeFunc is the function type for the tool with tool option.
// OptionableInvokeFunc 是带工具选项的工具函数类型。
type OptionableInvokeFunc[T, D any] func(ctx context.Context, input T, opts ...tool.Option) (output D, err error)

// InferTool creates an [tool.InvokableTool] by inferring the parameter JSON
// schema from the fields and tags of the input type T.
//
// The tool automatically JSON-decodes the model's argument string into T before
// calling fn, and JSON-encodes the D return value into the result string.
//
// Use [WithSchemaModifier] in opts to customise how struct tags are mapped to
// JSON schema fields.
//
// InferTool 通过从输入类型 T 的字段和标签推断参数 JSON schema，创建 [tool.InvokableTool]。
// 该工具会在调用 fn 前，自动将模型的参数字符串 JSON 解码为 T，并将 D 返回值 JSON 编码为结果字符串。
// 在 opts 中使用 [WithSchemaModifier] 可自定义 struct tag 到 JSON schema 字段的映射方式。
func InferTool[T, D any](toolName, toolDesc string, i InvokeFunc[T, D], opts ...Option) (tool.InvokableTool, error) {
	ti, err := goStruct2ToolInfo[T](toolName, toolDesc, opts...)
	if err != nil {
		return nil, err
	}

	return NewTool(ti, i, opts...), nil
}

// InferOptionableTool is like [InferTool] but the function also receives
// [tool.Option] values passed by ToolsNode at call time.
//
// InferOptionableTool 类似 [InferTool]，但该函数还会接收 ToolsNode 在调用时传入的 [tool.Option] 值。
func InferOptionableTool[T, D any](toolName, toolDesc string, i OptionableInvokeFunc[T, D], opts ...Option) (tool.InvokableTool, error) {
	ti, err := goStruct2ToolInfo[T](toolName, toolDesc, opts...)
	if err != nil {
		return nil, err
	}

	return newOptionableTool(ti, i, opts...), nil
}

// EnhancedInvokeFunc is the function type for the enhanced tool.
// EnhancedInvokeFunc 是增强工具的函数类型。
type EnhancedInvokeFunc[T any] func(ctx context.Context, input T) (output *schema.ToolResult, err error)

// OptionableEnhancedInvokeFunc is the function type for the enhanced tool with tool option.
// OptionableEnhancedInvokeFunc 是带工具选项的增强工具函数类型。
type OptionableEnhancedInvokeFunc[T any] func(ctx context.Context, input T, opts ...tool.Option) (output *schema.ToolResult, err error)

// InferEnhancedTool creates an [tool.EnhancedInvokableTool] by inferring the
// parameter JSON schema from type T. The function returns a [schema.ToolResult]
// for multimodal output (text, images, audio, video, files).
//
// InferEnhancedTool 通过从类型 T 推断参数 JSON schema，创建 [tool.EnhancedInvokableTool]。
// 该函数返回用于多模态输出（文本、图像、音频、视频、文件）的 [schema.ToolResult]。
func InferEnhancedTool[T any](toolName, toolDesc string, i EnhancedInvokeFunc[T], opts ...Option) (tool.EnhancedInvokableTool, error) {
	ti, err := goStruct2ToolInfo[T](toolName, toolDesc, opts...)
	if err != nil {
		return nil, err
	}

	return NewEnhancedTool(ti, i, opts...), nil
}

// InferOptionableEnhancedTool creates an EnhancedInvokableTool from a given function by inferring the ToolInfo from the function's request parameters, with tool option.
// InferOptionableEnhancedTool 通过从给定函数的请求参数推断 ToolInfo，创建带工具选项的 EnhancedInvokableTool。
func InferOptionableEnhancedTool[T any](toolName, toolDesc string, i OptionableEnhancedInvokeFunc[T], opts ...Option) (tool.EnhancedInvokableTool, error) {
	ti, err := goStruct2ToolInfo[T](toolName, toolDesc, opts...)
	if err != nil {
		return nil, err
	}

	return newOptionableEnhancedTool(ti, i, opts...), nil
}

// GoStruct2ParamsOneOf converts a Go struct's fields and tags into a
// [schema.ParamsOneOf] (JSON Schema 2020-12). Useful for ChatModel structured
// output via ResponseFormat without creating a full tool.
//
// GoStruct2ParamsOneOf 将 Go struct 的字段和标签转换为 [schema.ParamsOneOf] (JSON Schema 2020-12)。
// 适用于通过 ResponseFormat 实现 ChatModel 结构化输出，而无需创建完整工具。
func GoStruct2ParamsOneOf[T any](opts ...Option) (*schema.ParamsOneOf, error) {
	return goStruct2ParamsOneOf[T](opts...)
}

// GoStruct2ToolInfo converts a Go struct into a [schema.ToolInfo]. Useful for
// binding a typed schema to a ChatModel via BindTools for structured output,
// when you do not need a full executable tool.
//
// GoStruct2ToolInfo 将 Go struct 转换为 [schema.ToolInfo]。当不需要完整可执行工具时，可用于通过 BindTools 将类型化 schema 绑定到 ChatModel 以实现结构化输出。
func GoStruct2ToolInfo[T any](toolName, toolDesc string, opts ...Option) (*schema.ToolInfo, error) {
	return goStruct2ToolInfo[T](toolName, toolDesc, opts...)
}

func goStruct2ToolInfo[T any](toolName, toolDesc string, opts ...Option) (*schema.ToolInfo, error) {
	paramsOneOf, err := goStruct2ParamsOneOf[T](opts...)
	if err != nil {
		return nil, err
	}
	return &schema.ToolInfo{
		Name:        toolName,
		Desc:        toolDesc,
		ParamsOneOf: paramsOneOf,
	}, nil
}

func goStruct2ParamsOneOf[T any](opts ...Option) (*schema.ParamsOneOf, error) {
	options := getToolOptions(opts...)

	r := &jsonschema.Reflector{
		Anonymous:      true,
		DoNotReference: true,
		SchemaModifier: jsonschema.SchemaModifierFn(options.scModifier),
	}

	js := r.Reflect(generic.NewInstance[T]())
	js.Version = ""

	paramsOneOf := schema.NewParamsOneOfByJSONSchema(js)

	return paramsOneOf, nil
}

// NewTool creates an [tool.InvokableTool] from an explicit [schema.ToolInfo]
// and a typed function. Use this when the schema cannot be inferred from struct
// tags (e.g. dynamic or complex parameter schemas).
//
// Note: you are responsible for keeping desc.ParamsOneOf consistent with the
// actual fields of T — there is no compile-time check.
//
// NewTool 基于显式的 [schema.ToolInfo] 和类型化函数创建 [tool.InvokableTool]。当无法从 struct tag 推断 schema（如动态或复杂参数 schema）时使用。
// 注意：你需要负责保持 desc.ParamsOneOf 与 T 的实际字段一致——没有编译期检查。
func NewTool[T, D any](desc *schema.ToolInfo, i InvokeFunc[T, D], opts ...Option) tool.InvokableTool {
	return newOptionableTool(desc, func(ctx context.Context, input T, _ ...tool.Option) (D, error) {
		return i(ctx, input)
	}, opts...)
}

func newOptionableTool[T, D any](desc *schema.ToolInfo, i OptionableInvokeFunc[T, D], opts ...Option) tool.InvokableTool {
	to := getToolOptions(opts...)

	return &invokableTool[T, D]{
		info: desc,
		um:   to.um,
		m:    to.m,
		Fn:   i,
	}
}

type invokableTool[T, D any] struct {
	info *schema.ToolInfo

	um UnmarshalArguments
	m  MarshalOutput

	Fn OptionableInvokeFunc[T, D]
}

func (i *invokableTool[T, D]) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return i.info, nil
}

// InvokableRun invokes the tool with the given arguments.
// InvokableRun 使用给定参数调用工具。
func (i *invokableTool[T, D]) InvokableRun(ctx context.Context, arguments string, opts ...tool.Option) (output string, err error) {

	var inst T
	if i.um != nil {
		var val any
		val, err = i.um(ctx, arguments)
		if err != nil {
			return "", fmt.Errorf("[LocalFunc] failed to unmarshal arguments, toolName=%s, err=%w", i.getToolName(), err)
		}
		gt, ok := val.(T)
		if !ok {
			return "", fmt.Errorf("[LocalFunc] invalid type, toolName=%s, expected=%T, given=%T", i.getToolName(), inst, val)
		}
		inst = gt
	} else {
		inst = generic.NewInstance[T]()

		err = sonic.UnmarshalString(arguments, &inst)
		if err != nil {
			return "", fmt.Errorf("[LocalFunc] failed to unmarshal arguments in json, toolName=%s, err=%w", i.getToolName(), err)
		}
	}

	resp, err := i.Fn(ctx, inst, opts...)
	if err != nil {
		return "", fmt.Errorf("[LocalFunc] failed to invoke tool, toolName=%s, err=%w", i.getToolName(), err)
	}

	if i.m != nil {
		output, err = i.m(ctx, resp)
		if err != nil {
			return "", fmt.Errorf("[LocalFunc] failed to marshal output, toolName=%s, err=%w", i.getToolName(), err)
		}
	} else {
		output, err = marshalString(resp)
		if err != nil {
			return "", fmt.Errorf("[LocalFunc] failed to marshal output in json, toolName=%s, err=%w", i.getToolName(), err)
		}
	}

	return output, nil
}

func (i *invokableTool[T, D]) GetType() string {
	return snakeToCamel(i.getToolName())
}

func (i *invokableTool[T, D]) getToolName() string {
	if i.info == nil {
		return ""
	}

	return i.info.Name
}

// snakeToCamel converts a snake_case string to CamelCase.
// snakeToCamel 将 snake_case 字符串转换为 CamelCase。
func snakeToCamel(s string) string {
	if s == "" {
		return ""
	}

	parts := strings.Split(s, "_")

	for i := 0; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(string(parts[i][0])) + strings.ToLower(parts[i][1:])
		}
	}

	return strings.Join(parts, "")
}

// NewEnhancedTool creates an [tool.EnhancedInvokableTool] from an explicit
// [schema.ToolInfo] and a function that returns [schema.ToolResult].
//
// NewEnhancedTool 基于显式的 [schema.ToolInfo] 和返回 [schema.ToolResult] 的函数创建 [tool.EnhancedInvokableTool]。
func NewEnhancedTool[T any](desc *schema.ToolInfo, i EnhancedInvokeFunc[T], opts ...Option) tool.EnhancedInvokableTool {
	return newOptionableEnhancedTool(desc, func(ctx context.Context, input T, _ ...tool.Option) (*schema.ToolResult, error) {
		return i(ctx, input)
	}, opts...)
}

func newOptionableEnhancedTool[T any](desc *schema.ToolInfo, i OptionableEnhancedInvokeFunc[T], opts ...Option) tool.EnhancedInvokableTool {
	to := getToolOptions(opts...)

	return &enhancedInvokableTool[T]{
		info: desc,
		um:   to.um,
		Fn:   i,
	}
}

type enhancedInvokableTool[T any] struct {
	info *schema.ToolInfo

	um UnmarshalArguments

	Fn OptionableEnhancedInvokeFunc[T]
}

func (e *enhancedInvokableTool[T]) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return e.info, nil
}

func (e *enhancedInvokableTool[T]) InvokableRun(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
	var inst T
	var err error

	if e.um != nil {
		var val any
		val, err = e.um(ctx, toolArgument.Text)
		if err != nil {
			return nil, fmt.Errorf("[EnhancedLocalFunc] failed to unmarshal arguments, toolName=%s, err=%w", e.getToolName(), err)
		}
		gt, ok := val.(T)
		if !ok {
			return nil, fmt.Errorf("[EnhancedLocalFunc] invalid type, toolName=%s, expected=%T, given=%T", e.getToolName(), inst, val)
		}
		inst = gt
	} else {
		inst = generic.NewInstance[T]()

		err = sonic.UnmarshalString(toolArgument.Text, &inst)
		if err != nil {
			return nil, fmt.Errorf("[EnhancedLocalFunc] failed to unmarshal arguments in json, toolName=%s, err=%w", e.getToolName(), err)
		}
	}

	resp, err := e.Fn(ctx, inst, opts...)
	if err != nil {
		return nil, fmt.Errorf("[EnhancedLocalFunc] failed to invoke tool, toolName=%s, err=%w", e.getToolName(), err)
	}

	return resp, nil
}

func (e *enhancedInvokableTool[T]) GetType() string {
	return snakeToCamel(e.getToolName())
}

func (e *enhancedInvokableTool[T]) getToolName() string {
	if e.info == nil {
		return ""
	}

	return e.info.Name
}
