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
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"

	"github.com/bytedance/sonic"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/internal/safe"
	"github.com/cloudwego/eino/schema"
)

type toolsNodeOptions struct {
	ToolOptions []tool.Option
	ToolList    []tool.BaseTool

	ToolAliases map[string]ToolAliasConfig
}

// ToolsNodeOption is the option func type for ToolsNode.
// ToolsNodeOption 是 ToolsNode 的选项函数类型。
type ToolsNodeOption func(o *toolsNodeOptions)

// WithToolOption adds tool options to the ToolsNode.
// WithToolOption 为 ToolsNode 添加工具选项。
func WithToolOption(opts ...tool.Option) ToolsNodeOption {
	return func(o *toolsNodeOptions) {
		o.ToolOptions = append(o.ToolOptions, opts...)
	}
}

// WithToolList sets the tool list for the ToolsNode.
// WithToolList 设置 ToolsNode 的工具列表。
func WithToolList(tool ...tool.BaseTool) ToolsNodeOption {
	return func(o *toolsNodeOptions) {
		o.ToolList = tool
	}
}

// WithToolAliases sets the tool aliases for the ToolsNode call option.
// When used with WithToolList, it overrides the global alias configuration for the dynamic tool list.
// When used alone (without WithToolList), it replaces the global alias configuration while keeping the original tool list.
//
// WithToolAliases 设置 ToolsNode 调用选项的工具别名。
// 与 WithToolList 一起使用时，它会覆盖动态工具列表的全局别名配置。
// 单独使用（不带 WithToolList）时，它会保留原工具列表并替换全局别名配置。
func WithToolAliases(toolAliases map[string]ToolAliasConfig) ToolsNodeOption {
	return func(o *toolsNodeOptions) {
		o.ToolAliases = toolAliases
	}
}

// ToolsNode represents a node capable of executing tools within a graph.
// The Graph Node interface is defined as follows:
//
//	Invoke(ctx context.Context, input *schema.Message, opts ...ToolsNodeOption) ([]*schema.Message, error)
//	Stream(ctx context.Context, input *schema.Message, opts ...ToolsNodeOption) (*schema.StreamReader[[]*schema.Message], error)
//
// Input: An AssistantMessage containing ToolCalls
// Output: An array of ToolMessage where the order of elements corresponds to the order of ToolCalls in the input
//
// ToolsNode 表示图中能够执行工具的节点。
// Graph Node 接口定义如下：
// Invoke(ctx context.Context, input *schema.Message, opts ...ToolsNodeOption) ([]*schema.Message, error)
// Stream(ctx context.Context, input *schema.Message, opts ...ToolsNodeOption) (*schema.StreamReader[[]*schema.Message], error)
// 输入：包含 ToolCalls 的 AssistantMessage
// 输出：ToolMessage 数组，元素顺序与输入中 ToolCalls 的顺序对应
type ToolsNode struct {
	tuple                             *toolsTuple
	tools                             []tool.BaseTool
	unknownToolHandler                func(ctx context.Context, name, input string) (string, error)
	executeSequentially               bool
	toolArgumentsHandler              func(ctx context.Context, name, input string) (string, error)
	toolCallMiddlewares               []InvokableToolMiddleware
	streamToolCallMiddlewares         []StreamableToolMiddleware
	enhancedToolCallMiddlewares       []EnhancedInvokableToolMiddleware
	enhancedStreamToolCallMiddlewares []EnhancedStreamableToolMiddleware
	toolAliasConfigs                  map[string]ToolAliasConfig
}

// ToolInput represents the input parameters for a tool call execution.
// ToolInput 表示执行工具调用的输入参数。
type ToolInput struct {
	// Name is the name of the tool to be executed.
	// Name 是要执行的工具名称。
	Name string
	// Arguments contains the arguments for the tool call.
	// Arguments 包含工具调用的参数。
	Arguments string
	// CallID is the unique identifier for this tool call.
	// CallID 是此工具调用的唯一标识符。
	CallID string
	// CallOptions contains tool options for the execution.
	// CallOptions 包含执行所用的工具选项。
	CallOptions []tool.Option
}

// ToolOutput represents the result of a non-streaming tool call execution.
// ToolOutput 表示非流式工具调用执行的结果。
type ToolOutput struct {
	// Result contains the string output from the tool execution.
	// Result 包含工具执行返回的字符串输出。
	Result string
}

// StreamToolOutput represents the result of a streaming tool call execution.
// StreamToolOutput 表示流式工具调用执行的结果。
type StreamToolOutput struct {
	// Result is a stream reader that provides access to the tool's streaming output.
	// Result 是流读取器，用于访问工具的流式输出。
	Result *schema.StreamReader[string]
}

// EnhancedInvokableToolOutput represents the result of a non-streaming enhanced tool call execution.
// It supports returning structured multimodal content (text, images, audio, video, files) from tools.
//
// EnhancedInvokableToolOutput 表示非流式增强工具调用执行的结果。
// 它支持工具返回结构化多模态内容（text、images、audio、video、files）。
type EnhancedInvokableToolOutput struct {
	// Result contains the structured multimodal output from the tool execution.
	// Result 包含工具执行返回的结构化多模态输出。
	Result *schema.ToolResult
}

// EnhancedStreamableToolOutput represents the result of a streaming enhanced tool call execution.
// It provides a stream reader for accessing multimodal content progressively.
//
// EnhancedStreamableToolOutput 表示流式增强工具调用执行的结果。
// 它提供流读取器，用于渐进式访问多模态内容。
type EnhancedStreamableToolOutput struct {
	// Result is a stream reader that provides access to the tool's streaming multimodal output.
	// Result 是流读取器，用于访问工具的流式多模态输出。
	Result *schema.StreamReader[*schema.ToolResult]
}

// InvokableToolEndpoint is the function signature for non-streaming tool calls.
// InvokableToolEndpoint 是非流式工具调用的函数签名。
type InvokableToolEndpoint func(ctx context.Context, input *ToolInput) (*ToolOutput, error)

// StreamableToolEndpoint is the function signature for streaming tool calls.
// StreamableToolEndpoint 是流式工具调用的函数签名。
type StreamableToolEndpoint func(ctx context.Context, input *ToolInput) (*StreamToolOutput, error)

type EnhancedInvokableToolEndpoint func(ctx context.Context, input *ToolInput) (*EnhancedInvokableToolOutput, error)

type EnhancedStreamableToolEndpoint func(ctx context.Context, input *ToolInput) (*EnhancedStreamableToolOutput, error)

// InvokableToolMiddleware is a function that wraps InvokableToolEndpoint to add custom processing logic.
// It can be used to intercept, modify, or enhance tool call execution for non-streaming tools.
//
// InvokableToolMiddleware 是包装 InvokableToolEndpoint 以添加自定义处理逻辑的函数。
// 它可用于拦截、修改或增强非流式工具的工具调用执行。
type InvokableToolMiddleware func(InvokableToolEndpoint) InvokableToolEndpoint

// StreamableToolMiddleware is a function that wraps StreamableToolEndpoint to add custom processing logic.
// It can be used to intercept, modify, or enhance tool call execution for streaming tools.
//
// StreamableToolMiddleware 是包装 StreamableToolEndpoint 以添加自定义处理逻辑的函数。
// 它可用于拦截、修改或增强流式工具的工具调用执行。
type StreamableToolMiddleware func(StreamableToolEndpoint) StreamableToolEndpoint

type EnhancedInvokableToolMiddleware func(EnhancedInvokableToolEndpoint) EnhancedInvokableToolEndpoint

type EnhancedStreamableToolMiddleware func(EnhancedStreamableToolEndpoint) EnhancedStreamableToolEndpoint

// ToolMiddleware groups middleware hooks for invokable and streamable tool calls.
// ToolMiddleware 将可调用和可流式工具调用的 middleware hooks 分组。
type ToolMiddleware struct {
	// Invokable contains middleware function for non-streaming tool calls.
	// Note: This middleware only applies to tools that implement the InvokableTool interface.
	//
	// Invokable 包含非流式工具调用的 middleware function。
	// 注意：此 middleware 仅适用于实现 InvokableTool interface 的工具。
	Invokable InvokableToolMiddleware

	// Streamable contains middleware function for streaming tool calls.
	// Note: This middleware only applies to tools that implement the StreamableTool interface.
	//
	// Streamable 包含流式工具调用的 middleware function。
	// 注意：此 middleware 仅适用于实现 StreamableTool interface 的工具。
	Streamable StreamableToolMiddleware

	// EnhancedInvokable contains middleware function for non-streaming enhanced tool calls.
	// Note: This middleware only applies to tools that implement the EnhancedInvokableTool interface.
	//
	// EnhancedInvokable 包含非流式增强工具调用的 middleware function。
	// 注意：此 middleware 仅适用于实现 EnhancedInvokableTool interface 的工具。
	EnhancedInvokable EnhancedInvokableToolMiddleware

	// EnhancedStreamable contains middleware function for streaming enhanced tool calls.
	// Note: This middleware only applies to tools that implement the EnhancedStreamableTool interface.
	//
	// EnhancedStreamable 包含用于流式增强工具调用的中间件函数。
	// 注意：此中间件仅适用于实现 EnhancedStreamableTool 接口的工具。
	EnhancedStreamable EnhancedStreamableToolMiddleware
}

// ToolAliasConfig configures name and argument aliases for a single tool.
// ToolAliasConfig 配置单个工具的名称和参数别名。
type ToolAliasConfig struct {
	// NameAliases are alternative names for this tool.
	// If the model returns any of these names, it will be resolved to the canonical tool name.
	//
	// NameAliases 是此工具的替代名称。
	// 如果模型返回其中任一名称，会解析为规范工具名称。
	NameAliases []string

	// ArgumentsAliases maps canonical argument keys to their alias lists.
	// key=canonical, value=[]alias. Applied to top-level JSON keys before tool execution.
	// Example: {"query": ["q", "search_term"], "limit": ["max_results", "count"]}
	//
	// ArgumentsAliases 将规范参数键映射到其别名列表。
	// key=canonical，value=[]alias。在工具执行前应用于顶层 JSON 键。
	// 示例：{"query": ["q", "search_term"], "limit": ["max_results", "count"]}
	ArgumentsAliases map[string][]string
}

// ToolsNodeConfig is the config for ToolsNode.
// ToolsNodeConfig 是 ToolsNode 的配置。
type ToolsNodeConfig struct {
	// Tools specify the list of tools can be called which are BaseTool but must implement InvokableTool or StreamableTool.
	// Tools 指定可调用的工具列表，这些工具是 BaseTool，但必须实现 InvokableTool 或 StreamableTool。
	Tools []tool.BaseTool

	// ToolAliases configures name and argument aliases for tools.
	// Key is the canonical tool name, value defines its aliases.
	// This field is optional. When provided, tool name aliases will be resolved during tool dispatch,
	// and argument aliases will be remapped before ToolArgumentsHandler (if configured) and tool execution.
	// Execution order: ArgumentsAliases remapping → ToolArgumentsHandler → tool execution
	//
	// ToolAliases 配置工具的名称和参数别名。
	// 键为规范工具名称，值定义其别名。
	// 此字段可选。提供后，工具分发时会解析工具名称别名，
	// 并在 ToolArgumentsHandler（如已配置）和工具执行前重映射参数别名。
	// 执行顺序：ArgumentsAliases 重映射 → ToolArgumentsHandler → 工具执行
	ToolAliases map[string]ToolAliasConfig

	// UnknownToolsHandler handles tool calls for non-existent tools when LLM hallucinates.
	// This field is optional. When not set, calling a non-existent tool will result in an error.
	// When provided, if the LLM attempts to call a tool that doesn't exist in the Tools list,
	// this handler will be invoked instead of returning an error, allowing graceful handling of hallucinated tools.
	// Parameters:
	//   - ctx: The context for the tool call
	//   - name: The name of the non-existent tool
	//   - input: The tool call input generated by llm
	// Returns:
	//   - string: The response to be returned as if the tool was executed
	//   - error: Any error that occurred during handling
	//
	// UnknownToolsHandler 处理 LLM 产生幻觉时对不存在工具的工具调用。
	// 此字段可选。未设置时，调用不存在的工具会导致错误。
	// 提供后，如果 LLM 尝试调用 Tools 列表中不存在的工具，
	// 将调用此处理器而不是返回错误，从而优雅处理幻觉工具。
	// 参数：
	// - ctx：工具调用的 context
	// - name：不存在工具的名称
	// - input：llm 生成的工具调用输入
	// 返回：
	// - string：像工具已执行一样返回的响应
	// - error：处理期间发生的任何错误
	UnknownToolsHandler func(ctx context.Context, name, input string) (string, error)

	// ExecuteSequentially determines whether tool calls should be executed sequentially (in order) or in parallel.
	// When set to true, tool calls will be executed one after another in the order they appear in the input message.
	// When set to false (default), tool calls will be executed in parallel.
	//
	// ExecuteSequentially 决定工具调用应按顺序串行执行还是并行执行。
	// 设为 true 时，工具调用会按其在输入消息中出现的顺序逐个执行。
	// 设为 false（默认）时，工具调用会并行执行。
	ExecuteSequentially bool

	// ToolArgumentsHandler allows handling of tool arguments before execution.
	// When provided, this function will be called for each tool call to process the arguments.
	// Parameters:
	//   - ctx: The context for the tool call
	//   - name: The name of the tool being called
	//   - arguments: The original arguments string for the tool
	// Returns:
	//   - string: The processed arguments string to be used for tool execution
	//   - error: Any error that occurred during preprocessing
	//
	// ToolArgumentsHandler 允许在执行前处理工具参数。
	// 提供后，将为每个工具调用调用此函数以处理参数。
	// 参数：
	// - ctx：工具调用的 context
	// - name：被调用工具的名称
	// - arguments：工具的原始参数字符串
	// 返回：
	// - string：用于工具执行的处理后参数字符串
	// - error：预处理期间发生的任何错误
	ToolArgumentsHandler func(ctx context.Context, name, arguments string) (string, error)

	// ToolCallMiddlewares configures middleware for tool calls.
	// Each element can contain Invokable and/or Streamable middleware.
	// Invokable middleware only applies to tools implementing InvokableTool interface.
	// Streamable middleware only applies to tools implementing StreamableTool interface.
	//
	// ToolCallMiddlewares 配置工具调用的中间件。
	// 每个元素可包含 Invokable 和/或 Streamable 中间件。
	// Invokable 中间件仅适用于实现 InvokableTool 接口的工具。
	// Streamable 中间件仅适用于实现 StreamableTool 接口的工具。
	ToolCallMiddlewares []ToolMiddleware
}

// NewToolNode creates a new ToolsNode.
// e.g.
//
//	conf := &ToolsNodeConfig{
//		Tools: []tool.BaseTool{invokableTool1, streamableTool2},
//	}
//	toolsNode, err := NewToolNode(ctx, conf)
//
// NewToolNode 创建新的 ToolsNode。
// 例如：
// conf := &ToolsNodeConfig{
// Tools: []tool.BaseTool{invokableTool1, streamableTool2},
// }
// toolsNode, err := NewToolNode(ctx, conf)
func NewToolNode(ctx context.Context, conf *ToolsNodeConfig) (*ToolsNode, error) {
	var middlewares []InvokableToolMiddleware
	var streamMiddlewares []StreamableToolMiddleware
	var enhancedInvokableMiddlewares []EnhancedInvokableToolMiddleware
	var enhancedStreamableMiddlewares []EnhancedStreamableToolMiddleware

	for _, m := range conf.ToolCallMiddlewares {
		if m.Invokable != nil {
			middlewares = append(middlewares, m.Invokable)
		}
		if m.Streamable != nil {
			streamMiddlewares = append(streamMiddlewares, m.Streamable)
		}
		if m.EnhancedInvokable != nil {
			enhancedInvokableMiddlewares = append(enhancedInvokableMiddlewares, m.EnhancedInvokable)
		}
		if m.EnhancedStreamable != nil {
			enhancedStreamableMiddlewares = append(enhancedStreamableMiddlewares, m.EnhancedStreamable)
		}
	}

	params := convToolsParams{
		tools:        conf.Tools,
		aliasConfigs: conf.ToolAliases,
	}
	params.middlewares.invokable = middlewares
	params.middlewares.streamable = streamMiddlewares
	params.middlewares.enhancedInvokable = enhancedInvokableMiddlewares
	params.middlewares.enhancedStreamable = enhancedStreamableMiddlewares
	tuple, err := convTools(ctx, params)
	if err != nil {
		return nil, err
	}

	return &ToolsNode{
		tuple:                             tuple,
		tools:                             conf.Tools,
		unknownToolHandler:                conf.UnknownToolsHandler,
		executeSequentially:               conf.ExecuteSequentially,
		toolArgumentsHandler:              conf.ToolArgumentsHandler,
		toolCallMiddlewares:               middlewares,
		streamToolCallMiddlewares:         streamMiddlewares,
		enhancedToolCallMiddlewares:       enhancedInvokableMiddlewares,
		enhancedStreamToolCallMiddlewares: enhancedStreamableMiddlewares,
		toolAliasConfigs:                  conf.ToolAliases,
	}, nil
}

// ToolsInterruptAndRerunExtra carries interrupt metadata for ToolsNode reruns.
// ToolsInterruptAndRerunExtra 携带 ToolsNode 重新运行的中断元数据。
type ToolsInterruptAndRerunExtra struct {
	// ToolCalls contains all tool calls from the original assistant message.
	// ToolCalls 包含原始 assistant 消息中的所有工具调用。
	ToolCalls []schema.ToolCall

	// ExecutedTools maps tool call IDs to their string output for successfully executed standard tools.
	// ExecutedTools 将工具调用 ID 映射到已成功执行的标准工具的字符串输出。
	ExecutedTools map[string]string

	// ExecutedEnhancedTools maps tool call IDs to their structured multimodal output for successfully executed enhanced tools.
	// ExecutedEnhancedTools 将工具调用 ID 映射到已成功执行的增强工具的结构化多模态输出。
	ExecutedEnhancedTools map[string]*schema.ToolResult

	// RerunTools contains the IDs of tool calls that need to be re-executed.
	// RerunTools 包含需要重新执行的工具调用 ID。
	RerunTools []string

	// RerunExtraMap stores additional metadata for each tool call that needs rerun, keyed by tool call ID.
	// RerunExtraMap 存储每个需要重新运行的工具调用的附加元数据，以工具调用 ID 为键。
	RerunExtraMap map[string]any
}

func init() {
	schema.RegisterName[*ToolsInterruptAndRerunExtra]("_eino_compose_tools_interrupt_and_rerun_extra")
	schema.RegisterName[*toolsInterruptAndRerunState]("_eino_compose_tools_interrupt_and_rerun_state")
}

type toolsInterruptAndRerunState struct {
	Input                 *schema.Message
	ExecutedTools         map[string]string
	ExecutedEnhancedTools map[string]*schema.ToolResult
	RerunTools            []string
}

type toolsTuple struct {
	indexes                     map[string]int
	meta                        []*executorMeta
	endpoints                   []InvokableToolEndpoint
	streamEndpoints             []StreamableToolEndpoint
	enhancedInvokableEndpoints  []EnhancedInvokableToolEndpoint
	enhancedStreamableEndpoints []EnhancedStreamableToolEndpoint
	// argsAliasMap stores reverse argument alias mappings for each tool.
	// key: canonical tool name, value: map[aliasKey]canonicalKey (alias → canonical direction)
	//
	// argsAliasMap 存储每个工具的反向参数别名映射。
	// key：规范工具名称，value：map[aliasKey]canonicalKey（alias → canonical 方向）
	argsAliasMap map[string]map[string]string
	// canonicalNames stores the canonical name for each tool index
	// canonicalNames 存储每个工具索引对应的规范名称
	canonicalNames []string
	// toolInfos stores the ToolInfo for each tool index, used for alias validation
	// toolInfos 存储每个工具索引对应的 ToolInfo，用于别名校验
	toolInfos []*schema.ToolInfo
}

// remapArgs replaces alias keys in the JSON arguments string with canonical keys.
// aliasMap: alias → canonical mapping
//
// remapArgs 将 JSON 参数字符串中的别名键替换为规范键。
// aliasMap: alias → canonical 映射
func remapArgs(args string, aliasMap map[string]string) (string, error) {
	if len(aliasMap) == 0 {
		return args, nil
	}

	trimmed := strings.TrimSpace(args)
	if trimmed == "" || trimmed[0] != '{' {
		return args, nil
	}

	var m map[string]json.RawMessage
	if err := sonic.Unmarshal([]byte(args), &m); err != nil {
		return args, nil
	}

	changed := false
	for alias, canonical := range aliasMap {
		if v, ok := m[alias]; ok {
			// Only replace if canonical key doesn't exist.
			// If both alias and canonical are present (e.g. {"q":"a","query":"b"}),
			// the alias key is kept as-is and passed through as an unknown field.
			//
			// 仅在规范键不存在时替换。
			// 如果别名和规范键同时存在（例如 {"q":"a","query":"b"}），
			// 别名键会保持原样，并作为未知字段传递。
			if _, exists := m[canonical]; !exists {
				m[canonical] = v
				delete(m, alias)
				changed = true
			}
		}
	}

	if !changed {
		return args, nil
	}

	b, err := sonic.Marshal(m)
	return string(b), err
}

type convToolsParams struct {
	tools       []tool.BaseTool
	middlewares struct {
		invokable          []InvokableToolMiddleware
		streamable         []StreamableToolMiddleware
		enhancedInvokable  []EnhancedInvokableToolMiddleware
		enhancedStreamable []EnhancedStreamableToolMiddleware
	}
	aliasConfigs map[string]ToolAliasConfig
}

func (t *toolsTuple) applyAliasConfigs(aliasConfigs map[string]ToolAliasConfig) error {
	t.argsAliasMap = make(map[string]map[string]string)

	sortedToolNames := make([]string, 0, len(aliasConfigs))
	for toolName := range aliasConfigs {
		sortedToolNames = append(sortedToolNames, toolName)
	}
	sort.Strings(sortedToolNames)

	for _, toolName := range sortedToolNames {
		aliasConfig := aliasConfigs[toolName]
		var (
			toolIdx int
			exists  bool
		)
		if toolIdx, exists = t.indexes[toolName]; !exists {
			continue
		}

		if err := t.applyNameAliases(toolName, toolIdx, aliasConfig.NameAliases); err != nil {
			return err
		}

		if err := t.applyArgsAliases(toolName, toolIdx, aliasConfig.ArgumentsAliases); err != nil {
			return err
		}
	}

	return nil
}

// applyNameAliases validates and registers name aliases for a single tool into the indexes map.
// applyNameAliases 校验单个工具的名称别名，并注册到 indexes map 中。
func (t *toolsTuple) applyNameAliases(toolName string, toolIdx int, nameAliases []string) error {
	for _, alias := range nameAliases {
		if strings.TrimSpace(alias) == "" {
			return fmt.Errorf("tool '%s' has empty name alias", toolName)
		}
		if existingIdx, conflict := t.indexes[alias]; conflict {
			if existingIdx != toolIdx {
				conflictToolName := t.canonicalNames[existingIdx]
				if alias == conflictToolName {
					return fmt.Errorf("tool '%s': name alias '%s' conflicts with existing tool's canonical name", toolName, alias)
				}
				return fmt.Errorf("tool '%s': name alias '%s' conflicts with an alias already registered for tool '%s'", toolName, alias, conflictToolName)
			}
			continue
		}
		t.indexes[alias] = toolIdx
	}
	return nil
}

// applyArgsAliases validates argument aliases against the tool schema and builds a reverse alias map for a single tool.
// applyArgsAliases 根据工具 schema 校验参数别名，并为单个工具构建反向别名 map。
func (t *toolsTuple) applyArgsAliases(toolName string, toolIdx int, argumentsAliases map[string][]string) error {
	if len(argumentsAliases) == 0 {
		return nil
	}

	schemaKeys := make(map[string]bool)
	if info := t.toolInfos[toolIdx]; info != nil && info.ParamsOneOf != nil {
		js, err := info.ParamsOneOf.ToJSONSchema()
		if err != nil {
			return fmt.Errorf("tool '%s': failed to parse JSON schema for alias validation: %w", toolName, err)
		}
		if js != nil && js.Properties != nil {
			for pair := js.Properties.Oldest(); pair != nil; pair = pair.Next() {
				schemaKeys[pair.Key] = true
			}
		}
	}

	reverseMap := make(map[string]string)
	sortedCanonicals := make([]string, 0, len(argumentsAliases))
	for canonical := range argumentsAliases {
		sortedCanonicals = append(sortedCanonicals, canonical)
	}
	sort.Strings(sortedCanonicals)

	for _, canonical := range sortedCanonicals {
		aliases := argumentsAliases[canonical]
		if strings.TrimSpace(canonical) == "" {
			return fmt.Errorf("tool '%s' has empty canonical argument key", toolName)
		}
		if strings.Contains(canonical, ".") {
			return fmt.Errorf("tool '%s' has unsupported '.' in canonical argument key '%s': nested field matching is not yet supported",
				toolName, canonical)
		}
		for _, alias := range aliases {
			if strings.TrimSpace(alias) == "" {
				return fmt.Errorf("tool '%s' has empty argument alias for canonical key '%s'", toolName, canonical)
			}
			if schemaKeys[alias] {
				return fmt.Errorf("tool '%s' has arg alias '%s' that conflicts with existing schema property '%s'",
					toolName, alias, alias)
			}
			if existingCanonical, conflict := reverseMap[alias]; conflict {
				return fmt.Errorf("tool '%s' has conflicting arg alias '%s' mapped to both '%s' and '%s'",
					toolName, alias, existingCanonical, canonical)
			}
			reverseMap[alias] = canonical
		}
	}
	t.argsAliasMap[toolName] = reverseMap

	return nil
}

func convTools(ctx context.Context, params convToolsParams) (*toolsTuple, error) {
	ret := &toolsTuple{
		indexes:                     make(map[string]int),
		meta:                        make([]*executorMeta, len(params.tools)),
		endpoints:                   make([]InvokableToolEndpoint, len(params.tools)),
		streamEndpoints:             make([]StreamableToolEndpoint, len(params.tools)),
		enhancedInvokableEndpoints:  make([]EnhancedInvokableToolEndpoint, len(params.tools)),
		enhancedStreamableEndpoints: make([]EnhancedStreamableToolEndpoint, len(params.tools)),
		canonicalNames:              make([]string, len(params.tools)),
		toolInfos:                   make([]*schema.ToolInfo, len(params.tools)),
	}
	for idx, bt := range params.tools {
		tl, err := bt.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("(NewToolNode) failed to get tool info at idx= %d: %w", idx, err)
		}

		toolName := tl.Name
		var (
			st     tool.StreamableTool
			it     tool.InvokableTool
			eiTool tool.EnhancedInvokableTool
			esTool tool.EnhancedStreamableTool

			invokable          InvokableToolEndpoint
			streamable         StreamableToolEndpoint
			enhancedInvokable  EnhancedInvokableToolEndpoint
			enhancedStreamable EnhancedStreamableToolEndpoint

			ok   bool
			meta *executorMeta
		)

		meta = parseExecutorInfoFromComponent(components.ComponentOfTool, bt)

		if st, ok = bt.(tool.StreamableTool); ok {
			streamable = wrapStreamToolCall(st, params.middlewares.streamable, !meta.isComponentCallbackEnabled)
		}

		if it, ok = bt.(tool.InvokableTool); ok {
			invokable = wrapToolCall(it, params.middlewares.invokable, !meta.isComponentCallbackEnabled)
		}

		if eiTool, ok = bt.(tool.EnhancedInvokableTool); ok {
			enhancedInvokable = wrapEnhancedInvokableToolCall(eiTool, params.middlewares.enhancedInvokable, !meta.isComponentCallbackEnabled)
		}

		if esTool, ok = bt.(tool.EnhancedStreamableTool); ok {
			enhancedStreamable = wrapEnhancedStreamableToolCall(esTool, params.middlewares.enhancedStreamable, !meta.isComponentCallbackEnabled)
		}

		if st == nil && it == nil && eiTool == nil && esTool == nil {
			return nil, fmt.Errorf("tool %s is not invokable, streamable, enhanced invokable or enhanced streamable", toolName)
		}
		if streamable == nil && invokable != nil {
			streamable = invokableToStreamable(invokable)
		}
		if invokable == nil && streamable != nil {
			invokable = streamableToInvokable(streamable)
		}

		if enhancedStreamable == nil && enhancedInvokable != nil {
			enhancedStreamable = enhancedInvokableToEnhancedStreamable(enhancedInvokable)
		}
		if enhancedInvokable == nil && enhancedStreamable != nil {
			enhancedInvokable = enhancedStreamableToEnhancedInvokable(enhancedStreamable)
		}

		ret.indexes[toolName] = idx
		ret.meta[idx] = meta
		ret.endpoints[idx] = invokable
		ret.streamEndpoints[idx] = streamable
		ret.enhancedInvokableEndpoints[idx] = enhancedInvokable
		ret.enhancedStreamableEndpoints[idx] = enhancedStreamable
		ret.canonicalNames[idx] = toolName
		ret.toolInfos[idx] = tl
	}

	if len(params.aliasConfigs) > 0 {
		if err := ret.applyAliasConfigs(params.aliasConfigs); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func wrapToolCall(it tool.InvokableTool, middlewares []InvokableToolMiddleware, needCallback bool) InvokableToolEndpoint {
	middleware := func(next InvokableToolEndpoint) InvokableToolEndpoint {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
	if needCallback {
		it = &invokableToolWithCallback{it: it}
	}
	return middleware(func(ctx context.Context, input *ToolInput) (*ToolOutput, error) {
		result, err := it.InvokableRun(ctx, input.Arguments, input.CallOptions...)
		if err != nil {
			return nil, err
		}
		return &ToolOutput{Result: result}, nil
	})
}

func wrapStreamToolCall(st tool.StreamableTool, middlewares []StreamableToolMiddleware, needCallback bool) StreamableToolEndpoint {
	middleware := func(next StreamableToolEndpoint) StreamableToolEndpoint {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
	if needCallback {
		st = &streamableToolWithCallback{st: st}
	}
	return middleware(func(ctx context.Context, input *ToolInput) (*StreamToolOutput, error) {
		result, err := st.StreamableRun(ctx, input.Arguments, input.CallOptions...)
		if err != nil {
			return nil, err
		}
		return &StreamToolOutput{Result: result}, nil
	})
}

func wrapEnhancedInvokableToolCall(eiTool tool.EnhancedInvokableTool, middlewares []EnhancedInvokableToolMiddleware, needCallback bool) EnhancedInvokableToolEndpoint {
	middleware := func(next EnhancedInvokableToolEndpoint) EnhancedInvokableToolEndpoint {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
	if needCallback {
		eiTool = &enhancedInvokableToolWithCallback{eiTool: eiTool}
	}
	return middleware(func(ctx context.Context, input *ToolInput) (*EnhancedInvokableToolOutput, error) {
		result, err := eiTool.InvokableRun(ctx, &schema.ToolArgument{Text: input.Arguments}, input.CallOptions...)
		if err != nil {
			return nil, err
		}
		return &EnhancedInvokableToolOutput{Result: result}, nil
	})
}

func wrapEnhancedStreamableToolCall(est tool.EnhancedStreamableTool, middlewares []EnhancedStreamableToolMiddleware, needCallback bool) EnhancedStreamableToolEndpoint {
	middleware := func(next EnhancedStreamableToolEndpoint) EnhancedStreamableToolEndpoint {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
	if needCallback {
		est = &enhancedStreamableToolWithCallback{est: est}
	}
	return middleware(func(ctx context.Context, input *ToolInput) (*EnhancedStreamableToolOutput, error) {
		result, err := est.StreamableRun(ctx, &schema.ToolArgument{Text: input.Arguments}, input.CallOptions...)
		if err != nil {
			return nil, err
		}
		return &EnhancedStreamableToolOutput{Result: result}, nil
	})
}

type invokableToolWithCallback struct {
	it tool.InvokableTool
}

func (i *invokableToolWithCallback) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return i.it.Info(ctx)
}

func (i *invokableToolWithCallback) InvokableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
	return invokeWithCallbacks(i.it.InvokableRun)(ctx, argumentsInJSON, opts...)
}

type streamableToolWithCallback struct {
	st tool.StreamableTool
}

func (s *streamableToolWithCallback) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return s.st.Info(ctx)
}

func (s *streamableToolWithCallback) StreamableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*schema.StreamReader[string], error) {
	return streamWithCallbacks(s.st.StreamableRun)(ctx, argumentsInJSON, opts...)
}

type enhancedInvokableToolWithCallback struct {
	eiTool tool.EnhancedInvokableTool
}

func (e *enhancedInvokableToolWithCallback) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return e.eiTool.Info(ctx)
}

func (e *enhancedInvokableToolWithCallback) InvokableRun(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
	return invokeEnhancedWithCallbacks(e.eiTool.InvokableRun)(ctx, toolArgument, opts...)
}

type enhancedStreamableToolWithCallback struct {
	est tool.EnhancedStreamableTool
}

func (e *enhancedStreamableToolWithCallback) Info(ctx context.Context) (*schema.ToolInfo, error) {
	return e.est.Info(ctx)
}

func (e *enhancedStreamableToolWithCallback) StreamableRun(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error) {
	return streamEnhancedWithCallbacks(e.est.StreamableRun)(ctx, toolArgument, opts...)
}

func streamableToInvokable(e StreamableToolEndpoint) InvokableToolEndpoint {
	return func(ctx context.Context, input *ToolInput) (*ToolOutput, error) {
		so, err := e(ctx, input)
		if err != nil {
			return nil, err
		}
		o, err := concatStreamReader(so.Result)
		if err != nil {
			return nil, fmt.Errorf("failed to concat StreamableTool output message stream: %w", err)
		}
		return &ToolOutput{Result: o}, nil
	}
}

func invokableToStreamable(e InvokableToolEndpoint) StreamableToolEndpoint {
	return func(ctx context.Context, input *ToolInput) (*StreamToolOutput, error) {
		o, err := e(ctx, input)
		if err != nil {
			return nil, err
		}
		return &StreamToolOutput{Result: schema.StreamReaderFromArray([]string{o.Result})}, nil
	}
}

func enhancedStreamableToEnhancedInvokable(e EnhancedStreamableToolEndpoint) EnhancedInvokableToolEndpoint {
	return func(ctx context.Context, input *ToolInput) (*EnhancedInvokableToolOutput, error) {
		so, err := e(ctx, input)
		if err != nil {
			return nil, err
		}
		o, err := concatStreamReader(so.Result)
		if err != nil {
			return nil, fmt.Errorf("failed to concat EnhancedStreamableTool output message stream: %w", err)
		}
		return &EnhancedInvokableToolOutput{Result: o}, nil
	}
}

func enhancedInvokableToEnhancedStreamable(e EnhancedInvokableToolEndpoint) EnhancedStreamableToolEndpoint {
	return func(ctx context.Context, input *ToolInput) (*EnhancedStreamableToolOutput, error) {
		o, err := e(ctx, input)
		if err != nil {
			return nil, err
		}
		return &EnhancedStreamableToolOutput{Result: schema.StreamReaderFromArray([]*schema.ToolResult{o.Result})}, nil
	}
}

func invokeEnhancedWithCallbacks(i func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error)) func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
	return runWithCallbacks(i, onStart[*schema.ToolArgument], onEnd[*schema.ToolResult], onError)
}

func streamEnhancedWithCallbacks(s func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error)) func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error) {
	return runWithCallbacks(s, onStart[*schema.ToolArgument], onEndWithStreamOutput[*schema.ToolResult], onError)
}

type toolCallTask struct {
	// in
	endpoint                   InvokableToolEndpoint
	streamEndpoint             StreamableToolEndpoint
	enhancedInvokableEndpoint  EnhancedInvokableToolEndpoint
	enhancedStreamableEndpoint EnhancedStreamableToolEndpoint
	meta                       *executorMeta
	name                       string
	arg                        string
	callID                     string
	useEnhanced                bool

	// out
	executed        bool
	output          string
	sOutput         *schema.StreamReader[string]
	enhancedOutput  *schema.ToolResult
	enhancedSOutput *schema.StreamReader[*schema.ToolResult]
	err             error
}

func (tn *ToolsNode) genToolCallTasks(ctx context.Context, tuple *toolsTuple,
	input *schema.Message, executedTools map[string]string, executedEnhancedTools map[string]*schema.ToolResult, isStream bool) ([]toolCallTask, error) {

	if input.Role != schema.Assistant {
		return nil, fmt.Errorf("expected message role is Assistant, got %s", input.Role)
	}

	n := len(input.ToolCalls)
	if n == 0 {
		return nil, errors.New("no tool call found in input message")
	}

	toolCallTasks := make([]toolCallTask, n)

	for i := 0; i < n; i++ {
		toolCall := input.ToolCalls[i]
		if enhancedResult, executed := executedEnhancedTools[toolCall.ID]; executed {
			toolCallTasks[i].name = toolCall.Function.Name
			toolCallTasks[i].arg = toolCall.Function.Arguments
			toolCallTasks[i].callID = toolCall.ID
			toolCallTasks[i].executed = true
			toolCallTasks[i].useEnhanced = true
			if isStream {
				toolCallTasks[i].enhancedSOutput = schema.StreamReaderFromArray([]*schema.ToolResult{enhancedResult})
			} else {
				toolCallTasks[i].enhancedOutput = enhancedResult
			}
			continue
		}
		if result, executed := executedTools[toolCall.ID]; executed {
			toolCallTasks[i].name = toolCall.Function.Name
			toolCallTasks[i].arg = toolCall.Function.Arguments
			toolCallTasks[i].callID = toolCall.ID
			toolCallTasks[i].executed = true
			toolCallTasks[i].useEnhanced = false
			if isStream {
				toolCallTasks[i].sOutput = schema.StreamReaderFromArray([]string{result})
			} else {
				toolCallTasks[i].output = result
			}
			continue
		}
		index, ok := tuple.indexes[toolCall.Function.Name]
		if !ok {
			if tn.unknownToolHandler == nil {
				return nil, fmt.Errorf("tool %s not found in toolsNode indexes", toolCall.Function.Name)
			}
			toolCallTasks[i] = newUnknownToolTask(toolCall.Function.Name, toolCall.Function.Arguments, toolCall.ID, tn.unknownToolHandler)
		} else {
			toolCallTasks[i].meta = tuple.meta[index]
			toolCallTasks[i].name = toolCall.Function.Name
			toolCallTasks[i].callID = toolCall.ID

			if tuple.enhancedInvokableEndpoints[index] != nil && tuple.enhancedStreamableEndpoints[index] != nil {
				toolCallTasks[i].enhancedInvokableEndpoint = tuple.enhancedInvokableEndpoints[index]
				toolCallTasks[i].enhancedStreamableEndpoint = tuple.enhancedStreamableEndpoints[index]
				toolCallTasks[i].useEnhanced = true
			} else {
				toolCallTasks[i].endpoint = tuple.endpoints[index]
				toolCallTasks[i].streamEndpoint = tuple.streamEndpoints[index]
				toolCallTasks[i].useEnhanced = false
			}

			// Get canonical tool name for looking up argument aliases
			// 获取规范工具名称，用于查找参数别名
			canonicalToolName := tuple.canonicalNames[index]

			// Process argument aliases remapping
			// 处理参数别名重映射
			args := toolCall.Function.Arguments
			if aliasMap, hasAliases := tuple.argsAliasMap[canonicalToolName]; hasAliases {
				remappedArgs, err := remapArgs(args, aliasMap)
				if err != nil {
					return nil, fmt.Errorf("failed to remap args for tool[name:%s]: %w", canonicalToolName, err)
				}
				args = remappedArgs
			}

			if tn.toolArgumentsHandler != nil {
				arg, err := tn.toolArgumentsHandler(ctx, canonicalToolName, args)
				if err != nil {
					return nil, fmt.Errorf("failed to executed tool[name:%s arguments:%s] arguments handler: %w", toolCall.Function.Name, args, err)
				}
				toolCallTasks[i].arg = arg
			} else {
				toolCallTasks[i].arg = args
			}
		}
	}

	return toolCallTasks, nil
}

func newUnknownToolTask(name, arg, callID string, unknownToolHandler func(ctx context.Context, name, input string) (string, error)) toolCallTask {
	endpoint := func(ctx context.Context, input *ToolInput) (*ToolOutput, error) {
		result, err := unknownToolHandler(ctx, input.Name, input.Arguments)
		if err != nil {
			return nil, err
		}
		return &ToolOutput{
			Result: result,
		}, nil
	}
	return toolCallTask{
		endpoint:       endpoint,
		streamEndpoint: invokableToStreamable(endpoint),
		meta: &executorMeta{
			component:                  components.ComponentOfTool,
			isComponentCallbackEnabled: false,
			componentImplType:          "UnknownTool",
		},
		name:   name,
		arg:    arg,
		callID: callID,
	}
}

func runToolCallTaskByInvoke(ctx context.Context, task *toolCallTask, opts ...tool.Option) {
	if task.executed {
		return
	}
	ctx = callbacks.ReuseHandlers(ctx, &callbacks.RunInfo{
		Name:      task.name,
		Type:      task.meta.componentImplType,
		Component: task.meta.component,
	})

	ctx = setToolCallInfo(ctx, &toolCallInfo{toolCallID: task.callID})
	ctx = appendToolAddressSegment(ctx, task.name, task.callID)

	if task.useEnhanced {
		enhancedOutput, err := task.enhancedInvokableEndpoint(ctx, &ToolInput{
			Name:        task.name,
			Arguments:   task.arg,
			CallID:      task.callID,
			CallOptions: opts,
		})
		if err != nil {
			task.err = err
		} else {
			task.enhancedOutput = enhancedOutput.Result
			task.executed = true
		}
	} else {
		output, err := task.endpoint(ctx, &ToolInput{
			Name:        task.name,
			Arguments:   task.arg,
			CallID:      task.callID,
			CallOptions: opts,
		})
		if err != nil {
			task.err = err
		} else {
			task.output = output.Result
			task.executed = true
		}
	}
}

func runToolCallTaskByStream(ctx context.Context, task *toolCallTask, opts ...tool.Option) {
	ctx = callbacks.ReuseHandlers(ctx, &callbacks.RunInfo{
		Name:      task.name,
		Type:      task.meta.componentImplType,
		Component: task.meta.component,
	})

	ctx = setToolCallInfo(ctx, &toolCallInfo{toolCallID: task.callID})
	ctx = appendToolAddressSegment(ctx, task.name, task.callID)

	if task.useEnhanced {
		enhancedOutput, err := task.enhancedStreamableEndpoint(ctx, &ToolInput{
			Name:        task.name,
			Arguments:   task.arg,
			CallID:      task.callID,
			CallOptions: opts,
		})
		if err != nil {
			task.err = err
		} else {
			task.enhancedSOutput = enhancedOutput.Result
			task.executed = true
		}
	} else {
		output, err := task.streamEndpoint(ctx, &ToolInput{
			Name:        task.name,
			Arguments:   task.arg,
			CallID:      task.callID,
			CallOptions: opts,
		})
		if err != nil {
			task.err = err
		} else {
			task.sOutput = output.Result
			task.executed = true
		}
	}
}

func sequentialRunToolCall(ctx context.Context,
	run func(ctx2 context.Context, callTask *toolCallTask, opts ...tool.Option),
	tasks []toolCallTask, opts ...tool.Option) {

	for i := range tasks {
		if tasks[i].executed {
			continue
		}
		run(ctx, &tasks[i], opts...)
	}
}

func parallelRunToolCall(ctx context.Context,
	run func(ctx2 context.Context, callTask *toolCallTask, opts ...tool.Option),
	tasks []toolCallTask, opts ...tool.Option) {

	if len(tasks) == 1 {
		run(ctx, &tasks[0], opts...)
		return
	}

	var wg sync.WaitGroup
	for i := 1; i < len(tasks); i++ {
		if tasks[i].executed {
			continue
		}
		wg.Add(1)
		go func(ctx_ context.Context, t *toolCallTask, opts ...tool.Option) {
			defer wg.Done()
			defer func() {
				panicErr := recover()
				if panicErr != nil {
					t.err = safe.NewPanicErr(panicErr, debug.Stack())
				}
			}()
			run(ctx_, t, opts...)
		}(ctx, &tasks[i], opts...)
	}

	if !tasks[0].executed {
		run(ctx, &tasks[0], opts...)
	}

	wg.Wait()
}

// buildTupleFromOpts rebuilds a toolsTuple when call options override tools or aliases.
// buildTupleFromOpts 在调用选项覆盖工具或别名时重建 toolsTuple。
func (tn *ToolsNode) buildTupleFromOpts(ctx context.Context, opt *toolsNodeOptions) (*toolsTuple, error) {
	tools := opt.ToolList
	if tools == nil {
		tools = tn.tools
	}
	aliasConfigs := opt.ToolAliases
	if aliasConfigs == nil {
		aliasConfigs = tn.toolAliasConfigs
	}
	p := convToolsParams{
		tools:        tools,
		aliasConfigs: aliasConfigs,
	}
	p.middlewares.invokable = tn.toolCallMiddlewares
	p.middlewares.streamable = tn.streamToolCallMiddlewares
	p.middlewares.enhancedInvokable = tn.enhancedToolCallMiddlewares
	p.middlewares.enhancedStreamable = tn.enhancedStreamToolCallMiddlewares
	tuple, err := convTools(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("failed to convert tool list from call option: %w", err)
	}
	return tuple, nil
}

// Invoke calls the tools and collects the results of invokable tools.
// it's parallel if there are multiple tool calls in the input message.
//
// Invoke 调用工具并收集可调用工具的结果。
// 如果输入消息中有多个工具调用，则并行执行。
func (tn *ToolsNode) Invoke(ctx context.Context, input *schema.Message,
	opts ...ToolsNodeOption) ([]*schema.Message, error) {

	opt := getToolsNodeOptions(opts...)
	tuple := tn.tuple
	if opt.ToolList != nil || opt.ToolAliases != nil {
		var err error
		tuple, err = tn.buildTupleFromOpts(ctx, opt)
		if err != nil {
			return nil, err
		}
	}

	var executedTools map[string]string
	var executedEnhancedTools map[string]*schema.ToolResult
	if wasInterrupted, hasState, tnState := GetInterruptState[*toolsInterruptAndRerunState](ctx); wasInterrupted && hasState {
		input = tnState.Input
		if tnState.ExecutedTools != nil {
			executedTools = tnState.ExecutedTools
		}
		if tnState.ExecutedEnhancedTools != nil {
			executedEnhancedTools = tnState.ExecutedEnhancedTools
		}
	}

	tasks, err := tn.genToolCallTasks(ctx, tuple, input, executedTools, executedEnhancedTools, false)
	if err != nil {
		return nil, err
	}

	if tn.executeSequentially {
		sequentialRunToolCall(ctx, runToolCallTaskByInvoke, tasks, opt.ToolOptions...)
	} else {
		parallelRunToolCall(ctx, runToolCallTaskByInvoke, tasks, opt.ToolOptions...)
	}

	n := len(tasks)
	output := make([]*schema.Message, n)

	rerunExtra := &ToolsInterruptAndRerunExtra{
		ToolCalls:             input.ToolCalls,
		ExecutedTools:         make(map[string]string),
		ExecutedEnhancedTools: make(map[string]*schema.ToolResult),
		RerunExtraMap:         make(map[string]any),
	}
	rerunState := &toolsInterruptAndRerunState{
		Input:                 input,
		ExecutedTools:         make(map[string]string),
		ExecutedEnhancedTools: make(map[string]*schema.ToolResult),
	}

	var errs []error
	for i := 0; i < n; i++ {
		if tasks[i].err != nil {
			info, ok := IsInterruptRerunError(tasks[i].err)
			if !ok {
				return nil, fmt.Errorf("failed to invoke tool[name:%s id:%s]: %w", tasks[i].name, tasks[i].callID, tasks[i].err)
			}

			rerunExtra.RerunTools = append(rerunExtra.RerunTools, tasks[i].callID)
			rerunState.RerunTools = append(rerunState.RerunTools, tasks[i].callID)
			if info != nil {
				rerunExtra.RerunExtraMap[tasks[i].callID] = info
			}

			iErr := WrapInterruptAndRerunIfNeeded(ctx,
				AddressSegment{ID: tasks[i].callID, Type: AddressSegmentTool}, tasks[i].err)
			errs = append(errs, iErr)
			continue
		}
		if tasks[i].executed {
			if tasks[i].useEnhanced {
				rerunExtra.ExecutedEnhancedTools[tasks[i].callID] = tasks[i].enhancedOutput
				rerunState.ExecutedEnhancedTools[tasks[i].callID] = tasks[i].enhancedOutput
			} else {
				rerunExtra.ExecutedTools[tasks[i].callID] = tasks[i].output
				rerunState.ExecutedTools[tasks[i].callID] = tasks[i].output
			}
		}

		if len(errs) == 0 {
			if tasks[i].useEnhanced {
				output[i] = schema.ToolMessage("", tasks[i].callID, schema.WithToolName(tasks[i].name))
				output[i].UserInputMultiContent, err = tasks[i].enhancedOutput.ToMessageInputParts()
				if err != nil {
					return nil, err
				}
			} else {
				output[i] = schema.ToolMessage(tasks[i].output, tasks[i].callID, schema.WithToolName(tasks[i].name))
			}

		}
	}
	if len(errs) > 0 {
		return nil, CompositeInterrupt(ctx, rerunExtra, rerunState, errs...)
	}

	return output, nil
}

// Stream calls the tools and collects the results of stream readers.
// it's parallel if there are multiple tool calls in the input message.
//
// Stream 调用工具并收集流读取器的结果。
// 如果输入消息中有多个工具调用，则并行执行。
func (tn *ToolsNode) Stream(ctx context.Context, input *schema.Message,
	opts ...ToolsNodeOption) (*schema.StreamReader[[]*schema.Message], error) {

	opt := getToolsNodeOptions(opts...)
	tuple := tn.tuple
	if opt.ToolList != nil || opt.ToolAliases != nil {
		var err error
		tuple, err = tn.buildTupleFromOpts(ctx, opt)
		if err != nil {
			return nil, err
		}
	}

	var executedTools map[string]string
	var executedEnhancedTools map[string]*schema.ToolResult
	if wasInterrupted, hasState, tnState := GetInterruptState[*toolsInterruptAndRerunState](ctx); wasInterrupted && hasState {
		input = tnState.Input
		if tnState.ExecutedTools != nil {
			executedTools = tnState.ExecutedTools
		}
		if tnState.ExecutedEnhancedTools != nil {
			executedEnhancedTools = tnState.ExecutedEnhancedTools
		}
	}

	tasks, err := tn.genToolCallTasks(ctx, tuple, input, executedTools, executedEnhancedTools, true)
	if err != nil {
		return nil, err
	}

	if tn.executeSequentially {
		sequentialRunToolCall(ctx, runToolCallTaskByStream, tasks, opt.ToolOptions...)
	} else {
		parallelRunToolCall(ctx, runToolCallTaskByStream, tasks, opt.ToolOptions...)
	}

	n := len(tasks)

	rerunExtra := &ToolsInterruptAndRerunExtra{
		ToolCalls:             input.ToolCalls,
		ExecutedTools:         make(map[string]string),
		ExecutedEnhancedTools: make(map[string]*schema.ToolResult),
		RerunExtraMap:         make(map[string]any),
	}
	rerunState := &toolsInterruptAndRerunState{
		Input:                 input,
		ExecutedTools:         make(map[string]string),
		ExecutedEnhancedTools: make(map[string]*schema.ToolResult),
	}
	var errs []error
	// check rerun
	// 检查重新运行
	for i := 0; i < n; i++ {
		if tasks[i].err != nil {
			info, ok := IsInterruptRerunError(tasks[i].err)
			if !ok {
				return nil, fmt.Errorf("failed to stream tool call %s: %w", tasks[i].callID, tasks[i].err)
			}

			rerunExtra.RerunTools = append(rerunExtra.RerunTools, tasks[i].callID)
			rerunState.RerunTools = append(rerunState.RerunTools, tasks[i].callID)
			if info != nil {
				rerunExtra.RerunExtraMap[tasks[i].callID] = info
			}
			iErr := WrapInterruptAndRerunIfNeeded(ctx,
				AddressSegment{ID: tasks[i].callID, Type: AddressSegmentTool}, tasks[i].err)
			errs = append(errs, iErr)
			continue
		}
	}

	if len(errs) > 0 {
		// concat and save tool output
		// 拼接并保存工具输出
		for _, t := range tasks {
			if t.executed {
				if t.useEnhanced {
					eo, err_ := concatStreamReader(t.enhancedSOutput)
					if err_ != nil {
						return nil, fmt.Errorf("failed to concat enhanced tool[name:%s id:%s]'s stream output: %w", t.name, t.callID, err_)
					}
					rerunExtra.ExecutedEnhancedTools[t.callID] = eo
					rerunState.ExecutedEnhancedTools[t.callID] = eo

				} else {
					o, err_ := concatStreamReader(t.sOutput)
					if err_ != nil {
						return nil, fmt.Errorf("failed to concat tool[name:%s id:%s]'s stream output: %w", t.name, t.callID, err_)
					}
					rerunExtra.ExecutedTools[t.callID] = o
					rerunState.ExecutedTools[t.callID] = o
				}
			}
		}
		return nil, CompositeInterrupt(ctx, rerunExtra, rerunState, errs...)
	}

	// common return
	// 通用返回
	sOutput := make([]*schema.StreamReader[[]*schema.Message], n)
	for i := 0; i < n; i++ {
		index := i
		callID := tasks[i].callID
		callName := tasks[i].name
		if tasks[i].useEnhanced {
			cvt := func(tr *schema.ToolResult) ([]*schema.Message, error) {
				ret := make([]*schema.Message, n)
				ret[index] = schema.ToolMessage("", callID, schema.WithToolName(callName))
				ret[index].UserInputMultiContent, err = tr.ToMessageInputParts()
				if err != nil {
					return nil, err
				}
				return ret, nil
			}
			sOutput[i] = schema.StreamReaderWithConvert(tasks[i].enhancedSOutput, cvt)
		} else {
			cvt := func(s string) ([]*schema.Message, error) {
				ret := make([]*schema.Message, n)
				ret[index] = schema.ToolMessage(s, callID, schema.WithToolName(callName))
				return ret, nil
			}
			sOutput[i] = schema.StreamReaderWithConvert(tasks[i].sOutput, cvt)
		}

	}
	return schema.MergeStreamReaders(sOutput), nil
}

// GetType returns the component type string for the Tools node.
// GetType 返回 Tools 节点的组件类型字符串。
func (tn *ToolsNode) GetType() string {
	return ""
}

func getToolsNodeOptions(opts ...ToolsNodeOption) *toolsNodeOptions {
	o := &toolsNodeOptions{
		ToolOptions: make([]tool.Option, 0),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

type toolCallInfoKey struct{}
type toolCallInfo struct {
	toolCallID string
}

func setToolCallInfo(ctx context.Context, toolCallInfo *toolCallInfo) context.Context {
	return context.WithValue(ctx, toolCallInfoKey{}, toolCallInfo)
}

// GetToolCallID gets the current tool call id from the context.
// GetToolCallID 从 context 中获取当前工具调用 id。
func GetToolCallID(ctx context.Context) string {
	v := ctx.Value(toolCallInfoKey{})
	if v == nil {
		return ""
	}

	info, ok := v.(*toolCallInfo)
	if !ok {
		return ""
	}

	return info.toolCallID
}
