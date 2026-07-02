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

package reduction

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/slongfield/pyfmt"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

// TypedConfig is the configuration for tool reduction middleware.
// This middleware manages tool outputs in two phases to optimize context usage:
//
//  1. Truncation Phase:
//     Triggered immediately after a tool execution completes.
//     If the tool output length exceeds MaxLengthForTrunc, the full content is saved
//     to the configured Backend, and the tool output is replaced with a truncated notice.
//     This prevents immediate context overflow from a single large tool output.
//
//  2. Clear Phase:
//     Triggered before sending messages to the model (in BeforeModelRewriteState).
//     If the total token count exceeds MaxTokensForClear, the middleware iterates through
//     historical messages. Based on GenOffloadFilePath (or RootDir when GenOffloadFilePath is nil) and
//     ClearRetentionSuffixLimit, it offloads tool call arguments and results
//     to the Backend to reduce token usage, keeping the conversation within limits while retaining access to the
//     important information. After all, ClearPostProcess will be called, which you could save or notify current state.
//
// TypedConfig 是工具缩减中间件的配置。
// 该中间件分两个阶段管理工具输出，以优化 context 使用：
// 1. Truncation Phase：
// 在工具执行完成后立即触发。
// 如果工具输出长度超过 MaxLengthForTrunc，完整内容会保存到配置的 Backend，工具输出会替换为截断提示。
// 这可防止单个大型工具输出立即导致 context 溢出。
// 2. Clear Phase：
// 在发送消息给模型前触发（在 BeforeModelRewriteState 中）。
// 如果总 token 数超过 MaxTokensForClear，中间件会遍历历史消息。基于 GenOffloadFilePath（或 GenOffloadFilePath 为 nil 时的 RootDir）和 ClearRetentionSuffixLimit，它会将工具调用参数和结果卸载到 Backend 以降低 token 使用量，使对话保持在限制内，同时保留对重要信息的访问。最后会调用 ClearPostProcess，你可以在其中保存或通知当前状态。
type TypedConfig[M adk.MessageType] struct {
	// Backend is the storage backend where offloaded content will be saved.
	// Required when truncation is enabled (SkipTruncation is false).
	// Optional for clear-only usage. If Backend is nil, clear will still replace tool outputs with placeholders
	// but will not offload content.
	//
	// Backend 是保存已卸载内容的存储 backend。
	// 启用截断时必填（SkipTruncation 为 false）。
	// 仅清除场景下可选。如果 Backend 为 nil，clear 仍会用占位符替换工具输出，但不会卸载内容。
	Backend Backend

	// SkipTruncation skip truncating.
	// SkipTruncation 跳过截断。
	SkipTruncation bool

	// SkipClear skip clearing.
	// SkipClear 跳过清理。
	SkipClear bool

	// ReadFileToolName is tool name used to retrieve from file.
	// After offloading content to file, you should give agent the same tool to retrieve content.
	// Required. Default is "read_file".
	//
	// ReadFileToolName 是用于从文件检索内容的工具名称。
	// 将内容卸载到文件后，应为智能体提供同一个工具来检索内容。
	// 必填。默认值为 "read_file"。
	ReadFileToolName string

	// RootDir root dir to save truncated/cleared content.
	// Optional.
	// Default is /tmp, truncated content saves to ${root_dir}/trunc/{tool_call_id}, cleared content saves to  ${root_dir}/clear/{tool_call_id}
	//
	// RootDir 是保存截断/清理内容的根目录。
	// 可选。
	// 默认值为 /tmp，截断内容保存到 ${root_dir}/trunc/{tool_call_id}，清理内容保存到 ${root_dir}/clear/{tool_call_id}。
	RootDir string

	// GenTruncOffloadFilePath is used to generate offload file path for truncated content.
	// When GenTruncOffloadFilePath is configured, RootDir will be ignored.
	// This is useful when tool_call_id is not unique, which may cause incorrect offload file overwrite.
	// Optional. Default is nil.
	//
	// GenTruncOffloadFilePath 用于为截断内容生成卸载文件路径。
	// 配置 GenTruncOffloadFilePath 后，将忽略 RootDir。
	// 当 tool_call_id 不唯一、可能导致卸载文件被错误覆盖时，这很有用。
	// 可选。默认值为 nil。
	GenTruncOffloadFilePath func(ctx context.Context, toolDetail *ToolDetail) (filePath string, err error)

	// GenClearOffloadFilePath is used to generate offload file path for truncated content.
	// When GenClearOffloadFilePath is configured, RootDir will be ignored.
	// This is useful when tool_call_id is not unique, which may cause incorrect offload file overwrite.
	// Optional. Default is nil.
	//
	// GenClearOffloadFilePath 用于为截断内容生成卸载文件路径。
	// 配置 GenClearOffloadFilePath 后，将忽略 RootDir。
	// 当 tool_call_id 不唯一、可能导致卸载文件被错误覆盖时，这很有用。
	// 可选。默认值为 nil。
	GenClearOffloadFilePath func(ctx context.Context, toolDetail *ToolDetail) (filePath string, err error)

	// MaxLengthForTrunc is the maximum allowed length of the tool output.
	// If the output exceeds this length, it will be truncated.
	// Required. Default is 50000.
	//
	// MaxLengthForTrunc 是工具输出允许的最大长度。
	// 如果输出超过该长度，将被截断。
	// 必填。默认值为 50000。
	MaxLengthForTrunc int

	// TruncExcludeTools is list of tool names whose tool results should never be truncated.
	// Optional. Default is nil.
	//
	// TruncExcludeTools 是工具名称列表，这些工具的结果永不截断。
	// 可选。默认值为 nil。
	TruncExcludeTools []string

	// TokenCounter is used to count the number of tokens in the conversation messages.
	// It is used to determine when to trigger clearing based on token usage, and token usage after clearing.
	// Required.
	//
	// TokenCounter 用于统计对话消息中的 token 数量。
	// 它用于根据 token 使用量判断何时触发清理，以及清理后的 token 使用量。
	// 必填。
	TokenCounter func(ctx context.Context, msg []M, tools []*schema.ToolInfo) (int64, error)

	// MaxTokensForClear is the maximum number of tokens allowed in the conversation before clearing is attempted.
	// Required. Default is 160000.
	//
	// MaxTokensForClear 是尝试清理前对话允许的最大 token 数。
	// 必填。默认值为 160000。
	MaxTokensForClear int64

	// ClearRetentionSuffixLimit is the number of most recent messages to retain without clearing.
	// This ensures the model has some immediate context.
	// Optional. Default is 1.
	//
	// ClearRetentionSuffixLimit 是不进行清理而保留的最近消息数量。
	// 这可确保模型保留一些即时上下文。
	// 可选。默认值为 1。
	ClearRetentionSuffixLimit int

	// ClearAtLeastTokens ensures a minimum number of tokens is cleared each time the strategy activates.
	// If the strategy couldn't clear at least the specified amount, clear phase will not be applied.
	// This helps determine if context clearing is worth breaking your prompt cache.
	// Optional. Default is 0.
	//
	// ClearAtLeastTokens 确保策略每次激活时至少清理指定数量的 token。
	// 如果策略无法至少清理指定数量，则不会应用清理阶段。
	// 这有助于判断清理上下文是否值得打破 prompt cache。
	// 可选。默认值为 0。
	ClearAtLeastTokens int64

	// ClearExcludeTools is list of tool names whose tool uses and results should never be cleared.
	// Optional. Default is nil.
	//
	// ClearExcludeTools 是工具名称列表，这些工具的使用和结果永不清理。
	// 可选。默认值为 nil。
	ClearExcludeTools []string

	// ClearMessageRewriter is a pre-process handler before clearing specific tool call and tool response pairs.
	// You can rewrite tool call and tool messages extracted as parameters and return a rearranged message slice.
	// This can be useful when you want to remove some tool calls (e.g., write_file / edit_file) and rewrite them
	// as a user message (e.g. <system-reminder>).
	// Returned messages will replace the original tool call and tool messages and will count towards ClearAtLeastTokens.
	// If returned messagesAfterRewrite is nil, tool call and tool messages will be removed.
	// Optional. Default is nil, which means no rewrite.
	//
	// ClearMessageRewriter 是在清理特定工具调用和工具响应对之前的预处理处理器。
	// 你可以重写作为参数提取出的工具调用和工具消息，并返回重新排列后的消息切片。
	// 当你想移除某些工具调用（例如 write_file / edit_file）并将其重写为
	// 用户消息（例如 <system-reminder>）时，这很有用。
	// 返回的消息将替换原始工具调用和工具消息，并计入 ClearAtLeastTokens。
	// 如果返回的 messagesAfterRewrite 为 nil，工具调用和工具消息将被移除。
	// 可选。默认值为 nil，表示不重写。
	ClearMessageRewriter func(ctx context.Context, toolCallMsg M, toolResponseMsgs []M) (messagesAfterRewrite []M, err error)

	// ClearPostProcess is clear post process handler.
	// Optional.
	//
	// ClearPostProcess 是清理后的处理器。
	// 可选。
	ClearPostProcess func(ctx context.Context, state *adk.TypedChatModelAgentState[M]) context.Context

	// ToolConfig is the specific configuration that applies to tools by name.
	// This configuration takes precedence over GeneralConfig for the specified tools.
	// Optional.
	//
	// ToolConfig 是按工具名称应用的特定配置。
	// 对于指定工具，此配置优先于 GeneralConfig。
	// 可选。
	ToolConfig map[string]*ToolReductionConfig
}

// Config is the backward-compatible alias for TypedConfig with *schema.Message.
// Config 是 TypedConfig with *schema.Message 的向后兼容别名。
type Config = TypedConfig[*schema.Message]

type ToolReductionConfig struct {
	// Backend is the storage backend where offloaded content will be saved.
	// Required when truncation is enabled for this tool (SkipTruncation is false).
	// Optional for clear-only usage. If Backend is nil, clear will still replace tool outputs with placeholders
	// but will not offload content.
	//
	// Backend 是保存卸载内容的存储后端。
	// 当此工具启用截断（SkipTruncation 为 false）时必填。
	// 仅清理场景可选。如果 Backend 为 nil，清理仍会用占位符替换工具输出，
	// 但不会卸载内容。
	Backend Backend

	// SkipTruncation skip truncation for this tool.
	// SkipTruncation 跳过此工具的截断。
	SkipTruncation bool

	// TruncHandler is used to process tool call results during truncation.
	// Optional. Default using defaultTruncHandler when SkipTruncation is false but TruncHandler is nil.
	//
	// TruncHandler 用于在截断期间处理工具调用结果。
	// 可选。当 SkipTruncation 为 false 但 TruncHandler 为 nil 时，默认使用 defaultTruncHandler。
	TruncHandler func(ctx context.Context, detail *ToolDetail) (*TruncResult, error)

	// SkipClear skip clear for this tool.
	// SkipClear 跳过对此工具的清理。
	SkipClear bool

	// ClearHandler is used to process tool call arguments and results during clearing.
	// Optional. Default using defaultClearHandler when SkipClear is false but ClearHandler is nil.
	//
	// ClearHandler 用于在清理期间处理工具调用参数和结果。
	// 可选。SkipClear 为 false 但 ClearHandler 为 nil 时，默认使用 defaultClearHandler。
	ClearHandler func(ctx context.Context, detail *ToolDetail) (*ClearResult, error)
}

type ToolDetail struct {
	// ToolContext provides metadata about the tool call (e.g., tool name, call ID).
	// ToolContext 提供工具调用的元数据（例如工具名称、调用 ID）。
	ToolContext *adk.ToolContext

	// ToolArgument contains the arguments passed to the tool.
	// ToolArgument 包含传递给工具的参数。
	ToolArgument *schema.ToolArgument

	// ToolResult contains the output returned by the invokable tool.
	// ToolResult 包含可调用工具返回的输出。
	ToolResult *schema.ToolResult

	// StreamToolResult contains the output returned by the streamable tool.
	// StreamToolResult 包含可流式工具返回的输出。
	StreamToolResult *schema.StreamReader[*schema.ToolResult]
}

type TruncResult struct {
	// NeedTrunc indicates whether the tool result should be truncated.
	// NeedTrunc 表示是否应截断工具结果。
	NeedTrunc bool

	// ToolResult contains the result returned by the invokable tool after trunc.
	// Required when NeedTrunc is true and ToolDetail.ToolResult is not nil.
	//
	// ToolResult 包含截断后可调用工具返回的结果。
	// NeedTrunc 为 true 且 ToolDetail.ToolResult 非 nil 时必填。
	ToolResult *schema.ToolResult

	// StreamToolResult contains the output returned by the streamable tool after trunc.
	// Required when NeedTrunc is true and ToolDetail.StreamToolResult is not nil.
	//
	// StreamToolResult 包含截断后可流式工具返回的输出。
	// NeedTrunc 为 true 且 ToolDetail.StreamToolResult 非 nil 时必填。
	StreamToolResult *schema.StreamReader[*schema.ToolResult]

	// NeedOffload indicates whether the tool result should be offloaded.
	// NeedOffload 表示是否应卸载工具结果。
	NeedOffload bool

	// OffloadFilePath is the path where the offloaded content should be stored.
	// This path is typically relative to the backend's root.
	// Required when NeedOffload is true.
	//
	// OffloadFilePath 是卸载内容应存储到的路径。
	// 此路径通常相对于 backend 的根目录。
	// NeedOffload 为 true 时必填。
	OffloadFilePath string

	// OffloadContent is the actual content to be written to the storage backend.
	// Required when NeedOffload is true.
	//
	// OffloadContent 是要写入存储 backend 的实际内容。
	// NeedOffload 为 true 时必填。
	OffloadContent string
}

// ClearResult contains the result of the Handler's decision.
// ClearResult 包含 Handler 决策的结果。
type ClearResult struct {
	// NeedClear indicates whether the tool argument and result should be cleared.
	// NeedClear 表示是否应清理工具参数和结果。
	NeedClear bool

	// ToolArgument contains the arguments passed to the tool after clear.
	// Required when NeedClear is true.
	//
	// ToolArgument 包含清理后传递给工具的参数。
	// NeedClear 为 true 时必填。
	ToolArgument *schema.ToolArgument

	// ToolResult contains the output returned by the tool after clear.
	// Required when NeedClear is true
	//
	// ToolResult 包含清理后工具返回的输出。
	// NeedClear 为 true 时必填。
	ToolResult *schema.ToolResult

	// NeedOffload indicates whether the tool argument and result should be offloaded.
	// NeedOffload 表示是否应卸载工具参数和结果。
	NeedOffload bool

	// OffloadFilePath is the path where the offloaded content should be stored.
	// This path is typically relative to the backend's root.
	// Required when NeedOffload is true.
	//
	// OffloadFilePath 是卸载内容应存储到的路径。
	// 此路径通常相对于 backend 的根目录。
	// NeedOffload 为 true 时必填。
	OffloadFilePath string

	// OffloadContent is the actual content to be written to the storage backend.
	// Required when NeedOffload is true.
	//
	// OffloadContent 是要写入存储 backend 的实际内容。
	// NeedOffload 为 true 时必填。
	OffloadContent string
}

func (t *TypedConfig[M]) copyAndFillDefaults() (*TypedConfig[M], error) {
	cfg := &TypedConfig[M]{
		Backend:                   t.Backend,
		SkipTruncation:            t.SkipTruncation,
		SkipClear:                 t.SkipClear,
		ReadFileToolName:          t.ReadFileToolName,
		RootDir:                   t.RootDir,
		GenTruncOffloadFilePath:   t.GenTruncOffloadFilePath,
		GenClearOffloadFilePath:   t.GenClearOffloadFilePath,
		MaxLengthForTrunc:         t.MaxLengthForTrunc,
		TruncExcludeTools:         t.TruncExcludeTools,
		TokenCounter:              t.TokenCounter,
		MaxTokensForClear:         t.MaxTokensForClear,
		ClearRetentionSuffixLimit: t.ClearRetentionSuffixLimit,
		ClearAtLeastTokens:        t.ClearAtLeastTokens,
		ClearExcludeTools:         t.ClearExcludeTools,
		ClearMessageRewriter:      t.ClearMessageRewriter,
		ClearPostProcess:          t.ClearPostProcess,
	}
	if cfg.TokenCounter == nil {
		cfg.TokenCounter = getDefaultTokenCounter[M]()
	}
	if cfg.ClearRetentionSuffixLimit == 0 {
		cfg.ClearRetentionSuffixLimit = 1
	}
	if cfg.ReadFileToolName == "" {
		cfg.ReadFileToolName = "read_file"
	}
	if cfg.RootDir == "" {
		cfg.RootDir = "/tmp"
	}
	if cfg.GenTruncOffloadFilePath == nil {
		cfg.GenTruncOffloadFilePath = func(ctx context.Context, toolDetail *ToolDetail) (filePath string, err error) {
			tcID := toolDetail.ToolContext.CallID
			if tcID == "" {
				tcID = uuid.NewString()
			}
			return filepath.Join(cfg.RootDir, "trunc", tcID), nil
		}
	}
	if cfg.GenClearOffloadFilePath == nil {
		cfg.GenClearOffloadFilePath = func(ctx context.Context, toolDetail *ToolDetail) (filePath string, err error) {
			tcID := toolDetail.ToolContext.CallID
			if tcID == "" {
				tcID = uuid.NewString()
			}
			return filepath.Join(cfg.RootDir, "clear", tcID), nil
		}
	}
	if cfg.MaxLengthForTrunc == 0 {
		cfg.MaxLengthForTrunc = 50000
	}
	if cfg.MaxTokensForClear == 0 {
		cfg.MaxTokensForClear = 160000
	}
	if t.ToolConfig != nil {
		cfg.ToolConfig = make(map[string]*ToolReductionConfig, len(t.ToolConfig))
		for toolName, trc := range t.ToolConfig {
			cpConfig := &ToolReductionConfig{
				Backend:        trc.Backend,
				SkipTruncation: trc.SkipTruncation,
				SkipClear:      trc.SkipClear,
				TruncHandler:   trc.TruncHandler,
				ClearHandler:   trc.ClearHandler,
			}
			cfg.ToolConfig[toolName] = cpConfig
		}
	}

	return cfg, nil
}

// NewTyped creates a generic tool reduction middleware from config.
//
// This is the generic constructor that supports both *schema.Message and *schema.AgenticMessage.
// Both message types support the full truncation and clear phases.
//
// NewTyped 从 config 创建一个泛型工具 reduction middleware。
// 这是支持 *schema.Message 和 *schema.AgenticMessage 的泛型构造函数。
// 两种消息类型都支持完整的截断和清理阶段。
func NewTyped[M adk.MessageType](_ context.Context, config *TypedConfig[M]) (adk.TypedChatModelAgentMiddleware[M], error) {
	var err error
	if config == nil {
		return nil, fmt.Errorf("config must not be nil")
	}
	if config.Backend == nil && !config.SkipTruncation {
		return nil, fmt.Errorf("backend must be set when not skipping truncation")
	}

	config, err = config.copyAndFillDefaults()
	if err != nil {
		return nil, err
	}
	defaultReductionConfig := &ToolReductionConfig{
		Backend:        config.Backend,
		SkipTruncation: config.SkipTruncation,
		SkipClear:      config.SkipClear,
	}
	if !defaultReductionConfig.SkipTruncation {
		defaultReductionConfig.TruncHandler = defaultTruncHandler(config.GenTruncOffloadFilePath, config.MaxLengthForTrunc)
	}
	if !defaultReductionConfig.SkipClear {
		defaultReductionConfig.ClearHandler = defaultClearHandler(config.GenClearOffloadFilePath, config.Backend != nil, config.ReadFileToolName)
	}
	excludeTruncTools := make(map[string]struct{}, len(config.TruncExcludeTools))
	for _, toolName := range config.TruncExcludeTools {
		excludeTruncTools[toolName] = struct{}{}
	}
	excludeClearTools := make(map[string]struct{}, len(config.ClearExcludeTools))
	for _, toolName := range config.ClearExcludeTools {
		excludeClearTools[toolName] = struct{}{}
	}

	return &typedToolReductionMiddleware[M]{
		config:            config,
		defaultConfig:     defaultReductionConfig,
		excludeTruncTools: excludeTruncTools,
		excludeClearTools: excludeClearTools,
	}, nil
}

// New creates tool reduction middleware from config
// New 根据 config 创建工具归约中间件
func New(ctx context.Context, config *Config) (adk.ChatModelAgentMiddleware, error) {
	return NewTyped(ctx, config)
}

type typedToolReductionMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]

	config        *TypedConfig[M]
	defaultConfig *ToolReductionConfig

	excludeTruncTools map[string]struct{}
	excludeClearTools map[string]struct{}
}

// getDefaultTokenCounter returns a default token counter function that operates on []M.
// For *schema.Message it delegates to defaultTokenCounter.
// For *schema.AgenticMessage it uses a simple character-based estimation.
//
// getDefaultTokenCounter 返回一个作用于 []M 的默认 token 计数函数。
// 对于 *schema.Message，它委托给 defaultTokenCounter。
// 对于 *schema.AgenticMessage，它使用简单的按字符估算。
func getDefaultTokenCounter[M adk.MessageType]() func(ctx context.Context, msgs []M, tools []*schema.ToolInfo) (int64, error) {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		return any(func(ctx context.Context, msgs []*schema.Message, tools []*schema.ToolInfo) (int64, error) {
			return defaultTokenCounter(ctx, msgs, tools)
		}).(func(context.Context, []M, []*schema.ToolInfo) (int64, error))
	case *schema.AgenticMessage:
		return any(func(ctx context.Context, msgs []*schema.AgenticMessage, tools []*schema.ToolInfo) (int64, error) {
			return defaultAgenticTokenCounter(ctx, msgs, tools)
		}).(func(context.Context, []M, []*schema.ToolInfo) (int64, error))
	}
	panic("unreachable")
}

func defaultAgenticTokenCounter(_ context.Context, msgs []*schema.AgenticMessage, tools []*schema.ToolInfo) (int64, error) {
	var tokens int64
	for _, msg := range msgs {
		if msg == nil {
			continue
		}
		tokens += int64(len(msg.Role)) / 4
		for _, block := range msg.ContentBlocks {
			if block != nil {
				tokens += int64(len(block.String())) / 4
			}
		}
	}
	for _, tl := range tools {
		tl_ := *tl
		tl_.Extra = nil
		text, err := sonic.MarshalString(tl_)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal tool info: %w", err)
		}
		tokens += int64(len(text) / 4)
	}
	return tokens, nil
}

func (t *typedToolReductionMiddleware[M]) getToolConfig(toolName string, sc scene) *ToolReductionConfig {
	if t.config.ToolConfig != nil {
		if cfg, ok := t.config.ToolConfig[toolName]; ok {
			if (sc == sceneTruncation && !cfg.SkipTruncation && cfg.TruncHandler == nil) ||
				(sc == sceneClear && !cfg.SkipClear && cfg.ClearHandler == nil) {
				return t.defaultConfig
			}
			return cfg
		}
	}
	return t.defaultConfig
}

func (t *typedToolReductionMiddleware[M]) WrapInvokableToolCall(_ context.Context, endpoint adk.InvokableToolCallEndpoint, tCtx *adk.ToolContext) (adk.InvokableToolCallEndpoint, error) {
	cfg := t.getToolConfig(tCtx.Name, sceneTruncation)
	if cfg == nil || cfg.TruncHandler == nil {
		return endpoint, nil
	}
	if _, excluded := t.excludeTruncTools[tCtx.Name]; excluded {
		return endpoint, nil
	}

	return func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
		output, err := endpoint(ctx, argumentsInJSON, opts...)
		if err != nil {
			return "", err
		}
		detail := &ToolDetail{
			ToolContext: tCtx,
			ToolArgument: &schema.ToolArgument{
				Text: argumentsInJSON,
			},
			ToolResult: &schema.ToolResult{
				Parts: []schema.ToolOutputPart{
					{Type: schema.ToolPartTypeText, Text: output},
				},
			},
		}
		truncResult, err := cfg.TruncHandler(ctx, detail)
		if err != nil {
			return "", err
		}
		if !truncResult.NeedTrunc {
			return output, nil
		}
		if truncResult.NeedOffload {
			if cfg.Backend == nil {
				return "", fmt.Errorf("truncation: no backend for offload")
			}
			if err = cfg.Backend.Write(ctx, &filesystem.WriteRequest{
				FilePath: truncResult.OffloadFilePath,
				Content:  truncResult.OffloadContent,
			}); err != nil {
				return "", err
			}
		}
		return truncResult.ToolResult.Parts[0].Text, nil
	}, nil
}

func (t *typedToolReductionMiddleware[M]) WrapStreamableToolCall(_ context.Context, endpoint adk.StreamableToolCallEndpoint, tCtx *adk.ToolContext) (adk.StreamableToolCallEndpoint, error) {
	cfg := t.getToolConfig(tCtx.Name, sceneTruncation)
	if cfg == nil || cfg.TruncHandler == nil {
		return endpoint, nil
	}
	if _, excluded := t.excludeTruncTools[tCtx.Name]; excluded {
		return endpoint, nil
	}

	return func(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		output, err := endpoint(ctx, argumentsInJSON, opts...)
		if err != nil {
			return nil, err
		}

		readers := output.Copy(2)
		output = readers[0]
		origResp := readers[1]

		detail := &ToolDetail{
			ToolContext: tCtx,
			ToolArgument: &schema.ToolArgument{
				Text: argumentsInJSON,
			},
			StreamToolResult: schema.StreamReaderWithConvert(output, func(t string) (*schema.ToolResult, error) {
				return &schema.ToolResult{Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: t}}}, nil
			}),
		}
		truncResult, err := cfg.TruncHandler(ctx, detail)
		if err != nil {
			origResp.Close()
			return nil, err
		}
		if !truncResult.NeedTrunc {
			return origResp, nil
		}
		origResp.Close() // close err resp when not using it
		// 不使用 err resp 时将其关闭

		if truncResult.NeedOffload {
			if cfg.Backend == nil {
				return nil, fmt.Errorf("truncation: no backend for offload")
			}
			if err = cfg.Backend.Write(ctx, &filesystem.WriteRequest{
				FilePath: truncResult.OffloadFilePath,
				Content:  truncResult.OffloadContent,
			}); err != nil {
				return nil, err
			}
		}

		sr := schema.StreamReaderWithConvert(truncResult.StreamToolResult, func(t *schema.ToolResult) (string, error) {
			if t == nil || len(t.Parts) == 0 {
				return "", nil
			}
			return t.Parts[0].Text, nil
		})
		return sr, nil
	}, nil
}

func (t *typedToolReductionMiddleware[M]) WrapEnhancedInvokableToolCall(_ context.Context, endpoint adk.EnhancedInvokableToolCallEndpoint, tCtx *adk.ToolContext) (adk.EnhancedInvokableToolCallEndpoint, error) {
	cfg := t.getToolConfig(tCtx.Name, sceneTruncation)
	if cfg == nil || cfg.TruncHandler == nil {
		return endpoint, nil
	}
	if _, excluded := t.excludeTruncTools[tCtx.Name]; excluded {
		return endpoint, nil
	}

	return func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
		output, err := endpoint(ctx, toolArgument, opts...)
		if err != nil {
			return nil, err
		}
		detail := &ToolDetail{
			ToolContext:  tCtx,
			ToolArgument: toolArgument,
			ToolResult:   output,
		}
		truncResult, err := cfg.TruncHandler(ctx, detail)
		if err != nil {
			return nil, err
		}
		if !truncResult.NeedTrunc {
			return output, nil
		}
		if truncResult.NeedOffload {
			if cfg.Backend == nil {
				return nil, fmt.Errorf("truncation: no backend for offload")
			}
			if err = cfg.Backend.Write(ctx, &filesystem.WriteRequest{
				FilePath: truncResult.OffloadFilePath,
				Content:  truncResult.OffloadContent,
			}); err != nil {
				return nil, err
			}
		}
		return truncResult.ToolResult, nil
	}, nil
}

func (t *typedToolReductionMiddleware[M]) WrapEnhancedStreamableToolCall(_ context.Context, endpoint adk.EnhancedStreamableToolCallEndpoint, tCtx *adk.ToolContext) (adk.EnhancedStreamableToolCallEndpoint, error) {
	cfg := t.getToolConfig(tCtx.Name, sceneTruncation)
	if cfg == nil || cfg.TruncHandler == nil {
		return endpoint, nil
	}
	if _, excluded := t.excludeTruncTools[tCtx.Name]; excluded {
		return endpoint, nil
	}

	return func(ctx context.Context, toolArgument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error) {
		output, err := endpoint(ctx, toolArgument, opts...)
		if err != nil {
			return nil, err
		}

		readers := output.Copy(2)
		output = readers[0]
		origResp := readers[1]

		detail := &ToolDetail{
			ToolContext:      tCtx,
			ToolArgument:     toolArgument,
			StreamToolResult: output,
		}
		truncResult, err := cfg.TruncHandler(ctx, detail)
		if err != nil {
			origResp.Close()
			return nil, err
		}
		if !truncResult.NeedTrunc {
			return origResp, nil
		}
		origResp.Close() // close err resp when not using it
		// 不使用 err resp 时将其关闭

		if truncResult.NeedOffload {
			if cfg.Backend == nil {
				return nil, fmt.Errorf("truncation: no backend for offload")
			}
			if err = cfg.Backend.Write(ctx, &filesystem.WriteRequest{
				FilePath: truncResult.OffloadFilePath,
				Content:  truncResult.OffloadContent,
			}); err != nil {
				return nil, err
			}
		}

		return truncResult.StreamToolResult, nil
	}, nil
}

func (t *typedToolReductionMiddleware[M]) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[M], mc *adk.TypedModelContext[M]) (
	context.Context, *adk.TypedChatModelAgentState[M], error) {

	return t.beforeModelRewriteStateGeneric(ctx, state, mc)
}

func (t *typedToolReductionMiddleware[M]) beforeModelRewriteStateGeneric(ctx context.Context, state *adk.TypedChatModelAgentState[M], _ *adk.TypedModelContext[M]) (
	context.Context, *adk.TypedChatModelAgentState[M], error) {

	var (
		err             error
		estimatedTokens int64
	)

	// init msg tokens
	// 初始化消息 token
	estimatedTokens, err = t.config.TokenCounter(ctx, state.Messages, state.ToolInfos)
	if err != nil {
		return ctx, state, err
	}

	if estimatedTokens < t.config.MaxTokensForClear {
		return ctx, state, nil
	}

	// calc range
	// 计算范围
	var (
		start = 0
		end   = len(state.Messages)
	)
	for ; start < len(state.Messages); start++ {
		msg := state.Messages[start]
		if isAssistantMsg(msg) && !getMsgClearedFlagGeneric(msg) {
			break
		}
	}
	retention := t.config.ClearRetentionSuffixLimit
	for ; retention > 0 && end > 0; end-- {
		msg := state.Messages[end-1]
		if isAssistantMsg(msg) && hasToolCalls(msg) {
			retention--
			if retention == 0 {
				end--
				break
			}
		}
	}
	if start >= end {
		return ctx, state, nil
	}
	var (
		editTarget         []M
		clearAtLeastTokens = t.config.ClearAtLeastTokens
		offloadStash       []*offloadStashItem
	)

	editTarget, end, err = t.applyClearRewriteGeneric(ctx, state, start, end, clearAtLeastTokens)
	if err != nil {
		return ctx, state, err
	}

	// recursively handle
	// 递归处理
	toolCallMsgIndex := start

	for toolCallMsgIndex < end {
		toolCallMsg := editTarget[toolCallMsgIndex]
		toolCalls := getToolCallsGeneric(toolCallMsg)
		if isAssistantMsg(toolCallMsg) && len(toolCalls) > 0 {
			toolMsgIndex := toolCallMsgIndex
			for _, tc := range toolCalls {
				toolMsgIndex++
				if toolMsgIndex >= end {
					break
				}
				resultMsg := editTarget[toolMsgIndex]
				if !isToolResultMsg(resultMsg) { // unexpected
					break
				}
				if _, found := t.excludeClearTools[tc.Name]; found {
					continue
				}
				cfg := t.getToolConfig(tc.Name, sceneClear)
				if cfg == nil || cfg.ClearHandler == nil {
					continue
				}

				toolResult, fromContent, toolResultErr := toolResultFromMsgGeneric(resultMsg)
				if toolResultErr != nil {
					return ctx, state, toolResultErr
				}

				td := &ToolDetail{
					ToolContext: &adk.ToolContext{
						Name:   tc.Name,
						CallID: tc.CallID,
					},
					ToolArgument: &schema.ToolArgument{
						Text: tc.Arguments,
					},
					ToolResult: toolResult,
				}

				offloadInfo, offloadErr := cfg.ClearHandler(ctx, td)
				if offloadErr != nil {
					return ctx, state, offloadErr
				}
				if !offloadInfo.NeedClear {
					continue
				}
				if offloadInfo.NeedOffload {
					if cfg.Backend == nil {
						return ctx, state, fmt.Errorf("clear: no backend for offload")
					}
					if clearAtLeastTokens > 0 { // delay clear offloading
						// 延迟清理卸载
						offloadStash = append(offloadStash, &offloadStashItem{
							config:      cfg,
							offloadInfo: offloadInfo,
						})
					} else { // instant clear offloading
						// 立即清理卸载
						writeErr := cfg.Backend.Write(ctx, &filesystem.WriteRequest{
							FilePath: offloadInfo.OffloadFilePath,
							Content:  offloadInfo.OffloadContent,
						})
						if writeErr != nil {
							return ctx, state, writeErr
						}
					}
				}

				setToolCallArguments(toolCallMsg, tc.BlockIndex, offloadInfo.ToolArgument.Text)
				setToolResultContent(resultMsg, offloadInfo.ToolResult, fromContent)
			}

			// set dedup flag
			// 设置去重标记
			setMsgClearedFlagGeneric(toolCallMsg)
		}
		toolCallMsgIndex++
	}

	if clearAtLeastTokens > 0 {
		estimatedTokensAfterClear, err := t.config.TokenCounter(ctx, editTarget, state.ToolInfos)
		if err != nil {
			return ctx, state, err
		}
		tokensCleared := estimatedTokens - estimatedTokensAfterClear
		if tokensCleared < clearAtLeastTokens {
			// clear not applied, post process won't apply as well.
			// 未应用清理，后处理也不会应用。
			return ctx, state, nil
		}
		for _, item := range offloadStash {
			writeErr := item.config.Backend.Write(ctx, &filesystem.WriteRequest{
				FilePath: item.offloadInfo.OffloadFilePath,
				Content:  item.offloadInfo.OffloadContent,
			})
			if writeErr != nil {
				return ctx, state, writeErr
			}
		}
	}

	state.Messages = editTarget // replace original state messages
	// 替换原始 state 消息

	if t.config.ClearPostProcess != nil {
		ctx = t.config.ClearPostProcess(ctx, state)
	}

	return ctx, state, nil
}

func (t *typedToolReductionMiddleware[M]) applyClearRewriteGeneric(ctx context.Context, state *adk.TypedChatModelAgentState[M], start, end int, clearAtLeastTokens int64) (
	[]M, int, error) {
	var (
		editTarget      []M
		needProcessPart []M
	)

	editTarget = append(editTarget, state.Messages[:start]...)

	if clearAtLeastTokens > 0 {
		needProcessPart = copyMessagesGeneric(state.Messages[start:end])
	} else {
		needProcessPart = state.Messages[start:end]
	}

	if t.config.ClearMessageRewriter != nil {
		var (
			rewritten  []M
			origLength = len(needProcessPart)
		)
		for i := 0; i < len(needProcessPart); {
			msg := needProcessPart[i]
			if isSystemMsg(msg) || isUserMsg(msg) {
				rewritten = append(rewritten, msg)
				i++
			} else if isToolResultMsg(msg) {
				// tool result message (schema.Tool role or agentic user msg carrying FunctionToolResult)
				// 工具结果消息（schema.Tool 角色，或携带 FunctionToolResult 的 agentic user msg）
				i++
			} else if isAssistantMsg(msg) {
				toolCalls := getToolCallsGeneric(msg)
				if len(toolCalls) == 0 {
					rewritten = append(rewritten, msg)
					i++
					continue
				}
				var (
					toolResponseMessages []M
					trStart              = i + 1
					trEnd                = i + len(toolCalls) + 1
				)
				if trStart >= trEnd || trStart >= len(needProcessPart) || trEnd > len(needProcessPart) {
					toolResponseMessages = nil
				} else {
					toolResponseMessages = needProcessPart[trStart:trEnd]
				}

				rewrittenMessages, rewriteErr := t.config.ClearMessageRewriter(ctx, msg, toolResponseMessages)
				if rewriteErr != nil {
					return nil, 0, rewriteErr
				}
				rewritten = append(rewritten, rewrittenMessages...)
				i = trEnd
			} else { // unexpected
				return nil, 0, fmt.Errorf("[applyClearRewrite] unexpected message: %v", any(msg))
			}
		}
		editTarget = append(editTarget, rewritten...)
		editTarget = append(editTarget, state.Messages[end:]...)
		end = end - origLength + len(rewritten)
	} else {
		editTarget = append(editTarget, needProcessPart...)
		editTarget = append(editTarget, state.Messages[end:]...)
	}

	return editTarget, end, nil
}

type offloadStashItem struct {
	config      *ToolReductionConfig
	offloadInfo *ClearResult
}

// toolCallInfo represents a tool call extracted from a message for generic processing.
// toolCallInfo 表示从消息中提取的工具调用，用于通用处理。
type toolCallInfo struct {
	// BlockIndex is the index used to locate the tool call within the message.
	// For *schema.Message: index into msg.ToolCalls slice.
	// For *schema.AgenticMessage: index into msg.ContentBlocks slice.
	//
	// BlockIndex 是用于在消息中定位工具调用的索引。
	// 对于 *schema.Message：索引 msg.ToolCalls 切片。
	// 对于 *schema.AgenticMessage：索引 msg.ContentBlocks 切片。
	BlockIndex int
	CallID     string
	Name       string
	Arguments  string
}

// isAssistantMsg checks if a message has assistant role.
// isAssistantMsg 检查消息是否为 assistant 角色。
func isAssistantMsg[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.Assistant
	case *schema.AgenticMessage:
		return m.Role == schema.AgenticRoleTypeAssistant
	}
	return false
}

// isSystemMsg checks if a message has system role.
// isSystemMsg 检查消息是否为 system 角色。
func isSystemMsg[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.System
	case *schema.AgenticMessage:
		return m.Role == schema.AgenticRoleTypeSystem
	}
	return false
}

// isUserMsg checks if a message has user role (and is not a tool-result message).
// isUserMsg 检查消息是否为 user 角色（且不是工具结果消息）。
func isUserMsg[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.User
	case *schema.AgenticMessage:
		if m.Role != schema.AgenticRoleTypeUser {
			return false
		}
		// A user-role agentic message that contains any FunctionToolResult block
		// is a tool result message, not a normal user message — even if it also
		// carries UserInput blocks. This ensures the clear flow's tool-call grouping
		// remains correctly aligned.
		//
		// 包含任意 FunctionToolResult 块的 user-role agentic 消息
		// 是工具结果消息，而不是普通用户消息——即使它也携带 UserInput 块。
		// 这可确保清理流程中的工具调用分组保持正确对齐。
		for _, block := range m.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeFunctionToolResult {
				return false
			}
		}
		return len(m.ContentBlocks) > 0
	}
	return false
}

// hasToolCalls checks if an assistant message contains tool calls.
// hasToolCalls 检查 assistant 消息是否包含工具调用。
func hasToolCalls[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return len(m.ToolCalls) > 0
	case *schema.AgenticMessage:
		for _, block := range m.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeFunctionToolCall {
				return true
			}
		}
	}
	return false
}

// isToolResultMsg checks if a message is a tool result message.
// For *schema.Message: role == Tool.
// For *schema.AgenticMessage: user-role message with at least one FunctionToolResult block.
//
// isToolResultMsg 检查消息是否为工具结果消息。
// 对于 *schema.Message：role == Tool。
// 对于 *schema.AgenticMessage：包含至少一个 FunctionToolResult 块的 user-role 消息。
func isToolResultMsg[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.Tool
	case *schema.AgenticMessage:
		if m.Role != schema.AgenticRoleTypeUser {
			return false
		}
		for _, block := range m.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeFunctionToolResult {
				return true
			}
		}
	}
	return false
}

// isToolResultOnlyMsg checks if a message is exclusively a tool result message
// (no other content besides tool results).
// For *schema.Message: role == Tool.
// For *schema.AgenticMessage: user-role message where ALL content blocks are FunctionToolResult.
//
// isToolResultOnlyMsg 检查消息是否只包含工具结果
// （除工具结果外没有其他内容）。
// 对于 *schema.Message：role == Tool。
// 对于 *schema.AgenticMessage：所有内容块都是 FunctionToolResult 的 user-role 消息。
func isToolResultOnlyMsg[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.Tool
	case *schema.AgenticMessage:
		if m.Role != schema.AgenticRoleTypeUser || len(m.ContentBlocks) == 0 {
			return false
		}
		for _, block := range m.ContentBlocks {
			if block == nil || block.Type != schema.ContentBlockTypeFunctionToolResult {
				return false
			}
		}
		return true
	}
	return false
}

// getMsgClearedFlagGeneric checks if a message has the cleared flag set.
// getMsgClearedFlagGeneric 检查消息是否设置了 cleared 标记。
func getMsgClearedFlagGeneric[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return getMsgClearedFlag(m)
	case *schema.AgenticMessage:
		if m.Extra == nil {
			return false
		}
		v, ok := m.Extra[msgClearedFlag].(bool)
		return ok && v
	}
	return false
}

// setMsgClearedFlagGeneric sets the cleared flag on a message.
// setMsgClearedFlagGeneric 设置消息的 cleared 标记。
func setMsgClearedFlagGeneric[M adk.MessageType](msg M) {
	switch m := any(msg).(type) {
	case *schema.Message:
		setMsgClearedFlag(m)
	case *schema.AgenticMessage:
		if m.Extra == nil {
			m.Extra = make(map[string]any)
		}
		m.Extra[msgClearedFlag] = true
	}
}

// getToolCallsGeneric extracts tool call info from an assistant message.
// getToolCallsGeneric 从 assistant 消息中提取工具调用信息。
func getToolCallsGeneric[M adk.MessageType](msg M) []toolCallInfo {
	switch m := any(msg).(type) {
	case *schema.Message:
		if len(m.ToolCalls) == 0 {
			return nil
		}
		result := make([]toolCallInfo, 0, len(m.ToolCalls))
		for i, tc := range m.ToolCalls {
			result = append(result, toolCallInfo{
				BlockIndex: i,
				CallID:     tc.ID,
				Name:       tc.Function.Name,
				Arguments:  tc.Function.Arguments,
			})
		}
		return result
	case *schema.AgenticMessage:
		var result []toolCallInfo
		for i, block := range m.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeFunctionToolCall && block.FunctionToolCall != nil {
				result = append(result, toolCallInfo{
					BlockIndex: i,
					CallID:     block.FunctionToolCall.CallID,
					Name:       block.FunctionToolCall.Name,
					Arguments:  block.FunctionToolCall.Arguments,
				})
			}
		}
		return result
	}
	return nil
}

// setToolCallArguments updates the arguments for a tool call at the given block index.
// setToolCallArguments 更新给定块索引处工具调用的参数。
func setToolCallArguments[M adk.MessageType](msg M, blockIndex int, args string) {
	switch m := any(msg).(type) {
	case *schema.Message:
		m.ToolCalls[blockIndex].Function.Arguments = args
	case *schema.AgenticMessage:
		if m.ContentBlocks[blockIndex].FunctionToolCall != nil {
			m.ContentBlocks[blockIndex].FunctionToolCall.Arguments = args
		}
	}
}

// toolResultFromMsgGeneric extracts tool result from a message as a *schema.ToolResult.
// For *schema.Message: delegates to existing toolResultFromMessage.
// For *schema.AgenticMessage: iterates FunctionToolResult blocks.
// The fromContent flag indicates whether the result came from simple content (true)
// or multi-part content (false), which affects how setToolResultContent writes it back.
//
// toolResultFromMsgGeneric 从消息中提取工具结果，作为 *schema.ToolResult。
// 对于 *schema.Message：委托给现有的 toolResultFromMessage。
// 对于 *schema.AgenticMessage：遍历 FunctionToolResult 块。
// fromContent 标记表示结果是否来自简单内容（true）
// 还是多段内容（false），这会影响 setToolResultContent 写回的方式。
func toolResultFromMsgGeneric[M adk.MessageType](msg M) (result *schema.ToolResult, fromContent bool, err error) {
	switch m := any(msg).(type) {
	case *schema.Message:
		return toolResultFromMessage(m)
	case *schema.AgenticMessage:
		var found *schema.FunctionToolResult
		for _, block := range m.ContentBlocks {
			if block == nil || block.Type != schema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			if found != nil {
				return nil, false, fmt.Errorf("reduction: AgenticMessage contains multiple FunctionToolResult blocks; expected exactly one per message")
			}
			found = block.FunctionToolResult
		}
		if found == nil {
			return &schema.ToolResult{Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: ""}}}, true, nil
		}
		parts := toolResultToOutputParts(found)
		if len(parts) == 0 {
			return &schema.ToolResult{Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: ""}}}, true, nil
		}
		isSimple := len(parts) == 1 && parts[0].Type == schema.ToolPartTypeText
		return &schema.ToolResult{Parts: parts}, isSimple, nil
	}
	return nil, false, fmt.Errorf("unsupported message type")
}

// setToolResultContent updates the tool result content in a message.
// For *schema.Message: sets msg.Content or msg.UserInputMultiContent.
// For *schema.AgenticMessage: reconstructs FunctionToolResult.Content.
//
// setToolResultContent 更新消息中的工具结果内容。
// 对于 *schema.Message：设置 msg.Content 或 msg.UserInputMultiContent。
// 对于 *schema.AgenticMessage：重建 FunctionToolResult.Content。
func setToolResultContent[M adk.MessageType](msg M, toolResult *schema.ToolResult, fromContent bool) {
	switch m := any(msg).(type) {
	case *schema.Message:
		if fromContent {
			if len(toolResult.Parts) > 0 {
				m.Content = toolResult.Parts[0].Text
			}
		} else {
			convResult, convErr := toolResult.ToMessageInputParts()
			if convErr == nil {
				m.UserInputMultiContent = convResult
			}
		}
	case *schema.AgenticMessage:
		for _, block := range m.ContentBlocks {
			if block == nil || block.Type != schema.ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
				continue
			}
			setToolResultFromOutputParts(block.FunctionToolResult, toolResult.Parts)
			return
		}
	}
}

// copyMessagesGeneric deep-copies a slice of messages.
// copyMessagesGeneric 深拷贝消息切片。
func copyMessagesGeneric[M adk.MessageType](msgs []M) []M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		origMsgs := any(msgs).([]*schema.Message)
		copied := copyMessages(origMsgs)
		return any(copied).([]M)
	case *schema.AgenticMessage:
		origMsgs := any(msgs).([]*schema.AgenticMessage)
		copied := copyAgenticMessages(origMsgs)
		return any(copied).([]M)
	}
	panic("unreachable")
}

func copyAgenticMessages(msgs []*schema.AgenticMessage) []*schema.AgenticMessage {
	resp := make([]*schema.AgenticMessage, len(msgs))
	for i, msg := range msgs {
		if msg == nil {
			continue
		}
		copied := &schema.AgenticMessage{
			Role:         msg.Role,
			ResponseMeta: msg.ResponseMeta,
		}
		if msg.ContentBlocks != nil {
			copied.ContentBlocks = make([]*schema.ContentBlock, len(msg.ContentBlocks))
			for j, block := range msg.ContentBlocks {
				if block == nil {
					continue
				}
				cb := *block
				// Deep copy mutable sub-fields
				// 深拷贝可变子字段
				if block.FunctionToolCall != nil {
					ftc := *block.FunctionToolCall
					cb.FunctionToolCall = &ftc
				}
				if block.FunctionToolResult != nil {
					ftr := *block.FunctionToolResult
					if block.FunctionToolResult.Content != nil {
						ftr.Content = make([]*schema.FunctionToolResultContentBlock, len(block.FunctionToolResult.Content))
						for k, rb := range block.FunctionToolResult.Content {
							if rb != nil {
								rbCopy := *rb // shallow copy: Image/Audio/Video/File sub-fields are not deep-copied.
								// 浅拷贝：Image/Audio/Video/File 子字段不会被深拷贝。
								// This is safe because the clear logic replaces entire blocks rather than
								// mutating media fields in-place. Custom ClearHandlers should follow the same pattern.
								//
								// 这是安全的，因为清理逻辑会替换整个块，而不是原地修改媒体字段。
								// 自定义 ClearHandlers 也应遵循相同模式。
								if rb.Text != nil {
									t := *rb.Text
									rbCopy.Text = &t
								}
								ftr.Content[k] = &rbCopy
							}
						}
					}
					cb.FunctionToolResult = &ftr
				}
				if block.Extra != nil {
					cb.Extra = make(map[string]any, len(block.Extra))
					for k, v := range block.Extra {
						cb.Extra[k] = v
					}
				}
				copied.ContentBlocks[j] = &cb
			}
		}
		if msg.Extra != nil {
			copied.Extra = make(map[string]any, len(msg.Extra))
			for k, v := range msg.Extra {
				copied.Extra[k] = v
			}
		}
		resp[i] = copied
	}
	return resp
}

func copyMessages(msgs []*schema.Message) []*schema.Message {
	resp := make([]*schema.Message, len(msgs))
	for i, msg := range msgs {
		if msg == nil {
			continue
		}
		copied := &schema.Message{
			Role:                     msg.Role,
			Content:                  msg.Content,
			MultiContent:             msg.MultiContent,
			UserInputMultiContent:    msg.UserInputMultiContent,
			AssistantGenMultiContent: msg.AssistantGenMultiContent,
			Name:                     msg.Name,
			ToolCalls:                nil,
			ToolCallID:               msg.ToolCallID,
			ToolName:                 msg.ToolName,
			ResponseMeta:             msg.ResponseMeta,
			ReasoningContent:         msg.ReasoningContent,
			Extra:                    nil,
		}
		if msg.ToolCalls != nil {
			copied.ToolCalls = append(make([]schema.ToolCall, 0, len(msg.ToolCalls)), msg.ToolCalls...)
		}
		if msg.Extra != nil {
			copied.Extra = make(map[string]any, len(msg.Extra))
			for k, v := range msg.Extra {
				copied.Extra[k] = v
			}
		}
		resp[i] = copied
	}
	return resp
}

// defaultTokenCounter estimates tokens, which treats one token as ~4 characters of text for common English text.
// github.com/tiktoken-go/tokenizer is highly recommended to replace it.
//
// defaultTokenCounter 估算 token，常见英文文本按约 4 个字符算 1 个 token。
// 强烈建议用 github.com/tiktoken-go/tokenizer 替换它。
func defaultTokenCounter(_ context.Context, msgs []*schema.Message, tools []*schema.ToolInfo) (int64, error) {
	var tokens int64
	for _, msg := range msgs {
		if msg == nil {
			continue
		}

		var sb strings.Builder
		sb.WriteString(string(msg.Role))
		sb.WriteString("\n")
		sb.WriteString(msg.ReasoningContent)
		sb.WriteString("\n")
		sb.WriteString(msg.Content)
		sb.WriteString("\n")
		if msg.Role == schema.Assistant && len(msg.ToolCalls) > 0 {
			for _, tc := range msg.ToolCalls {
				sb.WriteString(tc.Function.Name)
				sb.WriteString("\n")
				sb.WriteString(tc.Function.Arguments)
			}
		}

		for _, mc := range msg.UserInputMultiContent {
			switch mc.Type {
			case schema.ChatMessagePartTypeText:
				sb.WriteString(mc.Text)
				sb.WriteString("\n")
			default:
				// do nothing for multi-modal content
				// 对多模态内容不做处理
			}
		}

		for _, mc := range msg.AssistantGenMultiContent {
			switch mc.Type {
			case schema.ChatMessagePartTypeText:
				sb.WriteString(mc.Text)
				sb.WriteString("\n")
			default:
				// do nothing for multi-modal content
				// 对多模态内容不做处理
			}
		}

		n := int64(len(sb.String()) / 4)
		tokens += n
	}

	for _, tl := range tools {
		tl_ := *tl
		tl_.Extra = nil
		text, err := sonic.MarshalString(tl_)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal tool info: %w", err)
		}

		tokens += int64(len(text) / 4)
	}

	return tokens, nil
}

// defaultTruncHandler applies the same truncation strategy to both non-streaming
// and streaming tool outputs.
//
// Processing steps:
//  1. Read and join tool output into a complete result:
//     - Non-streaming: use ToolResult directly.
//     - Streaming: consume the whole StreamToolResult, then concat all chunks.
//  2. If output is empty or total text length does not exceed truncMaxLength,
//     return NeedTrunc=false.
//  3. If exceeded, replace oversized text parts with truncation notices and
//     offload the full original content.
//
// Streaming-specific behavior:
//   - Truncation is not incremental. The handler waits until the entire stream is read
//     before deciding and producing output.
//   - If stream Recv() returns a non-EOF error, getJointToolResult treats it as
//     "skip processing" (needProcess=false, err=nil), so this handler returns
//     NeedTrunc=false and does not propagate that recv error.
//   - When truncation is applied to a streaming tool result, output is re-emitted as a
//     buffered single-result stream (not original chunk-by-chunk streaming semantics).
//
// If a tool requires strict incremental streaming behavior, provide a custom TruncHandler for that tool.
//
// defaultTruncHandler 对非流式和流式工具输出应用相同的截断策略。
// 处理步骤：
// 1. 读取并合并工具输出为完整结果：
// - 非流式：直接使用 ToolResult。
// - 流式：消费整个 StreamToolResult，然后拼接所有 chunk。
// 2. 如果输出为空或总文本长度未超过 truncMaxLength，
// 返回 NeedTrunc=false。
// 3. 如果超出限制，将过大的文本部分替换为截断提示，
// 并卸载完整原始内容。
// 流式相关行为：
// - 截断不是增量执行的。处理器会等待读完整个流
// 后再决定并生成输出。
// - 如果流的 Recv() 返回非 EOF 错误，getJointToolResult 会将其视为
// "skip processing"（needProcess=false, err=nil），因此该处理器返回
// NeedTrunc=false，且不会传播该 recv 错误。
// - 对流式工具结果应用截断时，输出会重新发射为
// 缓冲的单结果流（不是原始逐 chunk 流式语义）。
// 如果工具需要严格的增量流式行为，请为该工具提供自定义 TruncHandler。
func defaultTruncHandler(
	genOffloadFilePathFn func(ctx context.Context, toolDetail *ToolDetail) (filePath string, err error),
	truncMaxLength int,
) func(ctx context.Context, detail *ToolDetail) (truncResult *TruncResult, err error) {

	return func(ctx context.Context, detail *ToolDetail) (offloadInfo *TruncResult, err error) {
		isStreamResult := detail.StreamToolResult != nil
		resultParts, needProcess, err := getJointToolResult(detail)
		if err != nil {
			return nil, err
		}
		if !needProcess {
			return &TruncResult{NeedTrunc: false}, nil
		}

		fullLength, textPartsCnt := 0, 0
		for _, part := range resultParts {
			if part.Type == schema.ToolPartTypeText {
				fullLength += len(part.Text)
				textPartsCnt++
			}
		}
		if textPartsCnt == 0 || fullLength <= truncMaxLength {
			return &TruncResult{NeedTrunc: false}, nil
		}

		var (
			offloadContent  = stringifyToolOutputParts(resultParts)
			truncPartLength = truncMaxLength / textPartsCnt
			previewSize     = truncPartLength / 2
		)

		filePath, err := genOffloadFilePathFn(ctx, detail)
		if err != nil {
			return nil, err
		}

		for i, part := range resultParts {
			text := part.Text
			if part.Type != schema.ToolPartTypeText ||
				len(text) < truncPartLength {
				continue
			}
			truncNotify, fmtErr := pyfmt.Fmt(getTruncFmt(), map[string]any{
				"original_size": len(part.Text),
				"file_path":     filePath,
				"preview_size":  previewSize,
				"preview_first": clampPrefixToUTF8Boundary(text, previewSize),
				"preview_last":  clampSuffixToUTF8Boundary(text, previewSize),
			})
			if fmtErr != nil {
				return nil, fmtErr
			}
			resultParts[i].Text = truncNotify
		}

		tr := &TruncResult{
			NeedTrunc:       true,
			NeedOffload:     true,
			OffloadFilePath: filePath,
			OffloadContent:  offloadContent,
		}
		if !isStreamResult {
			tr.ToolResult = &schema.ToolResult{Parts: resultParts}
		} else {
			sr, sw := schema.Pipe[*schema.ToolResult](1)
			sw.Send(&schema.ToolResult{Parts: resultParts}, nil)
			sw.Close()
			tr.StreamToolResult = sr
		}
		return tr, nil
	}
}

func clampPrefixToUTF8Boundary(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func clampSuffixToUTF8Boundary(s string, n int) string {
	if n <= 0 {
		return ""
	}
	l := len(s)
	if n >= l {
		return s
	}
	start := l - n
	for start < l && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

func defaultClearHandler(
	genOffloadFilePathFn func(ctx context.Context, toolDetail *ToolDetail) (filePath string, err error),
	needOffload bool,
	readFileToolName string,
) func(ctx context.Context, detail *ToolDetail) (*ClearResult, error) {

	return func(ctx context.Context, detail *ToolDetail) (clearResult *ClearResult, err error) {
		resultParts, needProcess, err := getJointToolResult(detail)
		if err != nil {
			return nil, err
		}
		if !needProcess {
			return &ClearResult{NeedClear: false}, nil
		}

		if needOffload {
			filePath, err := genOffloadFilePathFn(ctx, detail)
			if err != nil {
				return nil, err
			}
			textPlaceHolder, fmtErr := pyfmt.Fmt(getClearWithOffloadingFmt(), map[string]any{
				"file_path":      filePath,
				"read_tool_name": readFileToolName,
			})
			if fmtErr != nil {
				return nil, fmtErr
			}

			offloadContent := stringifyToolOutputParts(resultParts)
			for i, part := range resultParts {
				if part.Type != schema.ToolPartTypeText {
					continue
				}
				resultParts[i].Text = textPlaceHolder
			}
			clearResult = &ClearResult{
				NeedClear:       true,
				ToolArgument:    detail.ToolArgument,
				ToolResult:      &schema.ToolResult{Parts: resultParts},
				NeedOffload:     true,
				OffloadFilePath: filePath,
				OffloadContent:  offloadContent,
			}
		} else {
			textPlaceHolder := getClearWithoutOffloadingFmt()
			for i, part := range resultParts {
				if part.Type != schema.ToolPartTypeText {
					continue
				}
				resultParts[i].Text = textPlaceHolder
			}
			clearResult = &ClearResult{
				NeedClear:    true,
				ToolArgument: detail.ToolArgument,
				ToolResult:   &schema.ToolResult{Parts: resultParts},
				NeedOffload:  false,
			}
		}

		return clearResult, nil
	}
}

func getJointToolResult(toolDetail *ToolDetail) (toolOutputParts []schema.ToolOutputPart, needProcess bool, err error) {
	if toolDetail.ToolResult == nil && toolDetail.StreamToolResult == nil {
		return nil, false, fmt.Errorf("ToolResult and StreamToolResult are both nil")
	}

	if toolDetail.ToolResult != nil {
		toolOutputParts = toolDetail.ToolResult.Parts
	} else {
		var toolResultChunks []*schema.ToolResult
		for {
			toolResultChunk, recvErr := toolDetail.StreamToolResult.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				// return original stream reader, not sending recvErr
				// 返回原始流读取器，不发送 recvErr
				return nil, false, nil
			}
			toolResultChunks = append(toolResultChunks, toolResultChunk)
		}
		toolResult, concatErr := schema.ConcatToolResults(toolResultChunks)
		if concatErr != nil {
			return nil, false, concatErr
		}
		toolOutputParts = toolResult.Parts
	}

	if len(toolOutputParts) == 0 {
		return nil, false, nil
	}

	return toolOutputParts, true, nil
}

func stringifyToolOutputParts(toolOutputParts []schema.ToolOutputPart) string {
	if len(toolOutputParts) == 0 {
		return ""
	} else if len(toolOutputParts) == 1 && toolOutputParts[0].Type == schema.ToolPartTypeText {
		return toolOutputParts[0].Text
	} else {
		b, _ := json.MarshalIndent(toolOutputParts, "", "\t")
		return string(b)
	}
}

func getMsgClearedFlag(msg *schema.Message) (offloaded bool) {
	if msg.Extra == nil {
		return false
	}
	v, ok := msg.Extra[msgClearedFlag].(bool)
	if !ok {
		return false
	}
	return v
}

func setMsgClearedFlag(msg *schema.Message) {
	if msg.Extra == nil {
		msg.Extra = make(map[string]any)
	}
	msg.Extra[msgClearedFlag] = true
}

func toolResultFromMessage(msg *schema.Message) (result *schema.ToolResult, fromContent bool, err error) {
	if msg.Role != schema.Tool {
		return nil, false, fmt.Errorf("message role %s is not a tool", msg.Role)
	}
	if len(msg.UserInputMultiContent) > 0 {
		result = &schema.ToolResult{Parts: make([]schema.ToolOutputPart, 0, len(msg.UserInputMultiContent))}
		for _, part := range msg.UserInputMultiContent {
			top, convErr := convMessageInputPartToToolOutputPart(part)
			if convErr != nil {
				return nil, false, convErr
			}
			result.Parts = append(result.Parts, top)
		}
		return result, false, nil
	}
	return &schema.ToolResult{Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: msg.Content}}}, true, nil
}

func convMessageInputPartToToolOutputPart(msgPart schema.MessageInputPart) (schema.ToolOutputPart, error) {
	switch msgPart.Type {
	case schema.ChatMessagePartTypeText:
		return schema.ToolOutputPart{
			Type: schema.ToolPartTypeText,
			Text: msgPart.Text,
		}, nil
	case schema.ChatMessagePartTypeImageURL:
		return schema.ToolOutputPart{
			Type: schema.ToolPartTypeImage,
			Image: &schema.ToolOutputImage{
				MessagePartCommon: msgPart.Image.MessagePartCommon,
			},
		}, nil
	case schema.ChatMessagePartTypeAudioURL:
		return schema.ToolOutputPart{
			Type: schema.ToolPartTypeAudio,
			Audio: &schema.ToolOutputAudio{
				MessagePartCommon: msgPart.Audio.MessagePartCommon,
			},
		}, nil
	case schema.ChatMessagePartTypeVideoURL:
		return schema.ToolOutputPart{
			Type: schema.ToolPartTypeVideo,
			Video: &schema.ToolOutputVideo{
				MessagePartCommon: msgPart.Video.MessagePartCommon,
			},
		}, nil
	case schema.ChatMessagePartTypeFileURL:
		return schema.ToolOutputPart{
			Type: schema.ToolPartTypeFile,
			File: &schema.ToolOutputFile{
				MessagePartCommon: msgPart.File.MessagePartCommon,
			},
		}, nil
	default:
		return schema.ToolOutputPart{}, fmt.Errorf("unknown msg part type: %v", msgPart.Type)
	}
}

// toolResultToOutputParts converts a FunctionToolResult's Content blocks to ToolOutputPart slice.
// toolResultToOutputParts 将 FunctionToolResult 的 Content 块转换为 ToolOutputPart 切片。
func toolResultToOutputParts(f *schema.FunctionToolResult) []schema.ToolOutputPart {
	var parts []schema.ToolOutputPart
	for _, block := range f.Content {
		if block == nil {
			continue
		}
		if block.Text != nil {
			parts = append(parts, schema.ToolOutputPart{Type: schema.ToolPartTypeText, Text: block.Text.Text})
		} else if block.Image != nil {
			parts = append(parts, schema.ToolOutputPart{
				Type:  schema.ToolPartTypeImage,
				Image: &schema.ToolOutputImage{MessagePartCommon: schema.MessagePartCommon{URL: strPtr(block.Image.URL), MIMEType: block.Image.MIMEType}},
			})
		} else if block.Audio != nil {
			parts = append(parts, schema.ToolOutputPart{
				Type:  schema.ToolPartTypeAudio,
				Audio: &schema.ToolOutputAudio{MessagePartCommon: schema.MessagePartCommon{URL: strPtr(block.Audio.URL), MIMEType: block.Audio.MIMEType}},
			})
		} else if block.Video != nil {
			parts = append(parts, schema.ToolOutputPart{
				Type:  schema.ToolPartTypeVideo,
				Video: &schema.ToolOutputVideo{MessagePartCommon: schema.MessagePartCommon{URL: strPtr(block.Video.URL), MIMEType: block.Video.MIMEType}},
			})
		} else if block.File != nil {
			parts = append(parts, schema.ToolOutputPart{
				Type: schema.ToolPartTypeFile,
				File: &schema.ToolOutputFile{MessagePartCommon: schema.MessagePartCommon{URL: strPtr(block.File.URL), MIMEType: block.File.MIMEType}},
			})
		}
	}
	return parts
}

// setToolResultFromOutputParts converts ToolOutputPart slice back to FunctionToolResultContentBlock
// slice and sets f.Content.
//
// setToolResultFromOutputParts 将 ToolOutputPart 切片转换回 FunctionToolResultContentBlock
// 切片，并设置 f.Content。
func setToolResultFromOutputParts(f *schema.FunctionToolResult, parts []schema.ToolOutputPart) {
	var newBlocks []*schema.FunctionToolResultContentBlock
	for _, part := range parts {
		switch part.Type {
		case schema.ToolPartTypeText:
			newBlocks = append(newBlocks, &schema.FunctionToolResultContentBlock{
				Type: schema.FunctionToolResultContentBlockTypeText,
				Text: &schema.UserInputText{Text: part.Text},
			})
		case schema.ToolPartTypeImage:
			if part.Image != nil {
				newBlocks = append(newBlocks, &schema.FunctionToolResultContentBlock{
					Type:  schema.FunctionToolResultContentBlockTypeImage,
					Image: &schema.UserInputImage{URL: ptrStr(part.Image.URL), MIMEType: part.Image.MIMEType},
				})
			}
		case schema.ToolPartTypeAudio:
			if part.Audio != nil {
				newBlocks = append(newBlocks, &schema.FunctionToolResultContentBlock{
					Type:  schema.FunctionToolResultContentBlockTypeAudio,
					Audio: &schema.UserInputAudio{URL: ptrStr(part.Audio.URL), MIMEType: part.Audio.MIMEType},
				})
			}
		case schema.ToolPartTypeVideo:
			if part.Video != nil {
				newBlocks = append(newBlocks, &schema.FunctionToolResultContentBlock{
					Type:  schema.FunctionToolResultContentBlockTypeVideo,
					Video: &schema.UserInputVideo{URL: ptrStr(part.Video.URL), MIMEType: part.Video.MIMEType},
				})
			}
		case schema.ToolPartTypeFile:
			if part.File != nil {
				newBlocks = append(newBlocks, &schema.FunctionToolResultContentBlock{
					Type: schema.FunctionToolResultContentBlockTypeFile,
					File: &schema.UserInputFile{URL: ptrStr(part.File.URL), MIMEType: part.File.MIMEType},
				})
			}
		}
	}
	f.Content = newBlocks
}

// strPtr returns a pointer to s, or nil if s is empty.
// strPtr 返回指向 s 的指针；如果 s 为空则返回 nil。
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ptrStr safely dereferences a *string, returning "" if nil.
// ptrStr 安全解引用 *string，nil 时返回 ""。
func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
