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

package filesystem

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

const (
	ToolNameLs        = "ls"
	ToolNameReadFile  = "read_file"
	ToolNameWriteFile = "write_file"
	ToolNameEditFile  = "edit_file"
	ToolNameGlob      = "glob"
	ToolNameGrep      = "grep"
	ToolNameExecute   = "execute"

	noFilesFound   = "No files found"
	noMatchesFound = "No matches found"
)

// ToolConfig configures a filesystem tool
// ToolConfig 配置 filesystem 工具
type ToolConfig struct {
	// Name overrides the tool name used in tool registration
	// optional, default tool name will be used if not set (empty string)
	//
	// Name 覆盖工具注册时使用的工具名称
	// 可选；未设置（空字符串）时使用默认工具名称
	Name string

	// Desc overrides the tool description used in tool registration
	// optional, default tool description will be used if not set (nil pointer)
	//
	// Desc 覆盖工具注册时使用的工具描述
	// 可选；未设置（nil pointer）时使用默认工具描述
	Desc *string

	// CustomTool provides a custom implementation for this tool.
	// If set, this custom tool will be used instead of the default implementation associated with Backend.
	// If not set, the default tool implementation associated with Backend will be created automatically.
	// optional
	//
	// CustomTool 为此工具提供自定义实现。
	// 如果设置，将使用此自定义工具，而不是与 Backend 关联的默认实现。
	// 如果未设置，将自动创建与 Backend 关联的默认工具实现。
	// 可选
	CustomTool tool.BaseTool

	// Disable disables this tool
	// If true, the tool will not be registered
	// optional, false by default
	//
	// Disable 禁用此工具
	// 如果为 true，则不会注册该工具
	// 可选，默认 false
	Disable bool
}

// Config is the configuration for the filesystem middleware
// Config 是 filesystem middleware 的配置
type Config struct {
	// Backend provides filesystem operations used by tools and offloading.
	// If set, filesystem tools (read_file, write_file, edit_file, glob, grep) will be registered.
	// At least one of Backend, Shell, or StreamingShell must be set.
	//
	// Backend 提供工具和 offloading 使用的 filesystem 操作。
	// 如果设置，将注册 filesystem 工具（read_file, write_file, edit_file, glob, grep）。
	// Backend、Shell 或 StreamingShell 至少必须设置一个。
	Backend filesystem.Backend

	// Shell provides shell command execution capability.
	// If set, an execute tool will be registered to support shell command execution.
	// At least one of Backend, Shell, or StreamingShell must be set.
	// Mutually exclusive with StreamingShell.
	//
	// Shell 提供 shell 命令执行能力。
	// 如果设置，将注册 execute 工具以支持 shell 命令执行。
	// Backend、Shell 或 StreamingShell 至少必须设置一个。
	// 与 StreamingShell 互斥。
	Shell filesystem.Shell
	// StreamingShell provides streaming shell command execution capability.
	// If set, a streaming execute tool will be registered to support streaming shell command execution.
	// At least one of Backend, Shell, or StreamingShell must be set.
	// Mutually exclusive with Shell.
	//
	// StreamingShell 提供流式 shell 命令执行能力。
	// 如果设置，将注册流式 execute 工具以支持流式 shell 命令执行。
	// Backend、Shell 或 StreamingShell 至少必须设置一个。
	// 与 Shell 互斥。
	StreamingShell filesystem.StreamingShell

	// LsToolConfig configures the ls tool
	// optional
	//
	// LsToolConfig 配置 ls 工具
	// 可选
	LsToolConfig *ToolConfig
	// ReadFileToolConfig configures the read_file tool.
	// This config applies to both the standard read_file tool (InvokableTool) and
	// the multimodal read_file tool (EnhancedInvokableTool) when UseMultiModalRead is true.
	// optional
	//
	// ReadFileToolConfig 配置 read_file 工具。
	// 当 UseMultiModalRead 为 true 时，此配置同时适用于标准 read_file 工具（InvokableTool）和多模态 read_file 工具（EnhancedInvokableTool）。
	// 可选
	ReadFileToolConfig *ToolConfig
	// WriteFileToolConfig configures the write_file tool
	// optional
	//
	// WriteFileToolConfig 配置 write_file 工具
	// 可选
	WriteFileToolConfig *ToolConfig
	// EditFileToolConfig configures the edit_file tool
	// optional
	//
	// EditFileToolConfig 配置 edit_file 工具
	// 可选
	EditFileToolConfig *ToolConfig
	// GlobToolConfig configures the glob tool
	// optional
	//
	// GlobToolConfig 配置 glob 工具
	// 可选
	GlobToolConfig *ToolConfig
	// GrepToolConfig configures the grep tool
	// optional
	//
	// GrepToolConfig 配置 grep 工具
	// 可选
	GrepToolConfig *ToolConfig

	// WithoutLargeToolResultOffloading disables automatic offloading of large tool result to Backend
	// optional, false(enabled) by default
	//
	// WithoutLargeToolResultOffloading 禁用将大型工具结果自动 offloading 到 Backend
	// 可选，默认 false（启用）
	WithoutLargeToolResultOffloading bool
	// LargeToolResultOffloadingTokenLimit sets the token threshold to trigger offloading
	// optional, 20000 by default
	//
	// LargeToolResultOffloadingTokenLimit 设置触发 offloading 的 token 阈值
	// 可选，默认 20000
	LargeToolResultOffloadingTokenLimit int
	// LargeToolResultOffloadingPathGen generates the write path for offloaded results based on context and ToolInput
	// optional, "/large_tool_result/{ToolCallID}" by default
	//
	// LargeToolResultOffloadingPathGen 基于 context 和 ToolInput 生成 offloaded 结果的写入路径
	// 可选，默认 "/large_tool_result/{ToolCallID}"
	LargeToolResultOffloadingPathGen func(ctx context.Context, input *compose.ToolInput) (string, error)

	// CustomSystemPrompt overrides the default ToolsSystemPrompt appended to agent instruction
	// optional, ToolsSystemPrompt by default
	//
	// CustomSystemPrompt 覆盖追加到 agent instruction 的默认 ToolsSystemPrompt
	// 可选，默认 ToolsSystemPrompt
	CustomSystemPrompt *string

	// CustomLsToolDesc overrides the ls tool description used in tool registration
	// optional, ListFilesToolDesc by default
	// Deprecated: Use LsToolConfig.Desc instead
	//
	// CustomLsToolDesc 覆盖工具注册中使用的 ls 工具描述
	// 可选，默认使用 ListFilesToolDesc
	// Deprecated: 改用 LsToolConfig.Desc
	CustomLsToolDesc *string
	// CustomReadFileToolDesc overrides the read_file tool description
	// optional, ReadFileToolDesc by default
	// Deprecated: Use ReadFileToolConfig.Desc instead
	//
	// CustomReadFileToolDesc 覆盖 read_file 工具描述
	// 可选，默认使用 ReadFileToolDesc
	// Deprecated: 改用 ReadFileToolConfig.Desc
	CustomReadFileToolDesc *string
	// CustomGrepToolDesc overrides the grep tool description
	// optional, GrepToolDesc by default
	// Deprecated: Use GrepToolConfig.Desc instead
	//
	// CustomGrepToolDesc 覆盖 grep 工具描述
	// 可选，默认使用 GrepToolDesc
	// Deprecated: 改用 GrepToolConfig.Desc
	CustomGrepToolDesc *string
	// CustomGlobToolDesc overrides the glob tool description
	// optional, GlobToolDesc by default
	// Deprecated: Use GlobToolConfig.Desc instead
	//
	// CustomGlobToolDesc 覆盖 glob 工具描述
	// 可选，默认使用 GlobToolDesc
	// Deprecated: 改用 GlobToolConfig.Desc
	CustomGlobToolDesc *string
	// CustomWriteFileToolDesc overrides the write_file tool description
	// optional, WriteFileToolDesc by default
	// Deprecated: Use WriteFileToolConfig.Desc instead
	//
	// CustomWriteFileToolDesc 覆盖 write_file 工具描述
	// 可选，默认使用 WriteFileToolDesc
	// Deprecated: 改用 WriteFileToolConfig.Desc
	CustomWriteFileToolDesc *string
	// CustomEditToolDesc overrides the edit_file tool description
	// optional, EditFileToolDesc by default
	// Deprecated: Use EditFileToolConfig.Desc instead
	//
	// CustomEditToolDesc 覆盖 edit_file 工具描述
	// 可选，默认使用 EditFileToolDesc
	// Deprecated: 改用 EditFileToolConfig.Desc
	CustomEditToolDesc *string
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config should not be nil")
	}
	if c.Backend == nil {
		return errors.New("backend should not be nil")
	}
	if c.StreamingShell != nil && c.Shell != nil {
		return errors.New("shell and streaming shell should not be both set")
	}
	return nil
}

// NewMiddleware constructs and returns the filesystem middleware.
//
// Deprecated: Use New instead. New returns
// a ChatModelAgentMiddleware which provides better context propagation through wrapper methods
// and is the recommended approach for new code. See ChatModelAgentMiddleware documentation
// for details on the benefits over AgentMiddleware.
//
// NewMiddleware 构造并返回 filesystem middleware。
// Deprecated: 改用 New。New 返回 ChatModelAgentMiddleware，它通过包装方法提供更好的 context 传播，是新代码的推荐方式。有关相比 AgentMiddleware 的优势，请参阅 ChatModelAgentMiddleware 文档。
func NewMiddleware(ctx context.Context, config *Config) (adk.AgentMiddleware, error) {
	err := config.Validate()
	if err != nil {
		return adk.AgentMiddleware{}, err
	}
	ts, err := getFilesystemTools(ctx, &MiddlewareConfig{
		Backend:                 config.Backend,
		Shell:                   config.Shell,
		StreamingShell:          config.StreamingShell,
		LsToolConfig:            config.LsToolConfig,
		ReadFileToolConfig:      config.ReadFileToolConfig,
		WriteFileToolConfig:     config.WriteFileToolConfig,
		EditFileToolConfig:      config.EditFileToolConfig,
		GlobToolConfig:          config.GlobToolConfig,
		GrepToolConfig:          config.GrepToolConfig,
		CustomSystemPrompt:      config.CustomSystemPrompt,
		CustomLsToolDesc:        config.CustomLsToolDesc,
		CustomReadFileToolDesc:  config.CustomReadFileToolDesc,
		CustomGrepToolDesc:      config.CustomGrepToolDesc,
		CustomGlobToolDesc:      config.CustomGlobToolDesc,
		CustomWriteFileToolDesc: config.CustomWriteFileToolDesc,
		CustomEditToolDesc:      config.CustomEditToolDesc,
	})
	if err != nil {
		return adk.AgentMiddleware{}, err
	}

	var systemPrompt string
	if config.CustomSystemPrompt != nil {
		systemPrompt = *config.CustomSystemPrompt
	}

	m := adk.AgentMiddleware{
		AdditionalInstruction: systemPrompt,
		AdditionalTools:       ts,
	}

	if !config.WithoutLargeToolResultOffloading {
		m.WrapToolCall = newToolResultOffloading(ctx, &toolResultOffloadingConfig{
			Backend:       config.Backend,
			TokenLimit:    config.LargeToolResultOffloadingTokenLimit,
			PathGenerator: config.LargeToolResultOffloadingPathGen,
		})
	}

	return m, nil
}

// MiddlewareConfig is the configuration for the filesystem middleware
// MiddlewareConfig 是 filesystem middleware 的配置
type MiddlewareConfig struct {
	// Backend provides filesystem operations used by tools and offloading.
	// required
	//
	// Backend 提供工具和卸载所用的 filesystem 操作。
	// 必填
	Backend filesystem.Backend

	// Shell provides shell command execution capability.
	// If set, an execute tool will be registered to support shell command execution.
	// optional, mutually exclusive with StreamingShell
	//
	// Shell 提供 shell 命令执行能力。
	// 如果设置，将注册 execute 工具以支持 shell 命令执行。
	// 可选，与 StreamingShell 互斥
	Shell filesystem.Shell
	// StreamingShell provides streaming shell command execution capability.
	// If set, a streaming execute tool will be registered for real-time output.
	// optional, mutually exclusive with Shell
	//
	// StreamingShell 提供流式 shell 命令执行能力。
	// 如果设置，将注册流式 execute 工具以支持实时输出。
	// 可选，与 Shell 互斥
	StreamingShell filesystem.StreamingShell

	// LsToolConfig configures the ls tool
	// optional
	//
	// LsToolConfig 配置 ls 工具
	// 可选
	LsToolConfig *ToolConfig
	// ReadFileToolConfig configures the read_file tool.
	// This config applies to both the standard read_file tool (InvokableTool) and
	// the multimodal read_file tool (EnhancedInvokableTool) when UseMultiModalRead is true.
	// optional
	//
	// ReadFileToolConfig 配置 read_file 工具。
	// 当 UseMultiModalRead 为 true 时，此配置同时适用于标准 read_file 工具（InvokableTool）和多模态 read_file 工具（EnhancedInvokableTool）。
	// 可选
	ReadFileToolConfig *ToolConfig
	// WriteFileToolConfig configures the write_file tool
	// optional
	//
	// WriteFileToolConfig 配置 write_file 工具
	// 可选
	WriteFileToolConfig *ToolConfig
	// EditFileToolConfig configures the edit_file tool
	// optional
	//
	// EditFileToolConfig 配置 edit_file 工具
	// 可选
	EditFileToolConfig *ToolConfig
	// GlobToolConfig configures the glob tool
	// optional
	//
	// GlobToolConfig 配置 glob 工具
	// 可选
	GlobToolConfig *ToolConfig
	// GrepToolConfig configures the grep tool
	// optional
	//
	// GrepToolConfig 配置 grep 工具
	// 可选
	GrepToolConfig *ToolConfig

	// UseMultiModalRead enables multimodal read_file tool (EnhancedInvokableTool).
	// When true, read_file returns results via schema.ToolResult.Parts instead of plain text string.
	//
	// Requires Backend to implement filesystem.MultiModalReader interface.
	// The default implementation supports reading image files (PNG, JPG, etc.)
	// and PDF files with page range selection.
	//
	// If you provide a custom MultiModalReader, you may need to override
	// ReadFileToolConfig.Desc to accurately describe your implementation's capabilities.
	// The default description is composed of ReadFileToolDesc + EnhancedReadFileDescSuffix.
	//
	// Note: When enabled, the read_file tool becomes an EnhancedInvokableTool.
	// If you use ChatModelAgentMiddleware, you must implement ChatModelAgentMiddleware.WrapEnhancedInvokableToolCall
	// for the middleware to take effect on the read_file tool.
	//
	// Default false, preserving backward compatibility.
	//
	// UseMultiModalRead 启用多模态 read_file 工具（EnhancedInvokableTool）。
	// 为 true 时，read_file 通过 schema.ToolResult.Parts 返回结果，而不是纯文本字符串。
	// 要求 Backend 实现 filesystem.MultiModalReader interface。
	// 默认实现支持读取图片文件（PNG、JPG 等）以及可选择页码范围的 PDF 文件。
	// 如果提供自定义 MultiModalReader，可能需要覆盖 ReadFileToolConfig.Desc，以准确描述实现能力。
	// 默认描述由 ReadFileToolDesc + EnhancedReadFileDescSuffix 组成。
	// 注意：启用后，read_file 工具会变为 EnhancedInvokableTool。
	// 如果使用 ChatModelAgentMiddleware，必须实现 ChatModelAgentMiddleware.WrapEnhancedInvokableToolCall，middleware 才能对 read_file 工具生效。
	// 默认 false，以保持向后兼容。
	UseMultiModalRead bool

	// CustomSystemPrompt overrides the default ToolsSystemPrompt appended to agent instruction
	// optional, ToolsSystemPrompt by default
	//
	// CustomSystemPrompt 覆盖追加到 agent instruction 的默认 ToolsSystemPrompt
	// 可选，默认使用 ToolsSystemPrompt
	CustomSystemPrompt *string

	// CustomLsToolDesc overrides the ls tool description used in tool registration
	// optional, ListFilesToolDesc by default
	// Deprecated: Use LsToolConfig.Desc instead
	//
	// CustomLsToolDesc 覆盖工具注册中使用的 ls 工具描述
	// 可选，默认使用 ListFilesToolDesc
	// Deprecated: 改用 LsToolConfig.Desc
	CustomLsToolDesc *string
	// CustomReadFileToolDesc overrides the read_file tool description
	// optional, ReadFileToolDesc by default
	// Deprecated: Use ReadFileToolConfig.Desc instead
	//
	// CustomReadFileToolDesc 覆盖 read_file 工具描述
	// 可选，默认使用 ReadFileToolDesc
	// Deprecated: 请改用 ReadFileToolConfig.Desc
	CustomReadFileToolDesc *string
	// CustomGrepToolDesc overrides the grep tool description
	// optional, GrepToolDesc by default
	// Deprecated: Use GrepToolConfig.Desc instead
	//
	// CustomGrepToolDesc 覆盖 grep 工具描述
	// 可选，默认使用 GrepToolDesc
	// Deprecated: 请改用 GrepToolConfig.Desc
	CustomGrepToolDesc *string
	// CustomGlobToolDesc overrides the glob tool description
	// optional, GlobToolDesc by default
	// Deprecated: Use GlobToolConfig.Desc instead
	//
	// CustomGlobToolDesc 覆盖 glob 工具描述
	// 可选，默认使用 GlobToolDesc
	// Deprecated: 请改用 GlobToolConfig.Desc
	CustomGlobToolDesc *string
	// CustomWriteFileToolDesc overrides the write_file tool description
	// optional, WriteFileToolDesc by default
	// Deprecated: Use WriteFileToolConfig.Desc instead
	//
	// CustomWriteFileToolDesc 覆盖 write_file 工具描述
	// 可选，默认使用 WriteFileToolDesc
	// Deprecated: 请改用 WriteFileToolConfig.Desc
	CustomWriteFileToolDesc *string
	// CustomEditToolDesc overrides the edit_file tool description
	// optional, EditFileToolDesc by default
	// Deprecated: Use EditFileToolConfig.Desc instead
	//
	// CustomEditToolDesc 覆盖 edit_file 工具描述
	// 可选，默认使用 EditToolDesc
	// Deprecated: 请改用 EditToolConfig.Desc
	CustomEditToolDesc *string
}

func (c *MiddlewareConfig) Validate() error {
	if c == nil {
		return errors.New("config should not be nil")
	}
	if c.Backend == nil {
		return errors.New("backend should not be nil")
	}
	if c.StreamingShell != nil && c.Shell != nil {
		return errors.New("shell and streaming shell should not be both set")
	}
	return nil
}

// mergeToolConfigWithDesc merges ToolConfig with legacy Desc field
// Priority: ToolConfig.Desc > legacy Desc
// Returns an empty ToolConfig if both are nil (to allow backend default implementation)
//
// mergeToolConfigWithDesc 合并 ToolConfig 与旧版 Desc 字段
// 优先级：ToolConfig.Desc > 旧版 Desc
// 如果两者都为 nil，则返回空 ToolConfig（以允许后端默认实现）
func (c *MiddlewareConfig) mergeToolConfigWithDesc(
	toolConfig *ToolConfig,
	legacyDesc *string,
) *ToolConfig {
	if toolConfig == nil && legacyDesc == nil {
		return &ToolConfig{}
	}

	if toolConfig == nil {
		return &ToolConfig{
			Desc: legacyDesc,
		}
	}

	if toolConfig.Desc == nil && legacyDesc != nil {
		merged := *toolConfig
		merged.Desc = legacyDesc
		return &merged
	}

	return toolConfig
}

// NewTyped constructs and returns the filesystem middleware as a TypedChatModelAgentMiddleware[M].
//
// This is the generic constructor that supports both *schema.Message and *schema.AgenticMessage.
// It returns a TypedChatModelAgentMiddleware[M] which provides:
//   - Better context propagation through WrapInvokableToolCall and WrapStreamableToolCall methods
//   - BeforeAgent hook for modifying agent instruction and tools at runtime
//   - More flexible extension points compared to the struct-based AgentMiddleware
//
// The middleware provides filesystem tools (ls, read_file, write_file, edit_file, glob, grep)
// and optionally an execute tool if the Backend implements ShellBackend or StreamingShellBackend.
//
// NewTyped 构造并返回作为 TypedChatModelAgentMiddleware[M] 的文件系统 middleware。
// 这是泛型构造函数，同时支持 *schema.Message 和 *schema.AgenticMessage。
// 它返回 TypedChatModelAgentMiddleware[M]，提供：
// - 通过 WrapInvokableToolCall 和 WrapStreamableToolCall 方法更好地传播 context
// - BeforeAgent hook，可在运行时修改智能体指令和工具
// - 相比基于 struct 的 AgentMiddleware，提供更灵活的扩展点
// 该 middleware 提供文件系统工具（ls、read_file、write_file、edit_file、glob、grep），
// 如果 Backend 实现了 ShellBackend 或 StreamingShellBackend，还可选提供 execute 工具。
func NewTyped[M adk.MessageType](ctx context.Context, config *MiddlewareConfig) (adk.TypedChatModelAgentMiddleware[M], error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	ts, err := getFilesystemTools(ctx, config)
	if err != nil {
		return nil, err
	}
	var systemPrompt string
	if config.CustomSystemPrompt != nil {
		systemPrompt = *config.CustomSystemPrompt
	}

	m := &typedFilesystemMiddleware[M]{
		additionalInstruction: systemPrompt,
		additionalTools:       ts,
	}

	return m, nil
}

// New constructs and returns the filesystem middleware as a ChatModelAgentMiddleware.
//
// This is the recommended constructor for new code. It returns a ChatModelAgentMiddleware which provides:
//   - Better context propagation through WrapInvokableToolCall and WrapStreamableToolCall methods
//   - BeforeAgent hook for modifying agent instruction and tools at runtime
//   - More flexible extension points compared to the struct-based AgentMiddleware
//
// The middleware provides filesystem tools (ls, read_file, write_file, edit_file, glob, grep)
// and optionally an execute tool if the Backend implements ShellBackend or StreamingShellBackend.
//
// Example usage:
//
//	middleware, err := filesystem.New(ctx, &filesystem.Config{
//	    Backend: myBackend,
//	})
//	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
//	    // ...
//	    Handlers: []adk.ChatModelAgentMiddleware{middleware},
//	})
//
// New 构造并返回作为 ChatModelAgentMiddleware 的文件系统 middleware。
// 这是新代码推荐使用的构造函数。它返回 ChatModelAgentMiddleware，提供：
// - 通过 WrapInvokableToolCall 和 WrapStreamableToolCall 方法更好地传播 context
// - BeforeAgent hook，可在运行时修改智能体指令和工具
// - 相比基于 struct 的 AgentMiddleware，提供更灵活的扩展点
// 该 middleware 提供文件系统工具（ls、read_file、write_file、edit_file、glob、grep），
// 如果 Backend 实现了 ShellBackend 或 StreamingShellBackend，还可选提供 execute 工具。
// 用法示例：
// middleware, err := filesystem.New(ctx, &filesystem.Config{
// Backend: myBackend,
// })
// agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
// ...
// Handlers: []adk.ChatModelAgentMiddleware{middleware},
// })
func New(ctx context.Context, config *MiddlewareConfig) (adk.ChatModelAgentMiddleware, error) {
	return NewTyped[*schema.Message](ctx, config)
}

type typedFilesystemMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
	additionalInstruction string
	additionalTools       []tool.BaseTool
}

func (m *typedFilesystemMiddleware[M]) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	if runCtx == nil {
		return ctx, runCtx, nil
	}

	nRunCtx := *runCtx
	if m.additionalInstruction != "" {
		nRunCtx.Instruction = nRunCtx.Instruction + "\n" + m.additionalInstruction
	}
	nRunCtx.Tools = append(nRunCtx.Tools, m.additionalTools...)
	return ctx, &nRunCtx, nil
}

// toolSpec defines a specification for creating a filesystem tool.
// It unifies the tool creation process by encapsulating the tool configuration,
// legacy descriptor, and the creation function.
//
// toolSpec 定义创建文件系统工具的规格。
// 它通过封装工具配置、旧版描述符和创建函数，统一工具创建流程。
type toolSpec struct {
	config     *ToolConfig
	legacyDesc *string
	createFunc func(name, desc string) (tool.BaseTool, error)
}

func getFilesystemTools(_ context.Context, middlewareConfig *MiddlewareConfig) ([]tool.BaseTool, error) {
	var tools []tool.BaseTool

	toolSpecs := []toolSpec{
		{
			config:     middlewareConfig.LsToolConfig,
			legacyDesc: middlewareConfig.CustomLsToolDesc,
			createFunc: func(name, desc string) (tool.BaseTool, error) {
				if middlewareConfig.Backend != nil {
					return newLsTool(middlewareConfig.Backend, name, desc)
				}
				return nil, nil
			},
		},
		{
			config:     middlewareConfig.ReadFileToolConfig,
			legacyDesc: middlewareConfig.CustomReadFileToolDesc,
			createFunc: func(name, desc string) (tool.BaseTool, error) {
				if middlewareConfig.Backend != nil {
					if middlewareConfig.UseMultiModalRead {
						return newMultiModalReadFileTool(middlewareConfig.Backend, name, desc)
					}
					return newReadFileTool(middlewareConfig.Backend, name, desc)
				}
				return nil, nil
			},
		},
		{
			config:     middlewareConfig.WriteFileToolConfig,
			legacyDesc: middlewareConfig.CustomWriteFileToolDesc,
			createFunc: func(name, desc string) (tool.BaseTool, error) {
				if middlewareConfig.Backend != nil {
					return newWriteFileTool(middlewareConfig.Backend, name, desc)
				}
				return nil, nil
			},
		},
		{
			config:     middlewareConfig.EditFileToolConfig,
			legacyDesc: middlewareConfig.CustomEditToolDesc,
			createFunc: func(name, desc string) (tool.BaseTool, error) {
				if middlewareConfig.Backend != nil {
					return newEditFileTool(middlewareConfig.Backend, name, desc)
				}
				return nil, nil
			},
		},
		{
			config:     middlewareConfig.GlobToolConfig,
			legacyDesc: middlewareConfig.CustomGlobToolDesc,
			createFunc: func(name, desc string) (tool.BaseTool, error) {
				if middlewareConfig.Backend != nil {
					return newGlobTool(middlewareConfig.Backend, name, desc)
				}
				return nil, nil
			},
		},
		{
			config:     middlewareConfig.GrepToolConfig,
			legacyDesc: middlewareConfig.CustomGrepToolDesc,
			createFunc: func(name, desc string) (tool.BaseTool, error) {
				if middlewareConfig.Backend != nil {
					return newGrepTool(middlewareConfig.Backend, name, desc)
				}
				return nil, nil
			},
		},
	}

	for _, spec := range toolSpecs {
		t, err := createToolFromSpec(middlewareConfig, spec)
		if err != nil {
			return nil, err
		}
		if t != nil {
			tools = append(tools, t)
		}
	}

	// Create execute tool if Shell or StreamingShell is available
	// 如果 Shell 或 StreamingShell 可用，则创建 execute 工具
	if middlewareConfig.StreamingShell != nil {
		executeDesc, err := selectToolDesc("", ExecuteToolDesc, ExecuteToolDescChinese)
		if err != nil {
			return nil, err
		}

		executeTool, err := newStreamingExecuteTool(middlewareConfig.StreamingShell, ToolNameExecute, executeDesc)
		if err != nil {
			return nil, err
		}
		tools = append(tools, executeTool)
	} else if middlewareConfig.Shell != nil {
		executeDesc, err := selectToolDesc("", ExecuteToolDesc, ExecuteToolDescChinese)
		if err != nil {
			return nil, err
		}

		executeTool, err := newExecuteTool(middlewareConfig.Shell, ToolNameExecute, executeDesc)
		if err != nil {
			return nil, err
		}
		tools = append(tools, executeTool)
	}

	return tools, nil
}

// createToolFromSpec creates a tool instance based on the provided toolSpec.
// It handles configuration merging (ToolConfig + legacy Desc), checks if the tool
// is disabled, and prioritizes CustomTool over the default implementation.
//
// createToolFromSpec 基于提供的 toolSpec 创建工具实例。
// 它处理配置合并（ToolConfig + 旧版 Desc）、检查工具是否被禁用，并优先使用 CustomTool 而非默认实现。
func createToolFromSpec(middlewareConfig *MiddlewareConfig, spec toolSpec) (tool.BaseTool, error) {
	mergedConfig := middlewareConfig.mergeToolConfigWithDesc(spec.config, spec.legacyDesc)

	if mergedConfig.Disable {
		return nil, nil
	}

	return getOrCreateTool(mergedConfig.CustomTool, func() (tool.BaseTool, error) {
		desc := ""
		if mergedConfig.Desc != nil {
			desc = *mergedConfig.Desc
		}
		return spec.createFunc(mergedConfig.Name, desc)
	})
}

func getOrCreateTool(customTool tool.BaseTool, createFunc func() (tool.BaseTool, error)) (tool.BaseTool, error) {
	if customTool != nil {
		return customTool, nil
	}
	return createFunc()
}

type lsArgs struct {
	Path string `json:"path"`
}

func newLsTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameLs)
	d, err := selectToolDesc(desc, ListFilesToolDesc, ListFilesToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input lsArgs) (string, error) {
		infos, err := fs.LsInfo(ctx, &filesystem.LsInfoRequest{Path: input.Path})
		if err != nil {
			return "", err
		}
		if len(infos) == 0 {
			return noFilesFound, nil
		}
		paths := make([]string, 0, len(infos))
		for _, fi := range infos {
			paths = append(paths, fi.Path)
		}
		return strings.Join(paths, "\n"), nil
	})
}

type readFileArgs struct {
	// FilePath is the path to the file to read.
	// FilePath 是要读取的文件路径。
	FilePath string `json:"file_path" jsonschema:"description=The path to the file to read"`

	// Offset is the line number to start reading from.
	// Offset 是开始读取的行号。
	Offset int `json:"offset" jsonschema:"description=The line number to start reading from. Only provide if the file is too large to read at once"`

	// Limit is the number of lines to read.
	// Limit 是要读取的行数。
	Limit int `json:"limit" jsonschema:"description=The number of lines to read. Only provide if the file is too large to read at once."`
}

// multiModalReadFileArgs extends readFileArgs with PDF-specific parameters for MultiModalReadFileTool.
// multiModalReadFileArgs 使用 PDF 专用参数扩展 readFileArgs，供 MultiModalReadFileTool 使用。
type multiModalReadFileArgs struct {
	readFileArgs

	// Pages is the page range for PDF files.
	// Pages 是 PDF 文件的页码范围。
	Pages string `json:"pages,omitempty" jsonschema:"description=Page range for PDF files (e.g.\\, \"1-5\"\\, \"3\"\\, \"10-20\"). Only applicable to PDF files. Maximum 20 pages per request."`
}

func newReadFileTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameReadFile)
	d, err := selectToolDesc(desc, ReadFileToolDesc, ReadFileToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input readFileArgs) (string, error) {
		if input.Offset <= 0 {
			input.Offset = 1
		}
		if input.Limit <= 0 {
			input.Limit = 2000
		}

		fileCt, err := fs.Read(ctx, &filesystem.ReadRequest{
			FilePath: input.FilePath,
			Offset:   input.Offset,
			Limit:    input.Limit,
		})
		if err != nil {
			return "", err
		}
		if fileCt == nil {
			return fmt.Sprintf("No content found at path: %s", input.FilePath), nil
		}

		return formatLineNumbers(fileCt.Content, input.Offset), nil
	})
}

// formatLineNumbers prefixes each line of content with a 1-based line number
// starting at startLine (e.g. "     1\tfoo"). startLine corresponds to the
// line number of the first line in content (usually ReadRequest.Offset).
//
// formatLineNumbers 为内容的每一行添加从 1 开始的行号前缀，
// 从 startLine 开始（例如 "     1\tfoo"）。startLine 对应 content 中第一行的行号（通常是 ReadRequest.Offset）。
func formatLineNumbers(content string, startLine int) string {
	lines := strings.Split(content, "\n")
	var b strings.Builder
	for i, line := range lines {
		if i < len(lines)-1 {
			fmt.Fprintf(&b, "%6d\t%s\n", startLine+i, line)
		} else {
			fmt.Fprintf(&b, "%6d\t%s", startLine+i, line)
		}
	}
	return b.String()
}

const maxPagesPerRequest = 20

func validatePages(pages string) error {
	parts := strings.SplitN(pages, "-", 2)
	start, err := strconv.Atoi(parts[0])
	if err != nil || start < 1 {
		return fmt.Errorf("invalid pages parameter %q: expected format like \"3\" or \"1-10\"", pages)
	}
	if len(parts) == 1 {
		return nil
	}
	if parts[1] == "" {
		return fmt.Errorf("invalid pages parameter %q: expected format like \"3\" or \"1-10\"", pages)
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil || end < 1 {
		return fmt.Errorf("invalid pages parameter %q: expected format like \"3\" or \"1-10\"", pages)
	}
	if end < start {
		return fmt.Errorf("invalid pages parameter %q: end page must be >= start page", pages)
	}
	if end-start+1 > maxPagesPerRequest {
		return fmt.Errorf("invalid pages parameter %q: range exceeds maximum of %d pages per request", pages, maxPagesPerRequest)
	}
	return nil
}

func newMultiModalReadFileTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	er, ok := fs.(filesystem.MultiModalReader)
	if !ok {
		return nil, fmt.Errorf("UseMultiModalRead is enabled, but backend (type %T) does not implement filesystem.MultiModalReader interface. "+
			"Either implement the MultiModalReader interface on your backend, or set UseMultiModalRead to false", fs)
	}
	toolName := selectToolName(name, ToolNameReadFile)
	d, err := selectToolDesc(desc, ReadFileToolDesc, ReadFileToolDescChinese)
	if err != nil {
		return nil, err
	}
	// Only append the multimodal suffix when falling back to the built-in desc.
	// A custom desc is expected to describe its own capabilities, so appending
	// would produce duplicated or contradictory descriptions.
	//
	// 仅在回退到内置 desc 时追加多模态后缀。
	// 自定义 desc 应自行描述其能力，因此追加会产生重复或矛盾的描述。
	if desc == "" {
		d += internal.SelectPrompt(internal.I18nPrompts{
			English: EnhancedReadFileDescSuffix,
			Chinese: EnhancedReadFileDescSuffixChinese,
		})
	}

	return utils.InferEnhancedTool(toolName, d, func(ctx context.Context, input multiModalReadFileArgs) (*schema.ToolResult, error) {
		if input.Offset <= 0 {
			input.Offset = 1
		}
		if input.Limit <= 0 {
			input.Limit = 2000
		}

		if input.Pages != "" {
			if err := validatePages(input.Pages); err != nil {
				return &schema.ToolResult{
					Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: err.Error()}},
				}, nil
			}
		}

		fileCt, err := er.MultiModalRead(ctx, &filesystem.MultiModalReadRequest{
			ReadRequest: filesystem.ReadRequest{
				FilePath: input.FilePath,
				Offset:   input.Offset,
				Limit:    input.Limit,
			},
			Pages: input.Pages,
		})
		if err != nil {
			return nil, err
		}

		if fileCt == nil {
			return &schema.ToolResult{
				Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: fmt.Sprintf("No content found at path: %s", input.FilePath)}},
			}, nil
		}

		// Multimodal result: convert FileContentPart to ToolOutputPart
		// 多模态结果：将 FileContentPart 转换为 ToolOutputPart
		if len(fileCt.Parts) > 0 {
			parts := make([]schema.ToolOutputPart, 0, len(fileCt.Parts))
			enc := base64Encoder{}
			for _, p := range fileCt.Parts {
				if len(p.Data) == 0 {
					return nil, fmt.Errorf("FileContentPart.Data is empty for type %s", p.Type)
				}
				if p.MIMEType == "" {
					return nil, fmt.Errorf("FileContentPart.MIMEType is empty for type %s", p.Type)
				}
				b64 := enc.encode(p.Data)
				switch p.Type {
				case filesystem.FileContentPartTypeImage:
					parts = append(parts, schema.ToolOutputPart{
						Type: schema.ToolPartTypeImage,
						Image: &schema.ToolOutputImage{
							MessagePartCommon: schema.MessagePartCommon{
								MIMEType:   p.MIMEType,
								Base64Data: &b64,
							},
						},
					})
				case filesystem.FileContentPartTypePDF:
					parts = append(parts, schema.ToolOutputPart{
						Type: schema.ToolPartTypeFile,
						File: &schema.ToolOutputFile{
							MessagePartCommon: schema.MessagePartCommon{
								MIMEType:   p.MIMEType,
								Base64Data: &b64,
							},
						},
					})
				default:
					// FileContentPartType is defined by Backend implementations.
					// Unrecognized types are unlikely but should fail explicitly rather than silently.
					//
					// FileContentPartType 由 Backend 实现定义。
					// 未识别的类型不太可能出现，但应显式失败，而不是静默处理。
					return nil, fmt.Errorf("unsupported FileContentPartType: %s", p.Type)
				}
			}
			return &schema.ToolResult{Parts: parts}, nil
		}

		if fileCt.FileContent == nil {
			return &schema.ToolResult{
				Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: fmt.Sprintf("No content found at path: %s", input.FilePath)}},
			}, nil
		}

		return &schema.ToolResult{
			Parts: []schema.ToolOutputPart{{Type: schema.ToolPartTypeText, Text: formatLineNumbers(fileCt.Content, input.Offset)}},
		}, nil
	})
}

type writeFileArgs struct {
	// FilePath is the path to the file to write.
	// FilePath 是要写入的文件路径。
	FilePath string `json:"file_path" jsonschema:"description=The path to the file to write"`

	// Content is the content to write to the file.
	// Content 是要写入文件的内容。
	Content string `json:"content" jsonschema:"description=The content to write to the file"`
}

func newWriteFileTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameWriteFile)
	d, err := selectToolDesc(desc, WriteFileToolDesc, WriteFileToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input writeFileArgs) (string, error) {
		err := fs.Write(ctx, &filesystem.WriteRequest{
			FilePath: input.FilePath,
			Content:  input.Content,
		})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Updated file %s", input.FilePath), nil
	})
}

type editFileArgs struct {
	// FilePath is the path to the file to modify.
	// FilePath 是要修改的文件路径。
	FilePath string `json:"file_path" jsonschema:"description=The path to the file to modify"`

	// OldString is the text to replace.
	// OldString 是要替换的文本。
	OldString string `json:"old_string" jsonschema:"description=The text to replace"`

	// NewString is the text to replace it with.
	// NewString 是用于替换它的文本。
	NewString string `json:"new_string" jsonschema:"description=The text to replace it with (must be different from old_string)"`

	// ReplaceAll indicates whether to replace all occurrences of old_string.
	// ReplaceAll 表示是否替换所有 old_string。
	ReplaceAll bool `json:"replace_all" jsonschema:"description=Replace all occurrences of old_string (default false),default=false"`
}

func newEditFileTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameEditFile)
	d, err := selectToolDesc(desc, EditFileToolDesc, EditFileToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input editFileArgs) (string, error) {
		err := fs.Edit(ctx, &filesystem.EditRequest{
			FilePath:   input.FilePath,
			OldString:  input.OldString,
			NewString:  input.NewString,
			ReplaceAll: input.ReplaceAll,
		})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Successfully replaced the string in '%s'", input.FilePath), nil
	})
}

type globArgs struct {
	// Pattern is the glob pattern to match files against.
	// Pattern 是用于匹配文件的 glob 模式。
	Pattern string `json:"pattern" jsonschema:"description=The glob pattern to match files against"`

	// Path is the directory to search in.
	// Path 是要搜索的目录。
	Path string `json:"path" jsonschema:"description=The directory to search in. If not specified\\, the current working directory will be used. IMPORTANT: Omit this field to use the default directory. DO NOT enter 'undefined' or 'null' - simply omit it for the default behavior. Must be a valid directory path if provided."`
}

func newGlobTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameGlob)
	d, err := selectToolDesc(desc, GlobToolDesc, GlobToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input globArgs) (string, error) {
		infos, err := fs.GlobInfo(ctx, &filesystem.GlobInfoRequest{
			Pattern: input.Pattern,
			Path:    input.Path,
		})
		if err != nil {
			return "", err
		}
		if len(infos) == 0 {
			return noFilesFound, nil
		}
		paths := make([]string, 0, len(infos))
		for _, fi := range infos {
			paths = append(paths, fi.Path)
		}
		return strings.Join(paths, "\n"), nil
	})
}

type grepArgs struct {
	// Pattern is the regular expression pattern to search for in file contents.
	// Pattern 是用于在文件内容中搜索的正则表达式模式。
	Pattern string `json:"pattern" jsonschema:"description=The regular expression pattern to search for in file contents"`

	// Path is the file or directory to search in. Defaults to current working directory.
	// Path 是要搜索的文件或目录。默认为当前工作目录。
	Path *string `json:"path,omitempty" jsonschema:"description=File or directory to search in (rg PATH). Defaults to current working directory."`

	// Glob is the glob pattern to filter files (e.g. "*.js", "*.{ts,tsx}").
	// Glob 是用于过滤文件的 glob 模式（例如 "*.js"、"*.{ts,tsx}"）。
	Glob *string `json:"glob,omitempty" jsonschema:"description=Glob pattern to filter files (e.g. '*.js'\\, '*.{ts\\,tsx}') - maps to rg --glob"`

	// OutputMode specifies the output format.
	// "content" shows matching lines (supports context, line numbers, head_limit).
	// "files_with_matches" shows file paths (supports head_limit).
	// "count" shows match counts (supports head_limit).
	// Defaults to "files_with_matches".
	//
	// OutputMode 指定输出格式。
	// "content" 显示匹配行（支持 context、line numbers、head_limit）。
	// "files_with_matches" 显示文件路径（支持 head_limit）。
	// "count" 显示匹配计数（支持 head_limit）。
	// 默认为 "files_with_matches"。
	OutputMode string `json:"output_mode,omitempty" jsonschema:"description=Output mode: 'content' shows matching lines (supports -A/-B/-C context\\, -n line numbers\\, head_limit)\\, 'files_with_matches' shows file paths (supports head_limit)\\, 'count' shows match counts (supports head_limit). Defaults to 'files_with_matches'.,enum=content,enum=files_with_matches,enum=count"`

	// Context is the number of lines to show before and after each match.
	// Only applicable when output_mode is "content".
	//
	// Context 是每个匹配项前后显示的行数。
	// 仅在 output_mode 为 "content" 时适用。
	Context *int `json:"-C,omitempty" jsonschema:"description=Number of lines to show before and after each match (rg -C). Requires output_mode: 'content'\\, ignored otherwise."`

	// BeforeLines is the number of lines to show before each match.
	// Only applicable when output_mode is "content".
	//
	// BeforeLines 是每个匹配项前显示的行数。
	// 仅在 output_mode 为 "content" 时适用。
	BeforeLines *int `json:"-B,omitempty" jsonschema:"description=Number of lines to show before each match (rg -B). Requires output_mode: 'content'\\, ignored otherwise."`

	// AfterLines is the number of lines to show after each match.
	// Only applicable when output_mode is "content".
	//
	// AfterLines 是每个匹配项后显示的行数。
	// 仅在 output_mode 为 "content" 时适用。
	AfterLines *int `json:"-A,omitempty" jsonschema:"description=Number of lines to show after each match (rg -A). Requires output_mode: 'content'\\, ignored otherwise."`

	// ShowLineNumbers enables showing line numbers in output.
	// Only applicable when output_mode is "content". Defaults to true.
	//
	// ShowLineNumbers 启用在输出中显示行号。
	// 仅在 output_mode 为 "content" 时适用。默认为 true。
	ShowLineNumbers *bool `json:"-n,omitempty" jsonschema:"description=Show line numbers in output (rg -n). Requires output_mode: 'content'\\, ignored otherwise. Defaults to true."`

	// CaseInsensitive enables case insensitive search.
	// CaseInsensitive 启用大小写不敏感搜索。
	CaseInsensitive *bool `json:"-i,omitempty" jsonschema:"description=Case insensitive search (rg -i)"`

	// FileType is the file type to search (e.g., js, py, rust, go, java).
	// More efficient than Glob for standard file types.
	//
	// FileType 是要搜索的文件类型（例如 js、py、rust、go、java）。
	// 对标准文件类型比 Glob 更高效。
	FileType *string `json:"type,omitempty" jsonschema:"description=File type to search (rg --type). Common types: js\\, py\\, rust\\, go\\, java\\, etc. More efficient than include for standard file types."`

	// HeadLimit limits output to first N lines/entries.
	// Works across all output modes. Defaults to 0 (unlimited).
	//
	// HeadLimit 将输出限制为前 N 行/条。
	// 适用于所有输出模式。默认为 0（不限制）。
	HeadLimit *int `json:"head_limit,omitempty" jsonschema:"description=Limit output to first N lines/entries\\, equivalent to '| head -N'. Works across all output modes: content (limits output lines)\\, files_with_matches (limits file paths)\\, count (limits count entries). Defaults to 0 (unlimited)."`

	// Offset skips first N lines/entries before applying HeadLimit.
	// Works across all output modes. Defaults to 0.
	//
	// Offset 在应用 HeadLimit 前跳过前 N 行/条。
	// 适用于所有输出模式。默认为 0。
	Offset *int `json:"offset,omitempty" jsonschema:"description=Skip first N lines/entries before applying head_limit\\, equivalent to '| tail -n +N | head -N'. Works across all output modes. Defaults to 0."`

	// Multiline enables multiline mode where patterns can span lines.
	//   - true: Allows patterns to match across lines, "." matches newlines
	//   - false: Default, matches only within single lines
	//
	// Multiline 启用多行模式，使模式可跨行匹配。
	// - true: 允许模式跨行匹配，"." 匹配换行符
	// - false: 默认值，仅在单行内匹配
	Multiline *bool `json:"multiline,omitempty" jsonschema:"description=Enable multiline mode where . matches newlines and patterns can span lines (rg -U --multiline-dotall). Default: false."`
}

func newGrepTool(fs filesystem.Backend, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameGrep)
	d, err := selectToolDesc(desc, GrepToolDesc, GrepToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input grepArgs) (string, error) {
		// Extract string parameters
		// 提取字符串参数
		path := valueOrDefault(input.Path, "")
		glob := valueOrDefault(input.Glob, "")
		fileType := valueOrDefault(input.FileType, "")
		var beforeLines, afterLines int

		if input.Context != nil {
			beforeLines = valueOrDefault(input.Context, 0)
			afterLines = valueOrDefault(input.Context, 0)
		} else {
			// Extract context parameters
			// 提取 context 参数
			beforeLines = valueOrDefault(input.BeforeLines, 0)
			afterLines = valueOrDefault(input.AfterLines, 0)
		}

		// Extract boolean flags
		// 提取布尔标志
		caseInsensitive := valueOrDefault(input.CaseInsensitive, false)
		enableMultiline := valueOrDefault(input.Multiline, false)

		// Extract pagination parameters
		// 提取分页参数
		headLimit := valueOrDefault(input.HeadLimit, 0)
		offset := valueOrDefault(input.Offset, 0)

		matches, err := fs.GrepRaw(ctx, &filesystem.GrepRequest{
			Pattern:         input.Pattern,
			Path:            path,
			Glob:            glob,
			FileType:        fileType,
			CaseInsensitive: caseInsensitive,
			AfterLines:      afterLines,
			BeforeLines:     beforeLines,
			EnableMultiline: enableMultiline,
		})
		if err != nil {
			return "", err
		}

		sort.SliceStable(matches, func(i, j int) bool {
			return filepath.Base(matches[i].Path) < filepath.Base(matches[j].Path)
		})

		switch input.OutputMode {
		case "content":
			matches = applyPagination(matches, offset, headLimit)
			return formatContentMatches(matches, valueOrDefault(input.ShowLineNumbers, true)), nil

		case "count":
			return formatCountMatches(matches, offset, headLimit), nil

		case "files_with_matches":
			return formatFileMatches(matches, offset, headLimit), nil

		default:
			return formatFileMatches(matches, offset, headLimit), nil
		}
	})
}

type executeArgs struct {
	Command string `json:"command"`
}

func newExecuteTool(sb filesystem.Shell, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameExecute)
	d, err := selectToolDesc(desc, ExecuteToolDesc, ExecuteToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferTool(toolName, d, func(ctx context.Context, input executeArgs) (string, error) {
		result, err := sb.Execute(ctx, &filesystem.ExecuteRequest{
			Command: input.Command,
		})
		if err != nil {
			return "", err
		}

		return convExecuteResponse(result), nil
	})
}

func newStreamingExecuteTool(sb filesystem.StreamingShell, name string, desc string) (tool.BaseTool, error) {
	toolName := selectToolName(name, ToolNameExecute)
	d, err := selectToolDesc(desc, ExecuteToolDesc, ExecuteToolDescChinese)
	if err != nil {
		return nil, err
	}
	return utils.InferStreamTool(toolName, d, func(ctx context.Context, input executeArgs) (*schema.StreamReader[string], error) {
		result, err := sb.ExecuteStreaming(ctx, &filesystem.ExecuteRequest{
			Command: input.Command,
		})
		if err != nil {
			return nil, err
		}
		sr, sw := schema.Pipe[string](10)
		go func() {
			defer func() {
				e := recover()
				if e != nil {
					sw.Send("", fmt.Errorf("panic: %v,\n stack: %s", e, string(debug.Stack())))
				}
				sw.Close()
			}()

			var hasSentContent bool
			var exitCode *int

			for {
				chunk, recvErr := result.Recv()
				if recvErr == io.EOF {
					break
				}
				if recvErr != nil {
					sw.Send("", recvErr)
					return
				}

				if chunk == nil {
					continue
				}
				if chunk.ExitCode != nil {
					exitCode = chunk.ExitCode
				}

				parts := make([]string, 0, 2)
				if chunk.Output != "" {
					parts = append(parts, chunk.Output)
				}
				if chunk.Truncated {
					parts = append(parts, "[Output was truncated due to size limits]")
				}
				if len(parts) > 0 {
					sw.Send(strings.Join(parts, "\n"), nil)
					hasSentContent = true
				}
			}

			if exitCode != nil && *exitCode != 0 {
				sw.Send(fmt.Sprintf("\n[Command failed with exit code %d]", *exitCode), nil)
			} else if !hasSentContent {
				sw.Send("[Command executed successfully with no output]", nil)
			}
		}()

		return sr, nil
	})
}

func convExecuteResponse(response *filesystem.ExecuteResponse) string {
	if response == nil {
		return ""
	}
	parts := []string{response.Output}
	if response.ExitCode != nil && *response.ExitCode != 0 {
		parts = append(parts, fmt.Sprintf("[Command failed with exit code %d]", *response.ExitCode))
	}
	if response.Truncated {
		parts = append(parts, "[Output was truncated due to size limits]")
	}

	result := strings.Join(parts, "\n")
	if result == "" && (response.ExitCode == nil || *response.ExitCode == 0) {
		return "[Command executed successfully with no output]"
	}
	return result
}

// valueOrDefault returns the value pointed to by ptr, or defaultValue if ptr is nil.
// valueOrDefault 返回 ptr 指向的值；如果 ptr 为 nil，则返回 defaultValue。
func valueOrDefault[T any](ptr *T, defaultValue T) T {
	if ptr != nil {
		return *ptr
	}
	return defaultValue
}

// base64Encoder reuses a buffer across multiple base64 encoding calls to reduce allocations.
// base64Encoder 在多次 base64 编码调用之间复用缓冲区，以减少分配。
type base64Encoder struct {
	buf []byte
}

func (e *base64Encoder) encode(data []byte) string {
	n := base64.StdEncoding.EncodedLen(len(data))
	if cap(e.buf) < n {
		e.buf = make([]byte, n)
	} else {
		e.buf = e.buf[:n]
	}
	base64.StdEncoding.Encode(e.buf, data)
	return string(e.buf)
}

func applyPagination[T any](items []T, offset, headLimit int) []T {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []T{}
	}
	items = items[offset:]

	if headLimit > 0 && headLimit < len(items) {
		items = items[:headLimit]
	}
	return items
}

func formatFileMatches(matches []filesystem.GrepMatch, offset, headLimit int) string {
	if len(matches) == 0 {
		return noFilesFound
	}
	seen := make(map[string]bool)
	var uniquePaths []string
	for _, match := range matches {
		if !seen[match.Path] {
			seen[match.Path] = true
			uniquePaths = append(uniquePaths, match.Path)
		}
	}
	totalFiles := len(uniquePaths)
	uniquePaths = applyPagination(uniquePaths, offset, headLimit)

	fileWord := "files"
	if totalFiles == 1 {
		fileWord = "file"
	}
	return fmt.Sprintf("Found %d %s\n%s", totalFiles, fileWord, strings.Join(uniquePaths, "\n"))
}

func formatContentMatches(matches []filesystem.GrepMatch, showLineNum bool) string {
	if len(matches) == 0 {
		return noMatchesFound
	}
	var b strings.Builder
	for _, match := range matches {
		b.WriteString(match.Path)
		if showLineNum {
			b.WriteString(":")
			b.WriteString(strconv.Itoa(match.Line))
		}
		b.WriteString(":")
		b.WriteString(match.Content)
		b.WriteString("\n")
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func formatCountMatches(matches []filesystem.GrepMatch, offset, headLimit int) string {
	countMap := make(map[string]int)
	for _, match := range matches {
		countMap[match.Path]++
	}

	var paths []string
	for path := range countMap {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	totalOccurrences := len(matches)
	totalFiles := len(paths)

	occurrenceWord := "occurrences"
	if totalOccurrences == 1 {
		occurrenceWord = "occurrence"
	}
	fileWord := "files"
	if totalFiles == 1 {
		fileWord = "file"
	}

	if totalOccurrences == 0 {
		return fmt.Sprintf("%s\n\nFound %d total %s across %d %s.", noMatchesFound, totalOccurrences, occurrenceWord, totalFiles, fileWord)
	}

	paths = applyPagination(paths, offset, headLimit)

	var b strings.Builder
	for _, path := range paths {
		b.WriteString(path)
		b.WriteString(":")
		b.WriteString(strconv.Itoa(countMap[path]))
		b.WriteString("\n")
	}
	result := strings.TrimSuffix(b.String(), "\n")
	return fmt.Sprintf("%s\n\nFound %d total %s across %d %s.", result, totalOccurrences, occurrenceWord, totalFiles, fileWord)
}

// selectToolDesc returns the custom description if provided, otherwise selects the appropriate
// i18n description based on the current language setting.
//
// selectToolDesc 在提供自定义描述时返回它，否则根据当前语言设置选择合适的 i18n 描述。
func selectToolDesc(customDesc string, defaultEnglish, defaultChinese string) (string, error) {
	if customDesc != "" {
		return customDesc, nil
	}
	return internal.SelectPrompt(internal.I18nPrompts{
		English: defaultEnglish,
		Chinese: defaultChinese,
	}), nil
}

// selectToolName returns the custom tool name if provided, otherwise returns the default name.
// selectToolName 在提供自定义工具名称时返回它，否则返回默认名称。
func selectToolName(customName string, defaultName string) string {
	if customName != "" {
		return customName
	}
	return defaultName
}
