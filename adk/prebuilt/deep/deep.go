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

// Package deep provides a prebuilt agent with deep task orchestration.
// Package deep 提供一个带深度任务编排的预构建智能体。
package deep

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/internal"
	filesystem2 "github.com/cloudwego/eino/adk/middlewares/filesystem"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/schema"
)

func init() {
	schema.RegisterName[TODO]("_eino_adk_prebuilt_deep_todo")
	schema.RegisterName[[]TODO]("_eino_adk_prebuilt_deep_todo_slice")
}

// TypedConfig defines the configuration for creating a DeepAgent parameterized by message type.
// An Agentic DeepAgent (M = *schema.AgenticMessage) only supports Agentic sub-agents,
// and a standard DeepAgent (M = *schema.Message) only supports standard sub-agents.
// This is enforced by the type system through the SubAgents field.
//
// TypedConfig 定义用于创建按消息类型参数化的 DeepAgent 的配置。
// Agentic DeepAgent (M = *schema.AgenticMessage) 仅支持 Agentic 子智能体，
// 标准 DeepAgent (M = *schema.Message) 仅支持标准子智能体。
// 这通过 SubAgents 字段由类型系统强制保证。
type TypedConfig[M adk.MessageType] struct {
	// Name is the identifier for the Deep agent.
	// Name 是 Deep agent 的标识符。
	Name string
	// Description provides a brief explanation of the agent's purpose.
	// Description 简要说明该智能体的用途。
	Description string

	// ChatModel is the model used by DeepAgent for reasoning and task execution.
	// If the agent uses any tools, this model must support the model.WithTools call option,
	// as that's how the agent configures the model with tool information.
	//
	// ChatModel 是 DeepAgent 用于推理和任务执行的 model。
	// 如果智能体使用任何工具，此 model 必须支持 model.WithTools call option，
	// 因为智能体就是通过它为 model 配置工具信息。
	ChatModel model.BaseModel[M]
	// Instruction contains the system prompt that guides the agent's behavior.
	// When empty, a built-in default system prompt will be used, which includes general assistant
	// behavior guidelines, security policies, coding style guidelines, and tool usage policies.
	//
	// Instruction 包含指导智能体行为的系统提示。
	// 为空时，将使用内置默认系统提示，其中包含通用助手
	// 行为指南、安全策略、编码风格指南和工具使用策略。
	Instruction string
	// SubAgents are specialized agents that can be invoked by the agent.
	// For M = *schema.AgenticMessage, only agentic sub-agents are accepted.
	//
	// SubAgents 是可由智能体调用的专用智能体。
	// 对于 M = *schema.AgenticMessage，仅接受 agentic 子智能体。
	SubAgents []adk.TypedAgent[M]
	// ToolsConfig provides the tools and tool-calling configurations available for the agent to invoke.
	// ToolsConfig 提供智能体可调用的工具及工具调用配置。
	ToolsConfig adk.ToolsConfig
	// MaxIteration limits the maximum number of reasoning iterations the agent can perform.
	// MaxIteration 限制智能体可执行的最大推理迭代次数。
	MaxIteration int

	// Backend provides filesystem operations used by tools and offloading.
	// If set, filesystem tools (read_file, write_file, edit_file, glob, grep) will be registered.
	// Optional.
	//
	// Backend 提供工具和 offloading 使用的文件系统操作。
	// 设置后，将注册文件系统工具（read_file, write_file, edit_file, glob, grep）。
	// 可选。
	Backend filesystem.Backend
	// Shell provides shell command execution capability.
	// If set, an execute tool will be registered to support shell command execution.
	// Optional. Mutually exclusive with StreamingShell.
	//
	// Shell 提供 shell 命令执行能力。
	// 设置后，将注册 execute 工具以支持 shell 命令执行。
	// 可选。与 StreamingShell 互斥。
	Shell filesystem.Shell
	// StreamingShell provides streaming shell command execution capability.
	// If set, a streaming execute tool will be registered to support streaming shell command execution.
	// Optional. Mutually exclusive with Shell.
	//
	// StreamingShell 提供流式 shell 命令执行能力。
	// 设置后，将注册流式 execute 工具以支持流式 shell 命令执行。
	// 可选。与 Shell 互斥。
	StreamingShell filesystem.StreamingShell

	// WithoutWriteTodos disables the built-in write_todos tool when set to true.
	// WithoutWriteTodos 设置为 true 时会禁用内置 write_todos 工具。
	WithoutWriteTodos bool
	// WithoutGeneralSubAgent disables the general-purpose subagent when set to true.
	// WithoutGeneralSubAgent 设置为 true 时会禁用通用子智能体。
	WithoutGeneralSubAgent bool
	// TaskToolDescriptionGenerator allows customizing the description for the task tool.
	// If provided, this function generates the tool description based on available subagents.
	//
	// TaskToolDescriptionGenerator 用于自定义 task 工具的描述。
	// 提供后，此函数会根据可用子智能体生成工具描述。
	TaskToolDescriptionGenerator func(ctx context.Context, availableAgents []adk.TypedAgent[M]) (string, error)

	Middlewares []adk.AgentMiddleware

	// Handlers configures interface-based handlers for extending agent behavior.
	// Unlike Middlewares (struct-based), Handlers allow users to:
	//   - Add custom methods to their handler implementations
	//   - Return modified context from handler methods
	//   - Centralize configuration in struct fields instead of closures
	//
	// Handlers are processed after Middlewares, in registration order.
	// See adk.ChatModelAgentMiddleware documentation for when to use Handlers vs Middlewares.
	//
	// Handlers 配置基于接口的处理器，用于扩展智能体行为。
	// 不同于 Middlewares（基于结构体），Handlers 允许用户：
	// - 为其处理器实现添加自定义方法
	// - 从处理器方法返回修改后的 context
	// - 在结构体字段中集中配置，而不是使用闭包
	// Handlers 会在 Middlewares 之后按注册顺序处理。
	// 何时使用 Handlers 与 Middlewares，请参见 adk.ChatModelAgentMiddleware 文档。
	Handlers []adk.TypedChatModelAgentMiddleware[M]

	ModelRetryConfig *adk.TypedModelRetryConfig[M]
	// ModelFailoverConfig configures failover behavior for the ChatModel.
	// When set, the agent will automatically fail over to alternative models on errors.
	// This config is also propagated to the general sub-agent.
	//
	// ModelFailoverConfig 配置 ChatModel 的故障转移行为。
	// 设置后，智能体会在出错时自动故障转移到备选模型。
	// 此配置也会传递给通用子智能体。
	ModelFailoverConfig *adk.ModelFailoverConfig[M]

	// OutputKey stores the agent's response in the session.
	// Optional. When set, stores output via AddSessionValue(ctx, outputKey, msg.Content).
	//
	// OutputKey 将智能体响应存储到 session 中。
	// 可选。设置后，通过 AddSessionValue(ctx, outputKey, msg.Content) 存储输出。
	OutputKey string
}

// Config defines the configuration for creating a standard DeepAgent.
// Config 定义用于创建标准 DeepAgent 的配置。
type Config = TypedConfig[*schema.Message]

// NewTyped creates a new typed Deep agent instance with the provided configuration.
// This function initializes built-in tools, creates a task tool for subagent orchestration,
// and returns a fully configured TypedChatModelAgent ready for execution.
//
// NewTyped 使用提供的配置创建新的 typed Deep 智能体实例。
// 此函数会初始化内置工具，创建用于子智能体编排的 task 工具，
// 并返回可执行的、配置完整的 TypedChatModelAgent。
func NewTyped[M adk.MessageType](ctx context.Context, cfg *TypedConfig[M]) (adk.TypedResumableAgent[M], error) {
	handlers, err := buildTypedBuiltinAgentMiddlewares(ctx, cfg)
	if err != nil {
		return nil, err
	}

	instruction := cfg.Instruction
	if len(instruction) == 0 {
		instruction = internal.SelectPrompt(internal.I18nPrompts{
			English: baseAgentInstruction,
			Chinese: baseAgentInstructionChinese,
		})
	}

	if !cfg.WithoutGeneralSubAgent || len(cfg.SubAgents) > 0 {
		tt, err := typedTaskToolMiddleware(
			ctx,
			cfg.TaskToolDescriptionGenerator,
			cfg.SubAgents,

			cfg.WithoutGeneralSubAgent,
			cfg.ChatModel,
			instruction,
			cfg.ToolsConfig,
			cfg.MaxIteration,
			cfg.Middlewares,
			append(handlers, cfg.Handlers...),
			cfg.ModelFailoverConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to new task tool: %w", err)
		}
		handlers = append(handlers, tt)
	}

	return adk.NewTypedChatModelAgent(ctx, &adk.TypedChatModelAgentConfig[M]{
		Name:          cfg.Name,
		Description:   cfg.Description,
		Instruction:   instruction,
		Model:         cfg.ChatModel,
		ToolsConfig:   cfg.ToolsConfig,
		MaxIterations: cfg.MaxIteration,
		Middlewares:   cfg.Middlewares,
		Handlers:      append(handlers, cfg.Handlers...),

		GenModelInput:       typedGenModelInput[M],
		ModelRetryConfig:    cfg.ModelRetryConfig,
		ModelFailoverConfig: cfg.ModelFailoverConfig,
		OutputKey:           cfg.OutputKey,
	})
}

// New creates a new Deep agent instance with the provided configuration.
// This function initializes built-in tools, creates a task tool for subagent orchestration,
// and returns a fully configured ChatModelAgent ready for execution.
//
// New 使用提供的配置创建新的 Deep 智能体实例。
// 此函数会初始化内置工具，创建用于子智能体编排的 task 工具，
// 并返回可执行的、配置完整的 ChatModelAgent。
func New(ctx context.Context, cfg *Config) (adk.ResumableAgent, error) {
	return NewTyped(ctx, cfg)
}

func typedGenModelInput[M adk.MessageType](_ context.Context, instruction string, input *adk.TypedAgentInput[M]) ([]M, error) {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		msgs := make([]*schema.Message, 0, len(input.Messages)+1)
		if instruction != "" {
			msgs = append(msgs, schema.SystemMessage(instruction))
		}
		// Type assertion is safe here because M = *schema.Message.
		// 这里的类型断言是安全的，因为 M = *schema.Message。
		for _, m := range input.Messages {
			msgs = append(msgs, any(m).(*schema.Message))
		}
		result := make([]M, len(msgs))
		for i, m := range msgs {
			result[i] = any(m).(M)
		}
		return result, nil
	case *schema.AgenticMessage:
		msgs := make([]*schema.AgenticMessage, 0, len(input.Messages)+1)
		if instruction != "" {
			msgs = append(msgs, schema.SystemAgenticMessage(instruction))
		}
		for _, m := range input.Messages {
			msgs = append(msgs, any(m).(*schema.AgenticMessage))
		}
		result := make([]M, len(msgs))
		for i, m := range msgs {
			result[i] = any(m).(M)
		}
		return result, nil
	}
	panic("unreachable")
}

func buildTypedBuiltinAgentMiddlewares[M adk.MessageType](ctx context.Context, cfg *TypedConfig[M]) ([]adk.TypedChatModelAgentMiddleware[M], error) {
	var ms []adk.TypedChatModelAgentMiddleware[M]
	if !cfg.WithoutWriteTodos {
		t, err := typedNewWriteTodos[M]()
		if err != nil {
			return nil, err
		}
		ms = append(ms, t)
	}

	if cfg.Backend != nil || cfg.Shell != nil || cfg.StreamingShell != nil {
		fm, err := filesystem2.NewTyped[M](ctx, &filesystem2.MiddlewareConfig{
			Backend:        cfg.Backend,
			Shell:          cfg.Shell,
			StreamingShell: cfg.StreamingShell,
		})
		if err != nil {
			return nil, err
		}
		ms = append(ms, fm)
	}

	return ms, nil
}

type TODO struct {
	Content    string `json:"content"`
	ActiveForm string `json:"activeForm"`
	Status     string `json:"status" jsonschema:"enum=pending,enum=in_progress,enum=completed"`
}

type writeTodosArguments struct {
	Todos []TODO `json:"todos"`
}

func typedNewWriteTodos[M adk.MessageType]() (adk.TypedChatModelAgentMiddleware[M], error) {
	toolDesc := internal.SelectPrompt(internal.I18nPrompts{
		English: writeTodosToolDescription,
		Chinese: writeTodosToolDescriptionChinese,
	})
	resultMsg := internal.SelectPrompt(internal.I18nPrompts{
		English: "Updated todo list to %s",
		Chinese: "已更新待办列表为 %s",
	})

	t, err := utils.InferTool("write_todos", toolDesc, func(ctx context.Context, input writeTodosArguments) (output string, err error) {
		adk.AddSessionValue(ctx, SessionKeyTodos, input.Todos)
		todos, err := sonic.MarshalString(input.Todos)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(resultMsg, todos), nil
	})
	if err != nil {
		return nil, err
	}

	return typedBuildAppendPromptTool[M]("", t), nil
}
