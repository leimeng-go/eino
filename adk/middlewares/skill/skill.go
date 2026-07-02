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

package skill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/slongfield/pyfmt"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// ContextMode defines the execution mode of a skill.
// ContextMode 定义 skill 的执行模式。
type ContextMode string

const (
	// ContextModeFork creates a new sub-agent without parent history
	// ContextModeFork 创建一个不带父级历史的新子智能体
	ContextModeFork ContextMode = "fork"
	// ContextModeForkWithContext creates a new sub-agent with parent history
	// ContextModeForkWithContext 创建一个带父级历史的新子智能体
	ContextModeForkWithContext ContextMode = "fork_with_context"
)

// FrontMatter defines the YAML frontmatter schema parsed from a SKILL.md file.
// FrontMatter 定义从 SKILL.md 文件解析出的 YAML frontmatter schema。
type FrontMatter struct {
	Name        string      `yaml:"name"`
	Description string      `yaml:"description"`
	Context     ContextMode `yaml:"context"`
	Agent       string      `yaml:"agent"`
	Model       string      `yaml:"model"`
}

// Skill represents a skill loaded from a backend.
// Skill 表示从 backend 加载的 skill。
type Skill struct {
	FrontMatter
	// Content is the markdown body after the frontmatter contains the skill instructions of a SKILL.md file.
	// Content 是 frontmatter 之后的 markdown 正文，包含 SKILL.md 文件的 skill 指令。
	Content string
	// BaseDirectory is the absolute directory path where the SKILL.md file is located (e.g., "/absolute/path/to/skills/my-skill").
	// BaseDirectory 是 SKILL.md 文件所在的绝对目录路径（例如 "/absolute/path/to/skills/my-skill"）。
	BaseDirectory string
}

// Backend loads skills and provides metadata for tool description rendering.
// Backend 加载 skills，并提供用于渲染工具描述的元数据。
type Backend interface {
	List(ctx context.Context) ([]FrontMatter, error)
	Get(ctx context.Context, name string) (Skill, error)
}

// TypedAgentHubOptions contains options passed to TypedAgentHub.Get when creating an agent for skill execution.
// TypedAgentHubOptions 包含创建用于 skill 执行的智能体时传给 TypedAgentHub.Get 的选项。
type TypedAgentHubOptions[M adk.MessageType] struct {
	// Model is the resolved model instance when a skill specifies a "model" field in frontmatter.
	// nil means the skill did not specify a model override; implementations should use their default.
	//
	// Model 是 skill 在 frontmatter 中指定 "model" 字段时解析出的模型实例。
	// nil 表示该 skill 未指定模型覆盖；实现应使用其默认模型。
	Model model.BaseModel[M]
}

// AgentHubOptions is a backward-compatible alias for TypedAgentHubOptions instantiated with *schema.Message.
// AgentHubOptions 是用 *schema.Message 实例化的 TypedAgentHubOptions 的向后兼容别名。
type AgentHubOptions = TypedAgentHubOptions[*schema.Message]

// TypedAgentHub provides agent instances for context mode (fork/fork_with_context) execution.
// TypedAgentHub 为 context mode（fork/fork_with_context）执行提供智能体实例。
type TypedAgentHub[M adk.MessageType] interface {
	// Get returns an Agent by name. When name is empty, implementations should return a default agent.
	// The opts parameter carries skill-level overrides (e.g., model) resolved by the framework.
	//
	// Get 按名称返回 Agent。name 为空时，实现应返回默认智能体。
	// opts 参数携带框架解析出的 skill 级覆盖项（例如 model）。
	Get(ctx context.Context, name string, opts *TypedAgentHubOptions[M]) (adk.TypedAgent[M], error)
}

// AgentHub is a backward-compatible alias for TypedAgentHub instantiated with *schema.Message.
// AgentHub 是用 *schema.Message 实例化的 TypedAgentHub 的向后兼容别名。
type AgentHub = TypedAgentHub[*schema.Message]

// TypedModelHub resolves model instances by name for skills that specify a "model" field in frontmatter.
// TypedModelHub 为在 frontmatter 中指定 "model" 字段的 skills 按名称解析模型实例。
type TypedModelHub[M adk.MessageType] interface {
	Get(ctx context.Context, name string) (model.BaseModel[M], error)
}

// ModelHub is a backward-compatible alias for TypedModelHub instantiated with *schema.Message.
// ModelHub 是用 *schema.Message 实例化的 TypedModelHub 的向后兼容别名。
type ModelHub = TypedModelHub[*schema.Message]

// SystemPromptFunc is a function that returns a custom system prompt.
// The toolName parameter is the name of the skill tool (default: "skill").
//
// SystemPromptFunc 是返回自定义系统提示的函数。
// toolName 参数是 skill 工具的名称（默认："skill"）。
type SystemPromptFunc func(ctx context.Context, toolName string) string

// ToolDescriptionFunc is a function that returns a custom tool description.
// The skills parameter contains all available skill front matters.
//
// ToolDescriptionFunc 是返回自定义工具描述的函数。
// skills 参数包含所有可用 skill 的 front matters。
type ToolDescriptionFunc func(ctx context.Context, skills []FrontMatter) string

// TypedSubAgentInput contains the context available when building the sub-agent's
// initial messages in fork/fork_with_context mode.
//
// TypedSubAgentInput 包含在 fork/fork_with_context 模式下构建子智能体初始消息时可用的上下文。
type TypedSubAgentInput[M adk.MessageType] struct {
	Skill        Skill
	Mode         ContextMode
	RawArguments string
	SkillContent string
	History      []M
	ToolCallID   string
}

// SubAgentInput is a backward-compatible alias for TypedSubAgentInput instantiated with *schema.Message.
// SubAgentInput 是用 *schema.Message 实例化的 TypedSubAgentInput 的向后兼容别名。
type SubAgentInput = TypedSubAgentInput[*schema.Message]

// TypedSubAgentOutput contains the sub-agent's execution results, available when
// formatting the final tool response.
//
// TypedSubAgentOutput 包含子智能体的执行结果，可在格式化最终工具响应时使用。
type TypedSubAgentOutput[M adk.MessageType] struct {
	Skill        Skill
	Mode         ContextMode
	RawArguments string
	Messages     []M
	Results      []string
}

// SubAgentOutput is a backward-compatible alias for TypedSubAgentOutput instantiated with *schema.Message.
// SubAgentOutput 是 TypedSubAgentOutput 以 *schema.Message 实例化后的向后兼容别名。
type SubAgentOutput = TypedSubAgentOutput[*schema.Message]

// TypedConfig is the configuration for the skill middleware.
// TypedConfig 是 skill 中间件的配置。
type TypedConfig[M adk.MessageType] struct {
	// Backend is the backend for retrieving skills.
	// Backend 是用于检索 skills 的后端。
	Backend Backend
	// SkillToolName is the custom name for the skill tool. If nil, the default name "skill" is used.
	// SkillToolName 是 skill 工具的自定义名称。若为 nil，则使用默认名称 "skill"。
	SkillToolName *string
	// Deprecated: Use adk.SetLanguage(adk.LanguageChinese) instead to enable Chinese prompts globally.
	// This field will be removed in a future version.
	//
	// Deprecated: 改用 adk.SetLanguage(adk.LanguageChinese) 全局启用中文提示。
	// 该字段将在未来版本中移除。
	UseChinese bool
	// AgentHub provides agent instances for context mode (fork/fork_with_context) execution.
	// Required when skills use "context: fork" or "context: fork_with_context" in frontmatter.
	// The agent factory is retrieved by agent name (skill.Agent) from this hub.
	// When skill.Agent is empty, AgentHub.Get is called with an empty string,
	// allowing the hub implementation to return a default agent.
	//
	// AgentHub 为 context 模式（fork/fork_with_context）执行提供智能体实例。
	// 当 skills 在 frontmatter 中使用 "context: fork" 或 "context: fork_with_context" 时必需。
	// 会从该 hub 按智能体名称（skill.Agent）获取智能体工厂。
	// 当 skill.Agent 为空时，会用空字符串调用 AgentHub.Get，
	// 允许 hub 实现返回默认智能体。
	AgentHub TypedAgentHub[M]
	// ModelHub provides model instances for skills that specify a "model" field in frontmatter.
	// Used in two scenarios:
	//   - With context mode (fork/fork_with_context): The model is passed to the AgentHub
	//   - Without context mode (inline): The model becomes active for subsequent ChatModel requests
	// If nil, skills with model specification will be ignored in inline mode,
	// or return an error in context mode.
	//
	// ModelHub 为在 frontmatter 中指定 "model" 字段的 skills 提供模型实例。
	// 用于两种场景：
	// - 带 context 模式（fork/fork_with_context）：模型会传给 AgentHub
	// - 不带 context 模式（inline）：模型会对后续 ChatModel 请求生效
	// 若为 nil，带模型指定的 skills 在 inline 模式下会被忽略，
	// 在 context 模式下会返回错误。
	ModelHub TypedModelHub[M]

	// CustomSystemPrompt allows customizing the system prompt injected into the agent.
	// If nil, the default system prompt is used.
	// The function receives the skill tool name as a parameter.
	//
	// CustomSystemPrompt 允许自定义注入到智能体中的系统提示。
	// 若为 nil，则使用默认系统提示。
	// 该函数接收 skill 工具名称作为参数。
	CustomSystemPrompt SystemPromptFunc
	// CustomToolDescription allows customizing the tool description for the skill tool.
	// If nil, the default tool description is used.
	// The function receives all available skill front matters as a parameter.
	//
	// CustomToolDescription 允许自定义 skill 工具的工具描述。
	// 若为 nil，则使用默认工具描述。
	// 该函数接收所有可用的 skill front matters 作为参数。
	CustomToolDescription ToolDescriptionFunc

	// CustomToolParams customizes tool parameters for the skill tool.
	// defaults is the default schema with only the required "skill" field.
	// optional
	//
	// CustomToolParams 自定义 skill 工具的工具参数。
	// defaults 是默认 schema，仅包含必需的 "skill" 字段。
	// 可选
	CustomToolParams func(ctx context.Context, defaults map[string]*schema.ParameterInfo) (map[string]*schema.ParameterInfo, error)

	// BuildContent customizes the skill content generated for this invocation.
	// rawArgs contains the original tool call arguments in JSON form.
	// optional
	//
	// BuildContent 自定义本次调用生成的 skill 内容。
	// rawArgs 包含原始工具调用参数的 JSON 形式。
	// 可选
	BuildContent func(ctx context.Context, skill Skill, rawArgs string) (string, error)

	// BuildForkMessages customizes the messages passed to the forked sub-agent.
	// When nil, fork uses [UserMessage(skillContent)] and fork_with_context uses
	// [history..., ToolMessage(skillContent, toolCallID)].
	// optional
	//
	// BuildForkMessages 自定义传递给 fork 后子智能体的消息。
	// 为 nil 时，fork 使用 [UserMessage(skillContent)]，fork_with_context 使用
	// [history..., ToolMessage(skillContent, toolCallID)]。
	// 可选
	BuildForkMessages func(ctx context.Context, in TypedSubAgentInput[M]) ([]M, error)

	// FormatForkResult customizes the final text returned from the forked sub-agent results.
	// When nil, assistant message contents emitted by the sub-agent are concatenated and returned
	// in a default formatted string.
	// optional
	//
	// FormatForkResult 自定义从 fork 后子智能体结果返回的最终文本。
	// 为 nil 时，会拼接子智能体发出的 assistant 消息内容，并以默认格式字符串返回。
	// 可选
	FormatForkResult func(ctx context.Context, in TypedSubAgentOutput[M]) (string, error)
}

// Config is a backward-compatible alias for TypedConfig instantiated with *schema.Message.
// Config 是 TypedConfig 以 *schema.Message 实例化后的向后兼容别名。
type Config = TypedConfig[*schema.Message]

// NewTyped creates a generic skill middleware handler for TypedChatModelAgent.
//
// This is the generic constructor that supports both *schema.Message and *schema.AgenticMessage.
// For *schema.AgenticMessage, tool execution is message-type-independent; the model override
// via ModelHub only takes effect when M is *schema.Message (for other types it is a no-op).
//
// See NewMiddleware for full usage documentation.
//
// NewTyped 为 TypedChatModelAgent 创建通用 skill 中间件处理器。
// 这是同时支持 *schema.Message 和 *schema.AgenticMessage 的通用构造函数。
// 对于 *schema.AgenticMessage，工具执行与消息类型无关；通过 ModelHub 进行的模型覆盖
// 仅在 M 为 *schema.Message 时生效（对其他类型为 no-op）。
// 完整用法文档见 NewMiddleware。
func NewTyped[M adk.MessageType](ctx context.Context, config *TypedConfig[M]) (adk.TypedChatModelAgentMiddleware[M], error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if config.Backend == nil {
		return nil, fmt.Errorf("backend is required")
	}

	name := toolName
	if config.SkillToolName != nil {
		name = *config.SkillToolName
	}

	var instruction string
	if config.CustomSystemPrompt != nil {
		instruction = config.CustomSystemPrompt(ctx, name)
	} else {
		var err error
		instruction, err = buildSystemPrompt(name, config.UseChinese)
		if err != nil {
			return nil, err
		}
	}

	return &typedSkillHandler[M]{
		instruction: instruction,
		tool: &typedSkillTool[M]{
			b:                 config.Backend,
			toolName:          name,
			useChinese:        config.UseChinese,
			agentHub:          config.AgentHub,
			modelHub:          config.ModelHub,
			customToolDesc:    config.CustomToolDescription,
			customToolParams:  config.CustomToolParams,
			buildContent:      config.BuildContent,
			buildForkMessages: config.BuildForkMessages,
			formatForkResult:  config.FormatForkResult,
		},
	}, nil
}

// NewMiddleware creates a new skill middleware handler for ChatModelAgent.
//
// The handler provides a skill tool that allows agents to load and execute skills
// defined in SKILL.md files. Skills can run in different modes based on their
// frontmatter configuration:
//
//   - Inline mode (default): Skill content is returned directly as tool result
//   - Fork mode (context: fork): Forks a new agent with a clean context, discarding message history
//   - Fork with context mode (context: fork_with_context): Forks a new agent carrying over message history
//
// Example usage:
//
//	handler, err := skill.NewMiddleware(ctx, &skill.Config{
//	    Backend:  backend,
//	    AgentHub: myAgentHub,
//	    ModelHub: myModelHub,
//	})
//	if err != nil {
//	    return err
//	}
//
//	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
//	    // ...
//	    Handlers: []adk.ChatModelAgentMiddleware{handler},
//	})
//
// NewMiddleware 为 ChatModelAgent 创建新的 skill 中间件处理器。
// 该处理器提供一个 skill 工具，使智能体能够加载并执行
// 定义在 SKILL.md 文件中的 skills。skills 可根据其
// frontmatter 配置以不同模式运行：
// - Inline mode（默认）：skill 内容直接作为工具结果返回
// - Fork mode（context: fork）：fork 一个带干净 context 的新智能体，丢弃消息历史
// - Fork with context mode（context: fork_with_context）：fork 一个携带消息历史的新智能体
// 示例用法：
// handler, err := skill.NewMiddleware(ctx, &skill.Config{
// Backend:  backend,
// AgentHub: myAgentHub,
// ModelHub: myModelHub,
// })
// if err != nil {
// return err
// }
// agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
// ...
// Handlers: []adk.ChatModelAgentMiddleware{handler},
// })
func NewMiddleware(ctx context.Context, config *Config) (adk.ChatModelAgentMiddleware, error) {
	return NewTyped(ctx, config)
}

type typedSkillHandler[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
	instruction string
	tool        *typedSkillTool[M]
}

func (h *typedSkillHandler[M]) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	runCtx.Instruction = runCtx.Instruction + "\n" + h.instruction
	runCtx.Tools = append(runCtx.Tools, h.tool)
	return ctx, runCtx, nil
}

func (h *typedSkillHandler[M]) WrapModel(ctx context.Context, m model.BaseModel[M], _ *adk.TypedModelContext[M]) (model.BaseModel[M], error) {
	if h.tool.modelHub == nil {
		return m, nil
	}
	modelName, found, err := adk.GetRunLocalValue(ctx, activeModelKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get active model from run local value: %w", err)
	}
	if !found {
		return m, nil
	}
	name, ok := modelName.(string)
	if !ok || name == "" {
		return m, nil
	}
	newModel, err := h.tool.modelHub.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get model '%s' from ModelHub: %w", name, err)
	}
	return newModel, nil
}

const activeModelKey = "__skill_active_model__"

// New creates a new skill middleware.
// It provides a tool for the agent to use skills.
//
// Deprecated: Use NewMiddleware instead. New does not support fork mode execution
// because AgentMiddleware cannot save message history for fork mode.
//
// New 创建新的 skill 中间件。
// 它提供一个工具供智能体使用 skills。
// Deprecated: 改用 NewMiddleware。New 不支持 fork 模式执行，
// 因为 AgentMiddleware 无法为 fork 模式保存消息历史。
func New(ctx context.Context, config *Config) (adk.AgentMiddleware, error) {
	if config == nil {
		return adk.AgentMiddleware{}, fmt.Errorf("config is required")
	}
	if config.Backend == nil {
		return adk.AgentMiddleware{}, fmt.Errorf("backend is required")
	}

	name := toolName
	if config.SkillToolName != nil {
		name = *config.SkillToolName
	}

	var sp string
	if config.CustomSystemPrompt != nil {
		sp = config.CustomSystemPrompt(ctx, name)
	} else {
		var err error
		sp, err = buildSystemPrompt(name, config.UseChinese)
		if err != nil {
			return adk.AgentMiddleware{}, err
		}
	}

	return adk.AgentMiddleware{
		AdditionalInstruction: sp,
		AdditionalTools: []tool.BaseTool{&typedSkillTool[*schema.Message]{
			b:              config.Backend,
			toolName:       name,
			useChinese:     config.UseChinese,
			customToolDesc: config.CustomToolDescription,
		}},
	}, nil
}

func buildSystemPrompt(skillToolName string, useChinese bool) (string, error) {
	var prompt string
	if useChinese {
		prompt = systemPromptChinese
	} else {
		prompt = internal.SelectPrompt(internal.I18nPrompts{
			English: systemPrompt,
			Chinese: systemPromptChinese,
		})
	}
	return pyfmt.Fmt(prompt, map[string]string{
		"tool_name": skillToolName,
	})
}

type typedSkillTool[M adk.MessageType] struct {
	b        Backend
	toolName string

	useChinese bool
	agentHub   TypedAgentHub[M]
	modelHub   TypedModelHub[M]

	customToolDesc ToolDescriptionFunc

	customToolParams func(ctx context.Context, defaults map[string]*schema.ParameterInfo) (map[string]*schema.ParameterInfo, error)
	buildContent     func(ctx context.Context, skill Skill, rawArgs string) (string, error)

	buildForkMessages func(ctx context.Context, in TypedSubAgentInput[M]) ([]M, error)
	formatForkResult  func(ctx context.Context, in TypedSubAgentOutput[M]) (string, error)
}

type descriptionTemplateHelper struct {
	Matters []FrontMatter
}

func (s *typedSkillTool[M]) Info(ctx context.Context) (*schema.ToolInfo, error) {
	skills, err := s.b.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list skills: %w", err)
	}

	var fullDesc string
	if s.customToolDesc != nil {
		fullDesc = s.customToolDesc(ctx, skills)
	} else {
		desc, renderErr := renderToolDescription(skills)
		if renderErr != nil {
			return nil, fmt.Errorf("failed to render skill tool description: %w", renderErr)
		}

		descBase := internal.SelectPrompt(internal.I18nPrompts{
			English: toolDescriptionBase,
			Chinese: toolDescriptionBaseChinese,
		})
		fullDesc = descBase + desc
	}

	oneOf, err := s.buildParamsOneOf(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to build skill tool params: %w", err)
	}

	return &schema.ToolInfo{
		Name:        s.toolName,
		Desc:        fullDesc,
		ParamsOneOf: oneOf,
	}, nil
}

type inputArguments struct {
	Skill string `json:"skill"`
}

func (s *typedSkillTool[M]) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	args := &inputArguments{}
	err := json.Unmarshal([]byte(argumentsInJSON), args)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}
	skill, err := s.b.Get(ctx, args.Skill)
	if err != nil {
		return "", fmt.Errorf("failed to get skill: %w", err)
	}

	switch skill.Context {
	case ContextModeForkWithContext:
		return s.runAgentMode(ctx, skill, true, argumentsInJSON)
	case ContextModeFork:
		return s.runAgentMode(ctx, skill, false, argumentsInJSON)
	default:
		if skill.Model != "" {
			s.setActiveModel(ctx, skill.Model)
		}
		return s.buildSkillResult(ctx, skill, argumentsInJSON)
	}
}

func (s *typedSkillTool[M]) setActiveModel(ctx context.Context, modelName string) {
	_ = adk.SetRunLocalValue(ctx, activeModelKey, modelName)
}

func defaultToolParams() map[string]*schema.ParameterInfo {
	skillParamDesc := internal.SelectPrompt(internal.I18nPrompts{
		English: "The skill name (no arguments). E.g., \"pdf\" or \"xlsx\"",
		Chinese: "Skill 名称（无需其他参数）。例如：\"pdf\" 或 \"xlsx\"",
	})
	return map[string]*schema.ParameterInfo{
		"skill": {
			Type:     schema.String,
			Desc:     skillParamDesc,
			Required: true,
		},
	}
}

func (s *typedSkillTool[M]) buildParamsOneOf(ctx context.Context) (*schema.ParamsOneOf, error) {
	defaults := defaultToolParams()
	if s.customToolParams == nil {
		return schema.NewParamsOneOfByParams(defaults), nil
	}

	params, err := s.customToolParams(ctx, defaults)
	if err != nil {
		return nil, err
	}
	if params == nil {
		params = defaults
	}

	if _, ok := params["skill"]; !ok {
		params["skill"] = defaults["skill"]
	}

	if p := params["skill"]; p != nil {
		p.Required = true
	}

	return schema.NewParamsOneOfByParams(params), nil
}

func (s *typedSkillTool[M]) buildSkillResult(ctx context.Context, skill Skill, rawArguments string) (string, error) {
	if s.buildContent == nil {
		return s.defaultSkillContent(skill), nil
	}
	content, err := s.buildContent(ctx, skill, rawArguments)
	if err != nil {
		return "", fmt.Errorf("failed to build skill result: %w", err)
	}
	return content, nil
}

func (s *typedSkillTool[M]) defaultSkillContent(skill Skill) string {
	resultFmt := internal.SelectPrompt(internal.I18nPrompts{
		English: toolResult,
		Chinese: toolResultChinese,
	})
	contentFmt := internal.SelectPrompt(internal.I18nPrompts{
		English: userContent,
		Chinese: userContentChinese,
	})

	return fmt.Sprintf(resultFmt, skill.Name) + fmt.Sprintf(contentFmt, skill.BaseDirectory, skill.Content)
}

func (s *typedSkillTool[M]) runAgentMode(ctx context.Context, skill Skill, forkHistory bool, rawArguments string) (string, error) {
	if s.agentHub == nil {
		return "", fmt.Errorf("skill '%s' requires context:%s but AgentHub is not configured", skill.Name, skill.Context)
	}

	opts := &TypedAgentHubOptions[M]{}
	if skill.Model != "" {
		if s.modelHub == nil {
			return "", fmt.Errorf("skill '%s' requires model '%s' but ModelHub is not configured", skill.Name, skill.Model)
		}
		m, err := s.modelHub.Get(ctx, skill.Model)
		if err != nil {
			return "", fmt.Errorf("failed to get model '%s' from ModelHub: %w", skill.Model, err)
		}
		opts.Model = m
	}

	agent, err := s.agentHub.Get(ctx, skill.Agent, opts)
	if err != nil {
		return "", fmt.Errorf("failed to get agent '%s' from AgentHub: %w", skill.Agent, err)
	}

	var messages []M
	skillContent, err := s.buildSkillResult(ctx, skill, rawArguments)
	if err != nil {
		return "", fmt.Errorf("failed to build skill result: %w", err)
	}

	var history []M
	var toolCallID string
	if forkHistory {
		history, err = s.getMessagesFromState(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get messages from state: %w", err)
		}
		toolCallID = compose.GetToolCallID(ctx)
	}

	if s.buildForkMessages != nil {
		messages, err = s.buildForkMessages(ctx, TypedSubAgentInput[M]{
			Skill:        skill,
			Mode:         skill.Context,
			RawArguments: rawArguments,
			SkillContent: skillContent,
			History:      history,
			ToolCallID:   toolCallID,
		})
		if err != nil {
			return "", fmt.Errorf("failed to build fork messages: %w", err)
		}
	} else {
		var zero M
		if forkHistory {
			var toolMsg M
			switch any(zero).(type) {
			case *schema.Message:
				toolMsg = any(schema.ToolMessage(skillContent, toolCallID)).(M)
			case *schema.AgenticMessage:
				toolMsg = any(&schema.AgenticMessage{
					Role: schema.AgenticRoleTypeUser,
					ContentBlocks: []*schema.ContentBlock{
						schema.NewContentBlock(&schema.FunctionToolResult{
							CallID: toolCallID,
							Name:   "",
							Content: []*schema.FunctionToolResultContentBlock{
								{Type: schema.FunctionToolResultContentBlockTypeText, Text: &schema.UserInputText{Text: skillContent}},
							},
						}),
					},
				}).(M)
			}
			messages = append(history, toolMsg)
		} else {
			var userMsg M
			switch any(zero).(type) {
			case *schema.Message:
				userMsg = any(schema.UserMessage(skillContent)).(M)
			case *schema.AgenticMessage:
				userMsg = any(schema.UserAgenticMessage(skillContent)).(M)
			}
			messages = []M{userMsg}
		}
	}

	input := &adk.TypedAgentInput[M]{
		Messages:        messages,
		EnableStreaming: false,
	}

	iter := agent.Run(ctx, input)

	var msgList []M
	var results []string
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}

		if event.Err != nil {
			return "", fmt.Errorf("failed to run agent event: %w", event.Err)
		}

		if event.Output == nil || event.Output.MessageOutput == nil {
			continue
		}

		msg, msgErr := event.Output.MessageOutput.GetMessage()
		if msgErr != nil {
			return "", fmt.Errorf("failed to get message from event: %w", msgErr)
		}

		if !isNilMessage(msg) {
			msgList = append(msgList, msg)
			var content string
			switch m := any(msg).(type) {
			case *schema.Message:
				content = m.Content
			case *schema.AgenticMessage:
				var parts []string
				for _, block := range m.ContentBlocks {
					if block == nil {
						continue
					}
					if block.AssistantGenText != nil {
						parts = append(parts, block.AssistantGenText.Text)
					}
				}
				content = strings.Join(parts, "\n")
			}
			if content != "" {
				results = append(results, content)
			}
		}
	}

	if s.formatForkResult != nil {
		out, err := s.formatForkResult(ctx, TypedSubAgentOutput[M]{
			Skill:        skill,
			Mode:         skill.Context,
			RawArguments: rawArguments,
			Messages:     msgList,
			Results:      results,
		})
		if err != nil {
			return "", fmt.Errorf("failed to format fork result: %w", err)
		}
		return out, nil
	}

	resultFmt := internal.SelectPrompt(internal.I18nPrompts{
		English: subAgentResultFormat,
		Chinese: subAgentResultFormatChinese,
	})

	return fmt.Sprintf(resultFmt, skill.Name, strings.Join(results, "\n")), nil
}

func isNilMessage[M adk.MessageType](msg M) bool {
	var zero M
	return any(msg) == any(zero)
}

func (s *typedSkillTool[M]) getMessagesFromState(ctx context.Context) ([]M, error) {
	var messages []M
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		err := compose.ProcessState(ctx, func(_ context.Context, st *adk.State) error {
			messages = make([]M, len(st.Messages))
			for i, m := range st.Messages {
				messages[i] = any(m).(M)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to process state: %w", err)
		}
	case *schema.AgenticMessage:
		// Fork mode is not supported for AgenticMessage because the internal
		// agent state type (agenticState) is unexported from the adk package,
		// making it inaccessible via compose.ProcessState from middleware packages.
		// Agent mode (the default) works normally for AgenticMessage.
		//
		// AgenticMessage 不支持 Fork mode，因为内部
		// 智能体状态类型（agenticState）未从 adk 包导出，
		// 导致中间件包无法通过 compose.ProcessState 访问它。
		// Agent mode（默认）对 AgenticMessage 可正常工作。
		return nil, fmt.Errorf("fork mode is not supported for AgenticMessage; use agent mode instead")
	}
	return messages, nil
}

func renderToolDescription(matters []FrontMatter) (string, error) {
	tpl, err := template.New("skills").Parse(toolDescriptionTemplate)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tpl.Execute(&buf, descriptionTemplateHelper{Matters: matters})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
