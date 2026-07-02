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

// Package toolsearch provides tool search middleware.
// Package toolsearch 提供工具搜索中间件。
package toolsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/internal"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

// Config is the configuration for the tool search middleware.
// Config 是工具搜索中间件的配置。
type Config struct {
	// DynamicTools is a list of tools that can be dynamically searched and loaded by the agent.
	// DynamicTools 是可由智能体动态搜索和加载的工具列表。
	DynamicTools []tool.BaseTool

	// UseModelToolSearch indicates whether the ChatModel natively supports tool search.
	//
	// When true, the middleware delegates tool search to the model's native capability.
	//
	// When false (default), the middleware manages tool visibility by filtering the tool list
	// based on tool_search results before each model call. Note that this approach may
	// invalidate the model's KV-cache (as the tool list changes between calls), and effectiveness
	// depends on the model's ability to work with a dynamically changing tool set.
	//
	// UseModelToolSearch 表示 ChatModel 是否原生支持工具搜索。
	// 当为 true 时，中间件会将工具搜索委托给模型的原生能力。
	// 当为 false（默认）时，中间件会在每次模型调用前根据 tool_search 结果过滤工具列表来管理工具可见性。注意，由于工具列表会在调用间变化，这种方式可能会使模型的 KV-cache 失效，其效果取决于模型处理动态变化工具集的能力。
	UseModelToolSearch bool
}

// NewTyped constructs and returns the generic tool search middleware.
//
// This is the generic constructor that supports both *schema.Message and *schema.AgenticMessage.
//
// NewTyped 构造并返回泛型工具搜索中间件。
// 这是支持 *schema.Message 和 *schema.AgenticMessage 的泛型构造函数。
func NewTyped[M adk.MessageType](ctx context.Context, config *Config) (adk.TypedChatModelAgentMiddleware[M], error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if len(config.DynamicTools) == 0 {
		return nil, fmt.Errorf("tools is required")
	}

	tpl, err := template.New("").Parse(systemReminderTpl)
	if err != nil {
		return nil, err
	}

	dynamicToolInfos := make([]*schema.ToolInfo, 0, len(config.DynamicTools))
	mapOfDynamicTools := make(map[string]*schema.ToolInfo, len(config.DynamicTools))
	toolNames := make([]string, 0, len(config.DynamicTools))
	for _, t := range config.DynamicTools {
		info, infoErr := t.Info(ctx)
		if infoErr != nil {
			return nil, fmt.Errorf("failed to get dynamic tool info: %w", infoErr)
		}

		if _, ok := mapOfDynamicTools[info.Name]; ok {
			return nil, fmt.Errorf("duplicate dynamic tool name: %s", info.Name)
		}

		toolNames = append(toolNames, info.Name)
		mapOfDynamicTools[info.Name] = info
		dynamicToolInfos = append(dynamicToolInfos, info)
	}

	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, systemReminder{Tools: toolNames})
	if err != nil {
		return nil, fmt.Errorf("failed to format system reminder template: %w", err)
	}

	return &typedMiddleware[M]{
		dynamicTools:       config.DynamicTools,
		mapOfDynamicTools:  mapOfDynamicTools,
		dynamicToolInfos:   dynamicToolInfos,
		useModelToolSearch: config.UseModelToolSearch,
		sr:                 buf.String(),
	}, nil
}

// New constructs and returns the tool search middleware.
//
// The tool search middleware enables dynamic tool selection for agents with large tool libraries.
// Instead of passing all tools to the model at once (which can overwhelm context limits),
// this middleware:
//
//  1. Adds a "tool_search" meta-tool that accepts keyword queries to search tools
//  2. Initially hides all dynamic tools from the model's tool list
//  3. When the model calls tool_search, matching tools become available for subsequent calls
//
// Example usage:
//
//	middleware, _ := toolsearch.New(ctx, &toolsearch.Config{
//	    DynamicTools: []tool.BaseTool{weatherTool, stockTool, currencyTool, ...},
//	})
//	agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
//	    // ...
//	    Handlers: []adk.ChatModelAgentMiddleware{middleware},
//	})
//
// New 构造并返回工具搜索中间件。
// 工具搜索中间件为拥有大型工具库的智能体启用动态工具选择。
// 此中间件不会一次性将所有工具传给模型（这可能压垮上下文限制），而是：
// 1. 添加一个接受关键字查询以搜索工具的 "tool_search" 元工具
// 2. 初始时从模型的工具列表中隐藏所有动态工具
// 3. 当模型调用 tool_search 时，匹配的工具会在后续调用中可用
// 示例用法：
// middleware, _ := toolsearch.New(ctx, &toolsearch.Config{
// DynamicTools: []tool.BaseTool{weatherTool, stockTool, currencyTool, ...},
// })
// agent, _ := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
// ...
// Handlers: []adk.ChatModelAgentMiddleware{middleware},
// })
func New(ctx context.Context, config *Config) (adk.ChatModelAgentMiddleware, error) {
	return NewTyped[*schema.Message](ctx, config)
}

type systemReminder struct {
	Tools []string
}

type typedMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
	dynamicTools       []tool.BaseTool
	mapOfDynamicTools  map[string]*schema.ToolInfo
	dynamicToolInfos   []*schema.ToolInfo
	useModelToolSearch bool
	sr                 string
}

func (m *typedMiddleware[M]) BeforeAgent(ctx context.Context, runCtx *adk.ChatModelAgentContext) (context.Context, *adk.ChatModelAgentContext, error) {
	if runCtx == nil {
		return ctx, runCtx, nil
	}

	nRunCtx := *runCtx
	nRunCtx.Tools = make([]tool.BaseTool, len(runCtx.Tools), len(runCtx.Tools)+1+len(m.dynamicTools))
	copy(nRunCtx.Tools, runCtx.Tools)
	nRunCtx.Tools = append(nRunCtx.Tools, newToolSearchTool(m.mapOfDynamicTools, m.useModelToolSearch))
	nRunCtx.Tools = append(nRunCtx.Tools, m.dynamicTools...)
	if m.useModelToolSearch {
		nRunCtx.ToolSearchTool = getToolSearchToolInfo()
	}
	return ctx, &nRunCtx, nil
}

const toolSearchInitializedKey = "__toolsearch_initialized__"
const toolSearchReminderExtraKey = "__toolsearch_reminder__"

func (m *typedMiddleware[M]) isInitialized(ctx context.Context) bool {
	val, ok, err := adk.GetRunLocalValue(ctx, toolSearchInitializedKey)
	if err != nil || !ok {
		return false
	}
	b, _ := val.(bool)
	return b
}

func (m *typedMiddleware[M]) markInitialized(ctx context.Context) {
	_ = adk.SetRunLocalValue(ctx, toolSearchInitializedKey, true)
}

func (m *typedMiddleware[M]) ensureReminder(msgs []M) []M {
	for _, msg := range msgs {
		if hasToolSearchReminderExtra(msg) {
			return msgs
		}
	}

	reminder := makeReminderMsg[M](m.sr)
	result := make([]M, 0, len(msgs)+1)
	inserted := false
	for _, msg := range msgs {
		if !inserted && !isSystemRoleTS(msg) {
			inserted = true
			result = append(result, reminder)
		}
		result = append(result, msg)
	}
	if !inserted {
		result = append(result, reminder)
	}
	return result
}

func isSystemRoleTS[M adk.MessageType](msg M) bool {
	switch m := any(msg).(type) {
	case *schema.Message:
		return m.Role == schema.System
	case *schema.AgenticMessage:
		return m.Role == schema.AgenticRoleTypeSystem
	}
	return false
}

func makeReminderMsg[M adk.MessageType](content string) M {
	var zero M
	switch any(zero).(type) {
	case *schema.Message:
		msg := schema.UserMessage(content)
		msg.Extra = map[string]any{toolSearchReminderExtraKey: true}
		return any(msg).(M)
	case *schema.AgenticMessage:
		msg := schema.UserAgenticMessage(content)
		msg.Extra = map[string]any{toolSearchReminderExtraKey: true}
		return any(msg).(M)
	}
	panic("unreachable")
}

func hasToolSearchReminderExtra[M adk.MessageType](msg M) bool {
	switch v := any(msg).(type) {
	case *schema.Message:
		if v.Extra != nil {
			if b, ok := v.Extra[toolSearchReminderExtraKey]; ok {
				if bVal, _ := b.(bool); bVal {
					return true
				}
			}
		}
	case *schema.AgenticMessage:
		if v.Extra != nil {
			if b, ok := v.Extra[toolSearchReminderExtraKey]; ok {
				if bVal, _ := b.(bool); bVal {
					return true
				}
			}
		}
	}
	return false
}

func (m *typedMiddleware[M]) extractDynamicTools(tools []*schema.ToolInfo) []*schema.ToolInfo {
	var result []*schema.ToolInfo
	for _, t := range tools {
		if _, ok := m.mapOfDynamicTools[t.Name]; ok {
			result = append(result, t)
		}
	}
	return result
}

func (m *typedMiddleware[M]) stripDynamicTools(tools []*schema.ToolInfo) []*schema.ToolInfo {
	var result []*schema.ToolInfo
	for _, t := range tools {
		if _, ok := m.mapOfDynamicTools[t.Name]; !ok {
			result = append(result, t)
		}
	}
	return result
}

func removeTool(tools []*schema.ToolInfo, name string) []*schema.ToolInfo {
	var result []*schema.ToolInfo
	for _, t := range tools {
		if t.Name != name {
			result = append(result, t)
		}
	}
	return result
}

func toolNameSet(tools []*schema.ToolInfo) map[string]bool {
	m := make(map[string]bool, len(tools))
	for _, t := range tools {
		m[t.Name] = true
	}
	return m
}

func (m *typedMiddleware[M]) BeforeModelRewriteState(ctx context.Context, state *adk.TypedChatModelAgentState[M], _ *adk.TypedModelContext[M]) (context.Context, *adk.TypedChatModelAgentState[M], error) {
	state.Messages = m.ensureReminder(state.Messages)

	if !m.isInitialized(ctx) {
		m.markInitialized(ctx)

		if m.useModelToolSearch {
			// Model-native search: move dynamic tools to DeferredToolInfos for server-side retrieval,
			// keep only static tools in ToolInfos, and remove the tool_search tool (the model handles search itself).
			//
			// 模型原生搜索：将动态工具移到 DeferredToolInfos 以进行服务端检索，
			// ToolInfos 中仅保留静态工具，并移除 tool_search 工具（由模型自行处理搜索）。
			state.DeferredToolInfos = m.extractDynamicTools(state.ToolInfos)
			state.ToolInfos = m.stripDynamicTools(state.ToolInfos)
			state.ToolInfos = removeTool(state.ToolInfos, toolSearchToolName)
		} else {
			// Client-side search: hide dynamic tools initially; they become visible
			// only after the model calls tool_search and forward selection adds them back.
			//
			// 客户端搜索：初始时隐藏动态工具；只有在模型调用 tool_search 且 forward selection 将其加回后，它们才会可见。
			state.ToolInfos = m.stripDynamicTools(state.ToolInfos)
		}
	}

	// Forward selection (client-side search only): scan tool_search results in the
	// conversation history and add the selected dynamic tools back to ToolInfos.
	//
	// 正向选择（仅客户端搜索）：扫描对话历史中的 tool_search 结果，并将选中的动态工具加回 ToolInfos。
	if !m.useModelToolSearch {
		existing := toolNameSet(state.ToolInfos)
		for _, msg := range state.Messages {
			content, ok := extractToolSearchResult(msg, toolSearchToolName)
			if !ok {
				continue
			}
			var result toolSearchResult
			if err := json.Unmarshal([]byte(content), &result); err != nil {
				continue
			}
			for _, name := range result.Matches {
				if existing[name] {
					continue
				}
				if info, ok := m.mapOfDynamicTools[name]; ok {
					state.ToolInfos = append(state.ToolInfos, info)
					existing[name] = true
				}
			}
		}
	}

	return ctx, state, nil
}

// extractToolSearchResult checks if the given message is a tool result from the tool_search tool,
// and if so returns the content string. Returns ("", false) if not a matching tool result.
//
// extractToolSearchResult 检查给定消息是否为 tool_search 工具的工具结果；如果是，则返回内容字符串。若不是匹配的工具结果，则返回 ("", false)。
func extractToolSearchResult[M adk.MessageType](msg M, toolName string) (string, bool) {
	switch v := any(msg).(type) {
	case *schema.Message:
		if v.Role == schema.Tool && v.ToolName == toolName {
			return v.Content, true
		}
	case *schema.AgenticMessage:
		for _, block := range v.ContentBlocks {
			if block != nil && block.Type == schema.ContentBlockTypeFunctionToolResult &&
				block.FunctionToolResult != nil && block.FunctionToolResult.Name == toolName {
				for _, b := range block.FunctionToolResult.Content {
					if b != nil && b.Text != nil {
						return b.Text.Text, true
					}
				}
			}
		}
	}
	return "", false
}

func newToolSearchTool(tools map[string]*schema.ToolInfo, useModelToolSearch bool) tool.BaseTool {
	if useModelToolSearch {
		return &modelToolSearchTool{tools: tools}
	}
	return &toolSearchTool{tools: tools}
}

type toolSearchArgs struct {
	Query      string `json:"query"`
	MaxResults *int   `json:"max_results,omitempty"`
}

type toolSearchResult struct {
	Matches []string `json:"matches"`
}

type toolSearchTool struct {
	tools map[string]*schema.ToolInfo
}

func (t *toolSearchTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return getToolSearchToolInfo(), nil
}

func (t *toolSearchTool) InvokableRun(_ context.Context, argumentsInJSON string, _ ...tool.Option) (string, error) {
	matches, err := search(argumentsInJSON, t.tools)
	if err != nil {
		return "", err
	}
	result := &toolSearchResult{}
	for _, m := range matches {
		result.Matches = append(result.Matches, m.Name)
	}
	b, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("failed to marshal tool search result: %w", err)
	}
	return string(b), nil
}

type modelToolSearchTool struct {
	tools map[string]*schema.ToolInfo
}

func (t *modelToolSearchTool) Info(_ context.Context) (*schema.ToolInfo, error) {
	return getToolSearchToolInfo(), nil
}

func (t *modelToolSearchTool) InvokableRun(_ context.Context, argumentsInJSON *schema.ToolArgument, _ ...tool.Option) (*schema.ToolResult, error) {
	ret, err := search(argumentsInJSON.Text, t.tools)
	if err != nil {
		return nil, err
	}

	return &schema.ToolResult{Parts: []schema.ToolOutputPart{
		{
			Type: schema.ToolPartTypeToolSearchResult,
			ToolSearchResult: &schema.ToolSearchResult{
				Tools: ret,
			},
		},
	}}, nil
}

const (
	toolSearchToolName = "tool_search"
	defaultMaxResults  = 5
)

func getToolSearchToolInfo() *schema.ToolInfo {
	return &schema.ToolInfo{
		Name: toolSearchToolName,
		Desc: internal.SelectPrompt(internal.I18nPrompts{
			English: toolDescription,
			Chinese: toolDescriptionChinese,
		}),
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"query": {
				Type:     schema.String,
				Desc:     "Query to find deferred tools. Use \"select:<tool_name>\" for direct selection, or keywords to search.",
				Required: true,
			},
			"max_results": {
				Type:     schema.Integer,
				Desc:     "Maximum number of results to return (default: 5)",
				Required: false,
			},
		}),
	}
}

func search(argumentsInJSON string, tools map[string]*schema.ToolInfo) ([]*schema.ToolInfo, error) {
	var args toolSearchArgs
	if err := json.Unmarshal([]byte(argumentsInJSON), &args); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tool search arguments: %w", err)
	}

	query := strings.TrimSpace(args.Query)
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	maxResults := defaultMaxResults
	if args.MaxResults != nil && *args.MaxResults > 0 {
		maxResults = *args.MaxResults
	}

	var matches []string

	// Direct selection mode: select:tool1,tool2
	// max_results is intentionally not applied here because the model has
	// already specified the exact tools it wants by name.
	//
	// 直接选择模式：select:tool1,tool2
	// 这里有意不应用 max_results，因为模型已经按名称明确指定了它想要的工具。
	if strings.HasPrefix(query, "select:") {
		names := strings.Split(strings.TrimPrefix(query, "select:"), ",")
		toolSet := make(map[string]bool, len(tools))
		for name := range tools {
			toolSet[name] = true
		}
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name != "" && toolSet[name] {
				matches = append(matches, name)
			}
		}
	} else {
		matches = keywordSearch(query, maxResults, tools)
	}

	ret := make([]*schema.ToolInfo, 0, len(matches))
	for _, name := range matches {
		ti, ok := tools[name]
		if !ok {
			continue
		}
		ret = append(ret, ti)
	}
	return ret, nil
}

func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// scoredTool pairs a tool name with its search score.
// scoredTool 将工具名称与其搜索分数组合在一起。
type scoredTool struct {
	name  string
	score int
}

// keywordSearch scores all tools against the query keywords and returns the top N.
// keywordSearch 根据查询关键词为所有工具打分，并返回前 N 个结果。
func keywordSearch(query string, maxResults int, tools map[string]*schema.ToolInfo) []string {
	keywords := parseKeywords(query)
	if len(keywords) == 0 {
		return nil
	}

	var scored []scoredTool

	for name, tm := range tools {
		nameParts := splitToolName(name)
		nameLower := strings.ToLower(name)
		descLower := strings.ToLower(tm.Desc)

		totalScore := 0
		allRequiredFound := true

		for _, kw := range keywords {
			kwLower := strings.ToLower(kw.word)
			kwScore := 0

			// Score against name parts
			// 按名称片段打分
			for _, part := range nameParts {
				partLower := strings.ToLower(part)
				if partLower == kwLower {
					kwScore = intMax(kwScore, 10)
				} else if strings.Contains(partLower, kwLower) {
					kwScore = intMax(kwScore, 5)
				}
			}

			// Score against full name
			// 按完整名称打分
			if strings.Contains(nameLower, kwLower) {
				kwScore = intMax(kwScore, 3)
			}

			// Score against description (substring match)
			// 按描述打分（子串匹配）
			if descLower != "" && strings.Contains(descLower, kwLower) {
				kwScore = intMax(kwScore, 2)
			}

			if kw.required && kwScore == 0 {
				allRequiredFound = false
				break
			}

			totalScore += kwScore
		}

		if !allRequiredFound {
			continue
		}

		if totalScore > 0 {
			scored = append(scored, scoredTool{name: name, score: totalScore})
		}
	}

	// Sort by score descending, then by name for stability
	// 按分数降序排序，再按名称排序以保持稳定性
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].name < scored[j].name
	})

	results := make([]string, 0, intMin(maxResults, len(scored)))
	for i := 0; i < len(scored) && i < maxResults; i++ {
		results = append(results, scored[i].name)
	}
	return results
}

// keyword represents a parsed search keyword.
// keyword 表示解析后的搜索关键词。
type keyword struct {
	word     string
	required bool
}

// parseKeywords splits a query string into keywords, handling the '+' required prefix.
// parseKeywords 将查询字符串拆分为关键词，并处理带 '+' 的必需前缀。
func parseKeywords(query string) (keywords []keyword) {
	parts := strings.Fields(query)
	for _, p := range parts {
		if strings.HasPrefix(p, "+") {
			word := strings.TrimPrefix(p, "+")
			if word != "" {
				keywords = append(keywords, keyword{word: word, required: true})
			}
		} else if p != "" {
			keywords = append(keywords, keyword{word: p, required: false})
		}
	}
	return
}

// splitToolName splits a tool name into parts by underscores, double underscores (MCP separator),
// and camelCase boundaries.
//
// splitToolName 按下划线、双下划线（MCP 分隔符）和 camelCase 边界拆分工具名称。
func splitToolName(name string) []string {
	// First split by double underscore (MCP server__tool separator)
	// 先按双下划线（MCP server__tool 分隔符）拆分
	segments := strings.Split(name, "__")

	var parts []string
	for _, seg := range segments {
		// Split each segment by single underscore
		// 将每个片段按单下划线拆分
		underscoreParts := strings.Split(seg, "_")
		for _, up := range underscoreParts {
			if up == "" {
				continue
			}
			// Further split by camelCase
			// 再按 camelCase 拆分
			camelParts := splitCamelCase(up)
			parts = append(parts, camelParts...)
		}
	}
	return parts
}

// splitCamelCase splits a camelCase or PascalCase string into its constituent words.
// splitCamelCase 将 camelCase 或 PascalCase 字符串拆分为组成单词。
func splitCamelCase(s string) []string {
	if s == "" {
		return nil
	}

	var parts []string
	runes := []rune(s)
	start := 0

	for i := 1; i < len(runes); i++ {
		if unicode.IsUpper(runes[i]) {
			if unicode.IsLower(runes[i-1]) {
				parts = append(parts, string(runes[start:i]))
				start = i
			} else if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
				parts = append(parts, string(runes[start:i]))
				start = i
			}
		}
	}
	parts = append(parts, string(runes[start:]))

	return parts
}
