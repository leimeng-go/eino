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

// Package components defines common interfaces that describe component
// types and callback capabilities used across Eino.
//
// Package components 定义 Eino 中通用的接口，用于描述组件类型和回调能力。
package components

// Typer provides a human-readable type name for a component implementation.
//
// When implemented, the component's full display name in DevOps tooling
// (visual debugger, IDE plugin, dashboards) becomes "{GetType()}{ComponentKind}"
// — e.g. "OpenAIChatModel". Use CamelCase naming.
//
// Also used by [utils.InferTool] and similar constructors to set the display
// name of tool instances.
//
// Typer 为组件实现提供人类可读的类型名称。
// 实现后，组件在 DevOps 工具（visual debugger、IDE plugin、dashboards）中的完整显示名称会变为 "{GetType()}{ComponentKind}"，例如 "OpenAIChatModel"。请使用 CamelCase 命名。
// 也被 [utils.InferTool] 及类似构造函数用于设置工具实例的显示名称。
type Typer interface {
	GetType() string
}

// GetType returns the type name for a component that implements Typer.
// GetType 返回实现 Typer 的组件的类型名。
func GetType(component any) (string, bool) {
	if typer, ok := component.(Typer); ok {
		return typer.GetType(), true
	}

	return "", false
}

// Checker controls whether the framework's automatic callback instrumentation
// is active for a component.
//
// When IsCallbacksEnabled returns true, the framework skips its default
// OnStart/OnEnd wrapping and trusts the component to invoke callbacks itself
// at the correct points. Implement this when your component needs precise
// control over callback timing or content — for example, when streaming
// requires callbacks to fire mid-stream rather than only at completion.
//
// Checker 控制框架的自动回调插桩是否对组件启用。
// 当 IsCallbacksEnabled 返回 true 时，框架会跳过默认的 OnStart/OnEnd 包装，并信任组件在正确时机自行调用回调。若组件需要精确控制回调时机或内容，请实现它，例如流式场景需要在流中途触发回调，而不是仅在完成时触发。
type Checker interface {
	IsCallbacksEnabled() bool
}

// IsCallbacksEnabled reports whether a component implements Checker and enables callbacks.
// IsCallbacksEnabled 报告组件是否实现 Checker 并启用回调。
func IsCallbacksEnabled(i any) bool {
	if checker, ok := i.(Checker); ok {
		return checker.IsCallbacksEnabled()
	}

	return false
}

// Component names representing the different categories of components.
// 表示不同组件类别的组件名称。
type Component string

const (
	// ComponentOfPrompt identifies chat template components.
	// ComponentOfPrompt 标识聊天模板组件。
	ComponentOfPrompt Component = "ChatTemplate"
	// ComponentOfAgenticPrompt identifies agentic template components.
	// ComponentOfAgenticPrompt 标识 agentic 模板组件。
	ComponentOfAgenticPrompt Component = "AgenticChatTemplate"
	// ComponentOfChatModel identifies chat model components.
	// ComponentOfChatModel 标识聊天模型组件。
	ComponentOfChatModel Component = "ChatModel"
	// ComponentOfAgenticModel identifies agentic model components.
	// ComponentOfAgenticModel 标识 agentic 模型组件。
	ComponentOfAgenticModel Component = "AgenticModel"
	// ComponentOfEmbedding identifies embedding components.
	// ComponentOfEmbedding 标识嵌入组件。
	ComponentOfEmbedding Component = "Embedding"
	// ComponentOfIndexer identifies indexer components.
	// ComponentOfIndexer 标识索引器组件。
	ComponentOfIndexer Component = "Indexer"
	// ComponentOfRetriever identifies retriever components.
	// ComponentOfRetriever 标识检索器组件。
	ComponentOfRetriever Component = "Retriever"
	// ComponentOfLoader identifies loader components.
	// ComponentOfLoader 标识加载器组件。
	ComponentOfLoader Component = "Loader"
	// ComponentOfTransformer identifies document transformer components.
	// ComponentOfTransformer 标识文档转换器组件。
	ComponentOfTransformer Component = "DocumentTransformer"
	// ComponentOfTool identifies tool components.
	// ComponentOfTool 标识工具组件。
	ComponentOfTool Component = "Tool"
)
