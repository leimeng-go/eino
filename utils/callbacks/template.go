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

// Package callbacks provides ready-to-use callback handler templates for components.
// Package callbacks 提供可直接使用的组件回调处理器模板。
package callbacks

import (
	"context"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/components/indexer"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// NewHandlerHelper creates a new component template handler builder.
// This builder can be used to configure and build a component template handler,
// which can handle callback events for different components with its own struct definition,
// and fallbackTemplate can be used to handle scenarios where none of the cases are hit as a fallback.
//
// NewHandlerHelper 创建一个新的组件模板处理器构建器。
// 该构建器可用于配置并构建组件模板处理器，
// 它能通过自身的结构体定义处理不同组件的回调事件，
// fallbackTemplate 可作为兜底，用于处理未命中任何 case 的场景。
func NewHandlerHelper() *HandlerHelper {
	return &HandlerHelper{
		composeTemplates: map[components.Component]callbacks.Handler{},
	}
}

// HandlerHelper is a builder for creating a callbacks.Handler with specific handlers for different component types.
// create a handler with callbacks.NewHandlerHelper().
// eg.
//
//	helper := template.NewHandlerHelper().
//		ChatModel(&model.IndexerCallbackHandler{}).
//		Prompt(&prompt.IndexerCallbackHandler{}).
//		Handler()
//
// then use the handler with runnable.Invoke(ctx, input, compose.WithCallbacks(handler))
//
// HandlerHelper 是用于创建 callbacks.Handler 的构建器，可为不同组件类型指定处理器。
// 使用 callbacks.NewHandlerHelper() 创建处理器。
// 例如：
// helper := template.NewHandlerHelper().
// ChatModel(&model.IndexerCallbackHandler{}).
// Prompt(&prompt.IndexerCallbackHandler{}).
// Handler()
// 然后通过 runnable.Invoke(ctx, input, compose.WithCallbacks(handler)) 使用该处理器
type HandlerHelper struct {
	promptHandler           *PromptCallbackHandler
	chatModelHandler        *ModelCallbackHandler
	embeddingHandler        *EmbeddingCallbackHandler
	indexerHandler          *IndexerCallbackHandler
	retrieverHandler        *RetrieverCallbackHandler
	loaderHandler           *LoaderCallbackHandler
	transformerHandler      *TransformerCallbackHandler
	toolHandler             *ToolCallbackHandler
	toolsNodeHandler        *ToolsNodeCallbackHandlers
	agentHandler            *AgentCallbackHandler
	agenticAgentHandler     *AgenticAgentCallbackHandler
	agenticPromptHandler    *AgenticPromptCallbackHandler
	agenticModelHandler     *AgenticModelCallbackHandler
	agenticToolsNodeHandler *AgenticToolsNodeCallbackHandlers
	composeTemplates        map[components.Component]callbacks.Handler
}

// Handler returns the callbacks.Handler created by HandlerHelper.
// Handler 返回由 HandlerHelper 创建的 callbacks.Handler。
func (c *HandlerHelper) Handler() callbacks.Handler {
	return &handlerTemplate{c}
}

// Prompt sets the prompt handler for the handler helper, which will be called when the prompt component is executed.
// Prompt 为 handler helper 设置提示处理器，在执行提示组件时会调用它。
func (c *HandlerHelper) Prompt(handler *PromptCallbackHandler) *HandlerHelper {
	c.promptHandler = handler
	return c
}

// ChatModel sets the chat model handler for the handler helper, which will be called when the chat model component is executed.
// ChatModel 为 handler helper 设置聊天模型处理器，在执行聊天模型组件时会调用它。
func (c *HandlerHelper) ChatModel(handler *ModelCallbackHandler) *HandlerHelper {
	c.chatModelHandler = handler
	return c
}

// Embedding sets the embedding handler for the handler helper, which will be called when the embedding component is executed.
// Embedding 为 handler helper 设置嵌入处理器，在执行嵌入组件时会调用它。
func (c *HandlerHelper) Embedding(handler *EmbeddingCallbackHandler) *HandlerHelper {
	c.embeddingHandler = handler
	return c
}

// Indexer sets the indexer handler for the handler helper, which will be called when the indexer component is executed.
// Indexer 为 handler helper 设置索引器处理器，在执行索引器组件时会调用它。
func (c *HandlerHelper) Indexer(handler *IndexerCallbackHandler) *HandlerHelper {
	c.indexerHandler = handler
	return c
}

// Retriever sets the retriever handler for the handler helper, which will be called when the retriever component is executed.
// Retriever 为 handler helper 设置检索器处理器，在执行检索器组件时会调用它。
func (c *HandlerHelper) Retriever(handler *RetrieverCallbackHandler) *HandlerHelper {
	c.retrieverHandler = handler
	return c
}

// Loader sets the loader handler for the handler helper, which will be called when the loader component is executed.
// Loader 为 handler helper 设置加载器处理器，在执行加载器组件时会调用它。
func (c *HandlerHelper) Loader(handler *LoaderCallbackHandler) *HandlerHelper {
	c.loaderHandler = handler
	return c
}

// Transformer sets the transformer handler for the handler helper, which will be called when the transformer component is executed.
// Transformer 为 handler helper 设置转换器处理器，在执行转换器组件时会调用它。
func (c *HandlerHelper) Transformer(handler *TransformerCallbackHandler) *HandlerHelper {
	c.transformerHandler = handler
	return c
}

// Tool sets the tool handler for the handler helper, which will be called when the tool component is executed.
// Tool 为 handler helper 设置工具处理器，在执行工具组件时会调用它。
func (c *HandlerHelper) Tool(handler *ToolCallbackHandler) *HandlerHelper {
	c.toolHandler = handler
	return c
}

// ToolsNode sets the tools node handler for the handler helper, which will be called when the tools node is executed.
// ToolsNode 为 handler helper 设置工具节点处理器，在执行工具节点时会调用它。
func (c *HandlerHelper) ToolsNode(handler *ToolsNodeCallbackHandlers) *HandlerHelper {
	c.toolsNodeHandler = handler
	return c
}

// AgenticPrompt sets the agentic prompt handler for the handler helper, which will be called when the agentic prompt component is executed.
// AgenticPrompt 为 handler helper 设置 agentic 提示处理器，在执行 agentic 提示组件时会调用它。
func (c *HandlerHelper) AgenticPrompt(handler *AgenticPromptCallbackHandler) *HandlerHelper {
	c.agenticPromptHandler = handler
	return c
}

// AgenticModel sets the agentic chat model handler for the handler helper, which will be called when the agentic chat model component is executed.
// AgenticModel 为 handler helper 设置 agentic 聊天模型处理器，在执行 agentic 聊天模型组件时会调用它。
func (c *HandlerHelper) AgenticModel(handler *AgenticModelCallbackHandler) *HandlerHelper {
	c.agenticModelHandler = handler
	return c
}

// AgenticToolsNode sets the agentic tools node handler for the handler helper, which will be called when the agentic tools node is executed.
// AgenticToolsNode 为 handler helper 设置 agentic 工具节点处理器，在执行 agentic 工具节点时会调用它。
func (c *HandlerHelper) AgenticToolsNode(handler *AgenticToolsNodeCallbackHandlers) *HandlerHelper {
	c.agenticToolsNodeHandler = handler
	return c
}

// Agent sets the agent handler for the handler helper, which will be called when the agent is executed.
// Agent 为 handler helper 设置智能体处理器，在执行智能体时会调用它。
func (c *HandlerHelper) Agent(handler *AgentCallbackHandler) *HandlerHelper {
	c.agentHandler = handler
	return c
}

// AgenticAgent sets the agentic agent callback handler for the handler helper, which will be called when an agentic agent is executed.
// AgenticAgent 为 handler helper 设置 agentic 智能体回调处理器，在执行 agentic 智能体时会调用它。
func (c *HandlerHelper) AgenticAgent(handler *AgenticAgentCallbackHandler) *HandlerHelper {
	c.agenticAgentHandler = handler
	return c
}

// Graph sets the graph handler for the handler helper, which will be called when the graph is executed.
// Graph 为 handler helper 设置图处理器，在执行图时会调用它。
func (c *HandlerHelper) Graph(handler callbacks.Handler) *HandlerHelper {
	c.composeTemplates[compose.ComponentOfGraph] = handler
	return c
}

// Chain sets the chain handler for the handler helper, which will be called when the chain is executed.
// Chain 为 handler helper 设置链处理器，在执行链时会调用它。
func (c *HandlerHelper) Chain(handler callbacks.Handler) *HandlerHelper {
	c.composeTemplates[compose.ComponentOfChain] = handler
	return c
}

// Lambda sets the lambda handler for the handler helper, which will be called when the lambda is executed.
// Lambda 设置 handler helper 的 lambda 处理器，该处理器会在 lambda 执行时被调用。
func (c *HandlerHelper) Lambda(handler callbacks.Handler) *HandlerHelper {
	c.composeTemplates[compose.ComponentOfLambda] = handler
	return c
}

type handlerTemplate struct {
	*HandlerHelper
}

// OnStart is the callback function for the start event of a component.
// implement the callbacks Handler interface.
//
// OnStart 是组件 start 事件的回调函数。
// 实现 callbacks Handler 接口。
func (c *handlerTemplate) OnStart(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
	switch info.Component {
	case components.ComponentOfPrompt:
		return c.promptHandler.OnStart(ctx, info, prompt.ConvCallbackInput(input))
	case components.ComponentOfAgenticPrompt:
		return c.agenticPromptHandler.OnStart(ctx, info, prompt.ConvCallbackInput(input))
	case components.ComponentOfChatModel:
		return c.chatModelHandler.OnStart(ctx, info, model.ConvCallbackInput(input))
	case components.ComponentOfAgenticModel:
		return c.agenticModelHandler.OnStart(ctx, info, model.ConvAgenticCallbackInput(input))
	case components.ComponentOfEmbedding:
		return c.embeddingHandler.OnStart(ctx, info, embedding.ConvCallbackInput(input))
	case components.ComponentOfIndexer:
		return c.indexerHandler.OnStart(ctx, info, indexer.ConvCallbackInput(input))
	case components.ComponentOfRetriever:
		return c.retrieverHandler.OnStart(ctx, info, retriever.ConvCallbackInput(input))
	case components.ComponentOfLoader:
		return c.loaderHandler.OnStart(ctx, info, document.ConvLoaderCallbackInput(input))
	case components.ComponentOfTransformer:
		return c.transformerHandler.OnStart(ctx, info, document.ConvTransformerCallbackInput(input))
	case components.ComponentOfTool:
		return c.toolHandler.OnStart(ctx, info, tool.ConvCallbackInput(input))
	case compose.ComponentOfToolsNode:
		return c.toolsNodeHandler.OnStart(ctx, info, convToolsNodeCallbackInput(input))
	case compose.ComponentOfAgenticToolsNode:
		return c.agenticToolsNodeHandler.OnStart(ctx, info, convAgenticToolsNodeCallbackInput(input))
	case adk.ComponentOfAgent:
		return c.agentHandler.OnStart(ctx, info, adk.ConvAgentCallbackInput(input))
	case adk.ComponentOfAgenticAgent:
		return c.agenticAgentHandler.OnStart(ctx, info, adk.ConvTypedCallbackInput[*schema.AgenticMessage](input))
	case compose.ComponentOfGraph,
		compose.ComponentOfChain,
		compose.ComponentOfLambda:
		return c.composeTemplates[info.Component].OnStart(ctx, info, input)
	default:
		return ctx
	}
}

// OnEnd is the callback function for the end event of a component.
// implement the callbacks Handler interface.
//
// OnEnd 是组件 end 事件的回调函数。
// 实现 callbacks Handler 接口。
func (c *handlerTemplate) OnEnd(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
	switch info.Component {
	case components.ComponentOfPrompt:
		return c.promptHandler.OnEnd(ctx, info, prompt.ConvCallbackOutput(output))
	case components.ComponentOfAgenticPrompt:
		return c.agenticPromptHandler.OnEnd(ctx, info, prompt.ConvCallbackOutput(output))
	case components.ComponentOfChatModel:
		return c.chatModelHandler.OnEnd(ctx, info, model.ConvCallbackOutput(output))
	case components.ComponentOfAgenticModel:
		return c.agenticModelHandler.OnEnd(ctx, info, model.ConvAgenticCallbackOutput(output))
	case components.ComponentOfEmbedding:
		return c.embeddingHandler.OnEnd(ctx, info, embedding.ConvCallbackOutput(output))
	case components.ComponentOfIndexer:
		return c.indexerHandler.OnEnd(ctx, info, indexer.ConvCallbackOutput(output))
	case components.ComponentOfRetriever:
		return c.retrieverHandler.OnEnd(ctx, info, retriever.ConvCallbackOutput(output))
	case components.ComponentOfLoader:
		return c.loaderHandler.OnEnd(ctx, info, document.ConvLoaderCallbackOutput(output))
	case components.ComponentOfTransformer:
		return c.transformerHandler.OnEnd(ctx, info, document.ConvTransformerCallbackOutput(output))
	case components.ComponentOfTool:
		return c.toolHandler.OnEnd(ctx, info, tool.ConvCallbackOutput(output))
	case compose.ComponentOfToolsNode:
		return c.toolsNodeHandler.OnEnd(ctx, info, convToolsNodeCallbackOutput(output))
	case compose.ComponentOfAgenticToolsNode:
		return c.agenticToolsNodeHandler.OnEnd(ctx, info, convAgenticToolsNodeCallbackOutput(output))
	case adk.ComponentOfAgent:
		return c.agentHandler.OnEnd(ctx, info, adk.ConvAgentCallbackOutput(output))
	case adk.ComponentOfAgenticAgent:
		return c.agenticAgentHandler.OnEnd(ctx, info, adk.ConvTypedCallbackOutput[*schema.AgenticMessage](output))
	case compose.ComponentOfGraph,
		compose.ComponentOfChain,
		compose.ComponentOfLambda:
		return c.composeTemplates[info.Component].OnEnd(ctx, info, output)
	default:
		return ctx
	}
}

// OnError is the callback function for the error event of a component.
// implement the callbacks Handler interface.
//
// OnError 是组件 error 事件的回调函数。
// 实现 callbacks Handler 接口。
func (c *handlerTemplate) OnError(ctx context.Context, info *callbacks.RunInfo, err error) context.Context {
	switch info.Component {
	case components.ComponentOfPrompt:
		return c.promptHandler.OnError(ctx, info, err)
	case components.ComponentOfAgenticPrompt:
		return c.agenticPromptHandler.OnError(ctx, info, err)
	case components.ComponentOfChatModel:
		return c.chatModelHandler.OnError(ctx, info, err)
	case components.ComponentOfAgenticModel:
		return c.agenticModelHandler.OnError(ctx, info, err)
	case components.ComponentOfEmbedding:
		return c.embeddingHandler.OnError(ctx, info, err)
	case components.ComponentOfIndexer:
		return c.indexerHandler.OnError(ctx, info, err)
	case components.ComponentOfRetriever:
		return c.retrieverHandler.OnError(ctx, info, err)
	case components.ComponentOfLoader:
		return c.loaderHandler.OnError(ctx, info, err)
	case components.ComponentOfTransformer:
		return c.transformerHandler.OnError(ctx, info, err)
	case components.ComponentOfTool:
		return c.toolHandler.OnError(ctx, info, err)
	case compose.ComponentOfToolsNode:
		return c.toolsNodeHandler.OnError(ctx, info, err)
	case compose.ComponentOfAgenticToolsNode:
		return c.agenticToolsNodeHandler.OnError(ctx, info, err)
	case compose.ComponentOfGraph,
		compose.ComponentOfChain,
		compose.ComponentOfLambda:
		return c.composeTemplates[info.Component].OnError(ctx, info, err)
	default:
		return ctx
	}
}

// OnStartWithStreamInput is the callback function for the start event of a component with stream input.
// implement the callbacks Handler interface.
//
// OnStartWithStreamInput 是带流输入组件的 start 事件回调函数。
// 实现 callbacks Handler 接口。
func (c *handlerTemplate) OnStartWithStreamInput(ctx context.Context, info *callbacks.RunInfo, input *schema.StreamReader[callbacks.CallbackInput]) context.Context {
	switch info.Component {
	// currently no components.Component receive stream as input
	// 当前没有 components.Component 接收流作为输入
	case compose.ComponentOfGraph,
		compose.ComponentOfChain,
		compose.ComponentOfLambda:
		return c.composeTemplates[info.Component].OnStartWithStreamInput(ctx, info, input)
	default:
		return ctx
	}
}

// OnEndWithStreamOutput is the callback function for the end event of a component with stream output.
// implement the callbacks Handler interface.
//
// OnEndWithStreamOutput 是带流输出组件的 end 事件回调函数。
// 实现 callbacks Handler 接口。
func (c *handlerTemplate) OnEndWithStreamOutput(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	switch info.Component {
	case components.ComponentOfChatModel:
		return c.chatModelHandler.OnEndWithStreamOutput(ctx, info,
			schema.StreamReaderWithConvert(output, func(item callbacks.CallbackOutput) (*model.CallbackOutput, error) {
				return model.ConvCallbackOutput(item), nil
			}))
	case components.ComponentOfAgenticModel:
		return c.agenticModelHandler.OnEndWithStreamOutput(ctx, info,
			schema.StreamReaderWithConvert(output, func(item callbacks.CallbackOutput) (*model.AgenticCallbackOutput, error) {
				return model.ConvAgenticCallbackOutput(item), nil
			}))
	case components.ComponentOfTool:
		return c.toolHandler.OnEndWithStreamOutput(ctx, info,
			schema.StreamReaderWithConvert(output, func(item callbacks.CallbackOutput) (*tool.CallbackOutput, error) {
				return tool.ConvCallbackOutput(item), nil
			}))
	case compose.ComponentOfToolsNode:
		return c.toolsNodeHandler.OnEndWithStreamOutput(ctx, info,
			schema.StreamReaderWithConvert(output, func(item callbacks.CallbackOutput) ([]*schema.Message, error) {
				return convToolsNodeCallbackOutput(item), nil
			}))
	case compose.ComponentOfAgenticToolsNode:
		return c.agenticToolsNodeHandler.OnEndWithStreamOutput(ctx, info,
			schema.StreamReaderWithConvert(output, func(item callbacks.CallbackOutput) ([]*schema.AgenticMessage, error) {
				return convAgenticToolsNodeCallbackOutput(item), nil
			}))
	case compose.ComponentOfGraph,
		compose.ComponentOfChain,
		compose.ComponentOfLambda:
		return c.composeTemplates[info.Component].OnEndWithStreamOutput(ctx, info, output)
	default:
		return ctx
	}
}

// Needed checks if the callback handler is needed for the given timing.
//
//nolint:cyclop
func (c *handlerTemplate) Needed(ctx context.Context, info *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	if info == nil {
		return false
	}

	switch info.Component {
	case components.ComponentOfChatModel:
		if c.chatModelHandler != nil && c.chatModelHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfAgenticModel:
		if c.agenticModelHandler != nil && c.agenticModelHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfEmbedding:
		if c.embeddingHandler != nil && c.embeddingHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfIndexer:
		if c.indexerHandler != nil && c.indexerHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfLoader:
		if c.loaderHandler != nil && c.loaderHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfPrompt:
		if c.promptHandler != nil && c.promptHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfAgenticPrompt:
		if c.agenticPromptHandler != nil && c.agenticPromptHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfRetriever:
		if c.retrieverHandler != nil && c.retrieverHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfTool:
		if c.toolHandler != nil && c.toolHandler.Needed(ctx, info, timing) {
			return true
		}
	case components.ComponentOfTransformer:
		if c.transformerHandler != nil && c.transformerHandler.Needed(ctx, info, timing) {
			return true
		}
	case compose.ComponentOfToolsNode:
		if c.toolsNodeHandler != nil && c.toolsNodeHandler.Needed(ctx, info, timing) {
			return true
		}
	case compose.ComponentOfAgenticToolsNode:
		if c.agenticToolsNodeHandler != nil && c.agenticToolsNodeHandler.Needed(ctx, info, timing) {
			return true
		}
	case adk.ComponentOfAgent:
		if c.agentHandler != nil && c.agentHandler.Needed(ctx, info, timing) {
			return true
		}
	case adk.ComponentOfAgenticAgent:
		if c.agenticAgentHandler != nil && c.agenticAgentHandler.Needed(ctx, info, timing) {
			return true
		}
	case compose.ComponentOfGraph,
		compose.ComponentOfChain,
		compose.ComponentOfLambda:
		handler := c.composeTemplates[info.Component]
		if handler != nil {
			checker, ok := handler.(callbacks.TimingChecker)
			if !ok || checker.Needed(ctx, info, timing) {
				return true
			}
		}
	default:
		return false
	}

	return false
}

// LoaderCallbackHandler is the handler for the loader callback.
// LoaderCallbackHandler 是 loader 回调的处理器。
type LoaderCallbackHandler struct {
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *document.LoaderCallbackInput) context.Context
	OnEnd   func(ctx context.Context, runInfo *callbacks.RunInfo, output *document.LoaderCallbackOutput) context.Context
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *LoaderCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// TransformerCallbackHandler is the handler for the transformer callback.
// TransformerCallbackHandler 是 transformer 回调的处理器。
type TransformerCallbackHandler struct {
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *document.TransformerCallbackInput) context.Context
	OnEnd   func(ctx context.Context, runInfo *callbacks.RunInfo, output *document.TransformerCallbackOutput) context.Context
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *TransformerCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// EmbeddingCallbackHandler is the handler for the embedding callback.
// EmbeddingCallbackHandler 是嵌入回调的处理器。
type EmbeddingCallbackHandler struct {
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *embedding.CallbackInput) context.Context
	OnEnd   func(ctx context.Context, runInfo *callbacks.RunInfo, output *embedding.CallbackOutput) context.Context
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *EmbeddingCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// IndexerCallbackHandler is the handler for the indexer callback.
// IndexerCallbackHandler 是索引器回调的处理器。
type IndexerCallbackHandler struct {
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *indexer.CallbackInput) context.Context
	OnEnd   func(ctx context.Context, runInfo *callbacks.RunInfo, output *indexer.CallbackOutput) context.Context
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *IndexerCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// ModelCallbackHandler is the handler for the model callback.
// ModelCallbackHandler 是 model 回调的处理器。
type ModelCallbackHandler struct {
	OnStart               func(ctx context.Context, runInfo *callbacks.RunInfo, input *model.CallbackInput) context.Context
	OnEnd                 func(ctx context.Context, runInfo *callbacks.RunInfo, output *model.CallbackOutput) context.Context
	OnEndWithStreamOutput func(ctx context.Context, runInfo *callbacks.RunInfo, output *schema.StreamReader[*model.CallbackOutput]) context.Context
	OnError               func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *ModelCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	case callbacks.TimingOnEndWithStreamOutput:
		return ch.OnEndWithStreamOutput != nil
	default:
		return false
	}
}

// PromptCallbackHandler is the handler for the callback.
// PromptCallbackHandler 是回调的处理器。
type PromptCallbackHandler struct {
	// OnStart is the callback function for the start of the callback.
	// OnStart 是回调开始时的回调函数。
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *prompt.CallbackInput) context.Context
	// OnEnd is the callback function for the end of the callback.
	// OnEnd 是回调结束时的回调函数。
	OnEnd func(ctx context.Context, runInfo *callbacks.RunInfo, output *prompt.CallbackOutput) context.Context
	// OnError is the callback function for the error of the callback.
	// OnError 是 callback 出错时的回调函数。
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查在给定 timing 下是否需要该回调处理器。
func (ch *PromptCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// RetrieverCallbackHandler is the handler for the retriever callback.
// RetrieverCallbackHandler 是 retriever 回调的处理器。
type RetrieverCallbackHandler struct {
	// OnStart is the callback function for the start of the retriever.
	// OnStart 是 retriever 启动时的回调函数。
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *retriever.CallbackInput) context.Context
	// OnEnd is the callback function for the end of the retriever.
	// OnEnd 是 retriever 结束时的回调函数。
	OnEnd func(ctx context.Context, runInfo *callbacks.RunInfo, output *retriever.CallbackOutput) context.Context
	// OnError is the callback function for the error of the retriever.
	// OnError 是 retriever 出错时的回调函数。
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查在给定 timing 下是否需要该回调处理器。
func (ch *RetrieverCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// ToolCallbackHandler is the handler for the tool callback.
// ToolCallbackHandler 是工具回调的处理器。
type ToolCallbackHandler struct {
	OnStart               func(ctx context.Context, info *callbacks.RunInfo, input *tool.CallbackInput) context.Context
	OnEnd                 func(ctx context.Context, info *callbacks.RunInfo, output *tool.CallbackOutput) context.Context
	OnEndWithStreamOutput func(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[*tool.CallbackOutput]) context.Context
	OnError               func(ctx context.Context, info *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查在给定 timing 下是否需要该回调处理器。
func (ch *ToolCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnEndWithStreamOutput:
		return ch.OnEndWithStreamOutput != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// ToolsNodeCallbackHandlers defines optional callbacks for the Tools node
// lifecycle events.
//
// ToolsNodeCallbackHandlers 定义 Tools 节点生命周期事件的可选回调。
type ToolsNodeCallbackHandlers struct {
	OnStart               func(ctx context.Context, info *callbacks.RunInfo, input *schema.Message) context.Context
	OnEnd                 func(ctx context.Context, info *callbacks.RunInfo, input []*schema.Message) context.Context
	OnEndWithStreamOutput func(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[[]*schema.Message]) context.Context
	OnError               func(ctx context.Context, info *callbacks.RunInfo, err error) context.Context
}

// Needed reports whether a handler is registered for the given timing.
// Needed 报告在给定 timing 下是否注册了处理器。
func (ch *ToolsNodeCallbackHandlers) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnEndWithStreamOutput:
		return ch.OnEndWithStreamOutput != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

func convToolsNodeCallbackInput(src callbacks.CallbackInput) *schema.Message {
	switch t := src.(type) {
	case *schema.Message:
		return t
	default:
		return nil
	}
}

func convToolsNodeCallbackOutput(src callbacks.CallbackInput) []*schema.Message {
	switch t := src.(type) {
	case []*schema.Message:
		return t
	default:
		return nil
	}
}

// AgentCallbackHandler handles callbacks for agents using *schema.Message.
// Use ComponentOfAgent to filter callback events to agent-related events.
//
// AgentCallbackHandler 处理使用 *schema.Message 的智能体回调。
// 使用 ComponentOfAgent 过滤与智能体相关的回调事件。
type AgentCallbackHandler struct {
	// OnStart is called when an agent run begins. Return a modified context to propagate values.
	// OnStart 在智能体运行开始时调用。返回修改后的 context 以传播值。
	OnStart func(ctx context.Context, info *callbacks.RunInfo, input *adk.AgentCallbackInput) context.Context
	// OnEnd is called when an agent run completes. The output's Events iterator should be
	// consumed asynchronously to avoid blocking.
	//
	// OnEnd 在智能体运行完成时调用。output 的 Events 迭代器应异步消费以避免阻塞。
	OnEnd func(ctx context.Context, info *callbacks.RunInfo, output *adk.AgentCallbackOutput) context.Context
}

func (ch *AgentCallbackHandler) Needed(ctx context.Context, info *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	default:
		return false
	}
}

// AgenticAgentCallbackHandler handles callbacks for agentic agents using *schema.AgenticMessage.
// Use ComponentOfAgenticAgent to filter callback events to agentic-agent-related events.
//
// AgenticAgentCallbackHandler 处理使用 *schema.AgenticMessage 的 agentic agents 回调。
// 使用 ComponentOfAgenticAgent 过滤与 agentic-agent 相关的回调事件。
type AgenticAgentCallbackHandler struct {
	// OnStart is called when an agentic agent run begins. Return a modified context to propagate values.
	// OnStart 在 agentic agent 运行开始时调用。返回修改后的 context 以传播值。
	OnStart func(ctx context.Context, info *callbacks.RunInfo, input *adk.TypedAgentCallbackInput[*schema.AgenticMessage]) context.Context
	// OnEnd is called when an agentic agent run completes. The output's Events iterator should be
	// consumed asynchronously to avoid blocking.
	//
	// OnEnd 在 agentic agent 运行完成时调用。output 的 Events 迭代器应异步消费以避免阻塞。
	OnEnd func(ctx context.Context, info *callbacks.RunInfo, output *adk.TypedAgentCallbackOutput[*schema.AgenticMessage]) context.Context
}

func (ch *AgenticAgentCallbackHandler) Needed(ctx context.Context, info *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	default:
		return false
	}
}

// AgenticPromptCallbackHandler is the handler for the agentic prompt callback.
// AgenticPromptCallbackHandler 是 agentic prompt 回调的处理器。
type AgenticPromptCallbackHandler struct {
	// OnStart is the callback function for the start of the agentic prompt.
	// OnStart 是 agentic prompt 启动时的回调函数。
	OnStart func(ctx context.Context, runInfo *callbacks.RunInfo, input *prompt.CallbackInput) context.Context
	// OnEnd is the callback function for the end of the agentic prompt.
	// OnEnd 是 agentic prompt 结束时的回调函数。
	OnEnd func(ctx context.Context, runInfo *callbacks.RunInfo, output *prompt.CallbackOutput) context.Context
	// OnError is the callback function for the error of the agentic prompt.
	// OnError 是 agentic prompt 出错时的回调函数。
	OnError func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *AgenticPromptCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

// AgenticModelCallbackHandler is the handler for the agentic chat model callback.
// AgenticModelCallbackHandler 是 agentic chat model 回调的处理器。
type AgenticModelCallbackHandler struct {
	OnStart               func(ctx context.Context, runInfo *callbacks.RunInfo, input *model.AgenticCallbackInput) context.Context
	OnEnd                 func(ctx context.Context, runInfo *callbacks.RunInfo, output *model.AgenticCallbackOutput) context.Context
	OnEndWithStreamOutput func(ctx context.Context, runInfo *callbacks.RunInfo, output *schema.StreamReader[*model.AgenticCallbackOutput]) context.Context
	OnError               func(ctx context.Context, runInfo *callbacks.RunInfo, err error) context.Context
}

// Needed checks if the callback handler is needed for the given timing.
// Needed 检查给定 timing 是否需要该回调处理器。
func (ch *AgenticModelCallbackHandler) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	case callbacks.TimingOnEndWithStreamOutput:
		return ch.OnEndWithStreamOutput != nil
	default:
		return false
	}
}

// AgenticToolsNodeCallbackHandlers defines optional callbacks for the Agentic Tools node
// lifecycle events.
//
// AgenticToolsNodeCallbackHandlers 定义 Agentic Tools 节点生命周期事件的可选回调。
type AgenticToolsNodeCallbackHandlers struct {
	OnStart               func(ctx context.Context, info *callbacks.RunInfo, input *schema.AgenticMessage) context.Context
	OnEnd                 func(ctx context.Context, info *callbacks.RunInfo, input []*schema.AgenticMessage) context.Context
	OnEndWithStreamOutput func(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[[]*schema.AgenticMessage]) context.Context
	OnError               func(ctx context.Context, info *callbacks.RunInfo, err error) context.Context
}

// Needed reports whether a handler is registered for the given timing.
// Needed 报告给定 timing 是否注册了处理器。
func (ch *AgenticToolsNodeCallbackHandlers) Needed(ctx context.Context, runInfo *callbacks.RunInfo, timing callbacks.CallbackTiming) bool {
	switch timing {
	case callbacks.TimingOnStart:
		return ch.OnStart != nil
	case callbacks.TimingOnEnd:
		return ch.OnEnd != nil
	case callbacks.TimingOnEndWithStreamOutput:
		return ch.OnEndWithStreamOutput != nil
	case callbacks.TimingOnError:
		return ch.OnError != nil
	default:
		return false
	}
}

func convAgenticToolsNodeCallbackInput(src callbacks.CallbackInput) *schema.AgenticMessage {
	switch t := src.(type) {
	case *schema.AgenticMessage:
		return t
	default:
		return nil
	}
}

func convAgenticToolsNodeCallbackOutput(src callbacks.CallbackInput) []*schema.AgenticMessage {
	switch t := src.(type) {
	case []*schema.AgenticMessage:
		return t
	default:
		return nil
	}
}
