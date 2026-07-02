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
	"fmt"

	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/components/indexer"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/components/retriever"
)

// NewParallel creates a new parallel type.
// it is useful when you want to run multiple nodes in parallel in a chain.
//
// NewParallel 创建一个新的 parallel 类型。
// 当你想在 Chain 中并行运行多个节点时很有用。
func NewParallel() *Parallel {
	return &Parallel{
		outputKeys: make(map[string]bool),
	}
}

// Parallel run multiple nodes in parallel
//
// use `NewParallel()` to create a new parallel type
// Example:
//
//	parallel := NewParallel()
//	parallel.AddChatModel("output_key01", chat01)
//	parallel.AddChatModel("output_key01", chat02)
//
//	chain := NewChain[any,any]()
//	chain.AppendParallel(parallel)
//
// Parallel 并行运行多个节点
// 使用 `NewParallel()` 创建新的 parallel 类型
// 示例：
// parallel := NewParallel()
// parallel.AddChatModel("output_key01", chat01)
// parallel.AddChatModel("output_key01", chat02)
// chain := NewChain[any,any]()
// chain.AppendParallel(parallel)
type Parallel struct {
	nodes      []nodeOptionsPair
	outputKeys map[string]bool
	err        error
}

// AddChatModel adds a chat model to the parallel.
// eg.
//
//	chatModel01, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
//		Model: "gpt-4o",
//	})
//
//	chatModel02, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
//		Model: "gpt-4o",
//	})
//
//	p.AddChatModel("output_key01", chatModel01)
//	p.AddChatModel("output_key02", chatModel02)
//
// AddChatModel 向 parallel 添加一个 chat model。
// 例如：
// chatModel01, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
// Model: "gpt-4o",
// })
// chatModel02, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
// Model: "gpt-4o",
// })
// p.AddChatModel("output_key01", chatModel01)
// p.AddChatModel("output_key02", chatModel02)
func (p *Parallel) AddChatModel(outputKey string, node model.BaseChatModel, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toChatModelNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddAgenticModel adds a agentic.Model to the parallel.
// eg.
//
//	model1, err := openai.NewAgenticModel(ctx, &openai.AgenticModelConfig{
//		Model: "gpt-4o",
//	})
//
//	model2, err := openai.NewAgenticModel(ctx, &openai.AgenticModelConfig{
//		Model: "gpt-4o",
//	})
//
//	p.AddAgenticModel("output_key1", model1)
//	p.AddAgenticModel("output_key2", model2)
//
// AddAgenticModel 向 parallel 添加一个 agentic.Model。
// 例如：
// model1, err := openai.NewAgenticModel(ctx, &openai.AgenticModelConfig{
// Model: "gpt-4o",
// })
// model2, err := openai.NewAgenticModel(ctx, &openai.AgenticModelConfig{
// Model: "gpt-4o",
// })
// p.AddAgenticModel("output_key1", model1)
// p.AddAgenticModel("output_key2", model2)
func (p *Parallel) AddAgenticModel(outputKey string, node model.AgenticModel, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toAgenticModelNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddChatTemplate adds a chat template to the parallel.
// eg.
//
//	chatTemplate01, err := prompt.FromMessages(schema.FString, &schema.Message{
//		Role:    schema.System,
//		Content: "You are acting as a {role}.",
//	})
//
//	p.AddChatTemplate("output_key01", chatTemplate01)
//
// AddChatTemplate 向 parallel 添加一个聊天模板。
// 例如：
// chatTemplate01, err := prompt.FromMessages(schema.FString, &schema.Message{
// Role:    schema.System,
// Content: "You are acting as a {role}.",
// })
// p.AddChatTemplate("output_key01", chatTemplate01)
func (p *Parallel) AddChatTemplate(outputKey string, node prompt.ChatTemplate, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toChatTemplateNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddAgenticChatTemplate adds a prompt.AgenticChatTemplate to the parallel.
// eg.
//
//	chatTemplate01, err := prompt.FromAgenticMessages(schema.FString, &schema.AgenticMessage{})
//
//	p.AddAgenticChatTemplate("output_key01", chatTemplate01)
//
// AddAgenticChatTemplate 向 parallel 添加一个 prompt.AgenticChatTemplate。
// 例如：
// chatTemplate01, err := prompt.FromAgenticMessages(schema.FString, &schema.AgenticMessage{})
// p.AddAgenticChatTemplate("output_key01", chatTemplate01)
func (p *Parallel) AddAgenticChatTemplate(outputKey string, node prompt.AgenticChatTemplate, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toAgenticChatTemplateNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddToolsNode adds a tools node to the parallel.
// eg.
//
//	toolsNode, err := compose.NewToolNode(ctx, &compose.ToolsNodeConfig{
//		Tools: []tool.BaseTool{...},
//	})
//
//	p.AddToolsNode("output_key01", toolsNode)
//
// AddToolsNode 向 parallel 添加一个工具节点。
// 例如：
// toolsNode, err := compose.NewToolNode(ctx, &compose.ToolsNodeConfig{
// Tools: []tool.BaseTool{...},
// })
// p.AddToolsNode("output_key01", toolsNode)
func (p *Parallel) AddToolsNode(outputKey string, node *ToolsNode, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toToolsNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddAgenticToolsNode adds a tools node to the parallel.
// eg.
//
//	toolsNode, err := compose.NewAgenticToolsNode(ctx, &compose.ToolsNodeConfig{
//		Tools: []tool.BaseTool{...},
//	})
//
//	p.AddAgenticToolsNode("output_key01", toolsNode)
//
// AddAgenticToolsNode 向 parallel 添加一个工具节点。
// 例如：
// toolsNode, err := compose.NewAgenticToolsNode(ctx, &compose.ToolsNodeConfig{
// Tools: []tool.BaseTool{...},
// })
// p.AddAgenticToolsNode("output_key01", toolsNode)
func (p *Parallel) AddAgenticToolsNode(outputKey string, node *AgenticToolsNode, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toAgenticToolsNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddLambda adds a lambda node to the parallel.
// eg.
//
//	lambdaFunc := func(ctx context.Context, input *schema.Message) ([]*schema.Message, error) {
//		return []*schema.Message{input}, nil
//	}
//
//	p.AddLambda("output_key01", compose.InvokeLambda(lambdaFunc))
//
// AddLambda 向 parallel 添加一个 lambda 节点。
// 例如：
// lambdaFunc := func(ctx context.Context, input *schema.Message) ([]*schema.Message, error) {
// return []*schema.Message{input}, nil
// }
// p.AddLambda("output_key01", compose.InvokeLambda(lambdaFunc))
func (p *Parallel) AddLambda(outputKey string, node *Lambda, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toLambdaNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddEmbedding adds an embedding node to the parallel.
// eg.
//
//	embeddingNode, err := openai.NewEmbedder(ctx, &openai.EmbeddingConfig{
//		Model: "text-embedding-3-small",
//	})
//
//	p.AddEmbedding("output_key01", embeddingNode)
//
// AddEmbedding 向 parallel 添加一个嵌入节点。
// 例如：
// embeddingNode, err := openai.NewEmbedder(ctx, &openai.EmbeddingConfig{
// Model: "text-embedding-3-small",
// })
// p.AddEmbedding("output_key01", embeddingNode)
func (p *Parallel) AddEmbedding(outputKey string, node embedding.Embedder, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toEmbeddingNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddRetriever adds a retriever node to the parallel.
// eg.
//
// retriever, err := vikingdb.NewRetriever(ctx, &vikingdb.RetrieverConfig{})
//
//	p.AddRetriever("output_key01", retriever)
//
// AddRetriever 向 parallel 添加一个检索器节点。
// 例如：
// retriever, err := vikingdb.NewRetriever(ctx, &vikingdb.RetrieverConfig{})
// p.AddRetriever("output_key01", retriever)
func (p *Parallel) AddRetriever(outputKey string, node retriever.Retriever, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toRetrieverNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddLoader adds a loader node to the parallel.
// eg.
//
//	loader, err := file.NewLoader(ctx, &file.LoaderConfig{})
//
//	p.AddLoader("output_key01", loader)
//
// AddLoader 向 parallel 添加一个 loader 节点。
// 例如：
// loader, err := file.NewLoader(ctx, &file.LoaderConfig{})
// p.AddLoader("output_key01", loader)
func (p *Parallel) AddLoader(outputKey string, node document.Loader, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toLoaderNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddIndexer adds an indexer node to the parallel.
// eg.
//
//	indexer, err := volc_vikingdb.NewIndexer(ctx, &volc_vikingdb.IndexerConfig{
//		Collection: "my_collection",
//	})
//
//	p.AddIndexer("output_key01", indexer)
//
// AddIndexer 向 parallel 添加一个索引器节点。
// 例如：
// indexer, err := volc_vikingdb.NewIndexer(ctx, &volc_vikingdb.IndexerConfig{
// Collection: "my_collection",
// })
// p.AddIndexer("output_key01", indexer)
func (p *Parallel) AddIndexer(outputKey string, node indexer.Indexer, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toIndexerNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddDocumentTransformer adds an Document Transformer node to the parallel.
// eg.
//
//	markdownSplitter, err := markdown.NewHeaderSplitter(ctx, &markdown.HeaderSplitterConfig{})
//
//	p.AddDocumentTransformer("output_key01", markdownSplitter)
//
// AddDocumentTransformer 向 parallel 添加一个 Document Transformer 节点。
// 例如：
// markdownSplitter, err := markdown.NewHeaderSplitter(ctx, &markdown.HeaderSplitterConfig{})
// p.AddDocumentTransformer("output_key01", markdownSplitter)
func (p *Parallel) AddDocumentTransformer(outputKey string, node document.Transformer, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toDocumentTransformerNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddGraph adds a graph node to the parallel.
// It is useful when you want to use a graph or a chain as a node in the parallel.
// eg.
//
//	graph, err := compose.NewChain[any,any]()
//
//	p.AddGraph("output_key01", graph)
//
// AddGraph 向 parallel 添加一个图节点。
// 当你想在 parallel 中把图或链作为节点使用时很有用。
// 例如：
// graph, err := compose.NewChain[any,any]()
// p.AddGraph("output_key01", graph)
func (p *Parallel) AddGraph(outputKey string, node AnyGraph, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toAnyGraphNode(node, append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

// AddPassthrough adds a passthrough node to the parallel.
// eg.
//
//	p.AddPassthrough("output_key01")
//
// AddPassthrough 向 parallel 添加一个 passthrough 节点。
// 例如：
// p.AddPassthrough("output_key01")
func (p *Parallel) AddPassthrough(outputKey string, opts ...GraphAddNodeOpt) *Parallel {
	gNode, options := toPassthroughNode(append(opts, WithOutputKey(outputKey))...)
	return p.addNode(outputKey, gNode, options)
}

func (p *Parallel) addNode(outputKey string, node *graphNode, options *graphAddNodeOpts) *Parallel {
	if p.err != nil {
		return p
	}

	if node == nil {
		p.err = fmt.Errorf("chain parallel add node invalid, node is nil")
		return p
	}

	if p.outputKeys == nil {
		p.outputKeys = make(map[string]bool)
	}

	if _, ok := p.outputKeys[outputKey]; ok {
		p.err = fmt.Errorf("parallel add node err, duplicate output key= %s", outputKey)
		return p
	}

	if node.nodeInfo == nil {
		p.err = fmt.Errorf("chain parallel add node invalid, nodeInfo is nil")
		return p
	}

	node.nodeInfo.outputKey = outputKey
	p.nodes = append(p.nodes, nodeOptionsPair{node, options})
	p.outputKeys[outputKey] = true
	return p
}
