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
	"fmt"
	"reflect"
	"time"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/components/indexer"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/components/retriever"
)

type graphCancelChanKey struct{}
type graphCancelChanVal struct {
	ch chan *time.Duration
}

type graphInterruptOptions struct {
	timeout *time.Duration
}

// GraphInterruptOption configures behavior when interrupting a running graph.
// GraphInterruptOption 配置中断运行中 graph 时的行为。
type GraphInterruptOption func(o *graphInterruptOptions)

// WithGraphInterruptTimeout specifies the max waiting time before generating an interrupt.
// After the max waiting time, the graph will force an interrupt. Any unfinished tasks will be re-run when the graph is resumed.
//
// WithGraphInterruptTimeout 指定生成中断前的最大等待时间。
// 超过最大等待时间后，graph 将强制中断。graph 恢复时，所有未完成任务都会重新运行。
func WithGraphInterruptTimeout(timeout time.Duration) GraphInterruptOption {
	return func(o *graphInterruptOptions) {
		o.timeout = &timeout
	}
}

// WithGraphInterrupt creates a context with graph cancellation support.
// When the returned context is used to invoke a graph or workflow, calling the interrupt function will trigger an interrupt.
// The graph will wait for current tasks to complete by default.
//
// Input Persistence: When WithGraphInterrupt is used, ALL nodes (in both root graph and subgraphs) will automatically
// persist their inputs (both streaming and non-streaming) before execution. If the graph is interrupted, these inputs
// are restored when the graph resumes from a checkpoint, ensuring interrupted nodes receive their original inputs.
//
// This behavior differs from internal interrupts triggered via compose.Interrupt() within a node's function body.
// Internal interrupts do NOT automatically persist inputs - the node author must manage input persistence manually,
// either by saving it in the global graph state or using compose.StatefulInterrupt() to store it in local interrupt state.
// WithGraphInterrupt enables automatic input persistence because external interrupts can occur at any point during
// node execution, making it impossible for the node to prepare for the interrupt.
//
// Why input persistence is not enabled by default for internal interrupts: Enabling it universally would break
// existing code that relies on checking "input == nil" to determine whether the node is running for the first time
// or resuming from an interrupt. The recommended approach is to use compose.GetInterruptState() to explicitly
// determine whether the current execution is a first run or a resume.
//
// WithGraphInterrupt 创建一个支持 graph 取消的 context。
// 使用返回的 context 调用 graph 或 workflow 时，调用 interrupt function 将触发中断。
// 默认情况下，graph 会等待当前任务完成。
// 输入持久化：使用 WithGraphInterrupt 时，所有节点（包括 root graph 和 subgraphs 中的节点）都会在执行前自动持久化其输入（流式和非流式）。如果 graph 被中断，这些输入会在 graph 从 checkpoint 恢复时还原，确保被中断的节点收到其原始输入。
// 这不同于在节点函数体内通过 compose.Interrupt() 触发的内部中断。内部中断不会自动持久化输入，节点作者必须手动管理输入持久化，可以将其保存在全局 graph state 中，或使用 compose.StatefulInterrupt() 将其存入本地 interrupt state。WithGraphInterrupt 会启用自动输入持久化，因为外部中断可能在节点执行期间的任意时刻发生，节点无法预先为中断做准备。
// 为什么内部中断默认不启用输入持久化：全局启用会破坏依赖检查 "input == nil" 来判断节点是首次运行还是从中断恢复的现有代码。推荐使用 compose.GetInterruptState() 显式判断当前执行是首次运行还是恢复。
func WithGraphInterrupt(parent context.Context) (ctx context.Context, interrupt func(opts ...GraphInterruptOption)) {
	ch := make(chan *time.Duration, 1)
	ctx = context.WithValue(parent, graphCancelChanKey{}, &graphCancelChanVal{
		ch: ch,
	})
	return ctx, func(opts ...GraphInterruptOption) {
		o := &graphInterruptOptions{}
		for _, opt := range opts {
			opt(o)
		}
		ch <- o.timeout
		close(ch)
	}
}

func getGraphCancel(ctx context.Context) *graphCancelChanVal {
	val, ok := ctx.Value(graphCancelChanKey{}).(*graphCancelChanVal)
	if !ok {
		return nil
	}
	return val
}

// Option is a functional option type for calling a graph.
// Option 是调用 graph 的函数式选项类型。
type Option struct {
	options []any
	handler []callbacks.Handler

	paths []*NodePath

	maxRunSteps         int
	checkPointID        *string
	writeToCheckPointID *string
	forceNewRun         bool
	stateModifier       StateModifier
}

func (o Option) deepCopy() Option {
	nOptions := make([]any, len(o.options))
	copy(nOptions, o.options)
	nHandler := make([]callbacks.Handler, len(o.handler))
	copy(nHandler, o.handler)
	nPaths := make([]*NodePath, len(o.paths))
	for i, path := range o.paths {
		nPath := *path
		nPaths[i] = &nPath
	}
	return Option{
		options:     nOptions,
		handler:     nHandler,
		paths:       nPaths,
		maxRunSteps: o.maxRunSteps,
	}
}

// DesignateNode sets the key of the node to which the option will be applied.
// notice: only effective at the top graph.
// e.g.
//
// embeddingOption := compose.WithEmbeddingOption(embedding.WithModel("text-embedding-3-small"))
// runnable.Invoke(ctx, "input", embeddingOption.DesignateNode("embedding_node_key"))
//
// DesignateNode 设置该选项要应用到的节点 key。
// 注意：仅在顶层 graph 生效。
// 例如：
// embeddingOption := compose.WithEmbeddingOption(embedding.WithModel("text-embedding-3-small"))
// runnable.Invoke(ctx, "input", embeddingOption.DesignateNode("embedding_node_key"))
func (o Option) DesignateNode(nodeKey ...string) Option {
	nKeys := make([]*NodePath, len(nodeKey))
	for i, k := range nodeKey {
		nKeys[i] = NewNodePath(k)
	}
	return o.DesignateNodeWithPath(nKeys...)
}

// DesignateNodeWithPath sets the path of the node(s) to which the option will be applied.
// You can specify a node in the subgraph through `NodePath` to make the option only take effect at this node.
//
// e.g.
// nodePath := NewNodePath("sub_graph_node_key", "node_key_within_sub_graph")
// DesignateNodeWithPath(nodePath)
//
// DesignateNodeWithPath 设置该选项要应用到的节点路径。
// 可以通过 `NodePath` 指定 subgraph 中的某个节点，使该选项仅在此节点生效。
// 例如：
// nodePath := NewNodePath("sub_graph_node_key", "node_key_within_sub_graph")
// DesignateNodeWithPath(nodePath)
func (o Option) DesignateNodeWithPath(path ...*NodePath) Option {
	o.paths = append(o.paths, path...)
	return o
}

// WithEmbeddingOption is a functional option type for embedding component.
// e.g.
//
//	embeddingOption := compose.WithEmbeddingOption(embedding.WithModel("text-embedding-3-small"))
//	runnable.Invoke(ctx, "input", embeddingOption)
//
// WithEmbeddingOption 是 embedding 组件的函数式选项类型。
// 例如：
// embeddingOption := compose.WithEmbeddingOption(embedding.WithModel("text-embedding-3-small"))
// runnable.Invoke(ctx, "input", embeddingOption)
func WithEmbeddingOption(opts ...embedding.Option) Option {
	return withComponentOption(opts...)
}

// WithRetrieverOption is a functional option type for retriever component.
// e.g.
//
//	retrieverOption := compose.WithRetrieverOption(retriever.WithIndex("my_index"))
//	runnable.Invoke(ctx, "input", retrieverOption)
//
// WithRetrieverOption 是 retriever 组件的函数式选项类型。
// 例如：
// retrieverOption := compose.WithRetrieverOption(retriever.WithIndex("my_index"))
// runnable.Invoke(ctx, "input", retrieverOption)
func WithRetrieverOption(opts ...retriever.Option) Option {
	return withComponentOption(opts...)
}

// WithLoaderOption is a functional option type for loader component.
// e.g.
//
//	loaderOption := compose.WithLoaderOption(document.WithCollection("my_collection"))
//	runnable.Invoke(ctx, "input", loaderOption)
//
// WithLoaderOption 是 loader 组件的函数式选项类型。
// 例如：
// loaderOption := compose.WithLoaderOption(document.WithCollection("my_collection"))
// runnable.Invoke(ctx, "input", loaderOption)
func WithLoaderOption(opts ...document.LoaderOption) Option {
	return withComponentOption(opts...)
}

// WithDocumentTransformerOption is a functional option type for document transformer component.
// WithDocumentTransformerOption 是 document transformer 组件的函数式选项类型。
func WithDocumentTransformerOption(opts ...document.TransformerOption) Option {
	return withComponentOption(opts...)
}

// WithIndexerOption is a functional option type for indexer component.
// e.g.
//
//	indexerOption := compose.WithIndexerOption(indexer.WithSubIndexes([]string{"my_sub_index"}))
//	runnable.Invoke(ctx, "input", indexerOption)
//
// WithIndexerOption 是 indexer 组件的函数式选项类型。
// 例如：
// indexerOption := compose.WithIndexerOption(indexer.WithSubIndexes([]string{"my_sub_index"}))
// runnable.Invoke(ctx, "input", indexerOption)
func WithIndexerOption(opts ...indexer.Option) Option {
	return withComponentOption(opts...)
}

// WithChatModelOption is a functional option type for chat model component.
// e.g.
//
//	chatModelOption := compose.WithChatModelOption(model.WithTemperature(0.7))
//	runnable.Invoke(ctx, "input", chatModelOption)
//
// WithChatModelOption 是 chat model 组件的函数式选项类型。
// 例如：
// chatModelOption := compose.WithChatModelOption(model.WithTemperature(0.7))
// runnable.Invoke(ctx, "input", chatModelOption)
func WithChatModelOption(opts ...model.Option) Option {
	return withComponentOption(opts...)
}

// WithChatTemplateOption is a functional option type for chat template component.
// WithChatTemplateOption 是 chat template 组件的函数式选项类型。
func WithChatTemplateOption(opts ...prompt.Option) Option {
	return withComponentOption(opts...)
}

// WithToolsNodeOption is a functional option type for tools node component.
// WithToolsNodeOption 是 tools node 组件的函数式选项类型。
func WithToolsNodeOption(opts ...ToolsNodeOption) Option {
	return withComponentOption(opts...)
}

// WithLambdaOption is a functional option type for lambda component.
// WithLambdaOption 是 lambda 组件的函数式选项类型。
func WithLambdaOption(opts ...any) Option {
	return Option{
		options: opts,
		paths:   make([]*NodePath, 0),
	}
}

// WithCallbacks set callback handlers for all components in a single call.
// e.g.
//
//	runnable.Invoke(ctx, "input", compose.WithCallbacks(&myCallbacks{}))
//
// WithCallbacks 一次性为所有组件设置 callback handlers。
// 例如：
// runnable.Invoke(ctx, "input", compose.WithCallbacks(&myCallbacks{}))
func WithCallbacks(cbs ...callbacks.Handler) Option {
	return Option{
		handler: cbs,
	}
}

// WithRuntimeMaxSteps sets the maximum number of steps for the graph runtime.
// e.g.
//
//	runnable.Invoke(ctx, "input", compose.WithRuntimeMaxSteps(20))
//
// WithRuntimeMaxSteps 设置 graph runtime 的最大步数。
// 例如：
// runnable.Invoke(ctx, "input", compose.WithRuntimeMaxSteps(20))
func WithRuntimeMaxSteps(maxSteps int) Option {
	return Option{
		maxRunSteps: maxSteps,
	}
}

func withComponentOption[TOption any](opts ...TOption) Option {
	o := make([]any, 0, len(opts))
	for i := range opts {
		o = append(o, opts[i])
	}
	return Option{
		options: o,
		paths:   make([]*NodePath, 0),
	}
}

func convertOption[TOption any](opts ...any) ([]TOption, error) {
	if len(opts) == 0 {
		return nil, nil
	}
	ret := make([]TOption, 0, len(opts))
	for i := range opts {
		o, ok := opts[i].(TOption)
		if !ok {
			return nil, fmt.Errorf("unexpected component option type, expected:%s, actual:%s", reflect.TypeOf((*TOption)(nil)).Elem().String(), reflect.TypeOf(opts[i]).String())
		}
		ret = append(ret, o)
	}
	return ret, nil
}
