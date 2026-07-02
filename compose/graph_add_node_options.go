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
	"reflect"

	"github.com/cloudwego/eino/internal/generic"
)

type graphAddNodeOpts struct {
	nodeOptions *nodeOptions
	processor   *processorOpts

	needState bool
}

// GraphAddNodeOpt is a functional option type for adding a node to a graph.
// e.g.
//
//	graph.AddNode("node_name", node, compose.WithInputKey("input_key"), compose.WithOutputKey("output_key"))
//
// GraphAddNodeOpt 是向图添加节点的函数式选项类型。
// 例如
// graph.AddNode("node_name", node, compose.WithInputKey("input_key"), compose.WithOutputKey("output_key"))
type GraphAddNodeOpt func(o *graphAddNodeOpts)

type nodeOptions struct {
	nodeName string

	nodeKey string

	inputKey  string
	outputKey string

	graphCompileOption []GraphCompileOption // when this node is itself an AnyGraph, this option will be used to compile the node as a nested graph
	// 当该节点本身是 AnyGraph 时，此选项用于将该节点编译为嵌套图
}

// WithNodeName sets the name of the node.
// WithNodeName 设置节点名称。
func WithNodeName(n string) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.nodeOptions.nodeName = n
	}
}

// WithNodeKey set the node key, which is used to identify the node in the chain.
// only for use in Chain/StateChain.
//
// WithNodeKey 设置节点 key，用于在链中标识该节点。
// 仅用于 Chain/StateChain。
func WithNodeKey(key string) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.nodeOptions.nodeKey = key
	}
}

// WithInputKey sets the input key of the node.
// this will change the input value of the node, for example, if the pre node's output is map[string]any{"key01": "value01"},
// and the current node's input key is "key01", then the current node's input value will be "value01".
//
// WithInputKey 设置节点的输入 key。
// 这会改变节点的输入值，例如，如果前一个节点的输出是 map[string]any{"key01": "value01"}，
// 且当前节点的输入 key 是 "key01"，则当前节点的输入值将是 "value01"。
func WithInputKey(k string) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.nodeOptions.inputKey = k
	}
}

// WithOutputKey sets the output key of the node.
// this will change the output value of the node, for example, if the current node's output key is "key01",
// then the node's output value will be map[string]any{"key01": value}.
//
// WithOutputKey 设置节点的输出 key。
// 这会改变节点的输出值，例如，如果当前节点的输出 key 是 "key01"，
// 则该节点的输出值将是 map[string]any{"key01": value}。
func WithOutputKey(k string) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.nodeOptions.outputKey = k
	}
}

// WithGraphCompileOptions when the node is an AnyGraph, use this option to set compile option for the node.
// e.g.
//
//	graph.AddNode("node_name", node, compose.WithGraphCompileOptions(compose.WithGraphName("my_sub_graph")))
//
// WithGraphCompileOptions 在节点为 AnyGraph 时，使用此选项设置该节点的编译选项。
// 例如
// graph.AddNode("node_name", node, compose.WithGraphCompileOptions(compose.WithGraphName("my_sub_graph")))
func WithGraphCompileOptions(opts ...GraphCompileOption) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.nodeOptions.graphCompileOption = opts
	}
}

// WithStatePreHandler modify node's input of I according to state S and input or store input information into state, and it's thread-safe.
// notice: this option requires Graph to be created with WithGenLocalState option.
// I: input type of the Node like ChatModel, Lambda, Retriever etc.
// S: state type defined in WithGenLocalState
//
// WithStatePreHandler 根据状态 S 和输入修改节点的 I 输入，或将输入信息存入状态，且线程安全。
// 注意：此选项要求 Graph 使用 WithGenLocalState 选项创建。
// I：节点的输入类型，如 ChatModel、Lambda、Retriever 等。
// S：WithGenLocalState 中定义的状态类型
func WithStatePreHandler[I, S any](pre StatePreHandler[I, S]) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.processor.statePreHandler = convertPreHandler(pre)
		o.processor.preStateType = generic.TypeOf[S]()
		o.needState = true
	}
}

// WithStatePostHandler modify node's output of O according to state S and output or store output information into state, and it's thread-safe.
// notice: this option requires Graph to be created with WithGenLocalState option.
// O: output type of the Node like ChatModel, Lambda, Retriever etc.
// S: state type defined in WithGenLocalState
//
// WithStatePostHandler 根据状态 S 和输出修改节点的 O 输出，或将输出信息存入状态，且线程安全。
// 注意：此选项要求 Graph 使用 WithGenLocalState 选项创建。
// O：节点的输出类型，如 ChatModel、Lambda、Retriever 等。
// S：WithGenLocalState 中定义的状态类型
func WithStatePostHandler[O, S any](post StatePostHandler[O, S]) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.processor.statePostHandler = convertPostHandler(post)
		o.processor.postStateType = generic.TypeOf[S]()
		o.needState = true
	}
}

// WithStreamStatePreHandler modify node's streaming input of I according to state S and input or store input information into state, and it's thread-safe.
// notice: this option requires Graph to be created with WithGenLocalState option.
// when to use: when upstream node's output is an actual stream, and you want the current node's input to remain an actual stream after state pre handler.
// caution: while StreamStatePreHandler is thread safe, modifying state within your own goroutine is NOT.
// I: input type of the Node like ChatModel, Lambda, Retriever etc.
// S: state type defined in WithGenLocalState
//
// WithStreamStatePreHandler 根据状态 S 和输入修改节点的流式 I 输入，或将输入信息存入状态，且线程安全。
// 注意：此选项要求 Graph 使用 WithGenLocalState 选项创建。
// 使用时机：当上游节点的输出是真实流，且你希望经过 state pre handler 后当前节点的输入仍保持真实流。
// 注意：虽然 StreamStatePreHandler 是线程安全的，但在你自己的 goroutine 中修改状态并不是线程安全的。
// I：节点的输入类型，如 ChatModel、Lambda、Retriever 等。
// S：WithGenLocalState 中定义的状态类型
func WithStreamStatePreHandler[I, S any](pre StreamStatePreHandler[I, S]) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.processor.statePreHandler = streamConvertPreHandler(pre)
		o.processor.preStateType = generic.TypeOf[S]()
		o.needState = true
	}
}

// WithStreamStatePostHandler modify node's streaming output of O according to state S and output or store output information into state, and it's thread-safe.
// notice: this option requires Graph to be created with WithGenLocalState option.
// when to use: when current node's output is an actual stream, and you want the downstream node's input to remain an actual stream after state post handler.
// caution: while StreamStatePostHandler is thread safe, modifying state within your own goroutine is NOT.
// O: output type of the Node like ChatModel, Lambda, Retriever etc.
// S: state type defined in WithGenLocalState
//
// WithStreamStatePostHandler 根据状态 S 和输出修改节点的流式 O 输出，或将输出信息存入状态，且线程安全。
// 注意：此选项要求 Graph 使用 WithGenLocalState 选项创建。
// 使用时机：当当前节点的输出是真实流，且你希望经过 state post handler 后下游节点的输入仍保持真实流。
// 注意：虽然 StreamStatePostHandler 是线程安全的，但在你自己的 goroutine 中修改状态并不是线程安全的。
// O：节点的输出类型，如 ChatModel、Lambda、Retriever 等。
// S：WithGenLocalState 中定义的状态类型
func WithStreamStatePostHandler[O, S any](post StreamStatePostHandler[O, S]) GraphAddNodeOpt {
	return func(o *graphAddNodeOpts) {
		o.processor.statePostHandler = streamConvertPostHandler(post)
		o.processor.postStateType = generic.TypeOf[S]()
		o.needState = true
	}
}

type processorOpts struct {
	statePreHandler *composableRunnable
	preStateType    reflect.Type // used for type validation
	// 用于类型校验
	statePostHandler *composableRunnable
	postStateType    reflect.Type // used for type validation
	// 用于类型校验
}

func getGraphAddNodeOpts(opts ...GraphAddNodeOpt) *graphAddNodeOpts {
	opt := &graphAddNodeOpts{
		nodeOptions: &nodeOptions{
			nodeName: "",
			nodeKey:  "",
		},
		processor: &processorOpts{
			statePreHandler:  nil,
			statePostHandler: nil,
		},
	}

	for _, fn := range opts {
		fn(opt)
	}

	return opt
}
