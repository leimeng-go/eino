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
	"reflect"

	"github.com/cloudwego/eino/components"
)

// GraphNodeInfo the info which end users pass in when they are adding nodes to graph.
// GraphNodeInfo 是最终用户向 graph 添加节点时传入的信息。
type GraphNodeInfo struct {
	Component             components.Component
	Instance              any
	GraphAddNodeOpts      []GraphAddNodeOpt
	InputType, OutputType reflect.Type // mainly for lambda, whose input and output types cannot be inferred by component type
	// 主要用于 lambda，其输入和输出类型无法通过组件类型推断。
	Name                string
	InputKey, OutputKey string
	GraphInfo           *GraphInfo
	Mappings            []*FieldMapping
}

// GraphInfo the info which end users pass in when they are compiling a graph.
// it is used in compile callback for user to get the node info and instance.
// you may need all details info of the graph for observation.
//
// GraphInfo 是终端用户在编译 graph 时传入的信息。
// 它用于 compile callback，让用户获取 node 信息和实例。
// 你可能需要 graph 的所有详细信息用于观测。
type GraphInfo struct {
	CompileOptions []GraphCompileOption
	Nodes          map[string]GraphNodeInfo // node key -> node info
	// node key -> node info
	Edges map[string][]string // edge start node key -> edge end node key, control edges
	// edge start node key -> edge end node key，control edges
	DataEdges map[string][]string
	Branches  map[string][]GraphBranch // branch start node key -> branch
	// branch start node key -> branch
	InputType, OutputType reflect.Type
	Name                  string

	NewGraphOptions []NewGraphOption
	GenStateFn      func(context.Context) any
}

// GraphCompileCallback is the callback which will be called when graph compilation finishes.
// GraphCompileCallback 是 graph 编译完成时会被调用的 callback。
type GraphCompileCallback interface {
	OnFinish(ctx context.Context, info *GraphInfo)
}
