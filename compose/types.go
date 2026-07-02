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
	"github.com/cloudwego/eino/components"
)

type component = components.Component

// built-in component types in graph node.
// it represents the type of the most primitive executable object provided by the user.
//
// 图节点中的内置组件类型。
// 它表示用户提供的最基础可执行对象的类型。
const (
	ComponentOfUnknown          component = "Unknown"
	ComponentOfGraph            component = "Graph"
	ComponentOfWorkflow         component = "Workflow"
	ComponentOfChain            component = "Chain"
	ComponentOfPassthrough      component = "Passthrough"
	ComponentOfToolsNode        component = "ToolsNode"
	ComponentOfAgenticToolsNode component = "AgenticToolsNode"
	ComponentOfLambda           component = "Lambda"
)

// NodeTriggerMode controls the triggering mode of graph nodes.
// NodeTriggerMode 控制图节点的触发模式。
type NodeTriggerMode string

const (
	// AnyPredecessor means that the node will be triggered when any of its predecessors is included in the previous completed super step.
	// Ref:https://www.cloudwego.io/docs/eino/core_modules/chain_and_graph_orchestration/orchestration_design_principles/#runtime-engine
	//
	// AnyPredecessor 表示当任一前驱包含在上一个已完成的 super step 中时，该节点会被触发。
	// Ref:https://www.cloudwego.io/docs/eino/core_modules/chain_and_graph_orchestration/orchestration_design_principles/#runtime-engine
	AnyPredecessor NodeTriggerMode = "any_predecessor"
	// AllPredecessor means that the current node will only be triggered when all of its predecessor nodes have finished running.
	// AllPredecessor 表示只有当前节点的所有前驱节点都运行完成后，当前节点才会被触发。
	AllPredecessor NodeTriggerMode = "all_predecessor"
)
