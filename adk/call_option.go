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

package adk

import "github.com/cloudwego/eino/callbacks"

type options struct {
	sharedParentSession  bool
	sessionValues        map[string]any
	checkPointID         *string
	skipTransferMessages bool
	handlers             []callbacks.Handler
	cancelCtx            *cancelContext
}

// AgentRunOption is the call option for adk Agent.
// AgentRunOption 是 adk Agent 的调用选项。
type AgentRunOption struct {
	implSpecificOptFn any

	// specify which Agent can see this AgentRunOption, if empty, all Agents can see this AgentRunOption
	// 指定哪些 Agent 可以看到此 AgentRunOption；如果为空，则所有 Agents 都可以看到此 AgentRunOption
	agentNames []string
}

func (o AgentRunOption) DesignateAgent(name ...string) AgentRunOption {
	o.agentNames = append(o.agentNames, name...)
	return o
}

func getCommonOptions(base *options, opts ...AgentRunOption) *options {
	if base == nil {
		base = &options{}
	}

	return GetImplSpecificOptions(base, opts...)
}

// WithSessionValues sets session-scoped values for the agent run.
// WithSessionValues 为 agent run 设置会话作用域的值。
func WithSessionValues(v map[string]any) AgentRunOption {
	return WrapImplSpecificOptFn(func(o *options) {
		o.sessionValues = v
	})
}

// WithSkipTransferMessages disables forwarding transfer messages during execution.
//
// NOT RECOMMENDED: Agent transfer with full context sharing between agents has not proven
// to be more effective empirically. Consider using ChatModelAgent with AgentTool
// or DeepAgent instead for most multi-agent scenarios.
//
// WithSkipTransferMessages 会在执行期间禁用转发 transfer messages。
// 不推荐：Agent 之间共享完整上下文的 Agent transfer 在实证上并未证明更有效。大多数多智能体场景建议改用 ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
func WithSkipTransferMessages() AgentRunOption {
	return WrapImplSpecificOptFn(func(t *options) {
		t.skipTransferMessages = true
	})
}

func withSharedParentSession() AgentRunOption {
	return WrapImplSpecificOptFn(func(o *options) {
		o.sharedParentSession = true
	})
}

// WithCallbacks adds callback handlers to receive agent lifecycle events.
// Handlers receive OnStart with AgentCallbackInput and OnEnd with AgentCallbackOutput.
// Multiple handlers can be added; each receives an independent copy of the event stream.
//
// WithCallbacks 添加回调处理器，用于接收智能体生命周期事件。
// 处理器会通过 OnStart 接收 AgentCallbackInput，通过 OnEnd 接收 AgentCallbackOutput。
// 可以添加多个处理器；每个处理器都会收到事件流的独立副本。
func WithCallbacks(handlers ...callbacks.Handler) AgentRunOption {
	return WrapImplSpecificOptFn(func(o *options) {
		o.handlers = append(o.handlers, handlers...)
	})
}

// WrapImplSpecificOptFn is the option to wrap the implementation specific option function.
// WrapImplSpecificOptFn 是用于包装实现特定选项函数的选项。
func WrapImplSpecificOptFn[T any](optFn func(*T)) AgentRunOption {
	return AgentRunOption{
		implSpecificOptFn: optFn,
	}
}

// GetImplSpecificOptions extract the implementation specific options from AgentRunOption list, optionally providing a base options with default values.
// e.g.
//
//	myOption := &MyOption{
//		Field1: "default_value",
//	}
//
//	myOption := model.GetImplSpecificOptions(myOption, opts...)
//
// GetImplSpecificOptions 从 AgentRunOption 列表中提取实现特定选项，可选地提供带默认值的基础选项。
// 例如：
// myOption := &MyOption{
// Field1: "default_value",
// }
// myOption := model.GetImplSpecificOptions(myOption, opts...)
func GetImplSpecificOptions[T any](base *T, opts ...AgentRunOption) *T {
	if base == nil {
		base = new(T)
	}

	for i := range opts {
		opt := opts[i]
		if opt.implSpecificOptFn != nil {
			optFn, ok := opt.implSpecificOptFn.(func(*T))
			if ok {
				optFn(base)
			}
		}
	}

	return base
}

// filterCallbackHandlersForNestedAgents removes callback handlers that have already been applied
// to the current agent before passing opts to nested inner agents.
//
// This is necessary for workflow agents (LoopAgent, SequentialAgent, ParallelAgent) because:
//  1. Callback handlers designated for the current agent are applied via initAgentCallbacks(),
//     which stores them in the context.
//  2. Nested inner agents inherit this context, so they automatically receive these callbacks.
//  3. If we also pass these handlers in opts to inner agents, they would be applied twice,
//     causing duplicate callback invocations.
//
// Note: This only applies to workflow agents where inner agents inherit context from the parent.
// For flowAgent's sub-agents (which are peer agents that transfer to each other), the full opts
// are passed since they don't inherit the parent's callback context.
//
// filterCallbackHandlersForNestedAgents 会在将 opts 传给嵌套的内部智能体前，移除已经应用到当前智能体的回调处理器。
// 这对 workflow agents（LoopAgent、SequentialAgent、ParallelAgent）是必要的，因为：
// 1. 指定给当前智能体的回调处理器会通过 initAgentCallbacks() 应用，并存入 context。
// 2. 嵌套的内部智能体会继承此 context，因此会自动收到这些回调。
// 3. 如果同时把这些处理器通过 opts 传给内部智能体，它们会被应用两次，导致回调重复调用。
// 注意：这仅适用于内部智能体会继承父级 context 的 workflow agents。
// 对于 flowAgent 的 sub-agents（它们是相互 transfer 的 peer agents），会传入完整 opts，因为它们不继承父级的 callback context。
func filterCallbackHandlersForNestedAgents(currentAgentName string, opts []AgentRunOption) []AgentRunOption {
	if len(opts) == 0 {
		return nil
	}
	var filteredOpts []AgentRunOption
	for i := range opts {
		opt := opts[i]
		if opt.implSpecificOptFn == nil {
			filteredOpts = append(filteredOpts, opt)
			continue
		}
		if _, isCallbackOpt := opt.implSpecificOptFn.(func(*options)); isCallbackOpt {
			testOpt := &options{}
			opt.implSpecificOptFn.(func(*options))(testOpt)
			if len(testOpt.handlers) > 0 {
				if len(opt.agentNames) == 0 {
					continue
				}
				matched := false
				for _, name := range opt.agentNames {
					if name == currentAgentName {
						matched = true
						break
					}
				}
				if matched {
					continue
				}
			}
		}
		filteredOpts = append(filteredOpts, opt)
	}
	return filteredOpts
}

// filterCancelOption removes any AgentRunOption that sets a cancelCtx on *options.
// This prevents inner (nested) agents from receiving the cancel option when the
// outer flowAgent owns the cancel lifecycle. Inner agents access the cancelContext
// via the Go context (getCancelContext) instead.
//
// filterCancelOption 会移除任何在 *options 上设置 cancelCtx 的 AgentRunOption。
// 这可防止外层 flowAgent 拥有取消生命周期时，内部（嵌套）智能体收到 cancel 选项。
// 内部智能体改为通过 Go context（getCancelContext）访问 cancelContext。
func filterCancelOption(opts []AgentRunOption) []AgentRunOption {
	if len(opts) == 0 {
		return nil
	}
	var filteredOpts []AgentRunOption
	for i := range opts {
		opt := opts[i]
		if opt.implSpecificOptFn == nil {
			filteredOpts = append(filteredOpts, opt)
			continue
		}
		if _, isCommonOpt := opt.implSpecificOptFn.(func(*options)); isCommonOpt {
			testOpt := &options{}
			opt.implSpecificOptFn.(func(*options))(testOpt)
			if testOpt.cancelCtx != nil {
				continue
			}
		}
		filteredOpts = append(filteredOpts, opt)
	}
	return filteredOpts
}

func filterOptions(agentName string, opts []AgentRunOption) []AgentRunOption {
	if len(opts) == 0 {
		return nil
	}
	var filteredOpts []AgentRunOption
	for i := range opts {
		opt := opts[i]
		if len(opt.agentNames) == 0 {
			filteredOpts = append(filteredOpts, opt)
			continue
		}
		for j := range opt.agentNames {
			if opt.agentNames[j] == agentName {
				filteredOpts = append(filteredOpts, opt)
				break
			}
		}
	}
	return filteredOpts
}
