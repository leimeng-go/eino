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

// Package supervisor implements the supervisor pattern for multi-agent systems,
// where a central agent coordinates a set of sub-agents.
//
// # Unified Tracing
//
// The supervisor pattern provides unified tracing support through an internal container.
// When using callbacks (e.g., for tracing or observability), the entire supervisor structure
// (supervisor agent + all sub-agents) shares a single trace root. This means:
//   - OnStart is invoked once at the supervisor container level
//   - The callback-enriched context (containing parent span info) is propagated to all agents
//   - All agents within the supervisor appear as children of the same trace root
//
// This is achieved by wrapping the supervisor structure in an internal container that acts
// as the single entry point for tracing. The container delegates all execution to the
// underlying agents while providing a unified identity for callbacks.
//
// Package supervisor 实现多智能体系统的 supervisor 模式，
// 其中一个中心智能体协调一组子智能体。
// # 统一追踪
// supervisor 模式通过内部容器提供统一追踪支持。
// 使用回调（例如用于追踪或可观测性）时，整个 supervisor 结构
// （supervisor 智能体 + 所有子智能体）共享同一个 trace root。这意味着：
// - OnStart 在 supervisor 容器级别只调用一次
// - 带有回调信息的 context（包含父 span 信息）会传播给所有智能体
// - supervisor 内的所有智能体都显示为同一 trace root 的子级
// 这是通过将 supervisor 结构包装在一个内部容器中实现的，该容器作为
// 追踪的唯一入口点。容器将所有执行委托给底层智能体，同时为回调提供统一身份。
package supervisor

import (
	"context"

	"github.com/cloudwego/eino/adk"
)

// Config is the configuration for creating a supervisor-based multi-agent system.
//
// NOT RECOMMENDED: Supervisor is built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// Config 是创建基于 supervisor 的多智能体系统的配置。
// 不推荐：Supervisor 基于 agent transfer 并共享完整 context，
// 其效果在经验上尚未证明更好。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
type Config struct {
	// Supervisor specifies the agent that will act as the supervisor, coordinating and managing the sub-agents.
	// Supervisor 指定充当 supervisor 的智能体，用于协调和管理子智能体。
	Supervisor adk.Agent

	// SubAgents specifies the list of agents that will be supervised and coordinated by the supervisor agent.
	// SubAgents 指定由 supervisor 智能体监督和协调的智能体列表。
	SubAgents []adk.Agent
}

// supervisorContainer wraps the entire supervisor structure to provide unified tracing.
// When callbacks are registered (e.g., via Runner.Query with WithCallbacks), OnStart/OnEnd
// are invoked once for this container, creating a single trace root. The callback-enriched
// context is then propagated to the supervisor and all sub-agents, ensuring they share
// the same trace parent.
//
// This container implements Agent and ResumableAgent by delegating to the inner agent.
// It provides its own Name and GetType for callback identification.
//
// supervisorContainer 包装整个 supervisor 结构以提供统一追踪。
// 注册回调时（例如通过 Runner.Query 配合 WithCallbacks），OnStart/OnEnd
// 会对该容器各调用一次，创建单个 trace root。带有回调信息的
// context 随后传播给 supervisor 和所有子智能体，确保它们共享
// 同一个 trace parent。
// 该容器通过委托给内部智能体来实现 Agent 和 ResumableAgent。
// 它提供自己的 Name 和 GetType 供回调识别。
type supervisorContainer struct {
	name  string
	inner adk.ResumableAgent
}

func (s *supervisorContainer) Name(_ context.Context) string {
	return s.name
}

func (s *supervisorContainer) Description(ctx context.Context) string {
	return s.inner.Description(ctx)
}

func (s *supervisorContainer) GetType() string {
	return "Supervisor"
}

func (s *supervisorContainer) Run(ctx context.Context, input *adk.AgentInput, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
	return s.inner.Run(ctx, input, opts...)
}

func (s *supervisorContainer) Resume(ctx context.Context, info *adk.ResumeInfo, opts ...adk.AgentRunOption) *adk.AsyncIterator[*adk.AgentEvent] {
	return s.inner.Resume(ctx, info, opts...)
}

// New creates a supervisor-based multi-agent system with the given configuration.
//
// In the supervisor pattern, a designated supervisor agent coordinates multiple sub-agents.
// The supervisor can delegate tasks to sub-agents and receive their responses, while
// sub-agents can only communicate with the supervisor (not with each other directly).
// This hierarchical structure enables complex problem-solving through coordinated agent interactions.
//
// The returned agent is wrapped in an internal container that provides unified tracing.
// When used with Runner and callbacks, all agents within the supervisor structure will
// share the same trace root, making it easy to observe the entire multi-agent execution
// as a single logical unit.
//
// NOT RECOMMENDED: Supervisor is built on agent transfer with full context sharing,
// which has not proven to be more effective empirically. Consider using
// ChatModelAgent with AgentTool or DeepAgent instead for most multi-agent scenarios.
//
// New 使用给定配置创建基于 supervisor 的多智能体系统。
// 在 supervisor 模式中，指定的 supervisor 智能体会协调多个子智能体。
// supervisor 可以将任务委托给子智能体并接收其响应，而
// 子智能体只能与 supervisor 通信（不能彼此直接通信）。
// 这种层级结构通过协调的智能体交互支持复杂问题求解。
// 返回的智能体会被包装在内部容器中，以提供统一追踪。
// 与 Runner 和回调一起使用时，supervisor 结构中的所有智能体将
// 共享同一个 trace root，便于将整个多智能体执行过程作为
// 单个逻辑单元进行观测。
// 不推荐：Supervisor 基于 agent transfer 并共享完整 context，
// 其效果在经验上尚未证明更好。多数多智能体场景建议改用
// ChatModelAgent 配合 AgentTool，或使用 DeepAgent。
func New(ctx context.Context, conf *Config) (adk.ResumableAgent, error) {
	subAgents := make([]adk.Agent, 0, len(conf.SubAgents))
	supervisorName := conf.Supervisor.Name(ctx)
	for _, subAgent := range conf.SubAgents {
		subAgents = append(subAgents, adk.AgentWithDeterministicTransferTo(ctx, &adk.DeterministicTransferConfig{
			Agent:        subAgent,
			ToAgentNames: []string{supervisorName},
		}))
	}

	inner, err := adk.SetSubAgents(ctx, conf.Supervisor, subAgents)
	if err != nil {
		return nil, err
	}

	return &supervisorContainer{
		name:  supervisorName,
		inner: inner,
	}, nil
}
