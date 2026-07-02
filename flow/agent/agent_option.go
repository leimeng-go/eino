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

// Package agent defines common option types used by agents and multi-agents.
// Package agent 定义智能体和多智能体使用的通用选项类型。
package agent

import "github.com/cloudwego/eino/compose"

// AgentOption is the common option type for various agent and multi-agent implementations.
// For options intended to use with underlying graph or components, use WithComposeOptions to specify.
// For options intended to use with particular agent/multi-agent implementations, use WrapImplSpecificOptFn to specify.
//
// AgentOption 是各种智能体和多智能体实现的通用选项类型。
// 用于底层图或组件的选项，请使用 WithComposeOptions 指定。
// 用于特定智能体/多智能体实现的选项，请使用 WrapImplSpecificOptFn 指定。
type AgentOption struct {
	implSpecificOptFn any
	composeOptions    []compose.Option
}

// GetComposeOptions returns all compose options from the given agent options.
// GetComposeOptions 返回给定智能体选项中的所有 compose 选项。
func GetComposeOptions(opts ...AgentOption) []compose.Option {
	var result []compose.Option
	for _, opt := range opts {
		result = append(result, opt.composeOptions...)
	}

	return result
}

// WithComposeOptions returns an agent option that specifies compose options.
// WithComposeOptions 返回用于指定 compose 选项的智能体选项。
func WithComposeOptions(opts ...compose.Option) AgentOption {
	return AgentOption{
		composeOptions: opts,
	}
}

// WrapImplSpecificOptFn returns an agent option that specifies a function to modify the implementation-specific options.
// WrapImplSpecificOptFn 返回用于指定修改实现特定选项函数的智能体选项。
func WrapImplSpecificOptFn[T any](optFn func(*T)) AgentOption {
	return AgentOption{
		implSpecificOptFn: optFn,
	}
}

// GetImplSpecificOptions returns the implementation-specific options from the given agent options.
// GetImplSpecificOptions 返回给定智能体选项中的实现特定选项。
func GetImplSpecificOptions[T any](base *T, opts ...AgentOption) *T {
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
