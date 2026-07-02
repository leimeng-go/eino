/*
 * Copyright 2026 CloudWeGo Authors
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

package summarization

import (
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

// TypedCustomizedAction is the generic customized action for summarization events.
// TypedCustomizedAction 是用于摘要事件的泛型自定义动作。
type TypedCustomizedAction[M adk.MessageType] struct {
	// Type is the action type.
	// Type 是动作类型。
	Type ActionType `json:"type"`

	// Before is set when Type is ActionTypeBeforeSummarize.
	// Emitted after trigger condition is met, before calling model to generate summary.
	//
	// 当 Type 为 ActionTypeBeforeSummarize 时设置 Before。
	// 在满足触发条件后、调用 model 生成摘要前发出。
	Before *TypedBeforeSummarizeAction[M] `json:"before,omitempty"`

	// After is set when Type is ActionTypeAfterSummarize.
	// Emitted after summarization.
	//
	// 当 Type 为 ActionTypeAfterSummarize 时设置 After。
	// 在摘要完成后发出。
	After *TypedAfterSummarizeAction[M] `json:"after,omitempty"`

	// GenerateSummary is set when Type is ActionTypeGenerateSummary.
	// Emitted on each summary generation attempt, including retries and failovers.
	//
	// 当 Type 为 ActionTypeGenerateSummary 时设置 GenerateSummary。
	// 在每次生成摘要尝试时发出，包括重试和 failover。
	GenerateSummary *TypedGenerateSummaryAction[M] `json:"generate_summary,omitempty"`
}

// CustomizedAction is the default action type using *schema.Message.
// CustomizedAction 是使用 *schema.Message 的默认动作类型。
type CustomizedAction = TypedCustomizedAction[*schema.Message]

// TypedBeforeSummarizeAction contains the state messages before summarization.
// TypedBeforeSummarizeAction 包含总结前的状态消息。
type TypedBeforeSummarizeAction[M adk.MessageType] struct {
	// Messages is the original state messages before summarization.
	// Messages 是总结前的原始状态消息。
	Messages []M `json:"messages,omitempty"`
}

// BeforeSummarizeAction is the default type using *schema.Message.
// BeforeSummarizeAction 是使用 *schema.Message 的默认类型。
type BeforeSummarizeAction = TypedBeforeSummarizeAction[*schema.Message]

// TypedAfterSummarizeAction contains the state messages after summarization.
// TypedAfterSummarizeAction 包含总结后的状态消息。
type TypedAfterSummarizeAction[M adk.MessageType] struct {
	// Messages is the final state messages after summarization.
	// Messages 是总结后的最终状态消息。
	Messages []M `json:"messages,omitempty"`
}

// AfterSummarizeAction is the default type using *schema.Message.
// AfterSummarizeAction 是使用 *schema.Message 的默认类型。
type AfterSummarizeAction = TypedAfterSummarizeAction[*schema.Message]

// GenerateSummaryPhase indicates which phase a model generate attempt belongs to during summarization.
// GenerateSummaryPhase 表示总结期间一次模型生成尝试所属的阶段。
type GenerateSummaryPhase string

const (
	// GenerateSummaryPhasePrimary indicates an attempt using the primary model.
	// Attempt=1 is the initial call; Attempt>1 indicates a retry.
	//
	// GenerateSummaryPhasePrimary 表示使用主模型的尝试。
	// Attempt=1 是首次调用；Attempt>1 表示重试。
	GenerateSummaryPhasePrimary GenerateSummaryPhase = "primary"

	// GenerateSummaryPhaseFailover indicates an attempt using a failover model
	// after the primary model exhausted all retries or was deemed unrecoverable.
	//
	// GenerateSummaryPhaseFailover 表示使用 failover 模型的尝试，
	// 发生在主模型耗尽所有重试或被判定为不可恢复之后。
	GenerateSummaryPhaseFailover GenerateSummaryPhase = "failover"
)

// TypedGenerateSummaryAction contains details of a single model generate attempt during summarization.
// Emitted on every attempt, whether it succeeds or fails.
//
// TypedGenerateSummaryAction 包含总结期间单次模型生成尝试的详细信息。
// 每次尝试都会发出，无论成功还是失败。
type TypedGenerateSummaryAction[M adk.MessageType] struct {
	// Attempt is the 1-based attempt number within the current phase.
	// For primary phase, Attempt=1 is the initial call and Attempt>1 indicates retries.
	// For failover phase, Attempt counts the failover rounds (1, 2, 3, ...).
	//
	// Attempt 是当前阶段内从 1 开始的尝试编号。
	// 对于 primary 阶段，Attempt=1 是首次调用，Attempt>1 表示重试。
	// 对于 failover 阶段，Attempt 统计 failover 轮次（1, 2, 3, ...）。
	Attempt int `json:"attempt"`

	// Phase indicates which phase this generate attempt belongs to.
	// Phase 表示本次生成尝试所属的阶段。
	Phase GenerateSummaryPhase `json:"phase"`

	// ModelResponse is the raw response returned by the model.
	// It may be nil when the model call fails without returning a response.
	//
	// ModelResponse 是模型返回的原始响应。
	// 当模型调用失败且未返回响应时，它可能为 nil。
	ModelResponse M `json:"model_response,omitempty"`

	// err is the error returned by the model call, if any. Use GetError to access it.
	// err 是模型调用返回的错误（如果有）。使用 GetError 访问它。
	err error
}

// GenerateSummaryAction is the default type using *schema.Message.
// GenerateSummaryAction 是使用 *schema.Message 的默认类型。
type GenerateSummaryAction = TypedGenerateSummaryAction[*schema.Message]

// GetError returns the error from the model call, if any.
// GetError 返回模型调用产生的错误（如果有）。
func (a *TypedGenerateSummaryAction[M]) GetError() error {
	return a.err
}
