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

package model

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

// messageType is the sealed type constraint for message types used in BaseModel.
// Only *schema.Message and *schema.AgenticMessage satisfy this constraint.
//
// messageType 是 BaseModel 中消息类型使用的密封类型约束。
// 只有 *schema.Message 和 *schema.AgenticMessage 满足此约束。
type messageType interface {
	*schema.Message | *schema.AgenticMessage
}

// BaseModel is the generic base model interface parameterized by message type M.
// It exposes two modes of interaction:
//   - [BaseModel.Generate]: blocks until the model returns a complete response.
//   - [BaseModel.Stream]: returns a [schema.StreamReader] that yields message
//     chunks incrementally as the model generates them.
//
// BaseModel 是按消息类型 M 参数化的泛型基础模型接口。
// 它提供两种交互模式：
// - [BaseModel.Generate]：阻塞直到模型返回完整响应。
// - [BaseModel.Stream]：返回 [schema.StreamReader]，在模型生成过程中增量产出消息片段。
type BaseModel[M messageType] interface {
	Generate(ctx context.Context, input []M, opts ...Option) (M, error)
	Stream(ctx context.Context, input []M, opts ...Option) (*schema.StreamReader[M], error)
}

// BaseChatModel is a backward-compatible type alias for BaseModel specialized
// with *schema.Message. All existing code using model.BaseChatModel continues
// to work without modification.
//
// It exposes two modes of interaction:
//   - [BaseChatModel.Generate]: blocks until the model returns a complete response.
//   - [BaseChatModel.Stream]: returns a [schema.StreamReader] that yields message
//     chunks incrementally as the model generates them.
//
// The input is a slice of [schema.Message] representing the conversation history.
// Messages carry a role (system, user, assistant, tool) and support multimodal
// content (text, images, audio, video). Tool messages must include a ToolCallID
// that correlates them with a prior assistant tool-call message.
//
// Stream usage — the caller is responsible for closing the reader:
//
//	reader, err := m.Stream(ctx, messages)
//	if err != nil { ... }
//	defer reader.Close()
//	for {
//	    chunk, err := reader.Recv()
//	    if errors.Is(err, io.EOF) { break }
//	    if err != nil { ... }
//	    // handle chunk
//	}
//
// Note: a [schema.StreamReader] can only be read once. If multiple consumers
// need the stream, it must be copied before reading.
//
//go:generate  mockgen -destination ../../internal/mock/components/model/ChatModel_mock.go --package model github.com/cloudwego/eino/components/model BaseChatModel,ChatModel,ToolCallingChatModel
type BaseChatModel = BaseModel[*schema.Message]

// Deprecated: Use [ToolCallingChatModel] instead.
//
// ChatModel extends [BaseChatModel] with tool binding via [ChatModel.BindTools].
// BindTools mutates the instance in place, which causes a race condition when
// the same instance is used concurrently: one goroutine's tool list can
// overwrite another's. Prefer [ToolCallingChatModel.WithTools], which returns
// a new immutable instance and is safe for concurrent use.
//
// Deprecated: 请改用 [ToolCallingChatModel]。
// ChatModel 通过 [ChatModel.BindTools] 扩展 [BaseChatModel] 以支持工具绑定。
// BindTools 会原地修改实例，当同一实例被并发使用时会导致竞态：一个 goroutine 的工具列表可能覆盖另一个的。建议使用 [ToolCallingChatModel.WithTools]，它返回新的不可变实例，并且可安全并发使用。
type ChatModel interface {
	BaseChatModel

	// BindTools bind tools to the model.
	// BindTools before requesting ChatModel generally.
	// notice the non-atomic problem of BindTools and Generate.
	//
	// BindTools 将工具绑定到模型。
	// 通常在请求 ChatModel 之前调用 BindTools。
	// 注意 BindTools 和 Generate 之间的非原子问题。
	BindTools(tools []*schema.ToolInfo) error
}

// ToolCallingChatModel extends [BaseChatModel] with safe tool binding.
//
// Unlike the deprecated [ChatModel.BindTools], [ToolCallingChatModel.WithTools]
// does not mutate the receiver — it returns a new instance with the given tools
// attached. This makes it safe to share a base model instance across goroutines
// and derive per-request variants with different tool sets:
//
//	base, _ := openai.NewChatModel(ctx, cfg)           // shared, no tools
//	withSearch, _ := base.WithTools([]*schema.ToolInfo{searchTool})
//	withCalc, _  := base.WithTools([]*schema.ToolInfo{calcTool})
//
// ToolCallingChatModel 扩展 [BaseChatModel]，提供安全的工具绑定。
// 与已弃用的 [ChatModel.BindTools] 不同，[ToolCallingChatModel.WithTools] 不会修改接收者——它会返回一个绑定了给定工具的新实例。这样即可安全地在多个 goroutine 间共享基础模型实例，并派生出不同工具集的按请求变体：
// base, _ := openai.NewChatModel(ctx, cfg)           // shared, no tools
// withSearch, _ := base.WithTools([]*schema.ToolInfo{searchTool})
// withCalc, _  := base.WithTools([]*schema.ToolInfo{calcTool})
type ToolCallingChatModel interface {
	BaseChatModel

	WithTools(tools []*schema.ToolInfo) (ToolCallingChatModel, error)
}

// AgenticModel is a type alias for BaseModel specialized with
// *schema.AgenticMessage. Unlike ToolCallingChatModel, agentic models do NOT
// expose a WithTools method; tools are passed at request time via the
// model.WithTools option, consistent with how ChatModelAgent binds tools.
//
// AgenticModel 是专用于 *schema.AgenticMessage 的 BaseModel 类型别名。不同于 ToolCallingChatModel，agentic models 不暴露 WithTools 方法；工具会通过 model.WithTools 选项在请求时传入，这与 ChatModelAgent 绑定工具的方式一致。
type AgenticModel = BaseModel[*schema.AgenticMessage]
