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

package prompt

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

var _ ChatTemplate = &DefaultChatTemplate{}
var _ AgenticChatTemplate = &DefaultAgenticChatTemplate{}

// ChatTemplate formats a variables map into a list of messages for a ChatModel.
//
// Format substitutes the values from vs into the template's message list and
// returns the resulting []*schema.Message. The exact substitution syntax
// (FString, GoTemplate, Jinja2) is determined at construction time.
//
// Variable keys present in the template but absent from vs produce a runtime
// error — there is no compile-time safety. Prefer consistent variable naming
// across templates and callers.
//
// In a Graph or Chain, ChatTemplate typically precedes ChatModel. Use
// compose.WithOutputKey to convert the prior node's output into the map[string]any
// that Format expects.
//
// See [FromMessages] and [schema.MessagesPlaceholder] for construction helpers.
//
// ChatTemplate 将变量 map 格式化为供 ChatModel 使用的消息列表。
// Format 将 vs 中的值替换到模板的消息列表中，并返回生成的 []*schema.Message。具体替换语法（FString、GoTemplate、Jinja2）在构造时确定。
// 模板中存在但 vs 中缺失的变量 key 会产生运行时错误——没有编译期安全保障。建议在模板和调用方之间保持变量命名一致。
// 在 Graph 或 Chain 中，ChatTemplate 通常位于 ChatModel 之前。使用 compose.WithOutputKey 将前一节点的输出转换为 Format 期望的 map[string]any。
// 构造辅助函数见 [FromMessages] 和 [schema.MessagesPlaceholder]。
type ChatTemplate interface {
	Format(ctx context.Context, vs map[string]any, opts ...Option) ([]*schema.Message, error)
}

// AgenticChatTemplate formats variables into a list of agentic messages according to a prompt schema.
// AgenticChatTemplate 根据 prompt schema 将变量格式化为 agentic 消息列表。
type AgenticChatTemplate interface {
	Format(ctx context.Context, vs map[string]any, opts ...Option) ([]*schema.AgenticMessage, error)
}
