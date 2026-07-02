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

// Package prompt defines the ChatTemplate component interface for building
// structured message lists from templates and runtime variables.
//
// # Overview
//
// A ChatTemplate takes a variables map and produces a []*schema.Message slice
// ready to pass to a [model.BaseChatModel]. It is typically the first node in
// a pipeline, sitting before the ChatModel.
//
// The built-in [DefaultChatTemplate] supports three template syntaxes:
//   - FString: {variable} substitution
//   - GoTemplate: Go's text/template with conditionals and loops
//   - Jinja2: Jinja2 template syntax
//
// # Construction
//
// Use [FromMessages] to build a template from a list of message templates:
//
//	tmpl := prompt.FromMessages(schema.FString,
//	    schema.SystemMessage("You are a helpful assistant."),
//	    schema.UserMessage("Answer this: {question}"),
//	)
//	msgs, err := tmpl.Format(ctx, map[string]any{"question": "What is eino?"})
//
// Use [schema.MessagesPlaceholder] to insert a dynamic list of messages
// (e.g. conversation history) at a fixed position in the template:
//
//	tmpl := prompt.FromMessages(schema.FString,
//	    schema.SystemMessage("You are a helpful assistant."),
//	    schema.MessagesPlaceholder("history", true),
//	    schema.UserMessage("{question}"),
//	)
//
// # Common Pitfall
//
// Variable mismatches (a key present in the template but missing from the
// variables map) produce a runtime error — there is no compile-time check.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/chat_template_guide/
//
// Package prompt 定义 ChatTemplate 组件接口，用于从模板和运行时变量构建结构化消息列表。
// # 概览
// ChatTemplate 接收变量 map，并生成可传递给 [model.BaseChatModel] 的 []*schema.Message 切片。它通常是 pipeline 中的第一个节点，位于 ChatModel 之前。
// 内置的 [DefaultChatTemplate] 支持三种模板语法：
// - FString: {variable} 替换
// - GoTemplate: Go 的 text/template，支持条件和循环
// - Jinja2: Jinja2 模板语法
// # 构造
// 使用 [FromMessages] 从消息模板列表构建模板：
// tmpl := prompt.FromMessages(schema.FString,
// schema.SystemMessage("You are a helpful assistant."),
// schema.UserMessage("Answer this: {question}"),
// )
// msgs, err := tmpl.Format(ctx, map[string]any{"question": "What is eino?"})
// 使用 [schema.MessagesPlaceholder] 在模板固定位置插入动态消息列表（例如会话历史）：
// tmpl := prompt.FromMessages(schema.FString,
// schema.SystemMessage("You are a helpful assistant."),
// schema.MessagesPlaceholder("history", true),
// schema.UserMessage("{question}"),
// )
// # 常见陷阱
// 变量不匹配（模板中存在某个 key，但 variables map 中缺失）会产生运行时错误——没有编译期检查。
// 见 https://www.cloudwego.io/docs/eino/core_modules/components/chat_template_guide/
package prompt
