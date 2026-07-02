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

// Package model defines the ChatModel component interface for interacting with
// large language models (LLMs).
//
// # Overview
//
// A ChatModel takes a slice of [schema.Message] as input and returns a response
// message — either in full ([BaseChatModel.Generate]) or incrementally as a
// stream ([BaseChatModel.Stream]). It is the most fundamental building block in
// an eino pipeline: every application that talks to an LLM goes through this
// interface.
//
// Concrete implementations (OpenAI, Ark, Ollama, …) live in eino-ext:
//
//	github.com/cloudwego/eino-ext/components/model/
//
// # Interface Hierarchy
//
//	BaseChatModel         — Generate + Stream (all implementations)
//	├── ToolCallingChatModel  — preferred; WithTools returns a new instance (concurrency-safe)
//	└── ChatModel             — deprecated; BindTools mutates state (avoid in new code)
//
// # Choosing Generate vs Stream
//
// Use [BaseChatModel.Generate] when the full response is needed before
// proceeding (e.g. structured extraction, classification).
// Use [BaseChatModel.Stream] when output should be forwarded to the caller
// incrementally (e.g. chat UI, long-form generation). Always close the
// [schema.StreamReader] returned by Stream — failing to do so leaks the
// underlying connection:
//
//	reader, err := model.Stream(ctx, messages)
//	if err != nil { ... }
//	defer reader.Close()
//
// # Implementing a ChatModel
//
// Implementations must call [GetCommonOptions] to extract standard options and
// [GetImplSpecificOptions] to extract their own options from the Option list.
// Expose implementation-specific options via [WrapImplSpecificOptFn].
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/chat_model_guide/
// for the full component guide.
//
// Package model 定义了用于与大语言模型（LLMs）交互的 ChatModel 组件接口。
// # 概览
// ChatModel 以 [schema.Message] 切片作为输入，并返回响应消息——可以是完整返回（[BaseChatModel.Generate]），也可以作为流（[BaseChatModel.Stream]）增量返回。它是 eino pipeline 中最基础的构建块：所有与 LLM 对话的应用都会经过这个接口。
// 具体实现（OpenAI、Ark、Ollama、…）位于 eino-ext：
// github.com/cloudwego/eino-ext/components/model/
// # 接口层级
// BaseChatModel         — Generate + Stream（所有实现）
// ├── ToolCallingChatModel  — 推荐；WithTools 返回新实例（并发安全）
// └── ChatModel             — 已弃用；BindTools 会修改状态（新代码应避免）
// # 选择 Generate 还是 Stream
// 当需要完整响应后再继续时，使用 [BaseChatModel.Generate]（例如结构化抽取、分类）。
// 当输出需要增量转发给调用方时，使用 [BaseChatModel.Stream]（例如聊天 UI、长文本生成）。务必关闭 Stream 返回的 [schema.StreamReader]——否则会泄漏底层连接：
// reader, err := model.Stream(ctx, messages)
// if err != nil { ... }
// defer reader.Close()
// # 实现 ChatModel
// 实现必须调用 [GetCommonOptions] 提取标准选项，并调用 [GetImplSpecificOptions] 从 Option 列表中提取自身选项。
// 通过 [WrapImplSpecificOptFn] 暴露实现专属选项。
// 完整组件指南见 https://www.cloudwego.io/docs/eino/core_modules/components/chat_model_guide/
package model
