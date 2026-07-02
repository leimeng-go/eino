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

// Package skill provides a Skill middleware, types, and a local filesystem backend.
//
// # Overview
//
// The Skill middleware is a ChatModelAgentMiddleware implementation that injects:
//   - a system instruction (Skills System)
//   - a tool (default name: "skill") to load and execute skills
//
// Skill definitions are stored in SKILL.md files with a YAML frontmatter. The frontmatter is
// parsed into FrontMatter (name/description/context/agent/model), and the remaining markdown
// is treated as the skill content.
//
// # Execution modes
//
// Skill execution is controlled by frontmatter "context":
//   - inline (default): returns the skill content as the tool result in the current agent
//   - fork: runs the skill with a new sub-agent without parent message history
//   - fork_with_context: runs the skill with a new sub-agent carrying parent message history
//
// # Extension points
//
//   - CustomToolParams customizes the tool parameter schema.
//   - BuildContent customizes how skill content is generated from raw tool arguments.
//   - BuildForkMessages customizes the initial messages passed to the sub-agent in fork modes.
//   - FormatForkResult customizes how sub-agent outputs are formatted back to the caller.
//
// # Filesystem backend
//
// NewBackendFromFilesystem loads skills from a filesystem backend. It scans only first-level
// subdirectories under BaseDir and reads each <dir>/SKILL.md as a skill definition.
//
// Package skill 提供 Skill 中间件、类型和本地文件系统 backend。
// # 概览
// Skill 中间件是一个 ChatModelAgentMiddleware 实现，会注入：
// - 系统指令（Skills System）
// - 一个工具（默认名称："skill"）用于加载并执行 skills
// Skill 定义存储在带有 YAML frontmatter 的 SKILL.md 文件中。frontmatter 会被
// 解析为 FrontMatter（name/description/context/agent/model），其余 markdown
// 视为 skill 内容。
// # 执行模式
// Skill 执行由 frontmatter "context" 控制：
// - inline（默认）：在当前智能体中将 skill 内容作为工具结果返回
// - fork：使用新的子智能体运行 skill，不带父消息历史
// - fork_with_context：使用新的子智能体运行 skill，并携带父消息历史
// # 扩展点
// - CustomToolParams 自定义工具参数 schema。
// - BuildContent 自定义如何从原始工具参数生成 skill 内容。
// - BuildForkMessages 自定义 fork 模式下传给子智能体的初始消息。
// - FormatForkResult 自定义如何将子智能体输出格式化后返回给调用方。
// # 文件系统 backend
// NewBackendFromFilesystem 从文件系统 backend 加载 skills。它只扫描 BaseDir 下第一层
// 子目录，并将每个 <dir>/SKILL.md 读取为 skill 定义。
package skill
