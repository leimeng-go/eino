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

// Package parser defines the Parser interface for converting raw byte streams
// into [schema.Document] values.
//
// # Overview
//
// A Parser is not a standalone pipeline component — it is used inside a
// [document.Loader] to handle format-specific decoding. The loader fetches
// raw bytes; the parser converts them into documents.
//
// # Built-in Implementations
//
//   - TextParser: treats the entire reader as plain text, one document per call
//   - ExtParser: selects a parser by file extension (from [Options.URI]), with
//     a configurable fallback for unknown extensions
//
// Use ExtParser when you want format-agnostic loading: pass the source URI
// via [WithURI] and ExtParser picks the right sub-parser automatically.
//
// # Reader Contract
//
// The [io.Reader] passed to [Parser.Parse] is consumed during the call —
// it cannot be read again. Loaders must not reuse the same reader across
// multiple Parse calls.
//
// # Metadata Propagation
//
// Use [WithExtraMeta] to attach key-value pairs that are merged into every
// document's MetaData. This is the standard way to tag documents with source
// information (URI, content type, etc.) at parse time.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/document_loader_guide/document_parser_interface_guide/
//
// Package parser 定义 Parser 接口，用于将原始字节流转换为 [schema.Document] 值。
// # 概览
// Parser 不是独立的流水线组件，而是在 [document.Loader] 内部用于处理特定格式解码。loader 获取原始字节；parser 将其转换为文档。
// # 内置实现
// - TextParser：将整个 reader 视为纯文本，每次调用生成一个文档
// - ExtParser：按文件扩展名（来自 [Options.URI]）选择 parser，并可为未知扩展名配置 fallback
// 当你想进行格式无关的加载时使用 ExtParser：通过 [WithURI] 传入源 URI，ExtParser 会自动选择合适的子 parser。
// # Reader 约定
// 传给 [Parser.Parse] 的 [io.Reader] 会在调用期间被消费，不能再次读取。Loaders 不得在多次 Parse 调用间复用同一个 reader。
// # 元数据传递
// 使用 [WithExtraMeta] 附加键值对，这些键值对会合并到每个文档的 MetaData 中。这是在解析时为文档标记来源信息（URI、内容类型等）的标准方式。
// 参见 https://www.cloudwego.io/docs/eino/core_modules/components/document_loader_guide/document_parser_interface_guide/
package parser
