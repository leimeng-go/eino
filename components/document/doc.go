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

// Package document defines the Loader and Transformer component interfaces
// for ingesting and processing documents in an eino pipeline.
//
// # Components
//
//   - [Loader]: reads raw content from an external source (file, URL, S3, …)
//     and returns [schema.Document] values. Parsing is typically delegated to
//     a [parser.Parser] configured on the loader.
//   - [Transformer]: takes a slice of [schema.Document] values and transforms
//     them — splitting, filtering, merging, re-ranking, etc.
//
// Concrete implementations live in eino-ext:
//
//	github.com/cloudwego/eino-ext/components/document/
//
// # Document Metadata
//
// [schema.Document].MetaData is the primary mechanism for carrying contextual
// information (source URI, scores, chunk indices, embeddings) through the
// pipeline. Transformers should preserve existing metadata and merge rather
// than replace when adding their own keys.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/document_loader_guide/
// See https://www.cloudwego.io/docs/eino/core_modules/components/document_transformer_guide/
//
// Package document 定义 Loader 和 Transformer 组件接口，用于在 eino pipeline 中摄取和处理文档。
// # 组件
// - [Loader]：从外部来源（文件、URL、S3、…）读取原始内容，并返回 [schema.Document] 值。解析通常委托给 loader 上配置的 [parser.Parser]。
// - [Transformer]：接收一组 [schema.Document] 值并进行转换 —— 拆分、过滤、合并、重排序等。
// 具体实现在 eino-ext 中：
// github.com/cloudwego/eino-ext/components/document/
// # 文档元数据
// [schema.Document].MetaData 是在 pipeline 中携带上下文信息（source URI、scores、chunk indices、embeddings）的主要机制。Transformers 添加自己的键时，应保留已有元数据并进行合并，而不是替换。
// 参见 https://www.cloudwego.io/docs/eino/core_modules/components/document_loader_guide/
// 参见 https://www.cloudwego.io/docs/eino/core_modules/components/document_transformer_guide/
package document
