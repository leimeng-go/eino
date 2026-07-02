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

// Package retriever defines the Retriever component interface for fetching
// relevant documents from a document store given a query.
//
// # Overview
//
// A Retriever is the read path of a RAG (Retrieval-Augmented Generation)
// pipeline. Given a query string it returns the most relevant [schema.Document]
// values from an underlying store (vector DB, keyword index, etc.).
//
// Concrete implementations (VikingDB, Milvus, Elasticsearch, …) live in
// eino-ext:
//
//	github.com/cloudwego/eino-ext/components/retriever/
//
// # Relationship to Indexer
//
// [Indexer] and Retriever are complementary:
//   - Indexer writes documents (and their vectors) to the store
//   - Retriever reads them back
//
// When both use an [embedding.Embedder], it must be the same model — vector
// dimensions must match or similarity scores will be meaningless.
//
// # Result Ordering
//
// Results are ordered by relevance score (descending). Scores and other
// backend metadata are available via [schema.Document].MetaData.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/retriever_guide/
//
// Package retriever 定义 Retriever 组件接口，用于根据查询从文档存储中获取相关文档。
// # 概览
// Retriever 是 RAG (Retrieval-Augmented Generation) 管道的读取路径。给定查询字符串，它会从底层存储（向量数据库、关键词索引等）返回最相关的 [schema.Document] 值。
// 具体实现（VikingDB、Milvus、Elasticsearch、…）位于 eino-ext:
// github.com/cloudwego/eino-ext/components/retriever/
// # 与 Indexer 的关系
// [Indexer] 和 Retriever 互为补充：
// - Indexer 将文档（及其向量）写入存储
// - Retriever 将它们读回
// 当两者都使用 [embedding.Embedder] 时，必须使用同一个模型——向量维度必须匹配，否则相似度分数将没有意义。
// # 结果排序
// 结果按相关性分数降序排列。分数和其他后端元数据可通过 [schema.Document].MetaData 获取。
// 参见 https://www.cloudwego.io/docs/eino/core_modules/components/retriever_guide/
package retriever
