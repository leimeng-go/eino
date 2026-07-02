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

// Package embedding defines the Embedder component interface for converting
// text into vector representations.
//
// # Overview
//
// An Embedder converts a batch of strings into dense float vectors. Semantically
// similar texts produce vectors that are close in the vector space, making
// embeddings the backbone of semantic search, RAG pipelines, and clustering.
//
// Concrete implementations (OpenAI, Ark, Ollama, …) live in eino-ext:
//
//	github.com/cloudwego/eino-ext/components/embedding/
//
// # Output Format
//
// [Embedder.EmbedStrings] returns `[][]float64` where:
//   - outer index corresponds to the input text at the same position
//   - inner slice is the embedding vector; its length (dimensions) is fixed by
//     the model and is the same for every text
//
// # Consistency Requirement
//
// The same model must be used for both indexing and retrieval. Mixing models
// produces vectors in different spaces — similarity scores become meaningless
// and semantic search breaks silently.
//
// See https://www.cloudwego.io/docs/eino/core_modules/components/embedding_guide/
//
// Package embedding 定义了 Embedder 组件接口，用于将文本转换为向量表示。
// # Overview
// Embedder 将一批字符串转换为稠密浮点向量。语义相近的文本会生成在向量空间中彼此接近的向量，因此 embedding 是语义搜索、RAG 流水线和聚类的基础。
// 具体实现（OpenAI、Ark、Ollama、…）位于 eino-ext：
// github.com/cloudwego/eino-ext/components/embedding/
// # Output Format
// [Embedder.EmbedStrings] 返回 `[][]float64`，其中：
// - 外层索引对应相同位置的输入文本
// - 内层切片是嵌入向量；其长度（维度）由模型固定，且对每段文本都相同
// # Consistency Requirement
// 索引和检索必须使用同一个模型。混用模型会产生位于不同空间的向量——相似度分数将失去意义，语义搜索也会静默失效。
// 参见 https://www.cloudwego.io/docs/eino/core_modules/components/embedding_guide/
package embedding
