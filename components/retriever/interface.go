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

package retriever

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

//go:generate mockgen -destination ../../internal/mock/components/retriever/retriever_mock.go --package retriever -source interface.go

// Retriever fetches the most relevant documents from a store for a given query.
//
// Retrieve accepts a natural-language query string and returns matching
// [schema.Document] values ordered by relevance (most relevant first).
// Relevance scores and backend-specific metadata are available in
// [schema.Document].MetaData.
//
// When [Options.Embedding] is set, the implementation converts the query to a
// vector before searching. The embedder must be the same model used at index
// time — see [indexer.Options.Embedding].
//
// [Options.ScoreThreshold] is a filter, not a sort: documents scoring below
// the threshold are excluded entirely. [Options.TopK] caps the number of
// results returned.
//
// Retrieve can be used standalone or added to a Graph via AddRetrieverNode:
//
//	retriever, _ := redis.NewRetriever(ctx, cfg)
//	docs, _ := retriever.Retrieve(ctx, "what is eino?", retriever.WithTopK(5))
//
//	graph.AddRetrieverNode("retriever", retriever)
//
// Retriever 根据给定查询从存储中获取最相关的文档。
// Retrieve 接受自然语言查询字符串，并返回按相关性排序（最相关在前）的匹配 [schema.Document] 值。相关性分数和后端特定元数据可在 [schema.Document].MetaData 中获取。
// 设置 [Options.Embedding] 时，实现会先将查询转换为向量再搜索。embedder 必须与索引时使用的模型相同——参见 [indexer.Options.Embedding]。
// [Options.ScoreThreshold] 是过滤条件，不是排序：分数低于阈值的文档会被完全排除。[Options.TopK] 限制返回结果数量。
// Retrieve 可单独使用，也可通过 AddRetrieverNode 添加到 Graph：
// retriever, _ := redis.NewRetriever(ctx, cfg)
// docs, _ := retriever.Retrieve(ctx, "what is eino?", retriever.WithTopK(5))
// graph.AddRetrieverNode("retriever", retriever)
type Retriever interface {
	Retrieve(ctx context.Context, query string, opts ...Option) ([]*schema.Document, error)
}
