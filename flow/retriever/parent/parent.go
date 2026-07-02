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

package parent

import (
	"context"
	"fmt"

	"github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/schema"
)

// Config configures the parent retriever.
// Config 配置父文档检索器。
type Config struct {
	// Retriever specifies the original retriever used to retrieve documents.
	// For example: a vector database retriever like Milvus, or a full-text search retriever like Elasticsearch.
	//
	// Retriever 指定用于检索文档的原始检索器。
	// 例如：像 Milvus 这样的向量数据库检索器，或像 Elasticsearch 这样的全文搜索检索器。
	Retriever retriever.Retriever
	// ParentIDKey specifies the key used in the sub-document metadata to store the parent document ID.
	// Documents without this key will be removed from the recall results.
	// For example: if ParentIDKey is "parent_id", it will look for metadata like:
	// {"parent_id": "original_doc_123"}
	//
	// ParentIDKey 指定子文档 metadata 中用于存储父文档 ID 的键。
	// 没有该键的文档会从召回结果中移除。
	// 例如：如果 ParentIDKey 为 "parent_id"，则会查找如下 metadata：
	// {"parent_id": "original_doc_123"}
	ParentIDKey string
	// OrigDocGetter specifies the method for getting original documents by ids from the sub-document metadata.
	// Parameters:
	//   - ctx: context for the operation
	//   - ids: slice of parent document IDs to retrieve
	// Returns:
	//   - []*schema.Document: slice of retrieved parent documents
	//   - error: any error encountered during retrieval
	//
	// For example: if sub-documents with parent IDs ["doc_1", "doc_2"] are retrieved,
	// OrigDocGetter will be called to fetch the original documents with these IDs.
	//
	// OrigDocGetter 指定根据子文档 metadata 中的 ids 获取原始文档的方法。
	// 参数：
	// - ctx：操作的 context
	// - ids：要检索的父文档 ID 切片
	// 返回：
	// - []*schema.Document：检索到的父文档切片
	// - error：检索过程中遇到的任何错误
	// 例如：如果检索到父 ID 为 ["doc_1", "doc_2"] 的子文档，
	// 则会调用 OrigDocGetter 获取这些 ID 对应的原始文档。
	OrigDocGetter func(ctx context.Context, ids []string) ([]*schema.Document, error)
}

// NewRetriever creates a new parent retriever that handles retrieving original documents
// based on sub-document search results.
//
// Parameters:
//   - ctx: context for the operation
//   - config: configuration for the parent retriever
//
// Example usage:
//
//	retriever, err := NewRetriever(ctx, &Config{
//	    Retriever: milvusRetriever,
//	    ParentIDKey: "source_doc_id",
//	    OrigDocGetter: func(ctx context.Context, ids []string) ([]*schema.Document, error) {
//	        return documentStore.GetByIDs(ctx, ids)
//	    },
//	})
//
// Returns:
//   - retriever.Retriever: the created parent retriever
//   - error: any error encountered during creation
//
// NewRetriever 创建一个新的父文档检索器，用于根据子文档搜索结果检索原始文档。
// 参数：
// - ctx：操作的 context
// - config：父文档检索器的配置
// 用法示例：
// retriever, err := NewRetriever(ctx, &Config{
// Retriever: milvusRetriever,
// ParentIDKey: "source_doc_id",
// OrigDocGetter: func(ctx context.Context, ids []string) ([]*schema.Document, error) {
// return documentStore.GetByIDs(ctx, ids)
// },
// })
// 返回：
// - retriever.Retriever：创建的父文档检索器
// - error：创建过程中遇到的任何错误
func NewRetriever(ctx context.Context, config *Config) (retriever.Retriever, error) {
	if config.Retriever == nil {
		return nil, fmt.Errorf("retriever is required")
	}
	if config.OrigDocGetter == nil {
		return nil, fmt.Errorf("orig doc getter is required")
	}
	return &parentRetriever{
		retriever:     config.Retriever,
		parentIDKey:   config.ParentIDKey,
		origDocGetter: config.OrigDocGetter,
	}, nil
}

type parentRetriever struct {
	retriever     retriever.Retriever
	parentIDKey   string
	origDocGetter func(ctx context.Context, ids []string) ([]*schema.Document, error)
}

func (p *parentRetriever) Retrieve(ctx context.Context, query string, opts ...retriever.Option) ([]*schema.Document, error) {
	subDocs, err := p.retriever.Retrieve(ctx, query, opts...)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(subDocs))
	for _, subDoc := range subDocs {
		if k, ok := subDoc.MetaData[p.parentIDKey]; ok {
			if s, okk := k.(string); okk && !inList(s, ids) {
				ids = append(ids, s)
			}
		}
	}
	return p.origDocGetter(ctx, ids)
}

func inList(elem string, list []string) bool {
	for _, v := range list {
		if v == elem {
			return true
		}
	}
	return false
}
