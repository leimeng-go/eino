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

package schema

const (
	docMetaDataKeySubIndexes   = "_sub_indexes"
	docMetaDataKeyScore        = "_score"
	docMetaDataKeyExtraInfo    = "_extra_info"
	docMetaDataKeyDSL          = "_dsl"
	docMetaDataKeyDenseVector  = "_dense_vector"
	docMetaDataKeySparseVector = "_sparse_vector"
)

// Document is a piece of text with a metadata map. It is the shared currency
// between Loader, Transformer, Indexer, and Retriever components.
//
// Metadata is an open map[string]any that lets pipeline stages attach typed
// values to a document without creating a new struct. Well-known keys are
// managed through typed accessor methods — Score, SubIndexes, DenseVector,
// SparseVector, DSLInfo, ExtraInfo — so callers never need to reference the
// raw key strings.
//
// Transformer implementations should preserve existing metadata and merge new
// keys rather than replacing the map outright, so provenance information
// accumulated by earlier stages is not lost.
//
// Document 是一段文本及其元数据 map。它是 Loader、Transformer、Indexer 和 Retriever 组件之间共享的交换单元。
// Metadata 是开放的 map[string]any，允许流水线阶段向 document 附加类型化值，而无需创建新结构体。约定键通过类型化访问器方法管理 — Score、SubIndexes、DenseVector、SparseVector、DSLInfo、ExtraInfo — 因此调用方无需引用原始键字符串。
// Transformer 实现应保留已有 metadata，并合并新键，而不是直接替换整个 map，以免丢失前序阶段累积的来源信息。
type Document struct {
	// ID is the unique identifier of the document.
	// ID 是 document 的唯一标识。
	ID string `json:"id"`
	// Content is the content of the document.
	// Content 是 document 的内容。
	Content string `json:"content"`
	// MetaData is the metadata of the document, can be used to store extra information.
	// MetaData 是 document 的元数据，可用于存储额外信息。
	MetaData map[string]any `json:"meta_data"`
}

// String returns the content of the document.
// String 返回 document 的内容。
func (d *Document) String() string {
	return d.Content
}

// WithSubIndexes sets the sub-indexes on the document metadata and returns the
// document for chaining. Sub-indexes let an Indexer route a document into
// multiple logical partitions of a vector store simultaneously.
// Use [Document.SubIndexes] to retrieve them.
//
// WithSubIndexes 在 document metadata 上设置 sub-indexes，并返回 document 以便链式调用。Sub-indexes 允许 Indexer 将一个 document 同时路由到向量存储的多个逻辑分区。
// 使用 [Document.SubIndexes] 获取它们。
func (d *Document) WithSubIndexes(indexes []string) *Document {
	if d.MetaData == nil {
		d.MetaData = make(map[string]any)
	}

	d.MetaData[docMetaDataKeySubIndexes] = indexes

	return d
}

// SubIndexes returns the sub indexes of the document.
// can use doc.WithSubIndexes() to set the sub indexes.
//
// SubIndexes 返回 document 的 sub indexes。
// 可以使用 doc.WithSubIndexes() 设置 sub indexes。
func (d *Document) SubIndexes() []string {
	if d.MetaData == nil {
		return nil
	}

	indexes, ok := d.MetaData[docMetaDataKeySubIndexes].([]string)
	if ok {
		return indexes
	}

	return nil
}

// WithScore sets the relevance score on the document, typically written by a
// Retriever after ranking results. A higher score means higher relevance.
// Note: [retriever.WithScoreThreshold] filters by this value, not sort order.
// Use [Document.Score] to retrieve it.
//
// WithScore 设置 document 的相关性分数，通常由 Retriever 在结果排序后写入。分数越高表示相关性越高。
// 注意：[retriever.WithScoreThreshold] 按此值过滤，而不是按排序顺序。
// 使用 [Document.Score] 获取它。
func (d *Document) WithScore(score float64) *Document {
	if d.MetaData == nil {
		d.MetaData = make(map[string]any)
	}

	d.MetaData[docMetaDataKeyScore] = score

	return d
}

// Score returns the score of the document.
// can use doc.WithScore() to set the score.
//
// Score 返回 document 的分数。
// 可以使用 doc.WithScore() 设置分数。
func (d *Document) Score() float64 {
	if d.MetaData == nil {
		return 0
	}

	score, ok := d.MetaData[docMetaDataKeyScore].(float64)
	if ok {
		return score
	}

	return 0
}

// WithExtraInfo sets the extra info of the document.
// can use doc.ExtraInfo() to get the extra info.
//
// WithExtraInfo 设置 document 的 extra info。
// 可以使用 doc.ExtraInfo() 获取 extra info。
func (d *Document) WithExtraInfo(extraInfo string) *Document {
	if d.MetaData == nil {
		d.MetaData = make(map[string]any)
	}

	d.MetaData[docMetaDataKeyExtraInfo] = extraInfo

	return d
}

// ExtraInfo returns the extra info of the document.
// can use doc.WithExtraInfo() to set the extra info.
//
// ExtraInfo 返回 document 的 extra info。
// 可以使用 doc.WithExtraInfo() 设置 extra info。
func (d *Document) ExtraInfo() string {
	if d.MetaData == nil {
		return ""
	}

	extraInfo, ok := d.MetaData[docMetaDataKeyExtraInfo].(string)
	if ok {
		return extraInfo
	}

	return ""
}

// WithDSLInfo attaches a domain-specific-language query description to the
// document. This is consumed by Retriever implementations that support
// structured queries (e.g., filter expressions) alongside vector search.
// Use [Document.DSLInfo] to retrieve it.
//
// WithDSLInfo 将领域特定语言查询描述附加到 document。
// 支持在向量搜索中结合结构化查询（如过滤表达式）的 Retriever 实现会使用它。
// 使用 [Document.DSLInfo] 获取它。
func (d *Document) WithDSLInfo(dslInfo map[string]any) *Document {
	if d.MetaData == nil {
		d.MetaData = make(map[string]any)
	}

	d.MetaData[docMetaDataKeyDSL] = dslInfo

	return d
}

// DSLInfo returns the dsl info of the document.
// can use doc.WithDSLInfo() to set the dsl info.
//
// DSLInfo 返回 document 的 dsl info。
// 可使用 doc.WithDSLInfo() 设置 dsl info。
func (d *Document) DSLInfo() map[string]any {
	if d.MetaData == nil {
		return nil
	}

	dslInfo, ok := d.MetaData[docMetaDataKeyDSL].(map[string]any)
	if ok {
		return dslInfo
	}

	return nil
}

// WithDenseVector sets the dense vector of the document.
// can use doc.DenseVector() to get the dense vector.
//
// WithDenseVector 设置 document 的 dense vector。
// 可使用 doc.DenseVector() 获取 dense vector。
func (d *Document) WithDenseVector(vector []float64) *Document {
	if d.MetaData == nil {
		d.MetaData = make(map[string]any)
	}

	d.MetaData[docMetaDataKeyDenseVector] = vector

	return d
}

// DenseVector returns the dense vector of the document.
// can use doc.WithDenseVector() to set the dense vector.
//
// DenseVector 返回 document 的 dense vector。
// 可使用 doc.WithDenseVector() 设置 dense vector。
func (d *Document) DenseVector() []float64 {
	if d.MetaData == nil {
		return nil
	}

	vector, ok := d.MetaData[docMetaDataKeyDenseVector].([]float64)
	if ok {
		return vector
	}

	return nil
}

// WithSparseVector sets the sparse vector of the document, key indices -> value vector.
// can use doc.SparseVector() to get the sparse vector.
//
// WithSparseVector 设置 document 的 sparse vector，key 为索引，value 为向量值。
// 可使用 doc.SparseVector() 获取 sparse vector。
func (d *Document) WithSparseVector(sparse map[int]float64) *Document {
	if d.MetaData == nil {
		d.MetaData = make(map[string]any)
	}

	d.MetaData[docMetaDataKeySparseVector] = sparse

	return d
}

// SparseVector returns the sparse vector of the document, key indices -> value vector.
// can use doc.WithSparseVector() to set the sparse vector.
//
// SparseVector 返回 document 的 sparse vector，key 为索引，value 为向量值。
// 可使用 doc.WithSparseVector() 设置 sparse vector。
func (d *Document) SparseVector() map[int]float64 {
	if d.MetaData == nil {
		return nil
	}

	sparse, ok := d.MetaData[docMetaDataKeySparseVector].(map[int]float64)
	if ok {
		return sparse
	}

	return nil
}
