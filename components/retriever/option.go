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

import "github.com/cloudwego/eino/components/embedding"

// Options is the options for the retriever.
// Options 是检索器的选项。
type Options struct {
	// Index is the index for the retriever, index in different retriever may be different.
	// Index 是检索器的索引，不同检索器中的 index 可能不同。
	Index *string
	// SubIndex is the sub index for the retriever, sub index in different retriever may be different.
	// SubIndex 是检索器的子索引，不同检索器中的 sub index 可能不同。
	SubIndex *string
	// TopK is the top k for the retriever, which means the top number of documents to retrieve.
	// TopK 是检索器的 top k，表示要检索的文档数量上限。
	TopK *int
	// ScoreThreshold is the score threshold for the retriever, eg 0.5 means the score of the document must be greater than 0.5.
	// ScoreThreshold 是检索器的分数阈值，例如 0.5 表示文档分数必须大于 0.5。
	ScoreThreshold *float64
	// Embedding is the embedder for the retriever, which is used to embed the query for retrieval	.
	// Embedding 是检索器的 embedder，用于嵌入查询以便检索。
	Embedding embedding.Embedder

	// DSLInfo carries backend-specific filter/query expressions. The structure and
	// semantics are defined by the underlying store implementation.
	//
	// DSLInfo 携带后端特定的过滤/查询表达式。其结构和语义由底层存储实现定义。
	DSLInfo map[string]any
}

// WithIndex wraps the index option.
// WithIndex 包装 index 选项。
func WithIndex(index string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Index = &index
		},
	}
}

// WithSubIndex wraps the sub index option.
// WithSubIndex 包装 sub index 选项。
func WithSubIndex(subIndex string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.SubIndex = &subIndex
		},
	}
}

// WithTopK wraps the top k option.
// WithTopK 包装 top k 选项。
func WithTopK(topK int) Option {
	return Option{
		apply: func(opts *Options) {
			opts.TopK = &topK
		},
	}
}

// WithScoreThreshold wraps the score threshold option.
// WithScoreThreshold 包装 score threshold 选项。
func WithScoreThreshold(threshold float64) Option {
	return Option{
		apply: func(opts *Options) {
			opts.ScoreThreshold = &threshold
		},
	}
}

// WithEmbedding wraps the embedder option.
// WithEmbedding 包装 embedder 选项。
func WithEmbedding(emb embedding.Embedder) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Embedding = emb
		},
	}
}

// WithDSLInfo wraps the dsl info option.
// WithDSLInfo 包装 dsl info 选项。
func WithDSLInfo(dsl map[string]any) Option {
	return Option{
		apply: func(opts *Options) {
			opts.DSLInfo = dsl
		},
	}
}

// Option is a call-time option for a Retriever.
// Option 是 Retriever 的调用时选项。
type Option struct {
	apply func(opts *Options)

	implSpecificOptFn any
}

// GetCommonOptions extracts standard [Options] from opts, merging onto base.
// Implementors must call this to honour caller-provided options:
//
//	func (r *MyRetriever) Retrieve(ctx context.Context, query string, opts ...retriever.Option) ([]*schema.Document, error) {
//	    options := retriever.GetCommonOptions(&retriever.Options{TopK: &r.defaultTopK}, opts...)
//	    // use options.TopK, options.ScoreThreshold, options.Embedding, etc.
//	}
//
// GetCommonOptions 从 opts 中提取标准 [Options]，并合并到 base。
// 实现者必须调用它以遵循调用方提供的选项：
// func (r *MyRetriever) Retrieve(ctx context.Context, query string, opts ...retriever.Option) ([]*schema.Document, error) {
// options := retriever.GetCommonOptions(&retriever.Options{TopK: &r.defaultTopK}, opts...)
// 使用 options.TopK、options.ScoreThreshold、options.Embedding 等。
// }
func GetCommonOptions(base *Options, opts ...Option) *Options {
	if base == nil {
		base = &Options{}
	}

	for i := range opts {
		if opts[i].apply != nil {
			opts[i].apply(base)
		}
	}

	return base
}

// WrapImplSpecificOptFn wraps an implementation-specific option function so it
// can be passed alongside standard options. For use by Retriever implementors.
//
// WrapImplSpecificOptFn 包装实现特定的选项函数，使其可与标准选项一起传入。供 Retriever 实现者使用。
func WrapImplSpecificOptFn[T any](optFn func(*T)) Option {
	return Option{
		implSpecificOptFn: optFn,
	}
}

// GetImplSpecificOptions extracts implementation-specific options from opts,
// merging onto base. Call alongside [GetCommonOptions] inside Retrieve.
//
// GetImplSpecificOptions 从 opts 中提取实现特定的选项，并合并到 base。
// 在 Retrieve 内与 [GetCommonOptions] 一起调用。
func GetImplSpecificOptions[T any](base *T, opts ...Option) *T {
	if base == nil {
		base = new(T)
	}

	for i := range opts {
		opt := opts[i]
		if opt.implSpecificOptFn != nil {
			optFn, ok := opt.implSpecificOptFn.(func(*T))
			if ok {
				optFn(base)
			}
		}
	}

	return base
}
