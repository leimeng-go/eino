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

package indexer

import "github.com/cloudwego/eino/components/embedding"

// Options is the options for the indexer.
// Options 是索引器的 option。
type Options struct {
	// Index is the index for the indexer, index in different indexers may be different.
	// Index 是索引器使用的索引，不同索引器中的 index 可能不同。
	Index *string
	// SubIndexes is the sub indexes to be indexed.
	// SubIndexes 是要建立索引的子索引。
	SubIndexes []string
	// Embedding is the embedding component.
	// Embedding 是嵌入组件。
	Embedding embedding.Embedder
}

// WithIndex wraps the index option.
// WithIndex 包装 index option。
func WithIndex(index string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Index = &index
		},
	}
}

// WithSubIndexes is the option to set the sub indexes for the indexer.
// WithSubIndexes 是为索引器设置子索引的 option。
func WithSubIndexes(subIndexes []string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.SubIndexes = subIndexes
		},
	}
}

// WithEmbedding is the option to set the embedder for the indexer, which convert document to embeddings.
// WithEmbedding 是为索引器设置 embedder 的 option，用于将文档转换为嵌入。
func WithEmbedding(emb embedding.Embedder) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Embedding = emb
		},
	}
}

// Option is a call-time option for an Indexer.
// Option 是 Indexer 的调用时 option。
type Option struct {
	apply func(opts *Options)

	implSpecificOptFn any
}

// GetCommonOptions extracts standard [Options] from opts, merging onto base.
// Implementors must call this inside Store:
//
//	func (idx *MyIndexer) Store(ctx context.Context, docs []*schema.Document, opts ...indexer.Option) ([]string, error) {
//	    options := indexer.GetCommonOptions(nil, opts...)
//	    // use options.Embedding to generate vectors before storage
//	}
//
// GetCommonOptions 从 opts 中提取标准 [Options]，并合并到 base。
// 实现者必须在 Store 内调用它：
// func (idx *MyIndexer) Store(ctx context.Context, docs []*schema.Document, opts ...indexer.Option) ([]string, error) {
// options := indexer.GetCommonOptions(nil, opts...)
// use options.Embedding to generate vectors before storage
// }
func GetCommonOptions(base *Options, opts ...Option) *Options {
	if base == nil {
		base = &Options{}
	}

	for i := range opts {
		opt := opts[i]
		if opt.apply != nil {
			opt.apply(base)
		}
	}

	return base
}

// WrapImplSpecificOptFn wraps an implementation-specific option function so it
// can be passed alongside standard options. For use by Indexer implementors.
//
// WrapImplSpecificOptFn 包装实现特定的 option 函数，使其可以与标准 option 一起传入。供 Indexer 实现者使用。
func WrapImplSpecificOptFn[T any](optFn func(*T)) Option {
	return Option{
		implSpecificOptFn: optFn,
	}
}

// GetImplSpecificOptions extracts implementation-specific options from opts,
// merging onto base. Call alongside [GetCommonOptions] inside Store.
//
// GetImplSpecificOptions 从 opts 中提取实现特定的 option，并合并到 base。在 Store 内与 [GetCommonOptions] 一起调用。
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
