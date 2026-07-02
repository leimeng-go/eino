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

package document

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

// Source identifies the external location of a document.
// URI can be a local file path or a remote URL reachable by the loader.
//
// Source 标识文档的外部位置。
// URI 可以是本地文件路径，也可以是 loader 可访问的远程 URL。
type Source struct {
	URI string
}

//go:generate  mockgen -destination ../../internal/mock/components/document/document_mock.go --package document -source interface.go

// Loader reads raw content from an external source and returns it as a slice
// of [schema.Document] values.
//
// The Source.URI may be a local file path or a remote URL. The loader is
// responsible for fetching the raw bytes; actual format parsing is typically
// delegated to a [parser.Parser] configured on the loader via
// [WithParserOptions].
//
// Document metadata ([schema.Document].MetaData) should be populated with at
// least the source URI so that downstream nodes can trace document provenance.
//
// Loader 从外部来源读取原始内容，并以 [schema.Document] 值切片返回。
// Source.URI 可以是本地文件路径，也可以是远程 URL。loader 负责获取原始字节；实际格式解析通常委托给通过 [WithParserOptions] 配置在 loader 上的 [parser.Parser]。
// 文档元数据（[schema.Document].MetaData）至少应填充 source URI，以便下游节点可以追踪文档来源。
type Loader interface {
	Load(ctx context.Context, src Source, opts ...LoaderOption) ([]*schema.Document, error)
}

// Transformer converts a slice of [schema.Document] values into another slice,
// applying operations such as splitting, filtering, merging, or re-ranking.
//
// Implementations should preserve existing MetaData keys and merge rather than
// replace when adding their own metadata. Downstream nodes (e.g. Indexer,
// Retriever) may depend on metadata set by earlier pipeline stages.
//
// Transformer 将一组 [schema.Document] 值转换为另一组，执行拆分、过滤、合并或重排序等操作。
// 实现添加自己的元数据时，应保留已有 MetaData 键并进行合并，而不是替换。下游节点（如 Indexer、Retriever）可能依赖早期 pipeline 阶段设置的元数据。
type Transformer interface {
	Transform(ctx context.Context, src []*schema.Document, opts ...TransformerOption) ([]*schema.Document, error)
}
