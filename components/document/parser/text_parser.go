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

package parser

import (
	"context"
	"io"

	"github.com/cloudwego/eino/schema"
)

const (
	// MetaKeySource is the metadata key storing the document's source URI.
	// MetaKeySource 是用于存储文档源 URI 的元数据键。
	MetaKeySource = "_source"
)

// TextParser is a simple parser that reads the text from a reader and returns a single document.
// eg:
//
//	docs, err := TextParser.Parse(ctx, strings.NewReader("hello world"))
//	fmt.Println(docs[0].Content) // "hello world"
//
// TextParser 是一个简单解析器，从读取器读取文本并返回单个文档。
// 例如：
// docs, err := TextParser.Parse(ctx, strings.NewReader("hello world"))
// fmt.Println(docs[0].Content) // "hello world"
type TextParser struct{}

// Parse reads the text from a reader and returns a single document.
// Parse 从读取器读取文本并返回单个文档。
func (dp TextParser) Parse(ctx context.Context, reader io.Reader, opts ...Option) ([]*schema.Document, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	opt := GetCommonOptions(&Options{}, opts...)

	meta := make(map[string]any)
	meta[MetaKeySource] = opt.URI

	for k, v := range opt.ExtraMeta {
		meta[k] = v
	}

	doc := &schema.Document{
		Content:  string(data),
		MetaData: meta,
	}

	return []*schema.Document{doc}, nil
}
