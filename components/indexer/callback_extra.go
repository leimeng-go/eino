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

import (
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/schema"
)

// CallbackInput is the input for the indexer callback.
// CallbackInput 是 indexer 回调的输入。
type CallbackInput struct {
	// Docs is the documents to be indexed.
	// Docs 是待索引的文档。
	Docs []*schema.Document
	// Extra is the extra information for the callback.
	// Extra 是回调的额外信息。
	Extra map[string]any
}

// CallbackOutput is the output for the indexer callback.
// CallbackOutput 是 indexer 回调的输出。
type CallbackOutput struct {
	// IDs is the ids of the indexed documents returned by the indexer.
	// IDs 是 indexer 返回的已索引文档 ID。
	IDs []string
	// Extra is the extra information for the callback.
	// Extra 是回调的额外信息。
	Extra map[string]any
}

// ConvCallbackInput converts the callback input to the indexer callback input.
// ConvCallbackInput 将回调输入转换为索引器回调输入。
func ConvCallbackInput(src callbacks.CallbackInput) *CallbackInput {
	switch t := src.(type) {
	case *CallbackInput:
		return t
	case []*schema.Document:
		return &CallbackInput{
			Docs: t,
		}
	default:
		return nil
	}
}

// ConvCallbackOutput converts the callback output to the indexer callback output.
// ConvCallbackOutput 将回调输出转换为索引器回调输出。
func ConvCallbackOutput(src callbacks.CallbackOutput) *CallbackOutput {
	switch t := src.(type) {
	case *CallbackOutput:
		return t
	case []string:
		return &CallbackOutput{
			IDs: t,
		}
	default:
		return nil
	}
}
