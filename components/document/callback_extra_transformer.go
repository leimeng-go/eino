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
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/schema"
)

// TransformerCallbackInput is the input for the transformer callback.
// TransformerCallbackInput 是 transformer 回调的输入。
type TransformerCallbackInput struct {
	// Input is the input documents.
	// Input 是输入文档。
	Input []*schema.Document

	// Extra is the extra information for the callback.
	// Extra 是回调用的额外信息。
	Extra map[string]any
}

// TransformerCallbackOutput is the output for the transformer callback.
// TransformerCallbackOutput 是 transformer 回调的输出。
type TransformerCallbackOutput struct {
	// Output is the output documents.
	// Output 是输出文档。
	Output []*schema.Document

	// Extra is the extra information for the callback.
	// Extra 是回调用的额外信息。
	Extra map[string]any
}

// ConvTransformerCallbackInput converts the callback input to the transformer callback input.
// ConvTransformerCallbackInput 将回调输入转换为 transformer 回调输入。
func ConvTransformerCallbackInput(src callbacks.CallbackInput) *TransformerCallbackInput {
	switch t := src.(type) {
	case *TransformerCallbackInput:
		return t
	case []*schema.Document:
		return &TransformerCallbackInput{
			Input: t,
		}
	default:
		return nil
	}
}

// ConvTransformerCallbackOutput converts the callback output to the transformer callback output.
// ConvTransformerCallbackOutput 将回调输出转换为 transformer 回调输出。
func ConvTransformerCallbackOutput(src callbacks.CallbackOutput) *TransformerCallbackOutput {
	switch t := src.(type) {
	case *TransformerCallbackOutput:
		return t
	case []*schema.Document:
		return &TransformerCallbackOutput{
			Output: t,
		}
	default:
		return nil
	}
}
