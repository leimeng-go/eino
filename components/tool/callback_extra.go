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

package tool

import (
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/schema"
)

// CallbackInput is the input for the tool callback.
// CallbackInput 是工具回调的输入。
type CallbackInput struct {
	// ArgumentsInJSON is the arguments in json format for the tool.
	// ArgumentsInJSON 是工具的 json 格式参数。
	ArgumentsInJSON string
	// Extra is the extra information for the tool.
	// Extra 是工具的额外信息。
	Extra map[string]any
}

// CallbackOutput is the output for the tool callback.
// CallbackOutput 是工具回调的输出。
type CallbackOutput struct {
	// Response is the response for the tool.
	// Response 是工具的响应。
	Response string
	// ToolOutput is the multimodal output for the tool. Used when the tool returns structured data.
	// ToolOutput 是工具的多模态输出。用于工具返回结构化数据时。
	ToolOutput *schema.ToolResult
	// Extra is the extra information for the tool.
	// Extra 是工具的额外信息。
	Extra map[string]any
}

// ConvCallbackInput converts the callback input to the tool callback input.
// ConvCallbackInput 将 callback input 转换为工具回调输入。
func ConvCallbackInput(src callbacks.CallbackInput) *CallbackInput {
	switch t := src.(type) {
	case *CallbackInput:
		return t
	case string:
		return &CallbackInput{ArgumentsInJSON: t}
	case *schema.ToolArgument:
		return &CallbackInput{ArgumentsInJSON: t.Text}
	default:
		return nil
	}
}

// ConvCallbackOutput converts the callback output to the tool callback output.
// ConvCallbackOutput 将 callback output 转换为工具回调输出。
func ConvCallbackOutput(src callbacks.CallbackOutput) *CallbackOutput {
	switch t := src.(type) {
	case *CallbackOutput:
		return t
	case string:
		return &CallbackOutput{Response: t}
	case *schema.ToolResult:
		return &CallbackOutput{ToolOutput: t}
	default:
		return nil
	}
}
