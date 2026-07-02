/*
 * Copyright 2026 CloudWeGo Authors
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

package prompt

import (
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/schema"
)

// AgenticCallbackInput is the input for the callback.
// AgenticCallbackInput 是回调的输入。
type AgenticCallbackInput struct {
	// Variables is the variables for the callback.
	// Variables 是回调的变量。
	Variables map[string]any
	// Templates is the agentic templates for the callback.
	// Templates 是回调的 agentic templates。
	Templates []schema.AgenticMessagesTemplate
	// Extra is the extra information for the callback.
	// Extra 是回调的额外信息。
	Extra map[string]any
}

// AgenticCallbackOutput is the output for the callback.
// AgenticCallbackOutput 是回调的输出。
type AgenticCallbackOutput struct {
	// Result is the agentic result for the callback.
	// Result 是回调的 agentic result。
	Result []*schema.AgenticMessage
	// Templates is the agentic templates for the callback.
	// Templates 是回调的 agentic templates。
	Templates []schema.AgenticMessagesTemplate
	// Extra is the extra information for the callback.
	// Extra 是回调的额外信息。
	Extra map[string]any
}

// ConvAgenticCallbackInput converts the callback input to the agentic prompt callback input.
// ConvAgenticCallbackInput 将回调输入转换为 agentic prompt 回调输入。
func ConvAgenticCallbackInput(src callbacks.CallbackInput) *AgenticCallbackInput {
	switch t := src.(type) {
	case *AgenticCallbackInput:
		return t
	case map[string]any:
		return &AgenticCallbackInput{
			Variables: t,
		}
	default:
		return nil
	}
}

// ConvAgenticCallbackOutput converts the callback output to the agentic prompt callback output.
// ConvAgenticCallbackOutput 将回调输出转换为 agentic prompt 回调输出。
func ConvAgenticCallbackOutput(src callbacks.CallbackOutput) *AgenticCallbackOutput {
	switch t := src.(type) {
	case *AgenticCallbackOutput:
		return t
	case []*schema.AgenticMessage:
		return &AgenticCallbackOutput{
			Result: t,
		}
	default:
		return nil
	}
}
