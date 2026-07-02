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

package embedding

import (
	"github.com/cloudwego/eino/callbacks"
)

// TokenUsage is the token usage for the embedding.
// TokenUsage 是嵌入的 token 使用情况。
type TokenUsage struct {
	// PromptTokens is the number of prompt tokens.
	// PromptTokens 是提示 token 数。
	PromptTokens int
	// CompletionTokens is the number of completion tokens.
	// CompletionTokens 是补全 token 数。
	CompletionTokens int
	// TotalTokens is the total number of tokens.
	// TotalTokens 是 token 总数。
	TotalTokens int
}

// Config is the config for the embedding.
// Config 是嵌入的配置。
type Config struct {
	// Model is the model name.
	// Model 是模型名称。
	Model string
	// EncodingFormat is the encoding format.
	// EncodingFormat 是编码格式。
	EncodingFormat string
}

// ComponentExtra is the extra information for the embedding.
// ComponentExtra 是嵌入的额外信息。
type ComponentExtra struct {
	// Config is the config for the embedding.
	// Config 是嵌入的配置。
	Config *Config
	// TokenUsage is the token usage for the embedding.
	// TokenUsage 是嵌入的 token 使用情况。
	TokenUsage *TokenUsage
}

// CallbackInput is the input for the embedding callback.
// CallbackInput 是嵌入回调的输入。
type CallbackInput struct {
	// Texts is the texts to be embedded.
	// Texts 是待嵌入的文本。
	Texts []string
	// Config is the config for the embedding.
	// Config 是嵌入的配置。
	Config *Config
	// Extra is the extra information for the callback.
	// Extra 是回调的额外信息。
	Extra map[string]any
}

// CallbackOutput is the output for the embedding callback.
// CallbackOutput 是 embedding 回调的输出。
type CallbackOutput struct {
	// Embeddings is the embeddings.
	// Embeddings 是嵌入结果。
	Embeddings [][]float64
	// Config is the config for creating the embedding.
	// Config 是创建 embedding 的配置。
	Config *Config
	// TokenUsage is the token usage for the embedding.
	// TokenUsage 是 embedding 的 token 用量。
	TokenUsage *TokenUsage
	// Extra is the extra information for the callback.
	// Extra 是回调的额外信息。
	Extra map[string]any
}

// ConvCallbackInput converts the callback input to the embedding callback input.
// ConvCallbackInput 将回调输入转换为 embedding 回调输入。
func ConvCallbackInput(src callbacks.CallbackInput) *CallbackInput {
	switch t := src.(type) {
	case *CallbackInput:
		return t
	case []string:
		return &CallbackInput{
			Texts: t,
		}
	default:
		return nil
	}
}

// ConvCallbackOutput converts the callback output to the embedding callback output.
// ConvCallbackOutput 将回调输出转换为 embedding 回调输出。
func ConvCallbackOutput(src callbacks.CallbackOutput) *CallbackOutput {
	switch t := src.(type) {
	case *CallbackOutput:
		return t
	case [][]float64:
		return &CallbackOutput{
			Embeddings: t,
		}
	default:
		return nil
	}
}
