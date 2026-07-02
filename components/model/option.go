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

package model

import "github.com/cloudwego/eino/schema"

// Options is the common options for the model.
// Options 是模型的通用选项。
type Options struct {
	// Temperature is the temperature for the model, which controls the randomness of the model.
	// Temperature 是模型的温度，用于控制模型的随机性。
	Temperature *float32
	// Model is the model name.
	// Model 是模型名称。
	Model *string
	// TopP is the top p for the model, which controls the diversity of the model.
	// TopP 是模型的 top p，用于控制模型的多样性。
	TopP *float32
	// Tools is a list of tools the model may call.
	// Tools 是模型可调用的工具列表。
	Tools []*schema.ToolInfo
	// DeferredTools is a list of tools to be registered with defer_loading=true
	// for the model's built-in (server-side) tool search capability.
	// These tools are sent to the model API but not loaded into context upfront —
	// only their names and descriptions are visible to the model. The model's
	// built-in tool search tool searches through them and loads matching ones
	// on demand.
	//
	// DeferredTools 是以 defer_loading=true 注册的工具列表，用于模型内置的服务端工具搜索能力。
	// 这些工具会发送给模型 API，但不会预先加载到上下文中——模型只能看到它们的名称和描述。模型内置的工具搜索工具会搜索它们，并按需加载匹配项。
	DeferredTools []*schema.ToolInfo

	ToolSearchTool *schema.ToolInfo

	// MaxTokens is the max number of tokens, if reached the max tokens, the model will stop generating, and mostly return a finish reason of "length".
	// MaxTokens 是最大 token 数；达到该数量后，模型会停止生成，通常返回 finish reason 为 "length"。
	MaxTokens *int
	// Stop is the stop words for the model, which controls the stopping condition of the model.
	// Stop 是模型的停止词，用于控制模型的停止条件。
	Stop []string

	// Options only available for chat model.
	// 仅适用于 chat model 的选项。

	// ToolChoice controls which tool is called by the model.
	// ToolChoice 控制模型调用哪个工具。
	ToolChoice *schema.ToolChoice
	// AllowedToolNames specifies a list of tool names that the model is allowed to call.
	// This allows for constraining the model to a specific subset of the available tools.
	//
	// AllowedToolNames 指定模型允许调用的工具名称列表。
	// 这可将模型限制为只能使用可用工具中的特定子集。
	AllowedToolNames []string

	// Options only available for agentic model.
	// 仅适用于 agentic model 的选项。

	// AgenticToolChoice controls how the agentic model calls tools.
	// AgenticToolChoice 控制 agentic model 调用工具的方式。
	AgenticToolChoice *schema.AgenticToolChoice
}

// Option is a call-time option for a ChatModel. Options are immutable and
// composable: each Option carries either a common-option setter (applied via
// [GetCommonOptions]) or an implementation-specific setter (applied via
// [GetImplSpecificOptions]), never both.
//
// Option 是 ChatModel 的调用时选项。Option 不可变且可组合：每个 Option 要么携带通用选项 setter（通过 [GetCommonOptions] 应用），要么携带实现特定的 setter（通过 [GetImplSpecificOptions] 应用），二者不会同时存在。
type Option struct {
	apply func(opts *Options)

	implSpecificOptFn any
}

// WithTemperature is the option to set the temperature for the model.
// WithTemperature 是用于设置模型 temperature 的选项。
func WithTemperature(temperature float32) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Temperature = &temperature
		},
	}
}

// WithMaxTokens is the option to set the max tokens for the model.
// WithMaxTokens 是用于设置模型最大 token 数的选项。
func WithMaxTokens(maxTokens int) Option {
	return Option{
		apply: func(opts *Options) {
			opts.MaxTokens = &maxTokens
		},
	}
}

// WithModel is the option to set the model name.
// WithModel 是用于设置模型名称的选项。
func WithModel(name string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Model = &name
		},
	}
}

// WithTopP is the option to set the top p for the model.
// WithTopP 是用于设置模型 top p 的选项。
func WithTopP(topP float32) Option {
	return Option{
		apply: func(opts *Options) {
			opts.TopP = &topP
		},
	}
}

// WithStop is the option to set the stop words for the model.
// WithStop 是用于设置模型停止词的选项。
func WithStop(stop []string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.Stop = stop
		},
	}
}

// WithTools is the option to set tools for the model.
// WithTools 是用于设置模型工具的选项。
func WithTools(tools []*schema.ToolInfo) Option {
	if tools == nil {
		tools = []*schema.ToolInfo{}
	}
	return Option{
		apply: func(opts *Options) {
			opts.Tools = tools
		},
	}
}

// WithToolSearchTool is the option to register a tool search tool with the model.
// When set, the model uses this tool to discover and load deferred tools on demand.
// Note: The tool search tool should NOT be included in WithTools.
//
// WithToolSearchTool 是用于向模型注册工具搜索工具的选项。
// 设置后，模型会使用该工具按需发现并加载延迟工具。
// 注意：工具搜索工具不应包含在 WithTools 中。
func WithToolSearchTool(tool *schema.ToolInfo) Option {
	return Option{
		apply: func(opts *Options) {
			opts.ToolSearchTool = tool
		},
	}
}

// WithDeferredTools is the option to set deferred tools for the model's
// built-in (server-side) tool search. These tools are registered with
// defer_loading=true so the model can discover and load them on demand
// via its native tool search capability.
// Note: Deferred tools should NOT be included in WithTools.
//
// WithDeferredTools 是用于设置模型内置服务端工具搜索所用延迟工具的选项。
// 这些工具会以 defer_loading=true 注册，使模型可通过其原生工具搜索能力按需发现并加载它们。
// 注意：延迟工具不应包含在 WithTools 中。
func WithDeferredTools(tools []*schema.ToolInfo) Option {
	if tools == nil {
		tools = []*schema.ToolInfo{}
	}
	return Option{
		apply: func(opts *Options) {
			opts.DeferredTools = tools
		},
	}
}

// WithToolChoice sets the tool choice for the model. It also allows for providing a list of
// tool names to constrain the model to a specific subset of the available tools.
// Only available for ChatModel.
//
// WithToolChoice 设置模型的工具选择。它还允许提供工具名称列表，将模型限制为只能使用可用工具中的特定子集。
// 仅适用于 ChatModel。
func WithToolChoice(toolChoice schema.ToolChoice, allowedToolNames ...string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.ToolChoice = &toolChoice
			opts.AllowedToolNames = allowedToolNames
		},
	}
}

// WithAgenticToolChoice is the option to set tool choice for the agentic model.
// Only available for AgenticModel.
//
// WithAgenticToolChoice 是用于设置 agentic model 工具选择的选项。
// 仅适用于 AgenticModel。
func WithAgenticToolChoice(toolChoice *schema.AgenticToolChoice) Option {
	return Option{
		apply: func(opts *Options) {
			opts.AgenticToolChoice = toolChoice
		},
	}
}

// WrapImplSpecificOptFn is the option to wrap the implementation specific option function.
// WrapImplSpecificOptFn wraps an implementation-specific option function into
// an [Option] so it can be passed alongside standard options.
//
// This is intended for ChatModel implementors, not callers. Define a typed
// setter for your own config struct and expose it as an Option:
//
//	// In your implementation package:
//	func WithMyParam(v string) model.Option {
//	    return model.WrapImplSpecificOptFn(func(o *MyOptions) {
//	        o.MyParam = v
//	    })
//	}
//
// Callers can then mix standard and implementation-specific options freely:
//
//	model.Generate(ctx, msgs,
//	    model.WithTemperature(0.7),
//	    mypkg.WithMyParam("value"),
//	)
//
// WrapImplSpecificOptFn 将实现特定的 option 函数包装为 [Option]，以便与标准 options 一起传入。
// 供 ChatModel 实现者使用，而非调用者。请为自己的配置结构定义类型化 setter，并将其暴露为 Option：
// 在你的实现包中：
// func WithMyParam(v string) model.Option {
// return model.WrapImplSpecificOptFn(func(o *MyOptions) {
// o.MyParam = v
// })
// }
// 调用者随后可以自由混用标准和实现特定的 options：
// model.Generate(ctx, msgs,
// model.WithTemperature(0.7),
// mypkg.WithMyParam("value"),
// )
func WrapImplSpecificOptFn[T any](optFn func(*T)) Option {
	return Option{
		implSpecificOptFn: optFn,
	}
}

// GetCommonOptions extracts standard [Options] from an Option list, merging
// them onto base. If base is nil, a zero-value Options is used.
//
// Implementors must call this to honour options passed by callers:
//
//	func (m *MyModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
//	    options := model.GetCommonOptions(&model.Options{Temperature: &m.defaultTemp}, opts...)
//	    // use options.Temperature, options.Tools, etc.
//	}
//
// GetCommonOptions 从 Option 列表中提取标准 [Options]，并合并到 base。若 base 为 nil，则使用零值 Options。
// 实现者必须调用它，以支持调用者传入的 options：
// func (m *MyModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
// options := model.GetCommonOptions(&model.Options{Temperature: &m.defaultTemp}, opts...)
// 使用 options.Temperature、options.Tools 等
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

// GetImplSpecificOptions extracts implementation-specific options from an
// Option list, merging them onto base. If base is nil, a zero-value T is used.
//
// Call this alongside [GetCommonOptions] to support both standard and custom
// options in your implementation:
//
//	type MyOptions struct { MyParam string }
//
//	func (m *MyModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
//	    common  := model.GetCommonOptions(nil, opts...)
//	    myOpts  := model.GetImplSpecificOptions(&MyOptions{MyParam: "default"}, opts...)
//	    // use common.Temperature, myOpts.MyParam, etc.
//	}
//
// GetImplSpecificOptions 从 Option 列表中提取实现特定的 options，并合并到 base。若 base 为 nil，则使用零值 T。
// 在实现中与 [GetCommonOptions] 一起调用，以同时支持标准和自定义 options：
// type MyOptions struct { MyParam string }
// func (m *MyModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
// common  := model.GetCommonOptions(nil, opts...)
// myOpts  := model.GetImplSpecificOptions(&MyOptions{MyParam: "default"}, opts...)
// 使用 common.Temperature、myOpts.MyParam 等
// }
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
