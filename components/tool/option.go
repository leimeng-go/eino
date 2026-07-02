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

// Option defines call option for InvokableTool or StreamableTool component, which is part of component interface signature.
// Each tool implementation could define its own options struct and option funcs within its own package,
// then wrap the impl specific option funcs into this type, before passing to InvokableRun or StreamableRun.
//
// Option 定义 InvokableTool 或 StreamableTool 组件的调用选项，是组件接口签名的一部分。
// 每个工具实现都可以在自己的包内定义自己的 options struct 和 option funcs，
// 然后在传给 InvokableRun 或 StreamableRun 前，将实现专用的 option funcs 包装为此类型。
type Option struct {
	implSpecificOptFn any
}

// WrapImplSpecificOptFn wraps the impl specific option functions into Option type.
// T: the type of the impl specific options struct.
// Tool implementations are required to use this function to convert its own option functions into the unified Option type.
// For example, if the tool defines its own options struct:
//
//	type customOptions struct {
//	    conf string
//	}
//
// Then the tool needs to provide an option function as such:
//
//	func WithConf(conf string) Option {
//	    return WrapImplSpecificOptFn(func(o *customOptions) {
//			o.conf = conf
//		}
//	}
//
// .
//
// WrapImplSpecificOptFn 将实现专用的 option functions 包装为 Option 类型。
// T: 实现专用 options struct 的类型。
// 工具实现需要使用此函数将自己的 option functions 转换为统一的 Option 类型。
// 例如，如果工具定义了自己的 options struct：
// type customOptions struct {
// conf string
// }
// 则工具需要提供如下 option function：
// func WithConf(conf string) Option {
// return WrapImplSpecificOptFn(func(o *customOptions) {
// o.conf = conf
// }
// }
// .
func WrapImplSpecificOptFn[T any](optFn func(*T)) Option {
	return Option{
		implSpecificOptFn: optFn,
	}
}

// GetImplSpecificOptions provides tool author the ability to extract their own custom options from the unified Option type.
// T: the type of the impl specific options struct.
// This function should be used within the tool implementation's InvokableRun or StreamableRun functions.
// It is recommended to provide a base T as the first argument, within which the tool author can provide default values for the impl specific options.
// eg.
//
//	type customOptions struct {
//	    conf string
//	}
//	defaultOptions := &customOptions{}
//
//	customOptions := tool.GetImplSpecificOptions(defaultOptions, opts...)
//
// GetImplSpecificOptions 让工具作者能够从统一的 Option 类型中提取自己的自定义选项。
// T: 实现专用 options struct 的类型。
// 此函数应在工具实现的 InvokableRun 或 StreamableRun 函数中使用。
// 建议将基础 T 作为第一个参数，工具作者可在其中提供实现专用选项的默认值。
// 例如：
// type customOptions struct {
// conf string
// }
// defaultOptions := &customOptions{}
// customOptions := tool.GetImplSpecificOptions(defaultOptions, opts...)
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
