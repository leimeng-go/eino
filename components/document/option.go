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

import "github.com/cloudwego/eino/components/document/parser"

// LoaderOptions configures document loaders, including parser options.
// LoaderOptions 配置文档 loader，包括 parser 选项。
type LoaderOptions struct {
	ParserOptions []parser.Option
}

// LoaderOption defines call option for Loader component, which is part of the component interface signature.
// Each Loader implementation could define its own options struct and option funcs within its own package,
// then wrap the impl specific option funcs into this type, before passing to Load.
//
// LoaderOption 定义 Loader 组件的调用选项，是组件接口签名的一部分。
// 每个 Loader 实现都可以在自己的 package 中定义自己的 options struct 和 option funcs，
// 然后在传给 Load 前，将实现专用的 option funcs 包装成此类型。
type LoaderOption struct {
	apply func(opts *LoaderOptions)

	implSpecificOptFn any
}

// WrapLoaderImplSpecificOptFn wraps the impl specific option functions into LoaderOption type.
// T: the type of the impl specific options struct.
// Loader implementations are required to use this function to convert its own option functions into the unified LoaderOption type.
// For example, if the Loader impl defines its own options struct:
//
//	type customOptions struct {
//	    conf string
//	}
//
// Then the impl needs to provide an option function as such:
//
//	func WithConf(conf string) Option {
//	    return WrapLoaderImplSpecificOptFn(func(o *customOptions) {
//			o.conf = conf
//		}
//	}
//
// WrapLoaderImplSpecificOptFn 将实现专用的选项函数包装为 LoaderOption 类型。
// T：实现专用 options struct 的类型。
// Loader 实现必须使用此函数，将自己的选项函数转换为统一的 LoaderOption 类型。
// 例如，如果 Loader 实现定义了自己的 options struct：
// type customOptions struct {
// conf string
// }
// 则该实现需要提供如下选项函数：
// func WithConf(conf string) Option {
// return WrapLoaderImplSpecificOptFn(func(o *customOptions) {
// o.conf = conf
// }
// }
func WrapLoaderImplSpecificOptFn[T any](optFn func(*T)) LoaderOption {
	return LoaderOption{
		implSpecificOptFn: optFn,
	}
}

// GetLoaderImplSpecificOptions provides Loader author the ability to extract their own custom options from the unified LoaderOption type.
// T: the type of the impl specific options struct.
// This function should be used within the Loader implementation's Load function.
// It is recommended to provide a base T as the first argument, within which the Loader author can provide default values for the impl specific options.
// eg.
//
//	myOption := &MyOption{
//		Field1: "default_value",
//	}
//	myOption := loader.GetLoaderImplSpecificOptions(myOption, opts...)
//
// GetLoaderImplSpecificOptions 让 Loader 作者能从统一的 LoaderOption 类型中提取自己的自定义选项。
// T：实现特定选项结构体的类型。
// 此函数应在 Loader 实现的 Load 函数中使用。
// 建议将一个基础 T 作为第一个参数传入，Loader 作者可在其中为实现特定选项提供默认值。
// 例如：
// myOption := &MyOption{
// Field1: "default_value",
// }
// myOption := loader.GetLoaderImplSpecificOptions(myOption, opts...)
func GetLoaderImplSpecificOptions[T any](base *T, opts ...LoaderOption) *T {
	if base == nil {
		base = new(T)
	}

	for i := range opts {
		opt := opts[i]
		if opt.implSpecificOptFn != nil {
			s, ok := opt.implSpecificOptFn.(func(*T))
			if ok {
				s(base)
			}
		}
	}

	return base
}

// GetLoaderCommonOptions extract loader Options from Option list, optionally providing a base Options with default values.
// GetLoaderCommonOptions 从 Option 列表中提取 loader Options，可选提供带默认值的基础 Options。
func GetLoaderCommonOptions(base *LoaderOptions, opts ...LoaderOption) *LoaderOptions {
	if base == nil {
		base = &LoaderOptions{}
	}

	for i := range opts {
		opt := opts[i]
		if opt.apply != nil {
			opt.apply(base)
		}
	}

	return base
}

// WithParserOptions attaches parser options to a loader request.
// WithParserOptions 将 parser 选项附加到 loader 请求。
func WithParserOptions(opts ...parser.Option) LoaderOption {
	return LoaderOption{
		apply: func(o *LoaderOptions) {
			o.ParserOptions = opts
		},
	}
}

// TransformerOption defines call option for Transformer component, which is part of the component interface signature.
// Each Transformer implementation could define its own options struct and option funcs within its own package,
// then wrap the impl specific option funcs into this type, before passing to Transform.
//
// TransformerOption 定义 Transformer 组件的调用选项，它是组件接口签名的一部分。
// 每个 Transformer 实现都可以在自己的 package 中定义自己的选项结构体和选项函数，
// 然后在传给 Transform 前，将实现特定的选项函数包装成此类型。
type TransformerOption struct {
	implSpecificOptFn any
}

// WrapTransformerImplSpecificOptFn wraps the impl specific option functions into TransformerOption type.
// T: the type of the impl specific options struct.
// Transformer implementations are required to use this function to convert its own option functions into the unified TransformerOption type.
// For example, if the Transformer impl defines its own options struct:
//
//	type customOptions struct {
//	    conf string
//	}
//
// Then the impl needs to provide an option function as such:
//
//	func WithConf(conf string) TransformerOption {
//	    return WrapTransformerImplSpecificOptFn(func(o *customOptions) {
//			o.conf = conf
//		}
//	}
//
// .
//
// WrapTransformerImplSpecificOptFn 将实现特定的选项函数包装成 TransformerOption 类型。
// T：实现特定选项结构体的类型。
// Transformer 实现需要使用此函数，将自己的选项函数转换为统一的 TransformerOption 类型。
// 例如，如果 Transformer 实现定义了自己的选项结构体：
// type customOptions struct {
// conf string
// }
// 则该实现需要提供如下选项函数：
// func WithConf(conf string) TransformerOption {
// return WrapTransformerImplSpecificOptFn(func(o *customOptions) {
// o.conf = conf
// }
// }
// .
func WrapTransformerImplSpecificOptFn[T any](optFn func(*T)) TransformerOption {
	return TransformerOption{
		implSpecificOptFn: optFn,
	}
}

// GetTransformerImplSpecificOptions provides Transformer author the ability to extract their own custom options from the unified TransformerOption type.
// T: the type of the impl specific options struct.
// This function should be used within the Transformer implementation's Transform function.
// It is recommended to provide a base T as the first argument, within which the Transformer author can provide default values for the impl specific options.
// eg.
//
//	myOption := &MyOption{
//		Field1: "default_value",
//	}
//	myOption := transformer.GetTransformerImplSpecificOptions(myOption, opts...)
//
// GetTransformerImplSpecificOptions 让 Transformer 作者能从统一的 TransformerOption 类型中提取自己的自定义选项。
// T：实现特定选项结构体的类型。
// 此函数应在 Transformer 实现的 Transform 函数中使用。
// 建议将一个基础 T 作为第一个参数传入，Transformer 作者可在其中为实现特定选项提供默认值。
// 例如：
// myOption := &MyOption{
// Field1: "default_value",
// }
// myOption := transformer.GetTransformerImplSpecificOptions(myOption, opts...)
func GetTransformerImplSpecificOptions[T any](base *T, opts ...TransformerOption) *T {
	if base == nil {
		base = new(T)
	}

	for i := range opts {
		opt := opts[i]
		if opt.implSpecificOptFn != nil {
			s, ok := opt.implSpecificOptFn.(func(*T))
			if ok {
				s(base)
			}
		}
	}

	return base
}
