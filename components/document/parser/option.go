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

// Options configures the document parser with source URI and extra metadata.
// Options 使用源 URI 和额外元数据配置 document parser。
type Options struct {
	// uri of source.
	// 源的 uri。
	URI string

	// extra metadata will merge to each document.
	// 额外元数据会合并到每个 document。
	ExtraMeta map[string]any
}

// Option defines call option for Parser component, which is part of the component interface signature.
// Each Parser implementation could define its own options struct and option funcs within its own package,
// then wrap the impl specific option funcs into this type, before passing to Transform.
//
// Option 定义 Parser 组件的调用选项，它是组件接口签名的一部分。
// 每个 Parser 实现都可以在自己的 package 中定义自己的选项结构体和选项函数，
// 然后在传给 Transform 前，将实现特定的选项函数包装成此类型。
type Option struct {
	apply func(opts *Options)

	implSpecificOptFn any
}

// WithURI specifies the source URI of the document.
// It will be used as to select parser in ExtParser.
//
// WithURI 指定文档的源 URI。
// 它将用于在 ExtParser 中选择 parser。
func WithURI(uri string) Option {
	return Option{
		apply: func(opts *Options) {
			opts.URI = uri
		},
	}
}

// WithExtraMeta attaches extra metadata to the parsed document.
// WithExtraMeta 将额外元数据附加到解析后的 document。
func WithExtraMeta(meta map[string]any) Option {
	return Option{
		apply: func(opts *Options) {
			opts.ExtraMeta = meta
		},
	}
}

// GetCommonOptions extract parser Options from Option list, optionally providing a base Options with default values.
// GetCommonOptions 从 Option 列表中提取解析器 Options，可选提供带默认值的基础 Options。
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

// WrapImplSpecificOptFn wraps the impl specific option functions into Option type.
// T: the type of the impl specific options struct.
// Parser implementations are required to use this function to convert its own option functions into the unified Option type.
// For example, if the Parser impl defines its own options struct:
//
//	type customOptions struct {
//	    conf string
//	}
//
// Then the impl needs to provide an option function as such:
//
//	func WithConf(conf string) Option {
//	    return WrapImplSpecificOptFn(func(o *customOptions) {
//			o.conf = conf
//		}
//	}
//
// .
//
// WrapImplSpecificOptFn 将实现特定的选项函数包装为 Option 类型。
// T：实现特定选项结构体的类型。
// Parser 实现需要使用此函数将自身的选项函数转换为统一的 Option 类型。
// 例如，如果 Parser 实现定义了自己的选项结构体：
// type customOptions struct {
// conf string
// }
// 则该实现需要提供如下选项函数：
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

// GetImplSpecificOptions provides Parser author the ability to extract their own custom options from the unified Option type.
// T: the type of the impl specific options struct.
// This function should be used within the Parser implementation's Transform function.
// It is recommended to provide a base T as the first argument, within which the Parser author can provide default values for the impl specific options.
//
// GetImplSpecificOptions 让 Parser 作者能够从统一的 Option 类型中提取自己的自定义选项。
// T：实现特定选项结构体的类型。
// 此函数应在 Parser 实现的 Transform 函数中使用。
// 建议将基础 T 作为第一个参数，Parser 作者可在其中为实现特定选项提供默认值。
func GetImplSpecificOptions[T any](base *T, opts ...Option) *T {
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
