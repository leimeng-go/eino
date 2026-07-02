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

package prompt

// Option is a call-time option for a ChatTemplate. The built-in
// [DefaultChatTemplate] has no common options — this type exists primarily for
// custom ChatTemplate implementations that need per-call configuration.
//
// Option 是 ChatTemplate 的调用时选项。内置的 [DefaultChatTemplate] 没有通用选项——此类型主要供需要按调用配置的自定义 ChatTemplate 实现使用。
type Option struct {
	implSpecificOptFn any
}

// WrapImplSpecificOptFn wraps an implementation-specific option function so it
// can be passed alongside any future standard options. For use by custom
// ChatTemplate implementors.
//
// WrapImplSpecificOptFn 包装实现特定的选项函数，使其可与未来的标准选项一起传入。供自定义 ChatTemplate 实现者使用。
func WrapImplSpecificOptFn[T any](optFn func(*T)) Option {
	return Option{
		implSpecificOptFn: optFn,
	}
}

// GetImplSpecificOptions extracts the implementation specific options from Option list, optionally providing a base options with default values.
// GetImplSpecificOptions 从 Option 列表中提取实现专属选项，可选择提供带默认值的基础选项。
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
