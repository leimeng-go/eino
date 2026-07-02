/*
 * Copyright 2025 CloudWeGo Authors
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

package adk

import "github.com/cloudwego/eino/adk/internal"

// Language represents the language setting for the ADK built-in prompts.
// Language 表示 ADK 内置提示的语言设置。
type Language = internal.Language

const (
	// LanguageEnglish represents English language.
	// LanguageEnglish 表示英语。
	LanguageEnglish Language = internal.LanguageEnglish
	// LanguageChinese represents Chinese language.
	// LanguageChinese 表示中文。
	LanguageChinese Language = internal.LanguageChinese
)

// SetLanguage sets the language for the ADK built-in prompts.
// The default language is English if not explicitly set.
//
// SetLanguage 设置 ADK 内置提示的语言。
// 如果未显式设置，默认语言为 English。
func SetLanguage(lang Language) error {
	return internal.SetLanguage(lang)
}
