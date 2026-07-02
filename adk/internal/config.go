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

// Package internal provides adk internal utils.
// Package internal 提供 adk 内部工具。
package internal

import (
	"fmt"
	"sync/atomic"
)

// Language represents the language setting for the ADK built-in prompts.
// Language 表示 ADK 内置提示的语言设置。
type Language uint8

const (
	// LanguageEnglish represents English language.
	// LanguageEnglish 表示英语。
	LanguageEnglish Language = iota
	// LanguageChinese represents Chinese language.
	// LanguageChinese 表示中文。
	LanguageChinese
)

var language atomic.Value

// SetLanguage sets the language for the ADK built-in prompts.
// The default language is English if not explicitly set.
//
// SetLanguage 设置 ADK 内置提示的语言。
// 如果未显式设置，默认语言为英语。
func SetLanguage(lang Language) error {
	if lang != LanguageEnglish &&
		lang != LanguageChinese {
		return fmt.Errorf("invalid language: %v", lang)
	}
	language.Store(lang)
	return nil
}

// GetLanguage returns the current language setting for the ADK built-in prompts.
// Returns LanguageEnglish if no language has been set.
//
// getLanguage 返回 ADK 内置提示的当前语言设置。
// 如果未设置语言，则返回 LanguageEnglish。
func getLanguage() Language {
	if l, ok := language.Load().(Language); ok {
		return l
	}
	return LanguageEnglish
}

// I18nPrompts holds prompt strings for different languages.
// I18nPrompts 保存不同语言的提示字符串。
type I18nPrompts struct {
	English string
	Chinese string
}

// SelectPrompt returns the appropriate prompt string based on the current language setting.
// Returns an error if the current language is not supported.
//
// SelectPrompt 根据当前语言设置返回相应的提示字符串。
// 如果当前语言不受支持，则返回错误。
func SelectPrompt(prompts I18nPrompts) string {
	lang := getLanguage()
	switch lang {
	case LanguageEnglish:
		return prompts.English
	case LanguageChinese:
		return prompts.Chinese
	default:
		// unreachable
		panic(fmt.Sprintf("invalid language: %v", lang))
	}
}
