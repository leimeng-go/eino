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

package agentsmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/adk/internal"
)

// importRegex matches @path/to/file anywhere in text.
// The path must start with a letter, digit, dot, underscore, slash, or tilde, followed by
// path characters (letters, digits, dots, slashes, hyphens, underscores).
// A post-match filter further requires the path to contain "/" or end with
// an allowed extension (see allowedImportExts), so bare words like @someone
// and email-like patterns like @example.com are ignored.
//
// importRegex 匹配文本中任意位置的 @path/to/file。
// 路径必须以字母、数字、点、下划线、斜杠或波浪号开头，后跟
// 路径字符（字母、数字、点、斜杠、连字符、下划线）。
// 匹配后的过滤还要求路径包含 "/" 或以
// 允许的扩展名结尾（见 allowedImportExts），因此会忽略 @someone 这类裸词
// 以及 @example.com 这类类似 email 的模式。
var importRegex = regexp.MustCompile(`@([a-zA-Z0-9_.~/][a-zA-Z0-9_.~/\-]*)`)

// allowedImportExts is the set of file extensions recognised as @import targets.
// Paths without "/" must end with one of these extensions to be treated as imports;
// this avoids false positives on email addresses (@example.com) and mentions (@foo.bar).
//
// allowedImportExts 是识别为 @import 目标的文件扩展名集合。
// 不含 "/" 的路径必须以这些扩展名之一结尾，才会被视为 import；
// 这样可避免误匹配 email 地址（@example.com）和提及（@foo.bar）。
var allowedImportExts = map[string]bool{
	".md":   true,
	".txt":  true,
	".mdx":  true,
	".yaml": true,
	".yml":  true,
	".json": true,
	".toml": true,
}

const maxImportDepth = 5

// ReadRequest is an alias for filesystem.ReadRequest.
// ReadRequest 是 filesystem.ReadRequest 的别名。
type ReadRequest = filesystem.ReadRequest
type FileContent = filesystem.FileContent

// Backend defines the file access interface for loading Agents.md files.
// Implementations can use local filesystem, remote storage, or any other backend.
//
// Backend 定义加载 Agents.md 文件的文件访问接口。
// 实现可以使用本地文件系统、远程存储或任何其他 backend。
type Backend interface {
	// Read reads the content of a file.
	// If the file does not exist, implementations should return an error wrapping
	// os.ErrNotExist (so that errors.Is(err, os.ErrNotExist) returns true). This allows the loader
	// to silently skip missing files and notify via OnLoadWarning callback.
	// Other errors (e.g. permission denied, I/O errors) will abort the loading process.
	//
	// Read 读取文件内容。
	// 如果文件不存在，实现应返回一个包装了
	// os.ErrNotExist 的错误（使 errors.Is(err, os.ErrNotExist) 返回 true）。这允许 loader
	// 静默跳过缺失文件，并通过 OnLoadWarning 回调通知。
	// 其他错误（例如权限拒绝、I/O 错误）会中止加载过程。
	Read(ctx context.Context, req *ReadRequest) (*FileContent, error)
}

// loaderConfig holds the immutable configuration for creating loaders.
// It is safe for concurrent use by multiple goroutines.
//
// loaderConfig 保存用于创建 loaders 的不可变配置。
// 它可被多个 goroutine 并发安全使用。
type loaderConfig struct {
	backend Backend
	files   []string // ordered file paths from config
	// 来自 config 的有序文件路径
	maxBytes int // cumulative read budget; 0 means unlimited
	// 累计读取预算；0 表示无限制
	onWarning func(filePath string, err error) // callback for non-fatal loading warnings
	// 用于非致命加载警告的回调
}

func newLoaderConfig(backend Backend, files []string, maxBytes int, onWarning func(filePath string, err error)) *loaderConfig {
	if onWarning == nil {
		onWarning = func(filePath string, err error) {
			log.Printf("[agentsmd] warning: %s: %v", filePath, err)
		}
	}
	return &loaderConfig{
		backend:   backend,
		files:     files,
		maxBytes:  maxBytes,
		onWarning: onWarning,
	}
}

// loader handles loading and @import resolution for agents.md files.
// A new loader is created for each load() call to avoid sharing mutable state
// (totalBytes) across concurrent invocations.
//
// loader 处理 agents.md 文件的加载和 @import 解析。
// 每次 load() 调用都会创建新的 loader，以避免在并发调用间共享可变状态（totalBytes）。
type loader struct {
	*loaderConfig
	totalBytes int // accumulated bytes during this load call
	// 本次 load 调用期间累积的字节数
}

func (cfg *loaderConfig) newLoader() *loader {
	return &loader{loaderConfig: cfg}
}

// load reads all agents.md files and returns the formatted content.
// Each top-level file and its @imported files appear as separate sections.
//
// load 读取所有 agents.md 文件并返回格式化后的内容。
// 每个顶层文件及其 @import 的文件都会作为独立章节出现。
func (cfg *loaderConfig) load(ctx context.Context) (string, error) {
	l := cfg.newLoader()

	var parts []loadedFile
	seen := make(map[string]bool) // dedup across all files and imports
	// 对所有文件和 import 去重

	for i, filePath := range l.files {
		files, err := l.loadFile(ctx, filePath, 0, make(map[string]bool), seen)
		if err != nil {
			return "", fmt.Errorf("failed to load %q: %w", filePath, err)
		}

		// If loading this file caused the budget to be exceeded, skip it
		// (but always include the first file).
		//
		// 如果加载此文件导致超出预算，则跳过它
		// （但始终包含第一个文件）。
		if i > 0 && l.maxBytes > 0 && l.totalBytes > l.maxBytes {
			l.onWarning(filePath, fmt.Errorf("skipped: cumulative size %d exceeds max bytes %d", l.totalBytes, l.maxBytes))
			break
		}

		parts = append(parts, files...)
	}

	return formatContent(parts), nil
}

// loadFile reads a file via Backend and collects @imported files as separate entries.
// Returns a slice where the first element is this file itself, followed by all
// transitively imported files (in encounter order, preserving @path in original text).
// visited tracks the current ancestor chain to detect circular imports.
// seen tracks globally loaded files to avoid duplicate reads and byte counting.
//
// loadFile 通过 Backend 读取文件，并将 @import 的文件收集为独立条目。
// 返回一个切片，其中第一个元素是该文件本身，后面是所有传递导入的文件（按遇到顺序，保留原文中的 @path）。
// visited 跟踪当前祖先链，用于检测循环导入。
// seen 跟踪全局已加载文件，以避免重复读取和字节计数。
func (l *loader) loadFile(ctx context.Context, filePath string, depth int, visited map[string]bool, seen map[string]bool) ([]loadedFile, error) {
	filePath = filepath.Clean(filePath)

	if depth > maxImportDepth {
		l.onWarning(filePath, fmt.Errorf("@import depth exceeds maximum of %d", maxImportDepth))
		return nil, nil
	}

	if visited[filePath] {
		l.onWarning(filePath, fmt.Errorf("circular @import detected"))
		return nil, nil
	}

	if seen[filePath] {
		return nil, nil
	}

	visited[filePath] = true
	defer delete(visited, filePath)

	fileContent, err := l.backend.Read(ctx, &ReadRequest{FilePath: filePath, Offset: 1})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			l.onWarning(filePath, fmt.Errorf("file not found, skipping"))
			return nil, nil
		}
		return nil, err
	}
	content := ""
	if fileContent != nil {
		content = fileContent.Content
	}

	l.totalBytes += len(content)
	seen[filePath] = true

	if content == "" {
		return nil, nil
	}

	// Collect imported files as separate sections (content stays untouched).
	// 将导入的文件收集为独立章节（内容保持不变）。
	imports, err := l.collectImports(ctx, filePath, content, depth, visited, seen)
	if err != nil {
		return nil, err
	}

	// This file first, then its imports.
	// 先是此文件，然后是其 imports。
	result := make([]loadedFile, 0, 1+len(imports))
	result = append(result, loadedFile{path: filePath, content: content})
	result = append(result, imports...)
	return result, nil
}

// collectImports scans content for @path/to/file references and loads each
// imported file (plus its transitive imports). The original content is NOT modified.
// Returns the list of imported loadedFile entries in encounter order.
// seen is shared across the entire load call to avoid duplicate reads.
// Non-fatal errors (file not found, depth exceeded, circular import) are reported
// via onWarning and skipped. Fatal errors (e.g. I/O) are returned.
//
// collectImports 扫描内容中的 @path/to/file 引用并加载每个导入文件（及其传递导入）。原始内容不会被修改。
// 返回按遇到顺序排列的 imported loadedFile 条目列表。
// seen 在整个 load 调用中共享，以避免重复读取。
// 非致命错误（文件未找到、深度超限、循环导入）会通过 onWarning 报告并跳过。致命错误（如 I/O）会返回。
func (l *loader) collectImports(ctx context.Context, hostPath, content string, depth int, visited map[string]bool, seen map[string]bool) ([]loadedFile, error) {
	dir := filepath.Dir(hostPath)
	var imports []loadedFile

	matches := importRegex.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		rawPath := match[1]

		// Only treat as import if path contains "/" or ends with an allowed extension.
		// This avoids false positives on email addresses and social mentions.
		//
		// 仅当路径包含 "/" 或以允许的扩展名结尾时，才视为 import。
		// 这可避免将电子邮件地址和社交提及误判为 import。
		if !strings.Contains(rawPath, "/") && !allowedImportExts[filepath.Ext(rawPath)] {
			continue
		}

		// If budget is exhausted, skip further imports.
		// 如果预算耗尽，则跳过后续 imports。
		if l.maxBytes > 0 && l.totalBytes > l.maxBytes {
			break
		}

		importPath := rawPath
		if !filepath.IsAbs(importPath) {
			importPath = filepath.Join(dir, importPath)
		}

		if seen[importPath] {
			continue
		}

		files, err := l.loadFile(ctx, importPath, depth+1, visited, seen)
		if err != nil {
			return nil, fmt.Errorf("failed to import %q from %q: %w", rawPath, hostPath, err)
		}

		imports = append(imports, files...)
	}

	return imports, nil
}

type loadedFile struct {
	path    string
	content string
}

const formatHeaderEn = `<system-reminder>
As you answer the user's questions, you can use the following context:
Codebase and user instructions are shown below. Be sure to adhere to these instructions. IMPORTANT: These instructions OVERRIDE any default behavior and you MUST follow them exactly as written.
`

const formatHeaderCn = `<system-reminder>
在回答用户问题时，你可以使用以下上下文：
代码库和用户指令如下。请务必遵守这些指令。重要提示：这些指令会覆盖任何默认行为，你必须严格按照要求执行。
`

const formatFileHeaderEn = "\nContents of "

const formatFileHeaderCn = "\n文件内容："

const formatFileLabelEn = " (instructions):\n\n"

const formatFileLabelCn = "（指令）：\n\n"

const formatFooterEn = `IMPORTANT: this context may or may not be relevant to your tasks. You should not respond to this context unless it is highly relevant to your task.
</system-reminder>`

const formatFooterCn = `重要提示：此上下文可能与你的任务相关，也可能不相关。除非此上下文与你的任务高度相关，否则不要响应此上下文。
</system-reminder>`

func formatContent(files []loadedFile) string {
	if len(files) == 0 {
		return ""
	}

	header := internal.SelectPrompt(internal.I18nPrompts{
		English: formatHeaderEn,
		Chinese: formatHeaderCn,
	})
	fileHeader := internal.SelectPrompt(internal.I18nPrompts{
		English: formatFileHeaderEn,
		Chinese: formatFileHeaderCn,
	})
	fileLabel := internal.SelectPrompt(internal.I18nPrompts{
		English: formatFileLabelEn,
		Chinese: formatFileLabelCn,
	})
	footer := internal.SelectPrompt(internal.I18nPrompts{
		English: formatFooterEn,
		Chinese: formatFooterCn,
	})

	var sb strings.Builder
	sb.WriteString(header)

	for _, f := range files {
		sb.WriteString(fileHeader)
		sb.WriteString(f.path)
		sb.WriteString(fileLabel)
		sb.WriteString(f.content)
		sb.WriteString("\n")
	}
	sb.WriteString(footer)
	return sb.String()
}
