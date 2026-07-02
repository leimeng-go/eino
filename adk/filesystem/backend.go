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

// Package filesystem provides file system operations.
// Package filesystem 提供文件系统操作。
package filesystem

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

// FileInfo represents basic file metadata information.
// FileInfo 表示基本文件元数据信息。
type FileInfo struct {
	// Path is the path of the file or directory, which can be a filename, relative path, or absolute path.
	// Path 是文件或目录的路径，可以是文件名、相对路径或绝对路径。
	Path string

	// IsDir indicates whether the entry is a directory.
	// true for directories, false for regular files.
	//
	// IsDir 表示该条目是否为目录。
	// 目录为 true，普通文件为 false。
	IsDir bool

	// Size is the file size in bytes.
	// For directories, this value may be 0 or platform-dependent.
	//
	// Size 是文件大小，单位为字节。
	// 对于目录，该值可能为 0 或与平台相关。
	Size int64

	// ModifiedAt is the last modification time in ISO 8601 format.
	// Example: "2025-01-15T10:30:00Z"
	//
	// ModifiedAt 是 ISO 8601 格式的最后修改时间。
	// 示例："2025-01-15T10:30:00Z"
	ModifiedAt string
}

// GrepMatch represents a single pattern match result.
// GrepMatch 表示单个模式匹配结果。
type GrepMatch struct {
	Content string

	// Path is the file path where the match was found.
	// Path 是找到匹配项的文件路径。
	Path string

	// Line is the 1-based line number of the match.
	// Line 是匹配项所在的行号，从 1 开始。
	Line int
}

// LsInfoRequest contains parameters for listing file information.
// LsInfoRequest 包含列出文件信息的参数。
type LsInfoRequest struct {
	// Path specifies the directory path to list.
	// Path 指定要列出的目录路径。
	Path string
}

// ReadRequest contains parameters for reading file content.
// ReadRequest 包含读取文件内容的参数。
type ReadRequest struct {
	// FilePath is the path to the file to be read.
	// FilePath 是要读取的文件路径。
	FilePath string

	// Offset specifies the starting line number (1-based) for reading.
	// Line 1 is the first line of the file.
	// Use this when the file is too large to read at once.
	// Defaults to 1 (start from the first line).
	// Values < 1 will be treated as 1.
	//
	// Offset 指定读取的起始行号（从 1 开始）。
	// 第 1 行是文件的第一行。
	// 当文件过大无法一次读取时使用。
	// 默认值为 1（从第一行开始）。
	// 小于 1 的值会按 1 处理。
	Offset int

	// Limit specifies the maximum number of lines to read.
	// When Limit is 0 (default), the entire file content is returned.
	//
	// Limit 指定最多读取的行数。
	// 当 Limit 为 0（默认）时，返回整个文件内容。
	Limit int
}

// MultiModalReadRequest extends ReadRequest with parameters only applicable
// to MultiModalReader implementations (e.g. PDF page ranges).
//
// MultiModalReadRequest 扩展 ReadRequest，加入仅适用于 MultiModalReader 实现的参数
// （例如 PDF 页码范围）。
type MultiModalReadRequest struct {
	ReadRequest

	// Pages specifies the page range for PDF files (e.g. "1-5", "3", "10-20").
	// Pages 指定 PDF 文件的页码范围（例如 "1-5"、"3"、"10-20"）。
	Pages string
}

// GrepRequest contains parameters for searching file content.
// GrepRequest 包含用于搜索文件内容的参数。
type GrepRequest struct {
	// ===== Search Parameters =====
	// ===== 搜索参数 =====

	// Pattern is the search pattern, supports full regular expression syntax.
	// Uses ripgrep syntax (not grep). Examples:
	//   - "log.*Error" matches lines with "log" followed by "Error"
	//   - "function\\s+\\w+" matches "function" followed by whitespace and word characters
	//   - Literal braces need escaping: "interface\\{\\}" matches "interface{}"
	//
	// Pattern 是搜索模式，支持完整的正则表达式语法。
	// 使用 ripgrep 语法（不是 grep）。示例：
	// - "log.*Error" 匹配包含 "log" 后跟 "Error" 的行
	// - "function\\s+\\w+" 匹配 "function" 后跟空白字符和单词字符
	// - 字面量大括号需要转义："interface\\{\\}" 匹配 "interface{}"
	Pattern string

	// Path is an optional directory path to limit the search scope.
	// Path 是可选的目录路径，用于限制搜索范围。
	Path string

	// ===== File Filtering =====
	// ===== 文件过滤 =====

	// Glob is an optional pattern to filter the files to be searched.
	// It filters by file path, not content. If empty, no files are filtered.
	// Supports standard glob wildcards:
	//   - `*` matches any characters except path separators.
	//   - `**` matches any directories recursively.
	//   - `?` matches a single character.
	//   - `[abc]` matches one character from the set.
	//
	// Glob 是可选模式，用于过滤要搜索的文件。
	// 它按文件路径过滤，而不是内容。为空时不过滤文件。
	// 支持标准 glob 通配符：
	// - `*` 匹配除路径分隔符外的任意字符。
	// - `**` 递归匹配任意目录。
	// - `?` 匹配单个字符。
	// - `[abc]` 匹配集合中的一个字符。
	Glob string

	// FileType is the file type filter, e.g., "js", "py", "rust".
	// More efficient than Glob for standard file types.
	//
	// FileType 是文件类型过滤器，例如 "js"、"py"、"rust"。
	// 对标准文件类型而言比 Glob 更高效。
	FileType string

	// ===== Search Options =====
	// ===== 搜索选项 =====

	// CaseInsensitive enables case insensitive search.
	// CaseInsensitive 启用大小写不敏感搜索。
	CaseInsensitive bool

	// EnableMultiline enables multiline mode where patterns can span lines.
	// Default: false (patterns match within single lines only).
	//
	// EnableMultiline 启用多行模式，模式可以跨行匹配。
	// 默认值：false（模式仅在单行内匹配）。
	EnableMultiline bool

	// ===== Context Display (Content mode only) =====
	// ===== 上下文显示（仅 content 模式） =====

	// AfterLines shows N lines after each match.
	// Only applicable when OutputMode is "content".
	// Values <= 0 are treated as unset.
	//
	// AfterLines 显示每个匹配项之后的 N 行。
	// 仅当 OutputMode 为 "content" 时适用。
	// 小于等于 0 的值视为未设置。
	AfterLines int

	// BeforeLines shows N lines before each match.
	// Only applicable when OutputMode is "content".
	// Values <= 0 are treated as unset.
	//
	// BeforeLines 显示每个匹配项之前的 N 行。
	// 仅当 OutputMode 为 "content" 时适用。
	// 小于等于 0 的值视为未设置。
	BeforeLines int
}

// GlobInfoRequest contains parameters for glob pattern matching.
// GlobInfoRequest 包含用于 glob 模式匹配的参数。
type GlobInfoRequest struct {
	// Pattern is the glob expression used to match file paths.
	// It supports standard glob syntax:
	//   - `*` matches any characters except path separators.
	//   - `**` matches any directories recursively.
	//   - `?` matches a single character.
	//   - `[abc]` matches one character from the set.
	//
	// Pattern 是用于匹配文件路径的 glob 表达式。
	// 它支持标准 glob 语法：
	// - `*` 匹配除路径分隔符外的任意字符。
	// - `**` 递归匹配任意目录。
	// - `?` 匹配单个字符。
	// - `[abc]` 匹配集合中的一个字符。
	Pattern string

	// Path is the base directory from which to start the search.
	// Path 是开始搜索的基准目录。
	Path string
}

// WriteRequest contains parameters for writing file content.
// WriteRequest 包含写入文件内容的参数。
type WriteRequest struct {
	// FilePath is the path of the file to write.
	// FilePath 是要写入的文件路径。
	FilePath string

	// Content is the data to be written to the file.
	// Content 是要写入文件的数据。
	Content string
}

// EditRequest contains parameters for editing file content.
// EditRequest 包含编辑文件内容的参数。
type EditRequest struct {
	// FilePath is the path of the file to edit.
	// FilePath 是要编辑的文件路径。
	FilePath string

	// OldString is the exact string to be replaced. It must be non-empty and will be matched literally, including whitespace.
	// OldString 是要被替换的精确字符串。它不能为空，并会按字面量匹配，包括空白字符。
	OldString string

	// NewString is the string that will replace OldString.
	// It must be different from OldString.
	// An empty string can be used to effectively delete OldString.
	//
	// NewString 是将替换 OldString 的字符串。
	// 它必须不同于 OldString。
	// 空字符串可用于实际删除 OldString。
	NewString string

	// ReplaceAll controls the replacement behavior.
	// If true, all occurrences of OldString are replaced.
	// If false, the operation fails unless OldString appears exactly once in the file.
	//
	// ReplaceAll 控制替换行为。
	// 如果为 true，则替换所有出现的 OldString。
	// 如果为 false，除非 OldString 在文件中恰好出现一次，否则操作失败。
	ReplaceAll bool
}

// FileContentPartType defines the type of a multimodal file content part.
// FileContentPartType 定义多模态文件内容部分的类型。
type FileContentPartType string

const (
	// FileContentPartTypeImage represents an image part (e.g. PNG, JPG).
	// FileContentPartTypeImage 表示图像部分（例如 PNG、JPG）。
	FileContentPartTypeImage FileContentPartType = "image"
	// FileContentPartTypePDF represents a file part (e.g. PDF).
	// FileContentPartTypePDF 表示文件部分（例如 PDF）。
	FileContentPartTypePDF FileContentPartType = "pdf"
)

// FileContentPart represents a multimodal part of file content.
// Data holds raw bytes; encoding (e.g. base64) is handled by the consumer.
//
// FileContentPart 表示文件内容的多模态部分。
// Data 保存原始字节；编码（例如 base64）由使用方处理。
type FileContentPart struct {
	// Type is the kind of content this part represents.
	// Required.
	//
	// Type 是此部分表示的内容类型。
	// 必填。
	Type FileContentPartType

	// MIMEType is the MIME type of the content (e.g. "image/png", "application/pdf").
	// Required.
	//
	// MIMEType 是内容的 MIME 类型（例如 "image/png"、"application/pdf"）。
	// 必填。
	MIMEType string

	// Data is the raw binary content.
	// Required.
	//
	// Data 是原始二进制内容。
	// 必填。
	Data []byte
}

// FileContent holds the result of a Read operation.
// FileContent 保存 Read 操作的结果。
type FileContent struct {
	// Content holds the plain text content of the file.
	// Content 保存文件的纯文本内容。
	Content string
}

// MultiFileContent holds the result of a MultiModalRead operation.
//
// FileContent and Parts are mutually exclusive (one-of):
//   - Set FileContent for plain text results (same as a normal Read).
//   - Set Parts for multimodal results (images, PDFs, etc.).
//
// When Parts is non-empty, FileContent is ignored.
//
// MultiFileContent 保存 MultiModalRead 操作的结果。
// FileContent 和 Parts 互斥（one-of）：
// - 为纯文本结果设置 FileContent（与普通 Read 相同）。
// - 为多模态结果（图像、PDF 等）设置 Parts。
// 当 Parts 非空时，FileContent 会被忽略。
type MultiFileContent struct {
	*FileContent

	// Parts holds multimodal output parts (e.g. image, PDF).
	// Parts 保存多模态输出部分（例如图像、PDF）。
	Parts []FileContentPart
}

// MultiModalReader is an optional extension interface for Backend.
// Backends that implement this interface support multimodal file reading,
// returning structured parts (images, PDFs) instead of plain text.
//
// For large file handling, there are two approaches to control output size:
//   - Implement size control within MultiModalRead (e.g. reject files exceeding a threshold,
//     downsample images, or limit PDF page counts at the backend level).
//   - Use ToolMiddleware's EnhancedInvokable to customize result transformation,
//     or use the built-in reduction middleware with configurable policies.
//
// MultiModalReader 是 Backend 的可选扩展接口。
// 实现此接口的 Backend 支持多模态文件读取，
// 返回结构化部分（图像、PDF）而非纯文本。
// 处理大文件时，有两种方式控制输出大小：
// - 在 MultiModalRead 内实现大小控制（例如拒绝超过阈值的文件、
// 下采样图像，或在 backend 层限制 PDF 页数）。
// - 使用 ToolMiddleware 的 EnhancedInvokable 自定义结果转换，
// 或使用内置的削减 middleware 并配置策略。
type MultiModalReader interface {
	MultiModalRead(ctx context.Context, req *MultiModalReadRequest) (*MultiFileContent, error)
}

// Backend is a pluggable, unified file backend protocol interface.
//
// All methods use struct-based parameters to allow future extensibility
// without breaking backward compatibility.
//
// Backend 是可插拔的统一文件后端协议接口。
// 所有方法都使用基于 struct 的参数，以便未来扩展且不破坏向后兼容性。
type Backend interface {
	// LsInfo lists file information under the given path.
	//
	// Returns:
	//   - []FileInfo: List of matching file information
	//   - error: Error if the operation fails
	//
	// LsInfo 列出给定路径下的文件信息。
	// 返回：
	// - []FileInfo：匹配的文件信息列表
	// - error：操作失败时的错误
	LsInfo(ctx context.Context, req *LsInfoRequest) ([]FileInfo, error)

	// Read reads file content with support for line-based offset and limit.
	//
	// Returns:
	//   - string: The file content read
	//   - error: Error if file does not exist or read fails
	//
	// Read 读取文件内容，支持基于行的 offset 和 limit。
	// 返回：
	// - string：读取到的文件内容
	// - error：文件不存在或读取失败时的错误
	Read(ctx context.Context, req *ReadRequest) (*FileContent, error)

	// GrepRaw searches for content matching the specified pattern in files.
	//
	// Returns:
	//   - []GrepMatch: List of all matching results
	//   - error: Error if the search fails
	//
	// GrepRaw 在文件中搜索匹配指定 pattern 的内容。
	// 返回：
	// - []GrepMatch：所有匹配结果列表
	// - error：搜索失败时的错误
	GrepRaw(ctx context.Context, req *GrepRequest) ([]GrepMatch, error)

	// GlobInfo returns file information matching the glob pattern.
	//
	// Returns:
	//   - []FileInfo: List of matching file information
	//   - error: Error if the pattern is invalid or operation fails
	//
	// GlobInfo 返回匹配 glob pattern 的文件信息。
	// 返回：
	// - []FileInfo：匹配的文件信息列表
	// - error：pattern 无效或操作失败时的错误
	GlobInfo(ctx context.Context, req *GlobInfoRequest) ([]FileInfo, error)

	// Write creates or updates file content.
	//
	// Returns:
	//   - error: Error if the write operation fails
	//
	// Write 创建或更新文件内容。
	// 返回：
	// - error：写入操作失败时的错误
	Write(ctx context.Context, req *WriteRequest) error

	// Edit replaces string occurrences in a file.
	//
	// Returns:
	//   - error: Error if file does not exist, OldString is empty, or OldString is not found
	//
	// Edit 替换文件中的字符串出现项。
	// 返回：
	// - error：文件不存在、OldString 为空或找不到 OldString 时的错误
	Edit(ctx context.Context, req *EditRequest) error
}

// ExecuteRequest contains parameters for executing a command.
// ExecuteRequest 包含执行命令的参数。
type ExecuteRequest struct {
	Command string // The command to execute
	// 要执行的命令
	RunInBackendGround bool
}

// ExecuteResponse contains the response result of command execution.
// ExecuteResponse 包含命令执行的响应结果。
type ExecuteResponse struct {
	Output string // Command output content
	// 命令输出内容
	ExitCode *int // Command exit code
	// 命令退出码
	Truncated bool // Whether the output was truncated
	// 输出是否被截断
}

type Shell interface {
	Execute(ctx context.Context, input *ExecuteRequest) (result *ExecuteResponse, err error)
}

type StreamingShell interface {
	ExecuteStreaming(ctx context.Context, input *ExecuteRequest) (result *schema.StreamReader[*ExecuteResponse], err error)
}
