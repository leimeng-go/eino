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
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/filesystem"
	"github.com/cloudwego/eino/schema"
)

// --- test helpers ---
// --- 测试辅助函数 ---

type memBackend struct {
	files map[string]string
}

func newMemBackend() *memBackend {
	return &memBackend{files: make(map[string]string)}
}

func (b *memBackend) set(path string, content string) {
	b.files[path] = content
}

func (b *memBackend) Read(_ context.Context, req *ReadRequest) (*filesystem.FileContent, error) {
	content, ok := b.files[req.FilePath]
	if !ok {
		return nil, fmt.Errorf("file not found: %s: %w", req.FilePath, os.ErrNotExist)
	}
	return &filesystem.FileContent{Content: content}, nil
}

// errBackend always returns a non-ErrNotExist error on Read, simulating I/O failures.
// errBackend 在 Read 时始终返回非 ErrNotExist 错误，用于模拟 I/O 故障。
type errBackend struct{}

func (b *errBackend) Read(_ context.Context, req *ReadRequest) (*filesystem.FileContent, error) {
	return nil, fmt.Errorf("permission denied: %s", req.FilePath)
}

// partialErrBackend returns content for known files and I/O error for others.
// partialErrBackend 对已知文件返回内容，对其他文件返回 I/O 错误。
type partialErrBackend struct {
	files map[string]string
}

func (b *partialErrBackend) Read(_ context.Context, req *ReadRequest) (*filesystem.FileContent, error) {
	content, ok := b.files[req.FilePath]
	if !ok {
		return nil, fmt.Errorf("I/O error reading %s", req.FilePath)
	}
	return &filesystem.FileContent{Content: content}, nil
}

// --- tests ---
// --- 测试 ---

func TestNew_Validation(t *testing.T) {
	ctx := context.Background()
	b := newMemBackend()

	_, err := New(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}

	_, err = New(ctx, &Config{})
	if err == nil {
		t.Fatal("expected error for empty config")
	}

	_, err = New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/test.md"}, AllAgentsMDMaxBytes: -1})
	if err == nil {
		t.Fatal("expected error for negative max bytes")
	}

	_, err = New(ctx, &Config{AgentsMDFiles: []string{"/test.md"}})
	if err == nil {
		t.Fatal("expected error for nil backend")
	}
}

func TestMiddleware_BasicInjection(t *testing.T) {
	b := newMemBackend()
	b.set("/agent.md", "You are a helpful assistant.")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	userMsg := &schema.Message{Role: schema.User, Content: "hello"}
	state := &adk.ChatModelAgentState{Messages: []*schema.Message{userMsg}}

	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(state.Messages))
	}
	if state.Messages[0].Role != schema.User {
		t.Fatalf("expected first message role User, got %s", state.Messages[0].Role)
	}
	if !strings.Contains(state.Messages[0].Content, "You are a helpful assistant.") {
		t.Fatalf("expected agent.md content in first message, got %q", state.Messages[0].Content)
	}
	if !strings.Contains(state.Messages[0].Content, "<system-reminder>") {
		t.Fatalf("expected system-reminder tag, got %q", state.Messages[0].Content)
	}
	if count := strings.Count(state.Messages[0].Content, "<system-reminder>"); count != 1 {
		t.Fatalf("expected exactly one opening system-reminder tag, got %d in %q", count, state.Messages[0].Content)
	}
	if count := strings.Count(state.Messages[0].Content, "</system-reminder>"); count != 1 {
		t.Fatalf("expected exactly one closing system-reminder tag, got %d in %q", count, state.Messages[0].Content)
	}
	if state.Messages[1].Content != "hello" {
		t.Fatalf("expected original message preserved, got %q", state.Messages[1].Content)
	}
}

func TestMiddleware_MultipleFiles(t *testing.T) {
	b := newMemBackend()
	b.set("/a.md", "instruction A")
	b.set("/b.md", "instruction B")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/a.md", "/b.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	content := state.Messages[0].Content
	idxA := strings.Index(content, "instruction A")
	idxB := strings.Index(content, "instruction B")
	if idxA < 0 || idxB < 0 {
		t.Fatalf("both files should be included, content: %q", content)
	}
	if idxA >= idxB {
		t.Fatal("file A should appear before file B")
	}
}

func TestMiddleware_ImportResolution(t *testing.T) {
	b := newMemBackend()
	b.set("/project/agent.md", "main instructions\n@sub/rules.md\nend")
	b.set("/project/sub/rules.md", "imported rule")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/project/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	content := state.Messages[0].Content
	// Original text should be preserved with @path intact.
	// 应保留原始文本，并保持 @path 不变。
	if !strings.Contains(content, "main instructions") {
		t.Fatalf("should contain original text, got %q", content)
	}
	if !strings.Contains(content, "@sub/rules.md") {
		t.Fatalf("@import reference should be preserved in original text, got %q", content)
	}
	if !strings.Contains(content, "end") {
		t.Fatalf("should contain original trailing text, got %q", content)
	}
	// Imported file should appear as a separate section.
	// 导入的文件应作为单独 section 出现。
	if !strings.Contains(content, "Contents of /project/sub/rules.md") {
		t.Fatalf("imported file should have its own section, got %q", content)
	}
	if !strings.Contains(content, "imported rule") {
		t.Fatalf("imported file content should be present, got %q", content)
	}
}

func TestMiddleware_RecursiveImport(t *testing.T) {
	b := newMemBackend()
	b.set("/a.md", "top\n@/b.md")
	b.set("/b.md", "middle\n@/c.md")
	b.set("/c.md", "leaf content")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/a.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	content := state.Messages[0].Content
	// All three files should appear as separate sections.
	// 三个文件都应作为单独 section 出现。
	for _, section := range []string{"Contents of /a.md", "Contents of /b.md", "Contents of /c.md"} {
		if !strings.Contains(content, section) {
			t.Fatalf("expected section %q in content, got %q", section, content)
		}
	}
	for _, text := range []string{"top", "middle", "leaf content"} {
		if !strings.Contains(content, text) {
			t.Fatalf("expected %q in content, got %q", text, content)
		}
	}
	// Sections should appear in order: a, b, c.
	// section 应按顺序出现：a、b、c。
	idxA := strings.Index(content, "Contents of /a.md")
	idxB := strings.Index(content, "Contents of /b.md")
	idxC := strings.Index(content, "Contents of /c.md")
	if !(idxA < idxB && idxB < idxC) {
		t.Fatalf("sections should appear in order a < b < c, got a=%d b=%d c=%d", idxA, idxB, idxC)
	}
}

func TestMiddleware_MaxImportDepth(t *testing.T) {
	b := newMemBackend()
	for i := 0; i < 7; i++ {
		var content string
		if i < 6 {
			content = fmt.Sprintf("level %d\n@/level%d.md", i, i+1)
		} else {
			content = fmt.Sprintf("level %d", i)
		}
		b.set(fmt.Sprintf("/level%d.md", i), content)
	}

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/level0.md"}})
	if err != nil {
		t.Fatal(err)
	}

	// Import failure at depth > 5 is logged, not returned as error.
	// depth > 5 时的导入失败会记录日志，而不是作为错误返回。
	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatalf("expected no error (depth exceeded is logged), got %v", err)
	}
	// Levels 0-5 should be present as sections; level 6 fails silently.
	// 级别 0-5 应作为 section 存在；级别 6 会静默失败。
	content := state.Messages[0].Content
	for i := 0; i <= 5; i++ {
		want := fmt.Sprintf("Contents of /level%d.md", i)
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in content, got %q", want, content)
		}
	}
	if strings.Contains(content, "Contents of /level6.md") {
		t.Fatalf("level6 should not be present (depth exceeded), got %q", content)
	}
}

func TestMiddleware_CircularImport(t *testing.T) {
	b := newMemBackend()
	b.set("/a.md", "@/b.md")
	b.set("/b.md", "@/a.md")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/a.md"}})
	if err != nil {
		t.Fatal(err)
	}

	// Circular import failure is logged, not returned as error.
	// 循环导入失败会记录日志，而不是作为错误返回。
	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatalf("expected no error (circular import is logged), got %v", err)
	}
	// /a.md and /b.md should both be present; the circular ref from b->a is skipped.
	// /a.md 和 /b.md 都应存在；从 b->a 的循环引用会被跳过。
	content := state.Messages[0].Content
	if !strings.Contains(content, "Contents of /a.md") {
		t.Fatalf("expected /a.md section, got %q", content)
	}
	if !strings.Contains(content, "Contents of /b.md") {
		t.Fatalf("expected /b.md section, got %q", content)
	}
}

func TestMiddleware_MaxBytesLimit(t *testing.T) {
	b := newMemBackend()
	b.set("/a.md", "AAAA") // 4 bytes
	b.set("/b.md", "BBBB") // 4 bytes

	ctx := context.Background()
	mw, err := New(ctx, &Config{
		Backend:             b,
		AgentsMDFiles:       []string{"/a.md", "/b.md"},
		AllAgentsMDMaxBytes: 5, // file a (4) fits, file b (4) would exceed
		// file a (4) 符合，file b (4) 会超出
	})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	content := state.Messages[0].Content
	if !strings.Contains(content, "AAAA") {
		t.Fatal("first file should be included")
	}
	if strings.Contains(content, "BBBB") {
		t.Fatal("second file should be excluded due to max bytes")
	}
}

func TestMiddleware_InjectedInState(t *testing.T) {
	b := newMemBackend()
	b.set("/agent.md", "agent instructions")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	originalMsgs := []*schema.Message{{Role: schema.User, Content: "hello"}}
	state := &adk.ChatModelAgentState{Messages: originalMsgs}
	_, newState, err := mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The original slice should not be modified (new slice is returned).
	// 不应修改原始 slice（返回新的 slice）。
	if len(originalMsgs) != 1 {
		t.Fatalf("original messages slice should not be modified, got %d messages", len(originalMsgs))
	}
	if originalMsgs[0].Content != "hello" {
		t.Fatalf("original message should be unchanged, got %q", originalMsgs[0].Content)
	}
	// The returned state should have the injected message.
	// 返回的 state 应包含注入的消息。
	if len(newState.Messages) != 2 {
		t.Fatalf("new state should have 2 messages (injected + original), got %d", len(newState.Messages))
	}
	if !strings.Contains(newState.Messages[0].Content, "agent instructions") {
		t.Fatalf("expected agentmd content in first message, got %q", newState.Messages[0].Content)
	}
	if newState.Messages[1].Content != "hello" {
		t.Fatalf("expected original user message preserved, got %q", newState.Messages[1].Content)
	}
}

func TestMiddleware_AbsoluteImportPath(t *testing.T) {
	b := newMemBackend()
	b.set("/project/main.md", "start\n@/shared/imported.md\nend")
	b.set("/shared/imported.md", "absolute import content")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/project/main.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	content := state.Messages[0].Content
	// @path preserved in original text.
	// @path 保留在原始文本中。
	if !strings.Contains(content, "@/shared/imported.md") {
		t.Fatalf("@import reference should be preserved, got %q", content)
	}
	// Imported content in separate section.
	// 导入的内容位于单独的 section。
	if !strings.Contains(content, "Contents of /shared/imported.md") {
		t.Fatalf("expected separate section for imported file, got %q", content)
	}
	if !strings.Contains(content, "absolute import content") {
		t.Fatalf("expected absolute import content, got %q", content)
	}
}

func TestMiddleware_ImportAsSeparateSection(t *testing.T) {
	b := newMemBackend()
	b.set("/project/agent.md", "Please read @sub/rules.md and also @sub/style.md for guidance.")
	b.set("/project/sub/rules.md", "RULE_CONTENT")
	b.set("/project/sub/style.md", "STYLE_CONTENT")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/project/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	content := state.Messages[0].Content
	// Original text preserved with @paths intact.
	// 保留原始文本，@paths 保持不变。
	if !strings.Contains(content, "Please read @sub/rules.md and also @sub/style.md for guidance.") {
		t.Fatalf("original text with @paths should be preserved, got %q", content)
	}
	// Imported files appear as separate sections.
	// 导入的文件显示为单独的 section。
	if !strings.Contains(content, "Contents of /project/sub/rules.md") {
		t.Fatalf("expected rules.md section, got %q", content)
	}
	if !strings.Contains(content, "RULE_CONTENT") {
		t.Fatalf("expected imported rule content, got %q", content)
	}
	if !strings.Contains(content, "Contents of /project/sub/style.md") {
		t.Fatalf("expected style.md section, got %q", content)
	}
	if !strings.Contains(content, "STYLE_CONTENT") {
		t.Fatalf("expected imported style content, got %q", content)
	}

	// Sections should be ordered: agent.md, rules.md, style.md.
	// section 应按顺序排列：agent.md、rules.md、style.md。
	idxAgent := strings.Index(content, "Contents of /project/agent.md")
	idxRules := strings.Index(content, "Contents of /project/sub/rules.md")
	idxStyle := strings.Index(content, "Contents of /project/sub/style.md")
	if !(idxAgent < idxRules && idxRules < idxStyle) {
		t.Fatalf("sections should appear in order agent < rules < style, got agent=%d rules=%d style=%d", idxAgent, idxRules, idxStyle)
	}
}

// --- loader-specific tests ---
// --- loader 专用测试 ---

func TestLoader_NoImportsPassthrough(t *testing.T) {
	// Content without any @path should be returned as-is in its section.
	// 不含任何 @path 的内容应在其 section 中原样返回。
	b := newMemBackend()
	b.set("/agent.md", "plain text without imports\nline two")

	l := newLoaderConfig(b, []string{"/agent.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "plain text without imports") {
		t.Fatalf("expected plain content, got %q", content)
	}
	if !strings.Contains(content, "line two") {
		t.Fatalf("expected second line, got %q", content)
	}
}

func TestLoader_ImportAsSeparateSection(t *testing.T) {
	// @path in the middle of a sentence should be preserved; imported file is a separate section.
	// 句子中间的 @path 应保留；导入的文件作为单独的 section。
	b := newMemBackend()
	b.set("/doc.md", "before @/snippet.md after")
	b.set("/snippet.md", "INJECTED")

	l := newLoaderConfig(b, []string{"/doc.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Original text preserved.
	// 保留原始文本。
	if !strings.Contains(content, "before @/snippet.md after") {
		t.Fatalf("original text should be preserved with @path, got %q", content)
	}
	// Imported file in separate section.
	// 导入的文件位于单独的 section。
	if !strings.Contains(content, "Contents of /snippet.md") {
		t.Fatalf("expected separate section for snippet.md, got %q", content)
	}
	if !strings.Contains(content, "INJECTED") {
		t.Fatalf("expected imported content, got %q", content)
	}
}

func TestLoader_MultipleImportsSameLine(t *testing.T) {
	// Multiple @path on one line should each get a separate section.
	// 同一行上的多个 @path 应各自生成单独的 section。
	b := newMemBackend()
	b.set("/doc.md", "see @/a.txt and @/b.txt here")
	b.set("/a.txt", "AAA")
	b.set("/b.txt", "BBB")

	l := newLoaderConfig(b, []string{"/doc.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Original text preserved.
	// 保留原始文本。
	if !strings.Contains(content, "see @/a.txt and @/b.txt here") {
		t.Fatalf("original text should be preserved, got %q", content)
	}
	// Each imported file has its own section.
	// 每个导入的文件都有自己的 section。
	if !strings.Contains(content, "Contents of /a.txt") {
		t.Fatalf("expected section for a.txt, got %q", content)
	}
	if !strings.Contains(content, "AAA") {
		t.Fatalf("expected a.txt content, got %q", content)
	}
	if !strings.Contains(content, "Contents of /b.txt") {
		t.Fatalf("expected section for b.txt, got %q", content)
	}
	if !strings.Contains(content, "BBB") {
		t.Fatalf("expected b.txt content, got %q", content)
	}
}

func TestLoader_SameFileTwiceOnSameLine(t *testing.T) {
	// The same file referenced twice should appear only once as a section (deduped).
	// 同一个文件被引用两次时，应只作为一个 section 出现（已去重）。
	b := newMemBackend()
	b.set("/doc.md", "@/shared.md and @/shared.md again")
	b.set("/shared.md", "SHARED")

	l := newLoaderConfig(b, []string{"/doc.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Original text preserved.
	// 保留原始文本。
	if !strings.Contains(content, "@/shared.md and @/shared.md again") {
		t.Fatalf("original text should be preserved, got %q", content)
	}
	// shared.md content should appear only once (deduped).
	// shared.md 内容应只出现一次（已去重）。
	count := strings.Count(content, "Contents of /shared.md")
	if count != 1 {
		t.Fatalf("expected shared.md section to appear once (deduped), got %d in %q", count, content)
	}
}

func TestLoader_ImportFileNotFound(t *testing.T) {
	b := newMemBackend()
	b.set("/doc.md", "load @/missing.md please")

	l := newLoaderConfig(b, []string{"/doc.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error (missing import is logged), got %v", err)
	}
	// Original text preserved; missing file simply has no section.
	// 保留原始文本；缺失文件不会生成 section。
	if !strings.Contains(content, "load @/missing.md please") {
		t.Fatalf("expected original text preserved, got %q", content)
	}
	if strings.Contains(content, "Contents of /missing.md") {
		t.Fatalf("missing file should not have a section, got %q", content)
	}
}

func TestLoader_RelativePathResolution(t *testing.T) {
	// Relative path should resolve relative to the host file's directory.
	// 相对路径应相对于宿主文件所在目录解析。
	b := newMemBackend()
	b.set("/a/b/host.md", "ref @../c/target.md done")
	b.set("/a/c/target.md", "TARGET")

	l := newLoaderConfig(b, []string{"/a/b/host.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Original text preserved.
	// 保留原始文本。
	if !strings.Contains(content, "ref @../c/target.md done") {
		t.Fatalf("original text should be preserved, got %q", content)
	}
	// Imported file as separate section.
	// 导入的文件作为独立小节。
	if !strings.Contains(content, "Contents of /a/c/target.md") {
		t.Fatalf("expected section for target.md, got %q", content)
	}
	if !strings.Contains(content, "TARGET") {
		t.Fatalf("expected imported content, got %q", content)
	}
}

func TestLoader_RelativeTopLevelPath(t *testing.T) {
	// Top-level file uses relative path; imports with ./ resolve correctly.
	// 顶层文件使用相对路径；带 ./ 的导入应正确解析。
	b := newMemBackend()
	b.set("sub/agents.md", "start @./other.md end")
	b.set("sub/other.md", "OTHER CONTENT")

	l := newLoaderConfig(b, []string{"sub/agents.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "start @./other.md end") {
		t.Fatalf("expected original text preserved, got %q", content)
	}
	if !strings.Contains(content, "OTHER CONTENT") {
		t.Fatalf("expected imported content, got %q", content)
	}
}

func TestLoader_RelativeTopLevelWithDotDotImport(t *testing.T) {
	// Top-level file uses relative path; import with ../ resolves correctly.
	// 顶层文件使用相对路径；带 ../ 的导入应正确解析。
	b := newMemBackend()
	b.set("sub/agents.md", "see @../shared/x.md here")
	b.set("shared/x.md", "SHARED X")

	l := newLoaderConfig(b, []string{"sub/agents.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "SHARED X") {
		t.Fatalf("expected imported content, got %q", content)
	}
	// filepath.Clean should normalize "sub/../shared/x.md" to "shared/x.md"
	// filepath.Clean 应将 "sub/../shared/x.md" 规范化为 "shared/x.md"
	if !strings.Contains(content, "Contents of shared/x.md") {
		t.Fatalf("expected normalized path in section header, got %q", content)
	}
}

func TestLoader_RelativeTopLevelDedup(t *testing.T) {
	// Two top-level relative paths that resolve to the same file via filepath.Clean
	// should be deduped (loaded only once).
	//
	// 两个顶层相对路径经 filepath.Clean 解析到同一文件时
	// 应去重（只加载一次）。
	b := newMemBackend()
	b.set("sub/a.md", "CONTENT A")

	l := newLoaderConfig(b, []string{"sub/a.md", "./sub/a.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := strings.Count(content, "CONTENT A")
	if count != 1 {
		t.Fatalf("expected file loaded once (deduped), got %d occurrences in %q", count, content)
	}
}

func TestLoader_AbsoluteTopLevelWithRelativeImport(t *testing.T) {
	// Absolute top-level path with relative @import resolves correctly.
	// 绝对顶层路径配合相对 @import 应正确解析。
	b := newMemBackend()
	b.set("/project/agents.md", "ref @./lib/helper.md done")
	b.set("/project/lib/helper.md", "HELPER")

	l := newLoaderConfig(b, []string{"/project/agents.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "HELPER") {
		t.Fatalf("expected imported content, got %q", content)
	}
	if !strings.Contains(content, "Contents of /project/lib/helper.md") {
		t.Fatalf("expected section header, got %q", content)
	}
}

func TestLoader_AbsoluteTopLevelWithDotDotImport(t *testing.T) {
	// Absolute top-level path; @import with ../ resolves and normalizes.
	// 绝对顶层路径；带 ../ 的 @import 应解析并规范化。
	b := newMemBackend()
	b.set("/project/sub/agents.md", "load @../shared/x.md here")
	b.set("/project/shared/x.md", "SHARED")

	l := newLoaderConfig(b, []string{"/project/sub/agents.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "SHARED") {
		t.Fatalf("expected imported content, got %q", content)
	}
	// filepath.Clean normalizes "/project/sub/../shared/x.md" to "/project/shared/x.md"
	// filepath.Clean 将 "/project/sub/../shared/x.md" 规范化为 "/project/shared/x.md"
	if !strings.Contains(content, "Contents of /project/shared/x.md") {
		t.Fatalf("expected normalized path in section header, got %q", content)
	}
}

func TestLoader_RelativeImportDedup(t *testing.T) {
	// Two different relative @import paths that resolve to the same file
	// should be deduped via filepath.Clean.
	//
	// 两个不同的相对 @import 路径若解析到同一文件
	// 应通过 filepath.Clean 去重。
	b := newMemBackend()
	b.set("/a/main.md", "first @/a/b/shared.md second @../a/b/shared.md end")
	b.set("/a/b/shared.md", "SHARED ONCE")

	l := newLoaderConfig(b, []string{"/a/main.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	count := strings.Count(content, "SHARED ONCE")
	if count != 1 {
		t.Fatalf("expected shared file loaded once (deduped), got %d in %q", count, content)
	}
}

func TestLoader_NestedRelativeImport(t *testing.T) {
	// File A imports B via relative path, B imports C via relative path.
	// All three should appear as separate sections.
	//
	// 文件 A 通过相对路径导入 B，B 通过相对路径导入 C。
	// 三者都应作为独立小节出现。
	b := newMemBackend()
	b.set("/root/main.md", "start @sub/mid.md end")
	b.set("/root/sub/mid.md", "mid @deep/leaf.md mid_end")
	b.set("/root/sub/deep/leaf.md", "LEAF")

	l := newLoaderConfig(b, []string{"/root/main.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, section := range []string{"Contents of /root/main.md", "Contents of /root/sub/mid.md", "Contents of /root/sub/deep/leaf.md"} {
		if !strings.Contains(content, section) {
			t.Fatalf("expected section %q, got %q", section, content)
		}
	}
	if !strings.Contains(content, "LEAF") {
		t.Fatalf("expected leaf content, got %q", content)
	}
}

func TestLoader_TransitiveImport(t *testing.T) {
	// Imported file itself contains @imports; all should appear as separate sections.
	// 导入的文件自身也包含 @imports；所有文件都应作为独立小节出现。
	b := newMemBackend()
	b.set("/main.md", "header @/mid.md footer")
	b.set("/mid.md", "mid-start @/leaf.md mid-end")
	b.set("/leaf.md", "LEAF_VALUE")

	l := newLoaderConfig(b, []string{"/main.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, section := range []string{"Contents of /main.md", "Contents of /mid.md", "Contents of /leaf.md"} {
		if !strings.Contains(content, section) {
			t.Fatalf("expected section %q, got %q", section, content)
		}
	}
	if !strings.Contains(content, "LEAF_VALUE") {
		t.Fatalf("expected leaf value, got %q", content)
	}
}

func TestLoader_EmptyFile(t *testing.T) {
	b := newMemBackend()
	b.set("/empty.md", "")

	l := newLoaderConfig(b, []string{"/empty.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Empty file is treated as non-existent, so output should be empty.
	// 空文件视为不存在，因此输出应为空。
	if content != "" {
		t.Fatalf("expected empty output for empty file, got %q", content)
	}
}

func TestLoader_MaxBytesFirstFileFull(t *testing.T) {
	// Even if the first file alone exceeds maxBytes, it should still be loaded in full.
	// 即使第一个文件本身超过 maxBytes，也应完整加载。
	b := newMemBackend()
	b.set("/big.md", "ABCDEFGHIJ") // 10 bytes

	l := newLoaderConfig(b, []string{"/big.md"}, 3, nil)
	content, err := l.load(context.Background()) // maxBytes=3, but first file always loads
	// maxBytes=3，但第一个文件总会加载
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "ABCDEFGHIJ") {
		t.Fatalf("first file should always load in full, got %q", content)
	}
}

func TestLoader_CircularImportInline(t *testing.T) {
	// Circular reference via @import should be detected, logged, and skipped.
	// 应检测、记录并跳过通过 @import 形成的循环引用。
	b := newMemBackend()
	b.set("/a.md", "text @/b.md more")
	b.set("/b.md", "ref @/a.md back")

	l := newLoaderConfig(b, []string{"/a.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error (circular import is logged), got %v", err)
	}
	// Both a and b should have sections; circular back-reference a from b is skipped.
	// a 和 b 都应有小节；从 b 循环回引 a 会被跳过。
	if !strings.Contains(content, "Contents of /a.md") {
		t.Fatalf("expected /a.md section, got %q", content)
	}
	if !strings.Contains(content, "Contents of /b.md") {
		t.Fatalf("expected /b.md section, got %q", content)
	}
}

func TestLoader_MaxDepthInline(t *testing.T) {
	// Deep chain via @import should be logged at depth > 5, not returned as error.
	// 通过 @import 形成的深链在深度 > 5 时应记录日志，而不是作为错误返回。
	b := newMemBackend()
	for i := 0; i < 7; i++ {
		var content string
		if i < 6 {
			content = fmt.Sprintf("level%d @/level%d.md tail", i, i+1)
		} else {
			content = fmt.Sprintf("level%d", i)
		}
		b.set(fmt.Sprintf("/level%d.md", i), content)
	}

	l := newLoaderConfig(b, []string{"/level0.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error (depth exceeded is logged), got %v", err)
	}
	// Levels 0-5 should have sections.
	// 层级 0-5 都应有小节。
	for i := 0; i <= 5; i++ {
		want := fmt.Sprintf("Contents of /level%d.md", i)
		if !strings.Contains(content, want) {
			t.Fatalf("expected %q in content, got %q", want, content)
		}
	}
	// Level 6 should not be present.
	// 不应出现第 6 层。
	if strings.Contains(content, "Contents of /level6.md") {
		t.Fatalf("level6 should not be present (depth exceeded), got %q", content)
	}
}

func TestLoader_DiamondDependency(t *testing.T) {
	// A imports B and D; B imports C; D also imports C.
	// C should appear only once (deduped across the whole load).
	//
	// A 导入 B 和 D；B 导入 C；D 也导入 C。
	// C 应只出现一次（在整个加载过程中去重）。
	b := newMemBackend()
	b.set("/a.md", "start @/b.md middle @/d.md end")
	b.set("/b.md", "B(@/c.md)")
	b.set("/d.md", "D(@/c.md)")
	b.set("/c.md", "SHARED")

	l := newLoaderConfig(b, []string{"/a.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("diamond dependency should not be circular, got error: %v", err)
	}

	// C should appear only once as a section (deduped).
	// C 应只作为一个 section 出现一次（已去重）。
	count := strings.Count(content, "Contents of /c.md")
	if count != 1 {
		t.Fatalf("expected /c.md section once (deduped), got %d in %q", count, content)
	}
	// All files should have sections.
	// 所有文件都应有 section。
	for _, section := range []string{"Contents of /a.md", "Contents of /b.md", "Contents of /c.md", "Contents of /d.md"} {
		if !strings.Contains(content, section) {
			t.Fatalf("expected section %q, got %q", section, content)
		}
	}
}

func TestLoader_AtSignInNormalText(t *testing.T) {
	// Bare @word without "/" or file extension should not trigger import.
	// Email-like patterns (@example.com) with non-allowed extensions should also be ignored.
	//
	// 不带 "/" 或文件扩展名的裸 @word 不应触发导入。
	// 类似 email 的模式（@example.com）如果扩展名不允许，也应被忽略。
	b := newMemBackend()
	b.set("/agent.md", "contact me @ anytime or @  spaces and @someone mentioned and user@example.com and @company.org")

	l := newLoaderConfig(b, []string{"/agent.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "contact me @ anytime") {
		t.Fatalf("bare @ should not trigger import, got %q", content)
	}
	if !strings.Contains(content, "@someone mentioned") {
		t.Fatalf("@someone without / or extension should not trigger import, got %q", content)
	}
	if !strings.Contains(content, "@example.com") {
		t.Fatalf("email-like @example.com should not trigger import, got %q", content)
	}
	if !strings.Contains(content, "@company.org") {
		t.Fatalf("email-like @company.org should not trigger import, got %q", content)
	}
}

func TestLoader_MaxBytesWithImports(t *testing.T) {
	// Two top-level files that both import the same shared file.
	// Budget should account for imported file bytes.
	//
	// 两个顶层文件都导入同一个共享文件。
	// 预算应计入导入文件的字节数。
	b := newMemBackend()
	b.set("/a.md", "A(@/shared.md)")
	b.set("/b.md", "B(@/shared.md)")
	b.set("/shared.md", strings.Repeat("X", 100)) // 100 bytes

	l := newLoaderConfig(b, []string{"/a.md", "/b.md"}, 120, nil)
	// /a.md = 14 bytes + /shared.md = 100 bytes => 114 total after /a.md.
	// Budget = 120: /b.md (14 bytes) would push to 128, exceeding budget.
	//
	// /a.md = 14 字节 + /shared.md = 100 字节 => 加载 /a.md 后总计 114。
	// 预算 = 120：/b.md（14 字节）会使总数达到 128，超出预算。
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}

	// /a.md and its import should be included.
	// 应包含 /a.md 及其导入。
	if !strings.Contains(content, strings.Repeat("X", 100)) {
		t.Fatal("expected /a.md with shared content to be included")
	}

	// /b.md should be excluded because totalBytes exceeded budget after loading /a.md.
	// 加载 /a.md 后 totalBytes 已超出预算，因此应排除 /b.md。
	if strings.Contains(content, "B(") {
		t.Fatalf("expected /b.md to be excluded due to budget, got %q", content)
	}
}

func TestNew_Validation_EmptyAgentFiles(t *testing.T) {
	ctx := context.Background()
	b := newMemBackend()

	_, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{}})
	if err == nil {
		t.Fatal("expected error for empty agent files")
	}
	if !strings.Contains(err.Error(), "at least one agent file path is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestMiddleware_GenerateError(t *testing.T) {
	// Non-ErrNotExist errors (e.g. permission denied) should propagate.
	// 非 ErrNotExist 错误（例如 permission denied）应向上传播。
	b := &errBackend{}

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/file.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hi"}}}
	_, _, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err == nil {
		t.Fatal("expected error when backend read fails with non-ErrNotExist")
	}
	if !strings.Contains(err.Error(), "failed to load agent files") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoader_DuplicateTopLevelFiles(t *testing.T) {
	// Same file listed twice in AgentFiles; second should be deduped via seen map.
	// 同一文件在 AgentFiles 中列出两次；第二次应通过 seen map 去重。
	b := newMemBackend()
	b.set("/agent.md", "unique content")

	l := newLoaderConfig(b, []string{"/agent.md", "/agent.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	count := strings.Count(content, "Contents of /agent.md")
	if count != 1 {
		t.Fatalf("expected /agent.md section once (deduped), got %d", count)
	}
}

func TestLoader_LoadFileError(t *testing.T) {
	// Missing file (ErrNotExist) is silently skipped.
	// 缺失文件（ErrNotExist）会被静默跳过。
	b := newMemBackend()
	l := newLoaderConfig(b, []string{"/missing.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected missing file to be skipped, got error: %v", err)
	}
	if content != "" {
		t.Fatalf("expected empty output, got %q", content)
	}
}

func TestLoader_MaxBytesStopsImports(t *testing.T) {
	// When budget is exhausted, further imports in collectImports should be skipped.
	// 预算耗尽时，应跳过 collectImports 中的后续导入。
	b := newMemBackend()
	b.set("/main.md", "@/big.md @/small.md")
	b.set("/big.md", strings.Repeat("B", 200))
	b.set("/small.md", "SMALL")

	l := newLoaderConfig(b, []string{"/main.md"}, 50, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// main.md itself is loaded (always), big.md pushes over budget,
	// small.md should be skipped.
	//
	// main.md 本身会被加载（始终如此），big.md 会使预算超限，
	// small.md 应被跳过。
	if !strings.Contains(content, "Contents of /main.md") {
		t.Fatal("main.md should be present")
	}
	if strings.Contains(content, "SMALL") {
		t.Fatal("small.md should be skipped after budget exhausted")
	}
}

func TestFormatContent_Empty(t *testing.T) {
	// formatContent with nil/empty slice should return empty string.
	// formatContent 对 nil/空切片应返回空字符串。
	if got := formatContent(nil); got != "" {
		t.Fatalf("expected empty string for nil, got %q", got)
	}
	if got := formatContent([]loadedFile{}); got != "" {
		t.Fatalf("expected empty string for empty slice, got %q", got)
	}
}

func TestMiddleware_AllFilesEmpty(t *testing.T) {
	// When all agent files have empty content, loader returns "" and
	// BeforeModelRewriteState returns the original state unchanged.
	//
	// 当所有 agent 文件内容都为空时，loader 返回 ""，
	// BeforeModelRewriteState 返回未修改的原始 state。
	b := newMemBackend()
	b.set("/agent.md", "")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	userMsg := []*schema.Message{{Role: schema.User, Content: "hello"}}
	state := &adk.ChatModelAgentState{Messages: userMsg}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Empty file produces no agentmd content, so original messages pass through unchanged.
	// 空文件不会产生 agentmd 内容，因此原始 messages 会原样传递。
	if len(state.Messages) != 1 {
		t.Fatalf("expected 1 message (no agentmd prepended), got %d", len(state.Messages))
	}
	if state.Messages[0].Content != "hello" {
		t.Fatalf("expected original message unchanged, got %q", state.Messages[0].Content)
	}
}

func TestLoader_ExactOutput(t *testing.T) {
	// Verify the exact output format matches the expected structure:
	// each file (top-level and imported) gets its own "Contents of ..." section,
	// @path references are preserved in the original text.
	//
	// 验证确切输出格式是否符合预期结构：
	// 每个文件（顶层和导入的）都有自己的 "Contents of ..." section，
	// @path 引用会保留在原始文本中。
	b := newMemBackend()
	b.set("/project/CLAUDE.md", "this is project claude.md\n\n- git workflow @git/git-instructions.md")
	b.set("/project/git/git-instructions.md", "this is git-instructions.md")

	l := newLoaderConfig(b, []string{"/project/CLAUDE.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	expected := `<system-reminder>
As you answer the user's questions, you can use the following context:
Codebase and user instructions are shown below. Be sure to adhere to these instructions. IMPORTANT: These instructions OVERRIDE any default behavior and you MUST follow them exactly as written.

Contents of /project/CLAUDE.md (instructions):

this is project claude.md

- git workflow @git/git-instructions.md

Contents of /project/git/git-instructions.md (instructions):

this is git-instructions.md
IMPORTANT: this context may or may not be relevant to your tasks. You should not respond to this context unless it is highly relevant to your task.
</system-reminder>`

	if content != expected {
		t.Fatalf("output mismatch.\n\ngot:\n%s\n\nexpected:\n%s", content, expected)
	}
}

func TestLoader_MissingFileSkipped(t *testing.T) {
	b := newMemBackend()
	b.set("/good.md", "GOOD CONTENT")
	// /missing.md is not set
	// 未设置 /missing.md

	l := newLoaderConfig(b, []string{"/missing.md", "/good.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error for missing file, got %v", err)
	}
	if !strings.Contains(content, "GOOD CONTENT") {
		t.Fatal("expected good.md content in output")
	}
}

func TestLoader_AllMissingFilesSkipped(t *testing.T) {
	b := newMemBackend()

	l := newLoaderConfig(b, []string{"/missing1.md", "/missing2.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error for missing files, got %v", err)
	}
	if content != "" {
		t.Fatalf("expected empty output when all files missing, got %q", content)
	}
}

func TestLoader_CircularImportSkipped(t *testing.T) {
	b := newMemBackend()
	b.set("/a.md", "A content @/b.md")
	b.set("/b.md", "B content @/a.md")

	// Circular import in collectImports is logged via onWarning and skipped.
	// collectImports 中的循环导入会通过 onWarning 记录并跳过。
	l := newLoaderConfig(b, []string{"/a.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(content, "A content") {
		t.Fatal("expected a.md content")
	}
	if !strings.Contains(content, "B content") {
		t.Fatal("expected b.md content")
	}
}

func TestLoader_DepthExceededSkipped(t *testing.T) {
	b := newMemBackend()
	// Create a chain that exceeds maxImportDepth (5)
	// 创建一条超过 maxImportDepth (5) 的链
	b.set("/l0.md", "@/l1.md")
	b.set("/l1.md", "@/l2.md")
	b.set("/l2.md", "@/l3.md")
	b.set("/l3.md", "@/l4.md")
	b.set("/l4.md", "@/l5.md")
	b.set("/l5.md", "@/l6.md")
	b.set("/l6.md", "DEEP")

	l := newLoaderConfig(b, []string{"/l0.md"}, 0, nil)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error for depth exceeded, got %v", err)
	}
	// Should have content up to the depth limit, deep file skipped.
	// 内容应包含到深度限制为止，跳过更深的文件。
	if !strings.Contains(content, "/l0.md") {
		t.Fatal("expected l0.md in output")
	}
}

func TestLoader_OnLoadWarningCallback(t *testing.T) {
	b := newMemBackend()
	b.set("/good.md", "GOOD CONTENT")

	var warnings []error
	onWarning := func(filePath string, err error) {
		warnings = append(warnings, fmt.Errorf("%s: %w", filePath, err))
	}

	l := newLoaderConfig(b, []string{"/missing.md", "/good.md"}, 0, onWarning)
	content, err := l.load(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(content, "GOOD CONTENT") {
		t.Fatal("expected good.md content in output")
	}
	if len(warnings) == 0 {
		t.Fatal("expected at least one warning for missing file")
	}
	if !strings.Contains(warnings[0].Error(), "file not found") {
		t.Fatalf("expected file not found warning, got %v", warnings[0])
	}
}

func TestMiddleware_MissingFile(t *testing.T) {
	b := newMemBackend()
	// /missing.md not set — will fail to read
	// /missing.md 未设置 — 读取会失败

	ctx := context.Background()
	mw, err := New(ctx, &Config{
		Backend:       b,
		AgentsMDFiles: []string{"/missing.md"},
	})
	if err != nil {
		t.Fatal(err)
	}

	userMsg := []*schema.Message{{Role: schema.User, Content: "hello"}}
	state := &adk.ChatModelAgentState{Messages: userMsg}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatalf("expected no error for missing file, got %v", err)
	}
	// No agent.md content, so original messages should be passed through unchanged.
	// 没有 agent.md 内容，因此原始 messages 应原样传递。
	if len(state.Messages) != 1 {
		t.Fatalf("expected 1 message (no agentmd prepended), got %d", len(state.Messages))
	}
}

func TestMiddleware_InsertBeforeFirstUserMessage(t *testing.T) {
	b := newMemBackend()
	b.set("/agent.md", "agent instructions")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	// Input has a System message before the User message.
	// 输入在 User message 之前有一条 System message。
	input := []*schema.Message{
		{Role: schema.System, Content: "system prompt"},
		{Role: schema.User, Content: "hello"},
	}
	state := &adk.ChatModelAgentState{Messages: input}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(state.Messages))
	}
	if state.Messages[0].Role != schema.System {
		t.Fatalf("expected first message role System, got %s", state.Messages[0].Role)
	}
	if state.Messages[0].Content != "system prompt" {
		t.Fatalf("expected system prompt preserved, got %q", state.Messages[0].Content)
	}
	if state.Messages[1].Role != schema.User || !strings.Contains(state.Messages[1].Content, "agent instructions") {
		t.Fatalf("expected agentmd message before user message, got role=%s content=%q", state.Messages[1].Role, state.Messages[1].Content)
	}
	if state.Messages[2].Role != schema.User || state.Messages[2].Content != "hello" {
		t.Fatalf("expected original user message at index 2, got role=%s content=%q", state.Messages[2].Role, state.Messages[2].Content)
	}
}

func TestMiddleware_InsertWithNoUserMessage(t *testing.T) {
	b := newMemBackend()
	b.set("/agent.md", "agent instructions")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	// Input has no User message at all.
	// 输入完全没有 User message。
	input := []*schema.Message{
		{Role: schema.System, Content: "system prompt"},
		{Role: schema.Assistant, Content: "assistant reply"},
	}
	state := &adk.ChatModelAgentState{Messages: input}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(state.Messages))
	}
	if state.Messages[0].Role != schema.System {
		t.Fatalf("expected System at index 0, got %s", state.Messages[0].Role)
	}
	if state.Messages[1].Role != schema.Assistant {
		t.Fatalf("expected Assistant at index 1, got %s", state.Messages[1].Role)
	}
	if state.Messages[2].Role != schema.User || !strings.Contains(state.Messages[2].Content, "agent instructions") {
		t.Fatalf("expected agentmd appended at end, got role=%s content=%q", state.Messages[2].Role, state.Messages[2].Content)
	}
}

func TestLoader_ImportIOError(t *testing.T) {
	// When an imported file returns a non-ErrNotExist error (e.g. I/O error),
	// the load should propagate the error (covers collectImports and loadFile error paths).
	//
	// 当导入的文件返回非 ErrNotExist 错误（例如 I/O 错误）时，
	// load 应传播该错误（覆盖 collectImports 和 loadFile 的错误路径）。
	b := &partialErrBackend{
		files: map[string]string{
			"/main.md": "content @/broken.md",
		},
		// /broken.md is NOT in the map, so Read returns I/O error (not ErrNotExist)
		// /broken.md 不在 map 中，因此 Read 返回 I/O 错误（不是 ErrNotExist）
	}

	l := newLoaderConfig(b, []string{"/main.md"}, 0, nil)
	_, err := l.load(context.Background())
	if err == nil {
		t.Fatal("expected error from I/O failure on imported file")
	}
	if !strings.Contains(err.Error(), "I/O error") {
		t.Fatalf("expected I/O error, got: %v", err)
	}
}

func TestMiddleware_Idempotency(t *testing.T) {
	// Calling BeforeModelRewriteState twice should NOT duplicate the agentsmd message.
	// The marker in msg.Extra[agentsMDExtraKey] prevents re-injection.
	//
	// 调用 BeforeModelRewriteState 两次不应重复添加 agentsmd message。
	// msg.Extra[agentsMDExtraKey] 中的 marker 会防止再次注入。
	b := newMemBackend()
	b.set("/agent.md", "agent instructions")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hello"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages after first call, got %d", len(state.Messages))
	}

	// Call again with the same state (which now contains the marker message).
	// 用同一个 state 再次调用（此时其中已包含 marker message）。
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages after second call (idempotent), got %d", len(state.Messages))
	}
	if !strings.Contains(state.Messages[0].Content, "agent instructions") {
		t.Fatalf("expected agentmd content preserved, got %q", state.Messages[0].Content)
	}
}

func TestMiddleware_ReinsertAfterRemoval(t *testing.T) {
	// If the marker message is removed from state.Messages, calling
	// BeforeModelRewriteState should re-insert it.
	//
	// 如果 marker message 从 state.Messages 中移除，调用
	// BeforeModelRewriteState 应重新插入它。
	b := newMemBackend()
	b.set("/agent.md", "agent instructions")

	ctx := context.Background()
	mw, err := New(ctx, &Config{Backend: b, AgentsMDFiles: []string{"/agent.md"}})
	if err != nil {
		t.Fatal(err)
	}

	state := &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hello"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages after first call, got %d", len(state.Messages))
	}

	// Simulate removal of the marker message (e.g., by summarization).
	// Keep only the original user message.
	//
	// 模拟移除 marker message（例如由 summarization 移除）。
	// 只保留原始 user message。
	state = &adk.ChatModelAgentState{Messages: []*schema.Message{{Role: schema.User, Content: "hello"}}}
	_, state, err = mw.BeforeModelRewriteState(ctx, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Messages) != 2 {
		t.Fatalf("expected 2 messages after re-insert, got %d", len(state.Messages))
	}
	if !strings.Contains(state.Messages[0].Content, "agent instructions") {
		t.Fatalf("expected agentmd content re-inserted, got %q", state.Messages[0].Content)
	}
}

func TestNewTypedAgenticMessage(t *testing.T) {
	ctx := context.Background()
	b := newMemBackend()
	b.set("/agent.md", "You are a helpful assistant.")

	mw, err := NewTyped[*schema.AgenticMessage](ctx, &Config{
		Backend:       b,
		AgentsMDFiles: []string{"/agent.md"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}

	var _ adk.TypedChatModelAgentMiddleware[*schema.AgenticMessage] = mw
}
