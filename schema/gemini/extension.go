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

// Package gemini defines the extension for gemini.
// Package gemini 定义 gemini 的扩展。
package gemini

import (
	"fmt"
)

type ResponseMetaExtension struct {
	ID            string             `json:"id,omitempty"`
	FinishReason  string             `json:"finish_reason,omitempty"`
	GroundingMeta *GroundingMetadata `json:"grounding_meta,omitempty"`
}

type GroundingMetadata struct {
	// List of supporting references retrieved from specified grounding source.
	// 从指定 grounding source 检索到的支持引用列表。
	GroundingChunks []*GroundingChunk `json:"grounding_chunks,omitempty"`
	// Optional. List of grounding support.
	// 可选。grounding support 列表。
	GroundingSupports []*GroundingSupport `json:"grounding_supports,omitempty"`
	// Optional. Google search entry for the following-up web searches.
	// 可选。用于后续网页搜索的 Google 搜索条目。
	SearchEntryPoint *SearchEntryPoint `json:"search_entry_point,omitempty"`
	// Optional. Web search queries for the following-up web search.
	// 可选。用于后续网页搜索的网页搜索查询。
	WebSearchQueries []string `json:"web_search_queries,omitempty"`
}

type GroundingChunk struct {
	// Grounding chunk from the web.
	// 来自网页的 grounding chunk。
	Web *GroundingChunkWeb `json:"web,omitempty"`
}

// GroundingChunkWeb is the chunk from the web.
// GroundingChunkWeb 是来自网页的 chunk。
type GroundingChunkWeb struct {
	// Domain of the (original) URI. This field is not supported in Gemini API.
	// （原始）URI 的域名。Gemini API 不支持此字段。
	Domain string `json:"domain,omitempty"`
	// Title of the chunk.
	// chunk 的标题。
	Title string `json:"title,omitempty"`
	// URI reference of the chunk.
	// chunk 的 URI 引用。
	URI string `json:"uri,omitempty"`
}

type GroundingSupport struct {
	// Confidence score of the support references. Ranges from 0 to 1. 1 is the most confident.
	// For Gemini 2.0 and before, this list must have the same size as the grounding_chunk_indices.
	// For Gemini 2.5 and after, this list will be empty and should be ignored.
	//
	// 支持引用的置信分数，范围为 0 到 1，1 表示最可信。
	// 对于 Gemini 2.0 及更早版本，此列表大小必须与 grounding_chunk_indices 相同。
	// 对于 Gemini 2.5 及以后版本，此列表会为空，应忽略。
	ConfidenceScores []float32 `json:"confidence_scores,omitempty"`
	// A list of indices (into 'grounding_chunk') specifying the citations associated with
	// the claim. For instance [1,3,4] means that grounding_chunk[1], grounding_chunk[3],
	// grounding_chunk[4] are the retrieved content attributed to the claim.
	//
	// 索引列表（指向 'grounding_chunk'），指定与该声明关联的引用。
	// 例如 [1,3,4] 表示 grounding_chunk[1]、grounding_chunk[3]、grounding_chunk[4] 是归因于该声明的检索内容。
	GroundingChunkIndices []int `json:"grounding_chunk_indices,omitempty"`
	// Segment of the content this support belongs to.
	// 此 support 所属的内容 segment。
	Segment *Segment `json:"segment,omitempty"`
}

// Segment of the content.
// 内容 segment。
type Segment struct {
	// Output only. End index in the given Part, measured in bytes. Offset from the start
	// of the Part, exclusive, starting at zero.
	//
	// 仅输出。给定 Part 中的结束索引，按字节计。从 Part 开头起的偏移量，不包含该位置，从零开始。
	EndIndex int `json:"end_index,omitempty"`
	// Output only. The index of a Part object within its parent Content object.
	// 仅输出。Part 对象在其父 Content 对象中的索引。
	PartIndex int `json:"part_index,omitempty"`
	// Output only. Start index in the given Part, measured in bytes. Offset from the start
	// of the Part, inclusive, starting at zero.
	//
	// 仅输出。给定 Part 中的起始索引，按字节计。从 Part 开头起的偏移量，包含该位置，从零开始。
	StartIndex int `json:"start_index,omitempty"`
	// Output only. The text corresponding to the segment from the response.
	// 仅输出。响应中对应片段的文本。
	Text string `json:"text,omitempty"`
}

// SearchEntryPoint is the Google search entry point.
// SearchEntryPoint 是 Google 搜索入口点。
type SearchEntryPoint struct {
	// Optional. Web content snippet that can be embedded in a web page or an app webview.
	// 可选。可嵌入网页或应用 webview 的 Web 内容片段。
	RenderedContent string `json:"rendered_content,omitempty"`
	// Optional. Base64 encoded JSON representing array of tuple.
	// 可选。表示 tuple 数组的 Base64 编码 JSON。
	SDKBlob []byte `json:"sdk_blob,omitempty"`
}

// ConcatResponseMetaExtensions concatenates multiple ResponseMetaExtension chunks into a single one.
// ConcatResponseMetaExtensions 将多个 ResponseMetaExtension 块拼接为一个。
func ConcatResponseMetaExtensions(chunks []*ResponseMetaExtension) (*ResponseMetaExtension, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no response meta extension found")
	}
	if len(chunks) == 1 {
		return chunks[0], nil
	}

	ret := &ResponseMetaExtension{}

	for _, ext := range chunks {
		if ext.ID != "" {
			ret.ID = ext.ID
		}
		if ext.FinishReason != "" {
			ret.FinishReason = ext.FinishReason
		}
		if ext.GroundingMeta != nil {
			ret.GroundingMeta = ext.GroundingMeta
		}
	}

	return ret, nil
}
