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

package schema

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/eino-contrib/jsonschema"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

// DataType is the type of the parameter.
// It must be one of the following values: "object", "number", "integer", "string", "array", "null", "boolean", which is the same as the type of the parameter in JSONSchema.
//
// DataType 是参数的类型。
// 它必须是以下值之一："object"、"number"、"integer"、"string"、"array"、"null"、"boolean"，与 JSONSchema 中参数的类型相同。
type DataType string

// Supported JSONSchema data types for tool parameters.
// 工具参数支持的 JSONSchema 数据类型。
const (
	Object  DataType = "object"
	Number  DataType = "number"
	Integer DataType = "integer"
	String  DataType = "string"
	Array   DataType = "array"
	Null    DataType = "null"
	Boolean DataType = "boolean"
)

// ToolChoice controls how the model uses the tools provided to it.
// Pass as part of the model option via [model.WithToolChoice].
//
// ToolChoice 控制模型如何使用提供给它的工具。
// 通过 [model.WithToolChoice] 作为模型 option 的一部分传入。
type ToolChoice string

const (
	// ToolChoiceForbidden instructs the model not to call any tools, even if
	// tools are bound. The model responds with a plain text message instead.
	// Corresponds to "none" in OpenAI Chat Completion.
	//
	// ToolChoiceForbidden 指示模型不要调用任何工具，即使已绑定工具。
	// 模型会改为返回纯文本消息。
	// 对应 OpenAI Chat Completion 中的 "none"。
	ToolChoiceForbidden ToolChoice = "forbidden"

	// ToolChoiceAllowed lets the model decide: it may generate a plain message
	// or call one or more tools. This is the default when tools are provided.
	// Corresponds to "auto" in OpenAI Chat Completion.
	//
	// ToolChoiceAllowed 让模型自行决定：可以生成普通消息，或调用一个或多个工具。
	// 这是提供工具时的默认值。
	// 对应 OpenAI Chat Completion 中的 "auto"。
	ToolChoiceAllowed ToolChoice = "allowed"

	// ToolChoiceForced requires the model to call at least one tool. Use this
	// when you want to guarantee structured output via tool calling.
	// Corresponds to "required" in OpenAI Chat Completion.
	//
	// ToolChoiceForced 要求模型至少调用一个工具。
	// 当你希望通过工具调用保证结构化输出时使用。
	// 对应 OpenAI Chat Completion 中的 "required"。
	ToolChoiceForced ToolChoice = "forced"
)

type AgenticToolChoice struct {
	// Type is the tool choice mode.
	// Type 是工具选择模式。
	Type ToolChoice

	// Allowed optionally specifies the list of tools that the model is permitted to call.
	// Optional.
	//
	// Allowed 可选地指定模型允许调用的工具列表。
	// 可选。
	Allowed *AgenticAllowedToolChoice

	// Forced optionally specifies the list of tools that the model is required to call.
	// Optional.
	//
	// Forced 可选地指定模型必须调用的工具列表。
	// 可选。
	Forced *AgenticForcedToolChoice
}

// AgenticAllowedToolChoice specifies a list of allowed tools for the model.
// AgenticAllowedToolChoice 指定模型允许使用的工具列表。
type AgenticAllowedToolChoice struct {
	// Tools is the list of allowed tools for the model to call.
	// Optional.
	//
	// Tools 是模型允许调用的工具列表。
	// 可选。
	Tools []*AllowedTool
}

// AgenticForcedToolChoice specifies a list of tools that the model must call.
// AgenticForcedToolChoice 指定模型必须调用的工具列表。
type AgenticForcedToolChoice struct {
	// Tools is the list of tools that the model must call.
	// Optional.
	//
	// Tools 是模型必须调用的工具列表。
	// 可选。
	Tools []*AllowedTool
}

// AllowedTool represents a tool that the model is allowed or forced to call.
// Exactly one of FunctionName, MCPTool, or ServerTool must be specified.
//
// AllowedTool 表示模型允许或被强制调用的工具。
// 必须且只能指定 FunctionName、MCPTool 或 ServerTool 中的一个。
type AllowedTool struct {
	// FunctionName specifies a function tool by name.
	// FunctionName 通过名称指定一个函数工具。
	FunctionName string

	// MCPTool specifies an MCP tool.
	// MCPTool 指定一个 MCP 工具。
	MCPTool *AllowedMCPTool

	// ServerTool specifies a server tool.
	// ServerTool 指定一个服务器工具。
	ServerTool *AllowedServerTool
}

// AllowedMCPTool contains the information for identifying an MCP tool.
// AllowedMCPTool 包含用于标识 MCP 工具的信息。
type AllowedMCPTool struct {
	// ServerLabel is the label of the MCP server.
	// ServerLabel 是 MCP 服务器的标签。
	ServerLabel string
	// Name is the name of the MCP tool.
	// Name 是 MCP 工具的名称。
	Name string
}

// AllowedServerTool contains the information for identifying a server tool.
// AllowedServerTool 包含用于标识服务器工具的信息。
type AllowedServerTool struct {
	// Name is the name of the server tool.
	// Name 是服务器工具的名称。
	Name string
}

// ToolInfo is the information of a tool.
// ToolInfo describes a tool that can be passed to a ChatModel via
// [ToolCallingChatModel.WithTools] or [ChatModel.BindTools].
//
// Name should be concise and unique within the tool set. Desc should explain
// when and why to use the tool; few-shot examples in Desc significantly improve
// model accuracy. ParamsOneOf may be nil for tools that take no arguments.
//
// ToolInfo 是工具的信息。
// ToolInfo 描述可通过 [ToolCallingChatModel.WithTools] 或 [ChatModel.BindTools] 传给 ChatModel 的工具。
// Name 应在工具集中简洁且唯一。Desc 应说明何时以及为什么使用该工具；在 Desc 中加入 few-shot 示例可显著提升模型准确性。ParamsOneOf 对无需参数的工具可为 nil。
type ToolInfo struct {
	// The unique name of the tool that clearly communicates its purpose.
	// 清晰表达用途的工具唯一名称。
	Name string
	// Used to tell the model how/when/why to use the tool.
	// You can provide few-shot examples as a part of the description.
	//
	// 用于告诉模型如何、何时以及为什么使用该工具。
	// 可以在描述中提供 few-shot 示例。
	Desc string
	// Extra is the extra information for the tool.
	// Extra 是工具的额外信息。
	Extra map[string]any

	// The parameters the functions accepts (different models may require different parameter types).
	// can be described in two ways:
	//  - use params: schema.NewParamsOneOfByParams(params)
	//  - use jsonschema: schema.NewParamsOneOfByJSONSchema(jsonschema)
	// If is nil, signals that the tool does not need any input parameter
	//
	// 函数接受的参数（不同模型可能需要不同的参数类型）。
	// 可用两种方式描述：
	// - 使用 params: schema.NewParamsOneOfByParams(params)
	// - 使用 jsonschema: schema.NewParamsOneOfByJSONSchema(jsonschema)
	// 如果为 nil，表示该工具不需要任何输入参数。
	*ParamsOneOf
}

type toolInfoForJSON struct {
	Name           string                    `json:"name,omitempty"`
	Desc           string                    `json:"desc,omitempty"`
	Extra          map[string]any            `json:"extra,omitempty"`
	HasParamsOneOf bool                      `json:"has_params_one_of,omitempty"`
	Params         map[string]*ParameterInfo `json:"params,omitempty"`
	JSONSchema     *jsonschema.Schema        `json:"json_schema,omitempty"`
}

type toolInfoForGob struct {
	Name           string
	Desc           string
	Extra          map[string]any
	HasParamsOneOf bool
	Params         map[string]*ParameterInfo
	JSONSchema     *string
}

func (t *ToolInfo) MarshalJSON() ([]byte, error) {
	tmp := &toolInfoForJSON{
		Name:  t.Name,
		Desc:  t.Desc,
		Extra: t.Extra,
	}
	if t.ParamsOneOf != nil {
		tmp.HasParamsOneOf = true
		tmp.Params = t.ParamsOneOf.params
		tmp.JSONSchema = t.ParamsOneOf.jsonschema
	}
	return json.Marshal(tmp)
}

func (t *ToolInfo) UnmarshalJSON(data []byte) error {
	tmp := &toolInfoForJSON{}
	if err := json.Unmarshal(data, tmp); err != nil {
		return err
	}
	t.Name = tmp.Name
	t.Desc = tmp.Desc
	t.Extra = tmp.Extra
	if tmp.HasParamsOneOf {
		t.ParamsOneOf = &ParamsOneOf{
			params:     tmp.Params,
			jsonschema: tmp.JSONSchema,
		}
		// An empty-but-non-nil params map is dropped by `omitempty` during
		// marshaling. When jsonschema is also absent, the params form was the
		// chosen representation, so restore the empty map to preserve the
		// roundtrip invariant.
		//
		// 一个空但非 nil 的 params map 会在 marshaling 时被 `omitempty` 丢弃。
		// 当 jsonschema 也不存在时，表示所选表示形式是 params，因此恢复空 map 以保持 roundtrip invariant。
		if t.ParamsOneOf.params == nil && t.ParamsOneOf.jsonschema == nil {
			t.ParamsOneOf.params = map[string]*ParameterInfo{}
		}
	}
	return nil
}

func (t *ToolInfo) GobEncode() ([]byte, error) {
	tmp := &toolInfoForGob{
		Name:  t.Name,
		Desc:  t.Desc,
		Extra: t.Extra,
	}
	if t.ParamsOneOf != nil {
		tmp.HasParamsOneOf = true
		tmp.Params = t.ParamsOneOf.params
		if t.ParamsOneOf.jsonschema != nil {
			b, err := json.Marshal(t.ParamsOneOf.jsonschema)
			if err != nil {
				return nil, err
			}
			str := string(b)
			tmp.JSONSchema = &str
		}
	}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(tmp); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *ToolInfo) GobDecode(b []byte) error {
	tmp := &toolInfoForGob{}
	if err := gob.NewDecoder(bytes.NewBuffer(b)).Decode(tmp); err != nil {
		return err
	}
	t.Name = tmp.Name
	t.Desc = tmp.Desc
	t.Extra = tmp.Extra
	if !tmp.HasParamsOneOf {
		return nil
	}
	t.ParamsOneOf = &ParamsOneOf{
		params: tmp.Params,
	}
	if tmp.JSONSchema != nil {
		s := &jsonschema.Schema{}
		if err := json.Unmarshal([]byte(*tmp.JSONSchema), s); err != nil {
			return err
		}
		t.ParamsOneOf.jsonschema = s
	}
	return nil
}

// ParameterInfo is the information of a parameter.
// It is used to describe the parameters of a tool.
//
// ParameterInfo 是参数的信息。
// 用于描述工具的参数。
type ParameterInfo struct {
	// The type of the parameter.
	// 参数的类型。
	Type DataType
	// The element type of the parameter, only for array.
	// 参数的元素类型，仅用于 array。
	ElemInfo *ParameterInfo
	// The sub parameters of the parameter, only for object.
	// 参数的子参数，仅用于 object。
	SubParams map[string]*ParameterInfo
	// The description of the parameter.
	// 参数的描述。
	Desc string
	// The enum values of the parameter, only for string.
	// 参数的 enum 值，仅用于 string。
	Enum []string
	// Whether the parameter is required.
	// 参数是否必填。
	Required bool
}

// ParamsOneOf holds a tool's parameter schema using exactly one of two
// representations. Choose the one that best fits your needs:
//
//  1. [NewParamsOneOfByParams] — lightweight: describe parameters as a
//     map[string]*[ParameterInfo]. Covers the most common cases (scalars,
//     arrays, nested objects, enums, required flags).
//
//  2. [NewParamsOneOfByJSONSchema] — powerful: supply a full
//     *jsonschema.Schema (JSON Schema 2020-12). Required when you need
//     features not expressible via ParameterInfo, such as anyOf, oneOf, or
//     $defs references. [utils.InferTool] generates this form automatically
//     from Go struct tags.
//
// You must use exactly one constructor — setting both fields is invalid.
// If ParamsOneOf is nil, the tool takes no input parameters.
//
// ParamsOneOf 使用以下两种表示之一来保存工具的参数 schema。请选择最适合需求的一种：
// 1. [NewParamsOneOfByParams] — 轻量级：用 map[string]*[ParameterInfo] 描述参数。覆盖最常见场景（标量、数组、嵌套对象、枚举、必填标记）。
// 2. [NewParamsOneOfByJSONSchema] — 功能更强：提供完整的 *jsonschema.Schema (JSON Schema 2020-12)。当需要 ParameterInfo 无法表达的功能时使用，例如 anyOf、oneOf 或 $defs 引用。[utils.InferTool] 会根据 Go struct tags 自动生成这种形式。
// 必须只使用一个构造函数——同时设置两个字段是无效的。
// 如果 ParamsOneOf 为 nil，则该工具不接收输入参数。
type ParamsOneOf struct {
	// use NewParamsOneOfByParams to set this field
	// 使用 NewParamsOneOfByParams 设置此字段
	params map[string]*ParameterInfo

	jsonschema *jsonschema.Schema
}

// NewParamsOneOfByParams creates a ParamsOneOf with map[string]*ParameterInfo.
// NewParamsOneOfByParams 创建带有 map[string]*ParameterInfo 的 ParamsOneOf。
func NewParamsOneOfByParams(params map[string]*ParameterInfo) *ParamsOneOf {
	return &ParamsOneOf{
		params: params,
	}
}

// NewParamsOneOfByJSONSchema creates a ParamsOneOf with *jsonschema.Schema.
// NewParamsOneOfByJSONSchema 创建带有 *jsonschema.Schema 的 ParamsOneOf。
func NewParamsOneOfByJSONSchema(s *jsonschema.Schema) *ParamsOneOf {
	return &ParamsOneOf{
		jsonschema: s,
	}
}

// ToJSONSchema parses ParamsOneOf, converts the parameter description that user actually provides, into the format ready to be passed to Model.
// ToJSONSchema 解析 ParamsOneOf，将用户实际提供的参数描述转换为可传给 Model 的格式。
func (p *ParamsOneOf) ToJSONSchema() (*jsonschema.Schema, error) {
	if p == nil {
		return nil, nil
	}

	if p.params != nil {
		sc := &jsonschema.Schema{
			Properties: orderedmap.New[string, *jsonschema.Schema](),
			Type:       string(Object),
			Required:   make([]string, 0, len(p.params)),
		}

		keys := make([]string, 0, len(p.params))
		for k := range p.params {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			v := p.params[k]
			sc.Properties.Set(k, paramInfoToJSONSchema(v))
			if v.Required {
				sc.Required = append(sc.Required, k)
			}
		}

		return sc, nil
	}

	return p.jsonschema, nil
}

func paramInfoToJSONSchema(paramInfo *ParameterInfo) *jsonschema.Schema {
	js := &jsonschema.Schema{
		Type:        string(paramInfo.Type),
		Description: paramInfo.Desc,
	}

	if len(paramInfo.Enum) > 0 {
		js.Enum = make([]any, len(paramInfo.Enum))
		for i, enum := range paramInfo.Enum {
			js.Enum[i] = enum
		}
	}

	if paramInfo.ElemInfo != nil {
		js.Items = paramInfoToJSONSchema(paramInfo.ElemInfo)
	}

	if len(paramInfo.SubParams) > 0 {
		required := make([]string, 0, len(paramInfo.SubParams))
		js.Properties = orderedmap.New[string, *jsonschema.Schema]()
		keys := make([]string, 0, len(paramInfo.SubParams))
		for k := range paramInfo.SubParams {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			v := paramInfo.SubParams[k]
			item := paramInfoToJSONSchema(v)
			js.Properties.Set(k, item)
			if v.Required {
				required = append(required, k)
			}
		}

		js.Required = required
	}

	return js
}

// ToolPartType defines the type of content in a tool output part.
// It is used to distinguish between different types of multimodal content returned by tools.
//
// ToolPartType 定义工具输出部分中的内容类型。
// 用于区分工具返回的不同类型的多模态内容。
type ToolPartType string

const (
	// ToolPartTypeText means the part is a text.
	// ToolPartTypeText 表示该部分是文本。
	ToolPartTypeText ToolPartType = "text"

	// ToolPartTypeImage means the part is an image url.
	// ToolPartTypeImage 表示该部分是图片 url。
	ToolPartTypeImage ToolPartType = "image"

	// ToolPartTypeAudio means the part is an audio url.
	// ToolPartTypeAudio 表示该部分是音频 url。
	ToolPartTypeAudio ToolPartType = "audio"

	// ToolPartTypeVideo means the part is a video url.
	// ToolPartTypeVideo 表示该部分是视频 url。
	ToolPartTypeVideo ToolPartType = "video"

	// ToolPartTypeFile means the part is a file url.
	// ToolPartTypeFile 表示该部分是文件 url。
	ToolPartTypeFile ToolPartType = "file"

	// ToolPartTypeToolSearchResult means the part contains tool search results.
	// ToolPartTypeToolSearchResult 表示该部分包含工具搜索结果。
	ToolPartTypeToolSearchResult ToolPartType = "tool_search_result"
)

// ToolOutputImage represents an image in tool output.
// It contains URL or Base64-encoded data along with MIME type information.
//
// ToolOutputImage 表示工具输出中的图片。
// 它包含 URL 或 Base64 编码数据，以及 MIME 类型信息。
type ToolOutputImage struct {
	MessagePartCommon
}

// ToolOutputAudio represents an audio file in tool output.
// It contains URL or Base64-encoded data along with MIME type information.
//
// ToolOutputAudio 表示工具输出中的音频文件。
// 它包含 URL 或 Base64 编码数据，以及 MIME 类型信息。
type ToolOutputAudio struct {
	MessagePartCommon
}

// ToolOutputVideo represents a video file in tool output.
// It contains URL or Base64-encoded data along with MIME type information.
//
// ToolOutputVideo 表示工具输出中的视频文件。
// 它包含 URL 或 Base64 编码数据，以及 MIME 类型信息。
type ToolOutputVideo struct {
	MessagePartCommon
}

// ToolOutputFile represents a generic file in tool output.
// It contains URL or Base64-encoded data along with MIME type information.
//
// ToolOutputFile 表示工具输出中的通用文件。
// 它包含 URL 或 Base64 编码数据，以及 MIME 类型信息。
type ToolOutputFile struct {
	MessagePartCommon
}

// ToolSearchResult represents the result of a tool search operation.
// When a model issues a tool search call, the framework searches for matching tools
// and returns the results via this struct.
//
// ToolSearchResult 表示工具搜索操作的结果。
// 当模型发出工具搜索调用时，框架会搜索匹配的工具，并通过此结构体返回结果。
type ToolSearchResult struct {
	// Tools contains the full definitions of matched tools that were not previously
	// registered. Their complete definitions are required so that the model can
	// understand their parameters and usage.
	//
	// Tools 包含此前未注册的匹配工具的完整定义。
	// 需要这些完整定义，以便模型理解其参数和用法。
	Tools []*ToolInfo
}

func (t *ToolSearchResult) String() string {
	sb := new(strings.Builder)
	sb.WriteString("ToolSearchResult[")
	for _, tool := range t.Tools {
		sb.WriteString(tool.Name)
		sb.WriteString(",")
	}
	sb.WriteString("]")
	return sb.String()
}

// ToolOutputPart represents a part of tool execution output.
// It supports streaming scenarios through the Index field for chunk merging.
//
// ToolOutputPart 表示工具执行输出的一部分。
// 它通过 Index 字段支持流式场景下的分块合并。
type ToolOutputPart struct {

	// Type is the type of the part, e.g., "text", "image_url", "audio_url", "video_url".
	// Type 是该部分的类型，例如 "text"、"image_url"、"audio_url"、"video_url"。
	Type ToolPartType `json:"type"`

	// Text is the text content, used when Type is "text".
	// Text 是文本内容，在 Type 为 "text" 时使用。
	Text string `json:"text,omitempty"`

	// Image is the image content, used when Type is ToolPartTypeImage.
	// Image 是图像内容，在 Type 为 ToolPartTypeImage 时使用。
	Image *ToolOutputImage `json:"image,omitempty"`

	// Audio is the audio content, used when Type is ToolPartTypeAudio.
	// Audio 是音频内容，在 Type 为 ToolPartTypeAudio 时使用。
	Audio *ToolOutputAudio `json:"audio,omitempty"`

	// Video is the video content, used when Type is ToolPartTypeVideo.
	// Video 是视频内容，在 Type 为 ToolPartTypeVideo 时使用。
	Video *ToolOutputVideo `json:"video,omitempty"`

	// File is the file content, used when Type is ToolPartTypeFile.
	// File 是文件内容，在 Type 为 ToolPartTypeFile 时使用。
	File *ToolOutputFile `json:"file,omitempty"`

	// ToolSearchResult holds the tool search results, used when Type is ToolPartTypeToolSearchResult.
	// ToolSearchResult 保存工具搜索结果，在 Type 为 ToolPartTypeToolSearchResult 时使用。
	ToolSearchResult *ToolSearchResult `json:"tool_search_result,omitempty"`

	// Extra is used to store extra information.
	// Extra 用于存储额外信息。
	Extra map[string]any `json:"extra,omitempty"`
}

// ToolArgument contains the input information for a tool call.
// It is used to pass tool call arguments to enhanced tools.
//
// ToolArgument 包含工具调用的输入信息。
// 它用于将工具调用参数传递给增强工具。
type ToolArgument struct {
	// Text contains the arguments for the tool call in JSON format.
	// Text 包含 JSON 格式的工具调用参数。
	Text string `json:"text,omitempty"`
}

// ToolResult represents the structured multimodal output from a tool execution.
// It is used when a tool needs to return more than just a simple string,
// such as images, files, or other structured data.
//
// ToolResult 表示工具执行产生的结构化多模态输出。
// 当工具需要返回的不只是简单字符串（如图像、文件或其他结构化数据）时使用。
type ToolResult struct {
	// Parts contains the multimodal output parts. Each part can be a different
	// type of content, like text, an image, or a file.
	//
	// Parts 包含多模态输出部分。每个部分可以是不同类型的内容，如文本、图像或文件。
	Parts []ToolOutputPart `json:"parts,omitempty"`
}

func convToolOutputPartToMessageInputPart(toolPart ToolOutputPart) (MessageInputPart, error) {
	switch toolPart.Type {
	case ToolPartTypeText:
		return MessageInputPart{
			Type:  ChatMessagePartTypeText,
			Text:  toolPart.Text,
			Extra: toolPart.Extra,
		}, nil
	case ToolPartTypeImage:
		if toolPart.Image == nil {
			return MessageInputPart{}, fmt.Errorf("image content is nil for tool part type %v", toolPart.Type)
		}
		return MessageInputPart{
			Type:  ChatMessagePartTypeImageURL,
			Image: &MessageInputImage{MessagePartCommon: toolPart.Image.MessagePartCommon},
			Extra: toolPart.Extra,
		}, nil
	case ToolPartTypeAudio:
		if toolPart.Audio == nil {
			return MessageInputPart{}, fmt.Errorf("audio content is nil for tool part type %v", toolPart.Type)
		}
		return MessageInputPart{
			Type:  ChatMessagePartTypeAudioURL,
			Audio: &MessageInputAudio{MessagePartCommon: toolPart.Audio.MessagePartCommon},
			Extra: toolPart.Extra,
		}, nil
	case ToolPartTypeVideo:
		if toolPart.Video == nil {
			return MessageInputPart{}, fmt.Errorf("video content is nil for tool part type %v", toolPart.Type)
		}
		return MessageInputPart{
			Type:  ChatMessagePartTypeVideoURL,
			Video: &MessageInputVideo{MessagePartCommon: toolPart.Video.MessagePartCommon},
			Extra: toolPart.Extra,
		}, nil
	case ToolPartTypeFile:
		if toolPart.File == nil {
			return MessageInputPart{}, fmt.Errorf("file content is nil for tool part type %v", toolPart.Type)
		}
		return MessageInputPart{
			Type:  ChatMessagePartTypeFileURL,
			File:  &MessageInputFile{MessagePartCommon: toolPart.File.MessagePartCommon},
			Extra: toolPart.Extra,
		}, nil
	case ToolPartTypeToolSearchResult:
		if toolPart.ToolSearchResult == nil {
			return MessageInputPart{}, fmt.Errorf("tool search result is nil for tool part type %v", toolPart.Type)
		}
		return MessageInputPart{
			Type:             ChatMessagePartTypeToolSearchResult,
			ToolSearchResult: toolPart.ToolSearchResult,
		}, nil
	default:
		return MessageInputPart{}, fmt.Errorf("unknown tool part type: %v", toolPart.Type)
	}
}

// ToMessageInputParts converts ToolOutputPart slice to MessageInputPart slice.
// This is used when passing tool results as input to the model.
//
// Parameters:
//   - None (method receiver is *ToolResult)
//
// Returns:
//   - []MessageInputPart: The converted message input parts that can be used in a Message.
//   - error: An error if conversion fails due to unknown part types or nil content fields.
//
// Example:
//
//	toolResult := &schema.ToolResult{
//	    Parts: []schema.ToolOutputPart{
//	        {Type: schema.ToolPartTypeText, Text: "Result text"},
//	        {Type: schema.ToolPartTypeImage, Image: &schema.ToolOutputImage{...}},
//	    },
//	}
//	inputParts, err := toolResult.ToMessageInputParts()
//
// ToMessageInputParts 将 ToolOutputPart 切片转换为 MessageInputPart 切片。
// 这用于将工具结果作为输入传递给模型。
// Parameters:
// - None（方法接收者为 *ToolResult）
// Returns:
// - []MessageInputPart：转换后的消息输入部分，可用于 Message。
// - error：当因未知部分类型或 nil 内容字段导致转换失败时返回的错误。
// Example:
// toolResult := &schema.ToolResult{
// Parts: []schema.ToolOutputPart{
// {Type: schema.ToolPartTypeText, Text: "Result text"},
// {Type: schema.ToolPartTypeImage, Image: &schema.ToolOutputImage{...}},
// },
// }
// inputParts, err := toolResult.ToMessageInputParts()
func (tr *ToolResult) ToMessageInputParts() ([]MessageInputPart, error) {
	if tr == nil || len(tr.Parts) == 0 {
		return nil, nil
	}
	result := make([]MessageInputPart, len(tr.Parts))
	for i, part := range tr.Parts {
		var err error
		result[i], err = convToolOutputPartToMessageInputPart(part)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
