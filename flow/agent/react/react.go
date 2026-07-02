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

package react

import (
	"context"
	"io"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/flow/agent"
	"github.com/cloudwego/eino/schema"
)

type toolResultSender func(toolName, callID, result string)

type enhancedToolResultSender func(toolName, callID string, result *schema.ToolResult)
type streamToolResultSender func(toolName, callID string, resultStream *schema.StreamReader[string])
type enhancedStreamToolResultSender func(toolName, callID string, resultStream *schema.StreamReader[*schema.ToolResult])
type toolResultSenders struct {
	sender       toolResultSender
	streamSender streamToolResultSender

	enhancedResultSender           enhancedToolResultSender
	enhancedStreamToolResultSender enhancedStreamToolResultSender
}

type toolResultSenderCtxKey struct{}

func setToolResultSendersToCtx(ctx context.Context, senders *toolResultSenders) context.Context {
	return context.WithValue(ctx, toolResultSenderCtxKey{}, senders)
}

func getToolResultSendersFromCtx(ctx context.Context) *toolResultSenders {
	v := ctx.Value(toolResultSenderCtxKey{})
	if v == nil {
		return nil
	}
	return v.(*toolResultSenders)
}

type state struct {
	Messages                 []*schema.Message
	ReturnDirectlyToolCallID string
}

func init() {
	schema.RegisterName[*state]("_eino_react_state")
}

func newToolResultCollectorMiddleware() compose.ToolMiddleware {
	return compose.ToolMiddleware{
		Invokable: func(next compose.InvokableToolEndpoint) compose.InvokableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.ToolOutput, error) {
				senders := getToolResultSendersFromCtx(ctx)
				output, err := next(ctx, input)
				if err != nil {
					return nil, err
				}
				if senders != nil && senders.sender != nil {
					senders.sender(input.Name, input.CallID, output.Result)
				}
				return output, nil
			}
		},
		Streamable: func(next compose.StreamableToolEndpoint) compose.StreamableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.StreamToolOutput, error) {
				senders := getToolResultSendersFromCtx(ctx)
				output, err := next(ctx, input)
				if err != nil {
					return nil, err
				}
				if senders != nil && senders.streamSender != nil {
					streams := output.Result.Copy(2)
					senders.streamSender(input.Name, input.CallID, streams[0])
					output.Result = streams[1]
				}
				return output, nil
			}
		},
		EnhancedInvokable: func(next compose.EnhancedInvokableToolEndpoint) compose.EnhancedInvokableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.EnhancedInvokableToolOutput, error) {
				senders := getToolResultSendersFromCtx(ctx)
				output, err := next(ctx, input)
				if err != nil {
					return nil, err
				}
				if senders != nil && senders.enhancedResultSender != nil {
					senders.enhancedResultSender(input.Name, input.CallID, output.Result)
				}
				return output, nil

			}
		},
		EnhancedStreamable: func(next compose.EnhancedStreamableToolEndpoint) compose.EnhancedStreamableToolEndpoint {
			return func(ctx context.Context, input *compose.ToolInput) (*compose.EnhancedStreamableToolOutput, error) {
				senders := getToolResultSendersFromCtx(ctx)
				output, err := next(ctx, input)
				if err != nil {
					return nil, err
				}
				if senders != nil && senders.enhancedStreamToolResultSender != nil {
					streams := output.Result.Copy(2)
					senders.enhancedStreamToolResultSender(input.Name, input.CallID, streams[0])
					output.Result = streams[1]
				}
				return output, nil
			}
		},
	}
}

const (
	nodeKeyTools = "tools"
	nodeKeyModel = "chat"
)

// MessageModifier modify the input messages before the model is called.
// MessageModifier 在调用 model 前修改输入消息。
type MessageModifier func(ctx context.Context, input []*schema.Message) []*schema.Message

// AgentConfig is the config for ReAct agent.
// AgentConfig 是 ReAct agent 的配置。
type AgentConfig struct {
	// ToolCallingModel is the chat model to be used for handling user messages with tool calling capability.
	// This is the recommended model field to use.
	//
	// ToolCallingModel 是用于处理用户消息、具备工具调用能力的 chat model。
	// 这是推荐使用的 model 字段。
	ToolCallingModel model.ToolCallingChatModel

	// Deprecated: Use ToolCallingModel instead.
	// Deprecated: 改用 ToolCallingModel。
	Model model.ChatModel

	// ToolsConfig is the config for tools node.
	// ToolsConfig 是 tools 节点的配置。
	ToolsConfig compose.ToolsNodeConfig

	// MessageModifier.
	// modify the input messages before the model is called, it's useful when you want to add some system prompt or other messages.
	//
	// MessageModifier。
	// 在调用 model 前修改输入消息，适合添加 system prompt 或其他消息。
	MessageModifier MessageModifier

	// MessageRewriter modifies message in the state, before the ChatModel is called.
	// It takes the messages stored accumulated in state, modify them, and put the modified version back into state.
	// Useful for compressing message history to fit the model context window,
	// or if you want to make changes to messages that take effect across multiple model calls.
	// NOTE: if both MessageModifier and MessageRewriter are set, MessageRewriter will be called before MessageModifier.
	//
	// MessageRewriter 在调用 ChatModel 前修改 state 中的消息。
	// 它会获取 state 中累积保存的消息，修改后再写回 state。
	// 适用于压缩消息历史以适配 model context window，
	// 或需要让消息修改在多次 model 调用间持续生效的场景。
	// NOTE: 如果同时设置了 MessageModifier 和 MessageRewriter，会先调用 MessageRewriter。
	MessageRewriter MessageModifier

	// MaxStep.
	// default 12 of steps in pregel (node num + 10).
	//
	// MaxStep。
	// 默认是 pregel 中的 12 步（节点数 + 10）。
	MaxStep int `json:"max_step"`

	// Tools that will make agent return directly when the tool is called.
	// When multiple tools are called and more than one tool is in the return directly list, only the first one will be returned.
	//
	// 调用这些工具时，agent 会直接返回。
	// 当调用多个工具且其中多个工具在直接返回列表中时，只返回第一个。
	ToolReturnDirectly map[string]struct{}

	// StreamToolCallChecker is a function to determine whether the model's streaming output contains tool calls.
	// Different models have different ways of outputting tool calls in streaming mode:
	// - Some models (like OpenAI) output tool calls directly
	// - Others (like Claude) output text first, then tool calls
	// This handler allows custom logic to check for tool calls in the stream.
	// It should return:
	// - true if the output contains tool calls and agent should continue processing
	// - false if no tool calls and agent should stop
	// Note: This field only needs to be configured when using streaming mode
	// Note: The handler MUST close the modelOutput stream before returning
	// Optional. By default, it checks if the first chunk contains tool calls.
	// Note: The default implementation does not work well with Claude, which typically outputs tool calls after text content.
	// Note: If your ChatModel doesn't output tool calls first, you can try adding prompts to constrain the model from generating extra text during the tool call.
	//
	// StreamToolCallChecker 是用于判断 model 流式输出是否包含工具调用的函数。
	// 不同 model 在流式模式下输出工具调用的方式不同：
	// - 有些 model（如 OpenAI）会直接输出工具调用
	// - 其他 model（如 Claude）会先输出文本，再输出工具调用
	// 此处理器允许用自定义逻辑检查流中的工具调用。
	// 它应返回：
	// - 如果输出包含工具调用且 agent 应继续处理，返回 true
	// - 如果没有工具调用且 agent 应停止，返回 false
	// Note: 仅在使用流式模式时需要配置此字段
	// Note: 该处理器返回前 MUST 关闭 modelOutput stream
	// 可选。默认会检查第一个 chunk 是否包含工具调用。
	// Note: 默认实现不太适合 Claude，因为 Claude 通常会在文本内容之后输出工具调用。
	// Note: 如果你的 ChatModel 不是先输出工具调用，可以尝试添加 prompts 来约束 model 在工具调用期间不要生成额外文本。
	StreamToolCallChecker func(ctx context.Context, modelOutput *schema.StreamReader[*schema.Message]) (bool, error)

	// GraphName is the graph name of the ReAct Agent.
	// Optional. Default `ReActAgent`.
	//
	// GraphName 是 ReAct Agent 的图名称。
	// 可选。默认 `ReActAgent`。
	GraphName string
	// ModelNodeName is the node name of the model node in the ReAct Agent graph.
	// Optional. Default `ChatModel`.
	//
	// ModelNodeName 是 ReAct Agent 图中 model 节点的名称。
	// 可选。默认 `ChatModel`。
	ModelNodeName string
	// ToolsNodeName is the node name of the tools node in the ReAct Agent graph.
	// Optional. Default `Tools`.
	//
	// ToolsNodeName 是 ReAct Agent 图中 tools 节点的名称。
	// 可选。默认 `Tools`。
	ToolsNodeName string
}

// NewPersonaModifier returns a MessageModifier that adds a persona message to the input.
// example:
//
//	persona := "You are an expert in golang."
//	config := AgentConfig{
//		Model: model,
//		MessageModifier: NewPersonaModifier(persona),
//	}
//	agent, err := NewAgent(ctx, config)
//	if err != nil {return}
//	msg, err := agent.Generate(ctx, []*schema.Message{{Role: schema.User, Content: "how to build agent with eino"}})
//	if err != nil {return}
//	println(msg.Content)
//
// Deprecated: Prefer directly including the persona message in the
// input when calling Generate or Stream to avoid extra copying.
//
// NewPersonaModifier 返回一个 MessageModifier，用于向输入添加 persona 消息。
// 示例：
// persona := "You are an expert in golang."
// config := AgentConfig{
// Model: model,
// MessageModifier: NewPersonaModifier(persona),
// }
// agent, err := NewAgent(ctx, config)
// if err != nil {return}
// msg, err := agent.Generate(ctx, []*schema.Message{{Role: schema.User, Content: "how to build agent with eino"}})
// if err != nil {return}
// println(msg.Content)
// Deprecated: 建议在调用 Generate 或 Stream 时直接在 input 中包含 persona 消息，
// 以避免额外拷贝。
func NewPersonaModifier(persona string) MessageModifier {
	return func(ctx context.Context, input []*schema.Message) []*schema.Message {
		res := make([]*schema.Message, 0, len(input)+1)

		res = append(res, schema.SystemMessage(persona))
		res = append(res, input...)
		return res
	}
}

func firstChunkStreamToolCallChecker(_ context.Context, sr *schema.StreamReader[*schema.Message]) (bool, error) {
	defer sr.Close()

	for {
		msg, err := sr.Recv()
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}

		if len(msg.ToolCalls) > 0 {
			return true, nil
		}

		if len(msg.Content) == 0 { // skip empty chunks at the front
			// 跳过开头的空 chunks
			continue
		}

		return false, nil
	}
}

// Default graph and node names for the ReAct agent.
// ReAct agent 的默认图和节点名称。
const (
	GraphName     = "ReActAgent"
	ModelNodeName = "ChatModel"
	ToolsNodeName = "Tools"
)

// SetReturnDirectly is a helper function that can be called within a tool's execution.
// It signals the ReAct agent to stop further processing and return the result of the current tool call directly.
// This is useful when the tool's output is the final answer and no more steps are needed.
// Note: If multiple tools call this function in the same step, only the last call will take effect.
// This setting has a higher priority than the AgentConfig.ToolReturnDirectly.
//
// SetReturnDirectly 是一个可在工具执行过程中调用的辅助函数。
// 它会通知 ReAct agent 停止后续处理，并直接返回当前工具调用的结果。
// 当工具输出就是最终答案、不需要更多步骤时很有用。
// Note: 如果同一步中多个工具调用此函数，只有最后一次调用会生效。
// 此设置的优先级高于 AgentConfig.ToolReturnDirectly。
func SetReturnDirectly(ctx context.Context) error {
	return compose.ProcessState(ctx, func(ctx context.Context, s *state) error {
		s.ReturnDirectlyToolCallID = compose.GetToolCallID(ctx)
		return nil
	})
}

// Agent is the ReAct agent.
// ReAct agent is a simple agent that handles user messages with a chat model and tools.
// ReAct will call the chat model, if the message contains tool calls, it will call the tools.
// if the tool is configured to return directly, ReAct will return directly.
// otherwise, ReAct will continue to call the chat model until the message contains no tool calls.
// e.g.
//
//	agent, err := ReAct.NewAgent(ctx, &react.AgentConfig{})
//	if err != nil {...}
//	msg, err := agent.Generate(ctx, []*schema.Message{{Role: schema.User, Content: "how to build agent with eino"}})
//	if err != nil {...}
//	println(msg.Content)
//
// Agent 是 ReAct agent。
// ReAct agent 是一个用 chat model 和 tools 处理用户消息的简单 agent。
// ReAct 会调用 chat model；如果消息包含工具调用，就会调用 tools。
// 如果工具配置为直接返回，ReAct 会直接返回。
// 否则，ReAct 会继续调用 chat model，直到消息不再包含工具调用。
// 例如：
// agent, err := ReAct.NewAgent(ctx, &react.AgentConfig{})
// if err != nil {...}
// msg, err := agent.Generate(ctx, []*schema.Message{{Role: schema.User, Content: "how to build agent with eino"}})
// if err != nil {...}
// println(msg.Content)
type Agent struct {
	runnable         compose.Runnable[[]*schema.Message, *schema.Message]
	graph            *compose.Graph[[]*schema.Message, *schema.Message]
	graphAddNodeOpts []compose.GraphAddNodeOpt
}

// NewAgent creates a ReAct agent that feeds tool response into next round of Chat Model generation.
//
// IMPORTANT!! For models that don't output tool calls in the first streaming chunk (e.g. Claude)
// the default StreamToolCallChecker may not work properly since it only checks the first chunk for tool calls.
// In such cases, you need to implement a custom StreamToolCallChecker that can properly detect tool calls.
//
// NewAgent 创建一个 ReAct agent，会将工具响应送入下一轮 Chat Model 生成。
// IMPORTANT!! 对于不会在第一个流式 chunk 中输出工具调用的 model（如 Claude），
// 默认的 StreamToolCallChecker 可能无法正常工作，因为它只检查第一个 chunk 是否有工具调用。
// 这种情况下，需要实现自定义 StreamToolCallChecker，以正确检测工具调用。
func NewAgent(ctx context.Context, config *AgentConfig) (_ *Agent, err error) {
	var (
		chatModel       model.BaseChatModel
		toolsNode       *compose.ToolsNode
		toolInfos       []*schema.ToolInfo
		toolCallChecker = config.StreamToolCallChecker
		messageModifier = config.MessageModifier
	)

	graphName := GraphName
	if config.GraphName != "" {
		graphName = config.GraphName
	}

	modelNodeName := ModelNodeName
	if config.ModelNodeName != "" {
		modelNodeName = config.ModelNodeName
	}

	toolsNodeName := ToolsNodeName
	if config.ToolsNodeName != "" {
		toolsNodeName = config.ToolsNodeName
	}

	if toolCallChecker == nil {
		toolCallChecker = firstChunkStreamToolCallChecker
	}

	if toolInfos, err = genToolInfos(ctx, config.ToolsConfig); err != nil {
		return nil, err
	}

	if chatModel, err = agent.ChatModelWithTools(config.Model, config.ToolCallingModel, toolInfos); err != nil {
		return nil, err
	}

	config.ToolsConfig.ToolCallMiddlewares = append(
		[]compose.ToolMiddleware{newToolResultCollectorMiddleware()},
		config.ToolsConfig.ToolCallMiddlewares...,
	)

	if toolsNode, err = compose.NewToolNode(ctx, &config.ToolsConfig); err != nil {
		return nil, err
	}

	graph := compose.NewGraph[[]*schema.Message, *schema.Message](compose.WithGenLocalState(func(ctx context.Context) *state {
		return &state{Messages: make([]*schema.Message, 0, config.MaxStep+1)}
	}))

	modelPreHandle := func(ctx context.Context, input []*schema.Message, state *state) ([]*schema.Message, error) {
		state.Messages = append(state.Messages, input...)

		if config.MessageRewriter != nil {
			state.Messages = config.MessageRewriter(ctx, state.Messages)
		}

		if messageModifier == nil {
			return state.Messages, nil
		}

		modifiedInput := make([]*schema.Message, len(state.Messages))
		copy(modifiedInput, state.Messages)
		return messageModifier(ctx, modifiedInput), nil
	}

	if err = graph.AddChatModelNode(nodeKeyModel, chatModel, compose.WithStatePreHandler(modelPreHandle), compose.WithNodeName(modelNodeName)); err != nil {
		return nil, err
	}

	if err = graph.AddEdge(compose.START, nodeKeyModel); err != nil {
		return nil, err
	}

	toolsNodePreHandle := func(ctx context.Context, input *schema.Message, state *state) (*schema.Message, error) {
		if input == nil {
			return state.Messages[len(state.Messages)-1], nil // used for rerun interrupt resume
			// 用于重跑中断恢复
		}
		state.Messages = append(state.Messages, input)
		state.ReturnDirectlyToolCallID = getReturnDirectlyToolCallID(input, config.ToolReturnDirectly)
		return input, nil
	}
	if err = graph.AddToolsNode(nodeKeyTools, toolsNode, compose.WithStatePreHandler(toolsNodePreHandle), compose.WithNodeName(toolsNodeName)); err != nil {
		return nil, err
	}

	modelPostBranchCondition := func(ctx context.Context, sr *schema.StreamReader[*schema.Message]) (endNode string, err error) {
		if isToolCall, err := toolCallChecker(ctx, sr); err != nil {
			return "", err
		} else if isToolCall {
			return nodeKeyTools, nil
		}
		return compose.END, nil
	}

	if err = graph.AddBranch(nodeKeyModel, compose.NewStreamGraphBranch(modelPostBranchCondition, map[string]bool{nodeKeyTools: true, compose.END: true})); err != nil {
		return nil, err
	}

	if err = buildReturnDirectly(graph); err != nil {
		return nil, err
	}

	compileOpts := []compose.GraphCompileOption{compose.WithMaxRunSteps(config.MaxStep), compose.WithNodeTriggerMode(compose.AnyPredecessor), compose.WithGraphName(graphName)}
	runnable, err := graph.Compile(ctx, compileOpts...)
	if err != nil {
		return nil, err
	}

	return &Agent{
		runnable:         runnable,
		graph:            graph,
		graphAddNodeOpts: []compose.GraphAddNodeOpt{compose.WithGraphCompileOptions(compileOpts...)},
	}, nil
}

func buildReturnDirectly(graph *compose.Graph[[]*schema.Message, *schema.Message]) (err error) {
	directReturn := func(ctx context.Context, msgs *schema.StreamReader[[]*schema.Message]) (*schema.StreamReader[*schema.Message], error) {
		return schema.StreamReaderWithConvert(msgs, func(msgs []*schema.Message) (*schema.Message, error) {
			var msg *schema.Message
			err = compose.ProcessState[*state](ctx, func(_ context.Context, state *state) error {
				for i := range msgs {
					if msgs[i] != nil && msgs[i].ToolCallID == state.ReturnDirectlyToolCallID {
						msg = msgs[i]
						return nil
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			if msg == nil {
				return nil, schema.ErrNoValue
			}
			return msg, nil
		}), nil
	}

	nodeKeyDirectReturn := "direct_return"
	if err = graph.AddLambdaNode(nodeKeyDirectReturn, compose.TransformableLambda(directReturn)); err != nil {
		return err
	}

	// this branch checks if the tool called should return directly. It either leads to END or back to ChatModel
	// 此分支检查被调用的工具是否应直接返回。它要么通向 END，要么回到 ChatModel
	err = graph.AddBranch(nodeKeyTools, compose.NewStreamGraphBranch(func(ctx context.Context, msgsStream *schema.StreamReader[[]*schema.Message]) (endNode string, err error) {
		msgsStream.Close()

		err = compose.ProcessState[*state](ctx, func(_ context.Context, state *state) error {
			if len(state.ReturnDirectlyToolCallID) > 0 {
				endNode = nodeKeyDirectReturn
			} else {
				endNode = nodeKeyModel
			}
			return nil
		})
		if err != nil {
			return "", err
		}
		return endNode, nil
	}, map[string]bool{nodeKeyModel: true, nodeKeyDirectReturn: true}))
	if err != nil {
		return err
	}

	return graph.AddEdge(nodeKeyDirectReturn, compose.END)
}

func genToolInfos(ctx context.Context, config compose.ToolsNodeConfig) ([]*schema.ToolInfo, error) {
	toolInfos := make([]*schema.ToolInfo, 0, len(config.Tools))
	for _, t := range config.Tools {
		tl, err := t.Info(ctx)
		if err != nil {
			return nil, err
		}

		toolInfos = append(toolInfos, tl)
	}

	return toolInfos, nil
}

func getReturnDirectlyToolCallID(input *schema.Message, toolReturnDirectly map[string]struct{}) string {
	if len(toolReturnDirectly) == 0 {
		return ""
	}

	for _, toolCall := range input.ToolCalls {
		if _, ok := toolReturnDirectly[toolCall.Function.Name]; ok {
			return toolCall.ID
		}
	}

	return ""
}

// Generate generates a response from the agent.
// Generate 从智能体生成响应。
func (r *Agent) Generate(ctx context.Context, input []*schema.Message, opts ...agent.AgentOption) (*schema.Message, error) {
	return r.runnable.Invoke(ctx, input, agent.GetComposeOptions(opts...)...)
}

// Stream calls the agent and returns a stream response.
// Stream 调用智能体并返回流式响应。
func (r *Agent) Stream(ctx context.Context, input []*schema.Message, opts ...agent.AgentOption) (output *schema.StreamReader[*schema.Message], err error) {
	return r.runnable.Stream(ctx, input, agent.GetComposeOptions(opts...)...)
}

// ExportGraph exports the underlying graph from Agent, along with the []compose.GraphAddNodeOpt to be used when adding this graph to another graph.
// ExportGraph 从 Agent 导出底层图，以及将此图添加到另一个图时使用的 []compose.GraphAddNodeOpt。
func (r *Agent) ExportGraph() (compose.AnyGraph, []compose.GraphAddNodeOpt) {
	return r.graph, r.graphAddNodeOpts
}
