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

package internal

import "github.com/google/uuid"

// EinoMsgIDKey is the Extra key used to store the eino-internal message ID.
// EinoMsgIDKey 是用于存储 eino 内部消息 ID 的 Extra key。
const EinoMsgIDKey = "_eino_msg_id"

// GetMessageID returns the message ID from Extra, or "" if not set.
// Works with any map[string]any (Message.Extra or AgenticMessage.Extra).
//
// GetMessageID 从 Extra 返回消息 ID；如果未设置则返回 ""。
// 适用于任何 map[string]any（Message.Extra 或 AgenticMessage.Extra）。
func GetMessageID(extra map[string]any) string {
	if extra == nil {
		return ""
	}
	id, _ := extra[EinoMsgIDKey].(string)
	return id
}

// SetMessageID sets the message ID in Extra and returns the resulting map.
//
// Copy-on-write: the input map is never mutated, because a message's Extra can
// be shared across fan-out stream readers and an in-place write would race with
// concurrent reads (fatal "concurrent map read and map write"). Reassigning the
// returned map to a shared Extra field is still a field-level race that cannot
// be eliminated while Extra is exported; COW only removes the map-level panic.
//
// SetMessageID 在 Extra 中设置消息 ID，并返回结果 map。
// 写时复制：永远不会修改输入 map，因为消息的 Extra 可能在扇出流读取器之间共享，原地写入会与并发读取竞争（致命错误 "concurrent map read and map write"）。将返回的 map 重新赋给共享的 Extra 字段仍然是字段级竞争，在 Extra 导出的情况下无法消除；COW 只消除 map 级 panic。
func SetMessageID(extra map[string]any, id string) map[string]any {
	next := make(map[string]any, len(extra)+1)
	for k, v := range extra {
		next[k] = v
	}
	next[EinoMsgIDKey] = id
	return next
}

// EnsureMessageID assigns a UUID v4 if no message ID is present.
// Idempotent: if ID already set, no-op.
// Returns the (possibly newly created) Extra map.
//
// EnsureMessageID 在没有消息 ID 时分配一个 UUID v4。
// 幂等：如果已设置 ID，则不执行操作。
// 返回（可能新建的）Extra map。
func EnsureMessageID(extra map[string]any) map[string]any {
	if GetMessageID(extra) != "" {
		return extra
	}
	return SetMessageID(extra, uuid.NewString())
}
