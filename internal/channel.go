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

package internal

import "sync"

// UnboundedChan represents a channel with unlimited capacity
// UnboundedChan 表示容量无限的 channel
type UnboundedChan[T any] struct {
	buffer []T // Internal buffer to store data
	// 用于存储数据的内部缓冲区
	mutex sync.Mutex // Mutex to protect buffer access
	// 用于保护缓冲区访问的互斥锁
	notEmpty *sync.Cond // Condition variable to wait for data
	// 用于等待数据的条件变量
	closed bool // Indicates if the channel has been closed
	// 指示 channel 是否已关闭
}

// NewUnboundedChan initializes and returns an UnboundedChan
// NewUnboundedChan 初始化并返回一个 UnboundedChan
func NewUnboundedChan[T any]() *UnboundedChan[T] {
	ch := &UnboundedChan[T]{}
	ch.notEmpty = sync.NewCond(&ch.mutex)
	return ch
}

// Send puts an item into the channel
// Send 将一个元素放入 channel
func (ch *UnboundedChan[T]) Send(value T) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	if ch.closed {
		panic("send on closed channel")
	}

	ch.buffer = append(ch.buffer, value)
	ch.notEmpty.Signal() // Wake up one goroutine waiting to receive
	// 唤醒一个等待接收的 goroutine
}

// TrySend attempts to put an item into the channel.
// Returns false if the channel is closed, true otherwise.
//
// TrySend 尝试将一个元素放入 channel。
// 如果 channel 已关闭则返回 false，否则返回 true。
func (ch *UnboundedChan[T]) TrySend(value T) bool {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	if ch.closed {
		return false
	}

	ch.buffer = append(ch.buffer, value)
	ch.notEmpty.Signal()
	return true
}

// Receive gets an item from the channel (blocks if empty).
// Returns (value, true) if an item was received.
// Returns (zero, false) if the channel was closed with no data remaining.
//
// Receive 从 channel 获取一项（为空时阻塞）。
// 收到项时返回 (value, true)。
// channel 已关闭且无剩余数据时返回 (zero, false)。
func (ch *UnboundedChan[T]) Receive() (T, bool) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	for len(ch.buffer) == 0 && !ch.closed {
		ch.notEmpty.Wait()
	}

	if len(ch.buffer) == 0 {
		var zero T
		return zero, false
	}

	val := ch.buffer[0]
	ch.buffer = ch.buffer[1:]
	return val, true
}

// Close marks the channel as closed
// Close 将 channel 标记为已关闭
func (ch *UnboundedChan[T]) Close() {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	if !ch.closed {
		ch.closed = true
		ch.notEmpty.Broadcast()
	}
}
