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

import (
	"sync"
	"testing"
	"time"
)

func TestUnboundedChan_Send(t *testing.T) {
	ch := NewUnboundedChan[string]()

	// Test sending a value
	// 测试发送一个值
	ch.Send("test")
	if len(ch.buffer) != 1 {
		t.Errorf("buffer length should be 1, got %d", len(ch.buffer))
	}
	if ch.buffer[0] != "test" {
		t.Errorf("expected 'test', got '%s'", ch.buffer[0])
	}

	// Test sending multiple values
	// 测试发送多个值
	ch.Send("test2")
	ch.Send("test3")
	if len(ch.buffer) != 3 {
		t.Errorf("buffer length should be 3, got %d", len(ch.buffer))
	}
}

func TestUnboundedChan_SendPanic(t *testing.T) {
	ch := NewUnboundedChan[int]()
	ch.Close()

	// Test sending to closed channel should panic
	// 测试向已关闭的 channel 发送应 panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("sending to closed channel should panic")
		}
	}()

	ch.Send(1)
}

func TestUnboundedChan_Receive(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Send values
	// 发送值
	ch.Send(1)
	ch.Send(2)

	// Test receiving values
	// 测试接收值
	val, ok := ch.Receive()
	if !ok {
		t.Error("receive should succeed")
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}

	val, ok = ch.Receive()
	if !ok {
		t.Error("receive should succeed")
	}
	if val != 2 {
		t.Errorf("expected 2, got %d", val)
	}
}

func TestUnboundedChan_ReceiveFromClosed(t *testing.T) {
	ch := NewUnboundedChan[int]()
	ch.Close()

	// Test receiving from closed, empty channel
	// 测试从已关闭且为空的 channel 接收
	val, ok := ch.Receive()
	if ok {
		t.Error("receive from closed, empty channel should return ok=false")
	}
	if val != 0 {
		t.Errorf("expected zero value, got %d", val)
	}

	// Test receiving from closed channel with values
	// 测试从有值的已关闭 channel 接收
	ch = NewUnboundedChan[int]()
	ch.Send(42)
	ch.Close()

	val, ok = ch.Receive()
	if !ok {
		t.Error("receive should succeed")
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	// After consuming all values
	// 消费完所有值后
	val, ok = ch.Receive()
	if ok {
		t.Error("receive from closed, empty channel should return ok=false")
	}
}

func TestUnboundedChan_Close(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Test closing
	// 测试关闭
	ch.Close()
	if !ch.closed {
		t.Error("channel should be marked as closed")
	}

	// Test double closing (should not panic)
	// 测试重复关闭（不应 panic）
	ch.Close()
}

func TestUnboundedChan_Concurrency(t *testing.T) {
	ch := NewUnboundedChan[int]()
	const numSenders = 5
	const numReceivers = 3
	const messagesPerSender = 100

	var rwg, swg sync.WaitGroup
	rwg.Add(numReceivers)
	swg.Add(numSenders)

	// Start senders
	// 启动发送方
	for i := 0; i < numSenders; i++ {
		go func(id int) {
			defer swg.Done()
			for j := 0; j < messagesPerSender; j++ {
				ch.Send(id*messagesPerSender + j)
				time.Sleep(time.Microsecond) // Small delay to increase concurrency chance
				// 短暂延迟以增加并发概率
			}
		}(i)
	}

	// Start receivers
	// 启动接收方
	received := make([]int, 0, numSenders*messagesPerSender)
	var mu sync.Mutex

	for i := 0; i < numReceivers; i++ {
		go func() {
			defer rwg.Done()
			for {
				val, ok := ch.Receive()
				if !ok {
					return
				}
				mu.Lock()
				received = append(received, val)
				mu.Unlock()
			}
		}()
	}

	// Wait for senders to finish
	// 等待发送方完成
	swg.Wait()
	ch.Close()

	// Wait for all goroutines to finish
	// 等待所有 goroutine 完成
	rwg.Wait()

	// Verify we received all messages
	// 验证已收到所有消息
	if len(received) != numSenders*messagesPerSender {
		t.Errorf("expected %d messages, got %d", numSenders*messagesPerSender, len(received))
	}

	// Create a map to check for duplicates and missing values
	// 创建 map 检查重复值和缺失值
	receivedMap := make(map[int]bool)
	for _, val := range received {
		receivedMap[val] = true
	}

	if len(receivedMap) != numSenders*messagesPerSender {
		t.Error("duplicate or missing messages detected")
	}
}

func TestUnboundedChan_BlockingReceive(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Test that Receive blocks when channel is empty
	// 测试 channel 为空时 Receive 会阻塞
	receiveDone := make(chan bool)
	go func() {
		ch.Receive()
		receiveDone <- true
	}()

	// Check that receive is blocked
	// 检查接收是否被阻塞
	select {
	case <-receiveDone:
		t.Error("Receive should block on empty channel")
	case <-time.After(50 * time.Millisecond):
		// This is expected
		// 这是预期行为
	}

	// Send a value to unblock
	// 发送一个值以解除阻塞
	ch.Send(1)

	// Now receive should complete
	// 现在接收应完成
	select {
	case <-receiveDone:
		// This is expected
		// 这是预期行为
	case <-time.After(50 * time.Millisecond):
		t.Error("Receive should have unblocked")
	}
}
