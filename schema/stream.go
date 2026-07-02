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
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/cloudwego/eino/internal/safe"
)

// ErrNoValue is a sentinel returned from the convert function passed to
// [StreamReaderWithConvert] to skip a stream element — the element is dropped
// and the next one is read without surfacing an error to the caller.
//
// Use it to filter out empty or irrelevant chunks:
//
//	outStream = schema.StreamReaderWithConvert(s,
//	    func(src string) (string, error) {
//	        if len(src) == 0 {
//	            return "", schema.ErrNoValue // skip empty chunks
//	        }
//	        return src, nil
//	    })
//
// DO NOT use ErrNoValue in any other context.
//
// ErrNoValue 是传给 [StreamReaderWithConvert] 的 convert 函数返回的哨兵错误，用于跳过某个流元素——该元素会被丢弃，并继续读取下一个元素，不会向调用方暴露错误。
// 可用它过滤空块或无关块：
// outStream = schema.StreamReaderWithConvert(s,
// func(src string) (string, error) {
// if len(src) == 0 {
// return "", schema.ErrNoValue // 跳过空块
// }
// return src, nil
// })
// 不要在其他任何上下文中使用 ErrNoValue。
var ErrNoValue = errors.New("no value")

// ErrRecvAfterClosed indicates that StreamReader.Recv was unexpectedly called after StreamReader.Close.
// This error should not occur during normal use of StreamReader.Recv. If it does, please check your application code.
//
// ErrRecvAfterClosed 表示在 StreamReader.Close 之后意外调用了 StreamReader.Recv。
// 正常使用 StreamReader.Recv 时不应出现此错误。如果出现，请检查你的应用代码。
var ErrRecvAfterClosed = errors.New("recv after stream closed")

// SourceEOF represents an EOF error from a specific source stream.
// It is only returned by the method Recv() of StreamReader created
// with MergeNamedStreamReaders when one of its source streams reaches EOF.
//
// SourceEOF 表示来自特定源流的 EOF 错误。
// 它只会由使用 MergeNamedStreamReaders 创建的 StreamReader 的 Recv() 方法在某个源流到达 EOF 时返回。
type SourceEOF struct {
	sourceName string
}

func (e *SourceEOF) Error() string {
	return fmt.Sprintf("EOF from source stream: %s", e.sourceName)
}

// GetSourceName extracts the source stream name from a SourceEOF error.
// It returns the source name and a boolean indicating whether the error was a SourceEOF.
// If the error is not a SourceEOF, it returns an empty string and false.
//
// GetSourceName 从 SourceEOF 错误中提取源流名称。
// 它返回源名称，以及一个表示该错误是否为 SourceEOF 的布尔值。
// 如果错误不是 SourceEOF，则返回空字符串和 false。
func GetSourceName(err error) (string, bool) {
	var sErr *SourceEOF
	if errors.As(err, &sErr) {
		return sErr.sourceName, true
	}

	return "", false
}

// Pipe creates a new stream with the given capacity that represented with StreamWriter and StreamReader.
// The capacity is the maximum number of items that can be buffered in the stream.
// e.g.
//
//	sr, sw := schema.Pipe[string](3)
//	go func() { // send data
//		defer sw.Close()
//		for i := 0; i < 10; i++ {
//			sw.Send(i, nil)
//		}
//	}
//
//	defer sr.Close()
//	for {
//		chunk, err := sr.Recv()
//		if errors.Is(err, io.EOF) {
//			break
//		}
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println(chunk)
//	}
//
// Pipe 使用给定容量创建一个新流，并以 StreamWriter 和 StreamReader 表示。
// 容量是流中可缓冲的最大元素数。
// 例如：
// sr, sw := schema.Pipe[string](3)
// go func() { // 发送数据
// defer sw.Close()
// for i := 0; i < 10; i++ {
// sw.Send(i, nil)
// }
// }
// defer sr.Close()
// for {
// chunk, err := sr.Recv()
// if errors.Is(err, io.EOF) {
// break
// }
// if err != nil {
// panic(err)
// }
// fmt.Println(chunk)
// }
func Pipe[T any](cap int) (*StreamReader[T], *StreamWriter[T]) {
	stm := newStream[T](cap)
	return stm.asReader(), &StreamWriter[T]{stm: stm}
}

// StreamWriter the sender of a stream.
// created by Pipe function.
// eg.
//
//	sr, sw := schema.Pipe[string](3)
//	go func() { // send data
//		defer sw.Close()
//		for i := 0; i < 10; i++ {
//			sw.Send(i, nil)
//		}
//	}
//
// StreamWriter 是流的发送端。
// 由 Pipe 函数创建。
// 例如：
// sr, sw := schema.Pipe[string](3)
// go func() { // 发送数据
// defer sw.Close()
// for i := 0; i < 10; i++ {
// sw.Send(i, nil)
// }
// }
type StreamWriter[T any] struct {
	stm *stream[T]
}

// Send sends a value to the stream.
// e.g.
//
//	closed := sw.Send(i, nil)
//	if closed {
//		// the stream is closed
//	}
//
// Send 向流发送一个值。
// 例如：
// closed := sw.Send(i, nil)
// if closed {
// 流已关闭
// }
func (sw *StreamWriter[T]) Send(chunk T, err error) (closed bool) {
	return sw.stm.send(chunk, err)
}

// Close notify the receiver that the stream sender has finished.
// The stream receiver will get an error of io.EOF from StreamReader.Recv().
// Notice: always remember to call Close() after sending all data.
// eg.
//
//	defer sw.Close()
//	for i := 0; i < 10; i++ {
//		sw.Send(i, nil)
//	}
//
// Close 通知接收方流发送端已完成。
// 流接收方会从 StreamReader.Recv() 获得 io.EOF 错误。
// 注意：发送完所有数据后，务必调用 Close()。
// 例如：
// defer sw.Close()
// for i := 0; i < 10; i++ {
// sw.Send(i, nil)
// }
func (sw *StreamWriter[T]) Close() {
	sw.stm.closeSend()
}

// StreamReader is the consumer side of an Eino stream.
//
// A StreamReader is read-once: only one goroutine should call Recv, and the
// reader must be closed exactly once (whether the loop finishes normally or
// exits early via break or return).
//
// Typical usage:
//
//	defer sr.Close() // always close, even after io.EOF
//	for {
//	    chunk, err := sr.Recv()
//	    if errors.Is(err, io.EOF) {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    process(chunk)
//	}
//
// To fan-out a single stream to N independent consumers, call [StreamReader.Copy]
// before any Recv; the original reader becomes unusable after the call.
//
// StreamReaders are created by [Pipe], [StreamReaderFromArray],
// [MergeStreamReaders], [MergeNamedStreamReaders], and [StreamReaderWithConvert].
//
// StreamReader 是 Eino 流的消费端。
// StreamReader 只能读取一次：只能由一个 goroutine 调用 Recv，并且必须且只能关闭一次（无论循环正常结束，还是通过 break 或 return 提前退出）。
// 典型用法：
// defer sr.Close() // 始终关闭，即使在 io.EOF 之后
// for {
// chunk, err := sr.Recv()
// if errors.Is(err, io.EOF) {
// break
// }
// if err != nil {
// return err
// }
// process(chunk)
// }
// 要将单个流 fan-out 给 N 个独立消费者，请在任何 Recv 之前调用 [StreamReader.Copy]；调用后原读取器将不可用。
// StreamReader 由 [Pipe]、[StreamReaderFromArray]、[MergeStreamReaders]、[MergeNamedStreamReaders] 和 [StreamReaderWithConvert] 创建。
type StreamReader[T any] struct {
	typ readerType

	st *stream[T]

	ar *arrayReader[T]

	msr *multiStreamReader[T]

	srw *streamReaderWithConvert[T]

	csr *childStreamReader[T]
}

// Recv receives a value from the stream.
// eg.
//
//	for {
//		chunk, err := sr.Recv()
//		if errors.Is(err, io.EOF) {
//			break
//		}
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println(chunk)
//	}
//
// Recv 从流中接收一个值。
// 例如：
// for {
// chunk, err := sr.Recv()
// if errors.Is(err, io.EOF) {
// break
// }
// if err != nil {
// panic(err)
// }
// fmt.Println(chunk)
// }
func (sr *StreamReader[T]) Recv() (T, error) {
	switch sr.typ {
	case readerTypeStream:
		return sr.st.recv()
	case readerTypeArray:
		return sr.ar.recv()
	case readerTypeMultiStream:
		return sr.msr.recv()
	case readerTypeWithConvert:
		return sr.srw.recv()
	case readerTypeChild:
		return sr.csr.recv()
	default:
		panic("impossible")
	}
}

// Close safely closes the StreamReader.
// It should be called only once, as multiple calls may not work as expected.
// Notice: always remember to call Close() after using Recv().
// e.g.
//
//	defer sr.Close()
//
//	for {
//		chunk, err := sr.Recv()
//		if errors.Is(err, io.EOF) {
//			break
//		}
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println(chunk)
//	}
//
// Close 安全地关闭 StreamReader。
// 它应只调用一次，多次调用可能无法按预期工作。
// 注意：使用 Recv() 后务必记得调用 Close()。
// 例如：
// defer sr.Close()
// for {
// chunk, err := sr.Recv()
// if errors.Is(err, io.EOF) {
// break
// }
// if err != nil {
// panic(err)
// }
// fmt.Println(chunk)
// }
func (sr *StreamReader[T]) Close() {
	switch sr.typ {
	case readerTypeStream:
		sr.st.closeRecv()
	case readerTypeArray:

	case readerTypeMultiStream:
		sr.msr.close()
	case readerTypeWithConvert:
		sr.srw.close()
	case readerTypeChild:
		sr.csr.close()
	default:
		panic("impossible")
	}
}

// Copy creates n independent StreamReaders that each receive every element of
// the original stream. The original StreamReader becomes unusable after Copy.
//
// Use Copy when two or more pipeline branches need the same stream —
// for example, when a stream must be fed to both a callback handler and the
// next node in a graph:
//
//	copies := sr.Copy(2)
//	sr1, sr2 := copies[0], copies[1]
//	defer sr1.Close()
//	defer sr2.Close()
//
//	// sr1 and sr2 independently read the same elements
//
// n must be at least 1. If n < 2, the original reader is returned unchanged.
//
// Copy 创建 n 个独立的 StreamReader，每个都会接收原始流的所有元素。
// 调用 Copy 后，原始 StreamReader 将不可再用。
// 当两个或多个流水线分支需要同一个流时使用 Copy ——
// 例如，需要将一个流同时传给 callback handler 和图中的下一个节点时：
// copies := sr.Copy(2)
// sr1, sr2 := copies[0], copies[1]
// defer sr1.Close()
// defer sr2.Close()
// sr1 and sr2 独立读取相同元素
// n 必须至少为 1。如果 n < 2，则原始读取器将原样返回。
func (sr *StreamReader[T]) Copy(n int) []*StreamReader[T] {
	if n < 2 {
		return []*StreamReader[T]{sr}
	}

	if sr.typ == readerTypeArray {
		ret := make([]*StreamReader[T], n)
		for i, ar := range sr.ar.copy(n) {
			ret[i] = &StreamReader[T]{typ: readerTypeArray, ar: ar}
		}
		return ret
	}

	return copyStreamReaders[T](sr, n)
}

// SetAutomaticClose sets the StreamReader to automatically close when it's no longer reachable and ready to be GCed.
// NOT concurrency safe.
//
// SetAutomaticClose 设置 StreamReader 在不可达且可被 GC 时自动关闭。
// 非并发安全。
func (sr *StreamReader[T]) SetAutomaticClose() {
	switch sr.typ {
	case readerTypeStream:
		if !sr.st.automaticClose {
			sr.st.automaticClose = true
			var flag uint32
			sr.st.closedFlag = &flag
			runtime.SetFinalizer(sr, func(s *StreamReader[T]) {
				s.Close()
			})
		}
	case readerTypeMultiStream:
		for _, s := range sr.msr.nonClosedStreams() {
			if !s.automaticClose {
				s.automaticClose = true
				var flag uint32
				s.closedFlag = &flag
				runtime.SetFinalizer(s, func(st *stream[T]) {
					st.closeRecv()
				})
			}
		}
	case readerTypeChild:
		parent := sr.csr.parent.sr
		parent.SetAutomaticClose()
	case readerTypeWithConvert:
		sr.srw.sr.SetAutomaticClose()
	case readerTypeArray:
		// no need to clean up
		// 无需清理
	default:
	}
}

func (sr *StreamReader[T]) recvAny() (any, error) {
	return sr.Recv()
}

func (sr *StreamReader[T]) copyAny(n int) []iStreamReader {
	ret := make([]iStreamReader, n)

	srs := sr.Copy(n)

	for i := 0; i < n; i++ {
		ret[i] = srs[i]
	}

	return ret
}

func arrToStream[T any](arr []T) *stream[T] {
	s := newStream[T](len(arr))
	for i := range arr {
		s.send(arr[i], nil)
	}
	s.closeSend()

	return s
}

func (sr *StreamReader[T]) toStream() *stream[T] {
	switch sr.typ {
	case readerTypeStream:
		return sr.st
	case readerTypeArray:
		return sr.ar.toStream()
	case readerTypeMultiStream:
		return sr.msr.toStream()
	case readerTypeWithConvert:
		return sr.srw.toStream()
	case readerTypeChild:
		return sr.csr.toStream()
	default:
		panic("impossible")
	}
}

type readerType int

const (
	readerTypeStream readerType = iota
	readerTypeArray
	readerTypeMultiStream
	readerTypeWithConvert
	readerTypeChild
)

type iStreamReader interface {
	recvAny() (any, error)
	copyAny(int) []iStreamReader
	Close()
	SetAutomaticClose()
}

// stream is a channel-based stream with 1 sender and 1 receiver.
// The sender calls closeSend() to notify the receiver that the stream sender has finished.
// The receiver calls closeRecv() to notify the sender that the receiver stop receiving.
//
// stream 是基于 channel 的流，包含 1 个发送方和 1 个接收方。
// 发送方调用 closeSend() 通知接收方流发送方已完成。
// 接收方调用 closeRecv() 通知发送方接收方停止接收。
type stream[T any] struct {
	items chan streamItem[T]

	closed chan struct{}

	automaticClose bool
	closedFlag     *uint32 // 0 = not closed, 1 = closed, only used when automaticClose is set
	// 0 = 未关闭，1 = 已关闭，仅在设置 automaticClose 时使用
}

type streamItem[T any] struct {
	chunk T
	err   error
}

func newStream[T any](cap int) *stream[T] {
	return &stream[T]{
		items:  make(chan streamItem[T], cap),
		closed: make(chan struct{}),
	}
}

func (s *stream[T]) asReader() *StreamReader[T] {
	return &StreamReader[T]{typ: readerTypeStream, st: s}
}

func (s *stream[T]) recv() (chunk T, err error) {
	item, ok := <-s.items

	if !ok {
		item.err = io.EOF
	}

	return item.chunk, item.err
}

func (s *stream[T]) send(chunk T, err error) (closed bool) {
	// if the stream is closed, return immediately
	// 如果流已关闭，立即返回
	select {
	case <-s.closed:
		return true
	default:
	}

	item := streamItem[T]{chunk, err}

	select {
	case <-s.closed:
		return true
	case s.items <- item:
		return false
	}
}

func (s *stream[T]) closeSend() {
	close(s.items)
}

func (s *stream[T]) closeRecv() {
	if s.automaticClose {
		if atomic.CompareAndSwapUint32(s.closedFlag, 0, 1) {
			close(s.closed)
		}
		return
	}

	close(s.closed)
}

// StreamReaderFromArray creates a StreamReader from a given slice of elements.
// It takes an array of type T and returns a pointer to a StreamReader[T].
// This allows for streaming the elements of the array in a controlled manner.
// eg.
//
//	sr := schema.StreamReaderFromArray([]int{1, 2, 3})
//	defer sr.Close()
//
//	for {
//		chunk, err := sr.Recv()
//		if errors.Is(err, io.EOF) {
//			break
//		}
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println(chunk)
//	}
//
// StreamReaderFromArray 从给定元素切片创建 StreamReader。
// 它接收类型为 T 的数组，并返回指向 StreamReader[T] 的指针。
// 这允许以受控方式流式传输数组元素。
// 例如：
// sr := schema.StreamReaderFromArray([]int{1, 2, 3})
// defer sr.Close()
// for {
// chunk, err := sr.Recv()
// if errors.Is(err, io.EOF) {
// break
// }
// if err != nil {
// panic(err)
// }
// fmt.Println(chunk)
// }
func StreamReaderFromArray[T any](arr []T) *StreamReader[T] {
	return &StreamReader[T]{ar: &arrayReader[T]{arr: arr}, typ: readerTypeArray}
}

type arrayReader[T any] struct {
	arr   []T
	index int
}

func (ar *arrayReader[T]) recv() (T, error) {
	if ar.index < len(ar.arr) {
		ret := ar.arr[ar.index]
		ar.index++

		return ret, nil
	}

	var t T
	return t, io.EOF
}

func (ar *arrayReader[T]) copy(n int) []*arrayReader[T] {
	ret := make([]*arrayReader[T], n)

	for i := 0; i < n; i++ {
		ret[i] = &arrayReader[T]{
			arr:   ar.arr,
			index: ar.index,
		}
	}

	return ret
}

func (ar *arrayReader[T]) toStream() *stream[T] {
	return arrToStream(ar.arr[ar.index:])
}

type multiArrayReader[T any] struct {
	ars   []*arrayReader[T]
	index int
}

type multiStreamReader[T any] struct {
	sts []*stream[T]

	itemsCases []reflect.SelectCase

	nonClosed []int

	sourceReaderNames []string
}

func newMultiStreamReader[T any](sts []*stream[T]) *multiStreamReader[T] {
	var itemsCases []reflect.SelectCase
	if len(sts) > maxSelectNum {
		itemsCases = make([]reflect.SelectCase, len(sts))
		for i, st := range sts {
			itemsCases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(st.items),
			}
		}
	}

	nonClosed := make([]int, len(sts))
	for i := range sts {
		nonClosed[i] = i
	}

	return &multiStreamReader[T]{
		sts:        sts,
		itemsCases: itemsCases,
		nonClosed:  nonClosed,
	}
}

func (msr *multiStreamReader[T]) recv() (T, error) {
	for len(msr.nonClosed) > 0 {
		var chosen int
		var ok bool
		if len(msr.nonClosed) > maxSelectNum {
			var recv reflect.Value
			chosen, recv, ok = reflect.Select(msr.itemsCases)
			if ok {
				item := recv.Interface().(streamItem[T])
				return item.chunk, item.err
			}
			msr.itemsCases[chosen].Chan = reflect.Value{}
		} else {
			var item *streamItem[T]
			chosen, item, ok = receiveN(msr.nonClosed, msr.sts)
			if ok {
				return item.chunk, item.err
			}
		}

		// delete the closed stream
		// 删除已关闭的流
		for i := range msr.nonClosed {
			if msr.nonClosed[i] == chosen {
				msr.nonClosed = append(msr.nonClosed[:i], msr.nonClosed[i+1:]...)
				break
			}
		}

		if len(msr.sourceReaderNames) > 0 {
			var t T
			return t, &SourceEOF{msr.sourceReaderNames[chosen]}
		}
	}

	var t T
	return t, io.EOF
}

func (msr *multiStreamReader[T]) nonClosedStreams() []*stream[T] {
	ret := make([]*stream[T], len(msr.nonClosed))

	for i, idx := range msr.nonClosed {
		ret[i] = msr.sts[idx]
	}

	return ret
}

func (msr *multiStreamReader[T]) close() {
	for _, s := range msr.sts {
		s.closeRecv()
	}
}

func (msr *multiStreamReader[T]) toStream() *stream[T] {
	return toStream[T, *multiStreamReader[T]](msr)
}

type streamReaderWithConvert[T any] struct {
	sr iStreamReader

	convert func(any) (T, error)

	errWrapper func(error) error
	onEOF      func() (T, error)
	eofDone    bool
}

func newStreamReaderWithConvert[T any](origin iStreamReader, convert func(any) (T, error), opts ...ConvertOption) *StreamReader[T] {
	opt := &convertOptions{}
	for _, o := range opts {
		o(opt)
	}

	srw := &streamReaderWithConvert[T]{
		sr:         origin,
		convert:    convert,
		errWrapper: opt.ErrWrapper,
	}

	if opt.OnEOF != nil {
		typedOnEOF := opt.OnEOF
		srw.onEOF = func() (T, error) {
			v, err := typedOnEOF()
			if err != nil {
				var t T
				return t, err
			}
			if v == nil {
				var t T
				return t, nil
			}
			return v.(T), nil
		}
	}

	return &StreamReader[T]{
		typ: readerTypeWithConvert,
		srw: srw,
	}
}

type convertOptions struct {
	ErrWrapper func(error) error
	OnEOF      func() (any, error)
}

type ConvertOption func(*convertOptions)

// WithErrWrapper wraps non-EOF errors from the underlying stream reader during
// conversion by StreamReaderWithConvert. Errors returned by the convert function
// itself are not wrapped.
// If the wrapper returns nil, the errored chunk is skipped and the next chunk
// is read. If the wrapper returns a non-nil error, that error is surfaced to
// the caller.
//
// WithErrWrapper 会在 StreamReaderWithConvert 转换期间包装底层 stream reader 的非 EOF 错误。
// convert 函数自身返回的错误不会被包装。
// 如果 wrapper 返回 nil，出错的 chunk 会被跳过并读取下一个 chunk。
// 如果 wrapper 返回非 nil 错误，该错误会暴露给调用方。
func WithErrWrapper(wrapper func(error) error) ConvertOption {
	return func(o *convertOptions) {
		o.ErrWrapper = wrapper
	}
}

// WithOnEOF registers a callback that fires once when the stream reaches EOF.
// The callback can inject an error or a value before the final io.EOF is returned.
// If the callback returns (nil, io.EOF), the stream ends normally.
// If it returns a non-EOF error, that error is delivered first, then subsequent Recv returns io.EOF.
// If it returns a non-nil value with nil error, that value is delivered first, then io.EOF.
//
// WithOnEOF 注册一个回调，在流到达 EOF 时触发一次。
// 该回调可在最终 io.EOF 返回前注入错误或值。
// 如果回调返回 (nil, io.EOF)，流正常结束。
// 如果返回非 EOF 错误，会先交付该错误，后续 Recv 返回 io.EOF。
// 如果返回非 nil 值且错误为 nil，会先交付该值，然后返回 io.EOF。
func WithOnEOF(fn func() (any, error)) ConvertOption {
	return func(o *convertOptions) {
		o.OnEOF = fn
	}
}

// StreamReaderWithConvert returns a new StreamReader[D] that wraps sr and
// applies convert to every element. The original reader sr must not be used
// after calling this function.
//
// Filtering: if convert returns [ErrNoValue], the element is silently dropped
// and the next element is read. This lets you strip empty or irrelevant chunks
// without surfacing an error to the caller.
//
// Error wrapping: use [WithErrWrapper] to wrap non-convert errors (e.g. those
// arriving from an upstream source) before they reach the caller.
//
//	intReader := schema.StreamReaderFromArray([]int{0, 1, 2, 3})
//	strReader := schema.StreamReaderWithConvert(intReader,
//	    func(i int) (string, error) {
//	        if i == 0 {
//	            return "", schema.ErrNoValue // skip zero
//	        }
//	        return fmt.Sprintf("val_%d", i), nil
//	    })
//	defer strReader.Close()
//	// Recv yields "val_1", "val_2", "val_3"
//
// StreamReaderWithConvert 返回一个新的 StreamReader[D]，它包装 sr 并对每个元素应用 convert。
// 调用此函数后，不得再使用原始读取器 sr。
// 过滤：如果 convert 返回 [ErrNoValue]，该元素会被静默丢弃，并读取下一个元素。
// 这可用于去除空的或无关的 chunk，而不会向调用方暴露错误。
// 错误包装：使用 [WithErrWrapper] 在非 convert 错误（例如来自上游源的错误）到达调用方前对其进行包装。
// intReader := schema.StreamReaderFromArray([]int{0, 1, 2, 3})
// strReader := schema.StreamReaderWithConvert(intReader,
// func(i int) (string, error) {
// if i == 0 {
// return "", schema.ErrNoValue // skip zero
// }
// return fmt.Sprintf("val_%d", i), nil
// })
// defer strReader.Close()
// Recv 产出 "val_1", "val_2", "val_3"
func StreamReaderWithConvert[T, D any](sr *StreamReader[T], convert func(T) (D, error), opts ...ConvertOption) *StreamReader[D] {
	c := func(a any) (D, error) {
		return convert(a.(T))
	}

	return newStreamReaderWithConvert(sr, c, opts...)
}

func (srw *streamReaderWithConvert[T]) recv() (T, error) {
	for {
		out, err := srw.sr.recvAny()

		if err != nil {
			var t T
			if err == io.EOF {
				if srw.onEOF != nil && !srw.eofDone {
					srw.eofDone = true
					val, onEOFErr := srw.onEOF()
					if onEOFErr != io.EOF {
						return val, onEOFErr
					}
				}
				return t, io.EOF
			}
			if srw.errWrapper != nil {
				err = srw.errWrapper(err)
				if err != nil {
					return t, err
				}

				continue
			}

			return t, err
		}

		t, err := srw.convert(out)
		if err == nil {
			return t, nil
		}

		if !errors.Is(err, ErrNoValue) {
			return t, err
		}
	}
}

func (srw *streamReaderWithConvert[T]) close() {
	srw.sr.Close()
}

type reader[T any] interface {
	recv() (T, error)
	close()
}

func toStream[T any, Reader reader[T]](r Reader) *stream[T] {
	ret := newStream[T](5)

	go func() {
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				e := safe.NewPanicErr(panicErr, debug.Stack())

				var chunk T
				_ = ret.send(chunk, e)
			}

			ret.closeSend()
			r.close()
		}()

		for {
			out, err := r.recv()
			if err == io.EOF {
				break
			}

			closed := ret.send(out, err)
			if closed {
				break
			}
		}
	}()

	return ret
}

func (srw *streamReaderWithConvert[T]) toStream() *stream[T] {
	return toStream[T, *streamReaderWithConvert[T]](srw)
}

type cpStreamElement[T any] struct {
	once sync.Once
	next *cpStreamElement[T]
	item streamItem[T]
}

// copyStreamReaders creates multiple independent StreamReaders from a single StreamReader.
// Each child StreamReader can read from the original stream independently.
//
// copyStreamReaders 从单个 StreamReader 创建多个独立的 StreamReader。
// 每个子 StreamReader 都可以独立读取原始流。
func copyStreamReaders[T any](sr *StreamReader[T], n int) []*StreamReader[T] {
	cpsr := &parentStreamReader[T]{
		sr:            sr,
		subStreamList: make([]*cpStreamElement[T], n),
		closedNum:     0,
	}

	// Initialize subStreamList with an empty element, which acts like a tail node.
	// A nil element (used for dereference) represents that the child has been closed.
	// It is challenging to link the previous and current elements when the length of the original channel is unknown.
	// Additionally, using a previous pointer complicates dereferencing elements, possibly requiring reference counting.
	//
	// 使用一个空元素初始化 subStreamList，它充当尾节点。
	// nil 元素（用于解除引用）表示子项已关闭。
	// 当原始 channel 长度未知时，很难将前一个元素和当前元素链接起来。
	// 此外，使用 previous 指针会让元素解除引用更复杂，可能需要引用计数。
	elem := &cpStreamElement[T]{}

	for i := range cpsr.subStreamList {
		cpsr.subStreamList[i] = elem
	}

	ret := make([]*StreamReader[T], n)
	for i := range ret {
		ret[i] = &StreamReader[T]{
			csr: &childStreamReader[T]{
				parent: cpsr,
				index:  i,
			},
			typ: readerTypeChild,
		}
	}

	return ret
}

type parentStreamReader[T any] struct {
	// sr is the original StreamReader.
	// sr 是原始 StreamReader。
	sr *StreamReader[T]

	// subStreamList maps each child's index to its latest read chunk.
	// Each value comes from a hidden linked list of cpStreamElement.
	//
	// subStreamList 将每个子项的索引映射到其最新读取的 chunk。
	// 每个值都来自隐藏的 cpStreamElement 链表。
	subStreamList []*cpStreamElement[T]

	// closedNum is the count of closed children.
	// closedNum 是已关闭子项的数量。
	closedNum uint32
}

// peek is not safe for concurrent use with the same idx but is safe for different idx.
// Ensure that each child StreamReader uses a for-loop in a single goroutine.
//
// peek 对同一个 idx 并发使用不安全，但对不同 idx 是安全的。
// 确保每个子 StreamReader 都在单个 goroutine 中使用 for-loop。
func (p *parentStreamReader[T]) peek(idx int) (t T, err error) {
	elem := p.subStreamList[idx]
	if elem == nil {
		// Unexpected call to receive after the child has been closed.
		// 子项关闭后意外调用 receive。
		return t, ErrRecvAfterClosed
	}

	// The sync.Once here is used to:
	// 1. Write the content of this cpStreamElement.
	// 2. Initialize the 'next' field of this cpStreamElement with an empty cpStreamElement,
	//    similar to the initialization in copyStreamReaders.
	//
	// 这里的 sync.Once 用于：
	// 1. 写入此 cpStreamElement 的内容。
	// 2. 用一个空的 cpStreamElement 初始化此 cpStreamElement 的 'next' 字段，
	// 类似于 copyStreamReaders 中的初始化。
	elem.once.Do(func() {
		t, err = p.sr.Recv()
		elem.item = streamItem[T]{chunk: t, err: err}
		if err != io.EOF {
			elem.next = &cpStreamElement[T]{}
			p.subStreamList[idx] = elem.next
		}
	})

	// The element has been set and will not be modified again.
	// Therefore, children can read this element's content and 'next' pointer concurrently.
	//
	// 该元素已设置，且不会再被修改。
	// 因此，子级可以并发读取该元素的内容和 'next' 指针。
	t = elem.item.chunk
	err = elem.item.err
	if err != io.EOF {
		p.subStreamList[idx] = elem.next
	}

	return t, err
}

func (p *parentStreamReader[T]) close(idx int) {
	if p.subStreamList[idx] == nil {
		return // avoid close multiple times
		// 避免多次 close
	}

	p.subStreamList[idx] = nil

	curClosedNum := atomic.AddUint32(&p.closedNum, 1)

	allClosed := int(curClosedNum) == len(p.subStreamList)
	if allClosed {
		p.sr.Close()
	}
}

type childStreamReader[T any] struct {
	parent *parentStreamReader[T]
	index  int
}

func (csr *childStreamReader[T]) recv() (T, error) {
	return csr.parent.peek(csr.index)
}

func (csr *childStreamReader[T]) toStream() *stream[T] {
	return toStream[T, *childStreamReader[T]](csr)
}

func (csr *childStreamReader[T]) close() {
	csr.parent.close(csr.index)
}

// MergeStreamReaders fans in multiple StreamReaders into a single StreamReader.
// Elements from all source streams are interleaved in arrival order (non-deterministic).
// The merged reader reaches EOF only after every source stream has been exhausted.
//
// Callers must still close the merged reader; it propagates the close signal
// to all underlying sources.
//
// Use [MergeNamedStreamReaders] instead when you need to know which source
// stream ended first (it emits a [SourceEOF] per-source EOF rather than
// silently discarding them).
//
// Returns nil if srs is empty.
//
// MergeStreamReaders 将多个 StreamReaders 扇入为单个 StreamReader。
// 来自所有源流的元素会按到达顺序交错（非确定性）。
// 只有所有源流都耗尽后，合并后的读取器才会到达 EOF。
// 调用方仍必须 close 合并后的读取器；它会将 close 信号
// 传播到底层所有源。
// 当需要知道哪个源流先结束时，请改用 [MergeNamedStreamReaders]
// （它会为每个源发出 [SourceEOF]，而不是静默丢弃它们）。
// 如果 srs 为空，则返回 nil。
func MergeStreamReaders[T any](srs []*StreamReader[T]) *StreamReader[T] {
	if len(srs) < 1 {
		return nil
	}

	if len(srs) < 2 {
		return srs[0]
	}

	var arr []T
	var ss []*stream[T]

	for _, sr := range srs {
		switch sr.typ {
		case readerTypeStream:
			ss = append(ss, sr.st)
		case readerTypeArray:
			arr = append(arr, sr.ar.arr[sr.ar.index:]...)
		case readerTypeMultiStream:
			ss = append(ss, sr.msr.nonClosedStreams()...)
		case readerTypeWithConvert:
			ss = append(ss, sr.srw.toStream())
		case readerTypeChild:
			ss = append(ss, sr.csr.toStream())
		default:
			panic("impossible")
		}
	}

	if len(ss) == 0 {
		return &StreamReader[T]{
			typ: readerTypeArray,
			ar: &arrayReader[T]{
				arr:   arr,
				index: 0,
			},
		}
	}

	if len(arr) != 0 {
		s := arrToStream(arr)
		ss = append(ss, s)
	}

	return &StreamReader[T]{
		typ: readerTypeMultiStream,
		msr: newMultiStreamReader(ss),
	}
}

// MergeNamedStreamReaders merges multiple named StreamReaders into one.
// Unlike [MergeStreamReaders], when a source stream reaches EOF the merged
// reader emits a [SourceEOF] error (instead of silently continuing) so you can
// detect exactly which source finished. Use [GetSourceName] to retrieve the
// name from a SourceEOF error. The merged reader itself signals io.EOF only
// after all named sources are exhausted.
//
// This is useful when downstream logic must react differently to each source
// completing — for example, draining one agent's output before proceeding:
//
//	namedStreams := map[string]*schema.StreamReader[string]{
//	    "agent_a": srA,
//	    "agent_b": srB,
//	}
//	merged := schema.MergeNamedStreamReaders(namedStreams)
//	defer merged.Close()
//	for {
//	    chunk, err := merged.Recv()
//	    if errors.Is(err, io.EOF) { break }
//	    if name, ok := schema.GetSourceName(err); ok {
//	        fmt.Printf("%s finished\n", name)
//	        continue
//	    }
//	    if err != nil { return err }
//	    process(chunk)
//	}
//
// Returns nil if srs is empty.
//
// MergeNamedStreamReaders 将多个命名的 StreamReaders 合并为一个。
// 不同于 [MergeStreamReaders]，当某个源流到达 EOF 时，合并后的
// 读取器会发出 [SourceEOF] 错误（而不是静默继续），以便你能
// 准确检测哪个源已结束。使用 [GetSourceName] 从 SourceEOF 错误中获取
// 名称。合并后的读取器自身只有在所有命名源都耗尽后才会发出 io.EOF。
// 当下游逻辑必须针对每个源的完成做出不同反应时很有用，
// 例如在继续前先排空某个 agent 的输出：
// namedStreams := map[string]*schema.StreamReader[string]{
// "agent_a": srA,
// "agent_b": srB,
// }
// merged := schema.MergeNamedStreamReaders(namedStreams)
// defer merged.Close()
// for {
// chunk, err := merged.Recv()
// if errors.Is(err, io.EOF) { break }
// if name, ok := schema.GetSourceName(err); ok {
// fmt.Printf("%s finished\n", name)
// continue
// }
// if err != nil { return err }
// process(chunk)
// }
// 如果 srs 为空，则返回 nil。
func MergeNamedStreamReaders[T any](srs map[string]*StreamReader[T]) *StreamReader[T] {
	if len(srs) < 1 {
		return nil
	}

	ss := make([]*StreamReader[T], len(srs))
	names := make([]string, len(srs))

	i := 0
	for name, sr := range srs {
		ss[i] = sr
		names[i] = name
		i++
	}

	return InternalMergeNamedStreamReaders(ss, names)
}

// InternalMergeNamedStreamReaders merges multiple readers with their names
// into a single multi-stream reader.
//
// InternalMergeNamedStreamReaders 将多个带名称的读取器
// 合并为单个多流读取器。
func InternalMergeNamedStreamReaders[T any](srs []*StreamReader[T], names []string) *StreamReader[T] {
	ss := make([]*stream[T], len(srs))

	for i, sr := range srs {
		ss[i] = sr.toStream()
	}

	msr := newMultiStreamReader(ss)
	msr.sourceReaderNames = names

	return &StreamReader[T]{
		typ: readerTypeMultiStream,
		msr: msr,
	}
}
