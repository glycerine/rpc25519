// Copyright 2009 The Go Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package sparsified

import (
	//"bytes"
	"errors"
	"io"
	//"strings"
	"unicode/utf8"
)

const (
	defaultBufSize = 4096
)

var (
	ErrInvalidUnreadByte = errors.New("bufio: invalid use of UnreadByte")
	ErrInvalidUnreadRune = errors.New("bufio: invalid use of UnreadRune")
	ErrBufferFull        = errors.New("bufio: buffer full")
	ErrNegativeCount     = errors.New("bufio: negative count")
)

const minReadBufferSize = 16
const maxConsecutiveEmptyReads = 100

// buffered output

// SparseBufWriter implements buffering for an [io.Writer] object.
// If an error occurs writing to a [SparseBufWriter], no more data will be
// accepted and all subsequent writes, and [SparseBufWriter.Flush], will return the error.
// After all data has been written, the client should call the
// [SparseBufWriter.Flush] method to guarantee all data has been forwarded to
// the underlying [io.Writer].
type SparseBufWriter struct {
	err error
	buf []byte
	n   int
	wr  io.Writer
}

// NewSparseBufWriterSize returns a new [SparseBufWriter] whose buffer has at least the specified
// size. If the argument io.Writer is already a [SparseBufWriter] with large enough
// size, it returns the underlying [SparseBufWriter].
func NewSparseBufWriterSize(w io.Writer, size int) *SparseBufWriter {
	// Is it already a SparseBufWriter?
	b, ok := w.(*SparseBufWriter)
	if ok && len(b.buf) >= size {
		return b
	}
	if size <= 0 {
		size = defaultBufSize
	}
	return &SparseBufWriter{
		buf: make([]byte, size),
		wr:  w,
	}
}

// NewSparseBufWriter returns a new [SparseBufWriter] whose buffer has the default size.
// If the argument io.Writer is already a [SparseBufWriter] with large enough buffer size,
// it returns the underlying [SparseBufWriter].
func NewSparseBufWriter(w io.Writer) *SparseBufWriter {
	return NewSparseBufWriterSize(w, defaultBufSize)
}

// Size returns the size of the underlying buffer in bytes.
func (b *SparseBufWriter) Size() int { return len(b.buf) }

// Reset discards any unflushed buffered data, clears any error, and
// resets b to write its output to w.
// Calling Reset on the zero value of [SparseBufWriter] initializes the internal buffer
// to the default size.
// Calling w.Reset(w) (that is, resetting a [SparseBufWriter] to itself) does nothing.
func (b *SparseBufWriter) Reset(w io.Writer) {
	// If a SparseBufWriter w is passed to NewSparseBufWriter, NewSparseBufWriter will return w.
	// Different layers of code may do that, and then later pass w
	// to Reset. Avoid infinite recursion in that case.
	if b == w {
		return
	}
	if b.buf == nil {
		b.buf = make([]byte, defaultBufSize)
	}
	b.err = nil
	b.n = 0
	b.wr = w
}

// Flush writes any buffered data to the underlying [io.SparseBufWriter].
func (b *SparseBufWriter) Flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.n = 0
	return nil
}

// Available returns how many bytes are unused in the buffer.
func (b *SparseBufWriter) Available() int { return len(b.buf) - b.n }

// AvailableBuffer returns an empty buffer with b.Available() capacity.
// This buffer is intended to be appended to and
// passed to an immediately succeeding [SparseBufWriter.Write] call.
// The buffer is only valid until the next write operation on b.
func (b *SparseBufWriter) AvailableBuffer() []byte {
	return b.buf[b.n:][:0]
}

// Buffered returns the number of bytes that have been written into the current buffer.
func (b *SparseBufWriter) Buffered() int { return b.n }

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (b *SparseBufWriter) Write(p []byte) (nn int, err error) {
	for len(p) > b.Available() && b.err == nil {
		var n int
		if b.Buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.n:], p)
			b.n += n
			b.Flush()
		}
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}

// WriteByte writes a single byte.
func (b *SparseBufWriter) WriteByte(c byte) error {
	if b.err != nil {
		return b.err
	}
	if b.Available() <= 0 && b.Flush() != nil {
		return b.err
	}
	b.buf[b.n] = c
	b.n++
	return nil
}

// WriteRune writes a single Unicode code point, returning
// the number of bytes written and any error.
func (b *SparseBufWriter) WriteRune(r rune) (size int, err error) {
	// Compare as uint32 to correctly handle negative runes.
	if uint32(r) < utf8.RuneSelf {
		err = b.WriteByte(byte(r))
		if err != nil {
			return 0, err
		}
		return 1, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	n := b.Available()
	if n < utf8.UTFMax {
		if b.Flush(); b.err != nil {
			return 0, b.err
		}
		n = b.Available()
		if n < utf8.UTFMax {
			// Can only happen if buffer is silly small.
			return b.WriteString(string(r))
		}
	}
	size = utf8.EncodeRune(b.buf[b.n:], r)
	b.n += size
	return size, nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (b *SparseBufWriter) WriteString(s string) (int, error) {
	var sw io.StringWriter
	tryStringWriter := true

	nn := 0
	for len(s) > b.Available() && b.err == nil {
		var n int
		if b.Buffered() == 0 && sw == nil && tryStringWriter {
			// Check at most once whether b.wr is a StringWriter.
			sw, tryStringWriter = b.wr.(io.StringWriter)
		}
		if b.Buffered() == 0 && tryStringWriter {
			// Large write, empty buffer, and the underlying writer supports
			// WriteString: forward the write to the underlying StringWriter.
			// This avoids an extra copy.
			n, b.err = sw.WriteString(s)
		} else {
			n = copy(b.buf[b.n:], s)
			b.n += n
			b.Flush()
		}
		nn += n
		s = s[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], s)
	b.n += n
	nn += n
	return nn, nil
}

// ReadFrom implements [io.ReaderFrom]. If the underlying writer
// supports the ReadFrom method, this calls the underlying ReadFrom.
// If there is buffered data and an underlying ReadFrom, this fills
// the buffer and writes it before calling ReadFrom.
func (b *SparseBufWriter) ReadFrom(r io.Reader) (n int64, err error) {
	if b.err != nil {
		return 0, b.err
	}
	readerFrom, readerFromOK := b.wr.(io.ReaderFrom)
	var m int
	for {
		if b.Available() == 0 {
			if err1 := b.Flush(); err1 != nil {
				return n, err1
			}
		}
		if readerFromOK && b.Buffered() == 0 {
			nn, err := readerFrom.ReadFrom(r)
			b.err = err
			n += nn
			return n, err
		}
		nr := 0
		for nr < maxConsecutiveEmptyReads {
			m, err = r.Read(b.buf[b.n:])
			if m != 0 || err != nil {
				break
			}
			nr++
		}
		if nr == maxConsecutiveEmptyReads {
			return n, io.ErrNoProgress
		}
		b.n += m
		n += int64(m)
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		// If we filled the buffer exactly, flush preemptively.
		if b.Available() == 0 {
			err = b.Flush()
		} else {
			err = nil
		}
	}
	return n, err
}
