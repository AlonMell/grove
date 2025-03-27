package sstable

import (
	"bufio"
	"cmp"
	"encoding/gob"
	"os"
	"sync"
)

type Writer[K cmp.Ordered, V any] struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	enc    *gob.Encoder
}

func NewWriter[K cmp.Ordered, V any](filename string) (*Writer[K, V], error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	bw := bufio.NewWriterSize(file, DefaultBufferSize)

	return &Writer[K, V]{
		file:   file,
		writer: bw,
		enc:    gob.NewEncoder(bw),
	}, nil
}

func (w *Writer[K, V]) Write(pairs []Pair[K, V]) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, pair := range pairs {
		if err := w.enc.Encode(pair); err != nil {
			panic(err)
		}
	}
}

func (w *Writer[K, V]) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	return w.file.Close()
}
