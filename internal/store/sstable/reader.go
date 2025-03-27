package sstable

import (
	"bufio"
	"cmp"
	"encoding/gob"
	"os"
)

type Reader[K cmp.Ordered, V any] struct {
	file    *os.File
	reader  *bufio.Reader
	dec     *gob.Decoder
	offsets map[K]int64 // Для быстрого поиска (опционально)
}

func NewReader[K cmp.Ordered, V any](filename string) (*Reader[K, V], error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	br := bufio.NewReaderSize(file, DefaultBufferSize)
	return &Reader[K, V]{
		file:   file,
		reader: br,
		dec:    gob.NewDecoder(br),
	}, nil
}

func (r *Reader[K, V]) Get(key K) (V, bool) {
	var current Pair[K, V]
	var zero V

	r.file.Seek(0, 0)
	r.reader.Reset(r.file)

	for {
		err := r.dec.Decode(&current)
		if err != nil {
			return zero, false
		}

		if current.Key == key {
			return current.Value, true
		}
	}
}
