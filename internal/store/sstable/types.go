package sstable

import (
	"cmp"
)

const (
	DefaultBufferSize = 4096 * 1024 // 4MB buffer
)

// Pair представляет ключ-значение с обобщенными типами
type Pair[K cmp.Ordered, V any] struct {
	Key   K
	Value V
}
