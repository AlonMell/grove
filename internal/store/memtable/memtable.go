package memtable

import (
	"bytes"
	"cmp"
	"encoding/gob"

	"github.com/grove/internal/datastruct/rbtree"
	"github.com/grove/internal/datastruct/types"
)

type MemTable[K cmp.Ordered, V any] struct {
	tree *rbtree.RBTree[K, V]
	size uint64
	enc  *gob.Encoder
	buf  *bytes.Buffer
}

func newGob[K cmp.Ordered, V any](buf *bytes.Buffer) *gob.Encoder {
	gob.Register(types.Pair[K, V]{})
	enc := gob.NewEncoder(buf)
	enc.Encode(types.Pair[K, V]{})
	buf.Reset()
	return enc
}

func New[K cmp.Ordered, V any]() *MemTable[K, V] {
	var mt MemTable[K, V]

	mt.tree = rbtree.New[K, V]()
	mt.buf = new(bytes.Buffer)
	mt.enc = newGob[K, V](mt.buf)
	mt.size = 0

	return &mt
}

func (mt *MemTable[K, V]) Size() uint64 {
	return mt.size
}

func (mt *MemTable[K, V]) Insert(key K, val V) {
	mt.tree.Insert(key, val)
	mt.enc.Encode(types.Pair[K, V]{Key: key, Val: val})
	mt.size += uint64(mt.buf.Len())
	mt.buf.Reset()
}

func (mt *MemTable[K, V]) Delete(key K) error {
	val, err := mt.tree.Delete(key)
	if err != nil {
		return err
	}
	mt.enc.Encode(types.Pair[K, V]{Key: key, Val: val})
	mt.size -= uint64(mt.buf.Len())
	mt.buf.Reset()
	return nil
}

func (mt *MemTable[K, V]) Search(key K) (V, bool) {
	return mt.tree.Find(key)
}

func (mt *MemTable[K, V]) Clean() {
	mt.tree = rbtree.New[K, V]()
	mt.buf = new(bytes.Buffer)
	mt.enc = newGob[K, V](mt.buf)
	mt.size = 0
}
