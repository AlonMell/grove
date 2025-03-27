package store

import (
	"cmp"
	"errors"

	"github.com/grove/internal/datastruct/rbtree"
)

type LSMTree[K cmp.Ordered, V any] struct {
	memTable *rbtree.RBTree[K, V]
}

func New[K cmp.Ordered, V any]() *LSMTree[K, V] {
	var lt LSMTree[K, V]
	lt.memTable = rbtree.New[K, V]()
	return &lt
}

func (lt *LSMTree[K, V]) Set(key K, val V) error {
	lt.memTable.Insert(key, val)
	return nil
}

func (lt *LSMTree[K, V]) Get(key K) (V, error) {
	var zero V
	if val, exist := lt.memTable.Find(key); exist {
		return val, nil
	}
	return zero, errors.New("Key not found")
}
