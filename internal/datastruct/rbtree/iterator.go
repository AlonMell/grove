package rbtree

import "cmp"

type InOrderIterator[T cmp.Ordered, K any] struct {
	current *Node[T, K]
	stack   []*Node[T, K]
	nilNode *Node[T, K]
}

func (t *RBTree[T, K]) Iterator() *InOrderIterator[T, K] {
	return &InOrderIterator[T, K]{
		current: t.root,
		stack:   make([]*Node[T, K], 0),
		nilNode: t.nilNode,
	}
}

func (it *InOrderIterator[T, K]) Next() bool {
	for it.current != it.nilNode || len(it.stack) > 0 {
		for it.current != it.nilNode {
			it.stack = append(it.stack, it.current)
			it.current = it.current.left
		}
		it.current = it.stack[len(it.stack)-1]
		it.stack = it.stack[:len(it.stack)-1]
		return true
	}
	return false
}

func (it *InOrderIterator[T, K]) Key() T {
	return it.current.key
}

func (it *InOrderIterator[T, K]) Value() K {
	return it.current.value
}
