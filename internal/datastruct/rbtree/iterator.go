package rbtree

import (
	"iter"
)

func (t *RBTree[K, V]) InOrder() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		stack := []*Node[K, V]{}
		current := t.root
		for current != t.nilNode || len(stack) > 0 {

			for current != t.nilNode {
				stack = append(stack, current)
				current = current.left
			}

			current = stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if !yield(current.Key, current.Val) {
				return
			}

			current = current.right
		}
	}
}
