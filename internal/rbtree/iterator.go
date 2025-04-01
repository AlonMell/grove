package rbtree

import (
	"iter"
)

func (t *RBTree) InOrder() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		stack := []*Node{}
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
