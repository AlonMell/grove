// Package rbtree implements a Red-Black Tree data structure
// with insertion, deletion and search operations.
//
// Red-Black Tree is a self-balancing binary search tree that guarantees
// O(log n) time complexity for basic operations.
package rbtree

import (
	"cmp"
	"errors"

	"github.com/grove/internal/datastruct/types"
)

type Color bool

const (
	Red   Color = true
	Black Color = false
)

type Node[K cmp.Ordered, V any] struct {
	types.Pair[K, V]
	color               Color
	left, right, parent *Node[K, V]
}

// RBTree represents a Red-Black Tree instance.
// Use New() to create a new tree instance.
type RBTree[K cmp.Ordered, V any] struct {
	root    *Node[K, V]
	nilNode *Node[K, V] // Sentinel node
}

// New creates and returns a new empty Red-Black Tree.
func New[K cmp.Ordered, V any]() *RBTree[K, V] {
	nilNode := &Node[K, V]{color: Black}
	return &RBTree[K, V]{
		root:    nilNode,
		nilNode: nilNode,
	}
}

// Insert adds a new key to the tree while maintaining
// Red-Black Tree properties.
func (t *RBTree[K, V]) Insert(key K, value V) {
	newNode := &Node[K, V]{
		Pair: types.Pair[K, V]{
			Key: key,
			Val: value,
		},
		color:  Red,
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
	}

	parent := t.nilNode
	current := t.root

	for current != t.nilNode {
		parent = current
		if newNode.Key < current.Key {
			current = current.left
		} else {
			current = current.right
		}
	}

	newNode.parent = parent
	if parent == t.nilNode {
		t.root = newNode
	} else if newNode.Key < parent.Key {
		parent.left = newNode
	} else {
		parent.right = newNode
	}

	t.fixInsert(newNode)
}

func (t *RBTree[K, V]) fixInsert(x *Node[K, V]) {
	for x.parent.color == Red {
		if x.parent == x.parent.parent.left {
			y := x.parent.parent.right
			if y.color == Red {
				x.parent.color = Black
				y.color = Black
				x.parent.parent.color = Red
				x = x.parent.parent
			} else {
				if x == x.parent.right {
					x = x.parent
					t.leftRotate(x)
				}
				x.parent.color = Black
				x.parent.parent.color = Red
				t.rightRotate(x.parent.parent)
			}
		} else {
			y := x.parent.parent.left
			if y.color == Red {
				x.parent.color = Black
				y.color = Black
				x.parent.parent.color = Red
				x = x.parent.parent
			} else {
				if x == x.parent.left {
					x = x.parent
					t.rightRotate(x)
				}
				x.parent.color = Black
				x.parent.parent.color = Red
				t.leftRotate(x.parent.parent)
			}
		}
	}
	t.root.color = Black
}

func (t *RBTree[K, V]) leftRotate(x *Node[K, V]) {
	/*
		Left rotation around node x:
			    Before:               After:
		          P                    P
		          |                    |
		          x                    y
		         / \                  / \
		        A   y       →        x   C
		           / \              / \
		          B   C            A   B
	*/
	y := x.right
	x.right = y.left
	if y.left != t.nilNode {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == t.nilNode {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

func (t *RBTree[K, V]) rightRotate(y *Node[K, V]) {
	/*
		Right rotation around node y:
		    Before:               After:
		       P                    P
		       |                    |
		       y                    x
		      / \                  / \
		     x   C       →        A   y
		    / \                      / \
		   A   B                    B   C
	*/
	x := y.left
	y.left = x.right
	if x.right != t.nilNode {
		x.right.parent = y
	}
	x.parent = y.parent
	if y.parent == t.nilNode {
		t.root = x
	} else if y == y.parent.right {
		y.parent.right = x
	} else {
		y.parent.left = x
	}
	x.right = y
	y.parent = x
}

// Delete removes a key from the tree while maintaining
// Red-Black Tree properties. If key doesn't exist,
// the operation is a no-op.
func (t *RBTree[K, V]) Delete(key K) (V, error) {
	z := t.findNode(key)
	var zero V
	if z == t.nilNode {
		return zero, errors.New("Key not found")
	}

	var x *Node[K, V]
	y := z
	yOriginalColor := y.color

	if z.left == t.nilNode {
		x = z.right
		t.transplant(z, z.right)
	} else if z.right == t.nilNode {
		x = z.left
		t.transplant(z, z.left)
	} else {
		y = t.minimum(z.right)
		yOriginalColor = y.color
		x = y.right
		if y.parent == z {
			x.parent = y
		} else {
			t.transplant(y, y.right)
			y.right = z.right
			y.right.parent = y
		}
		t.transplant(z, y)
		y.left = z.left
		y.left.parent = y
		y.color = z.color
	}

	if yOriginalColor == Black {
		t.fixDelete(x)
	}

	return z.Val, nil
}

func (t *RBTree[K, V]) transplant(u, v *Node[K, V]) {
	if u.parent == t.nilNode {
		t.root = v
	} else if u == u.parent.left {
		u.parent.left = v
	} else {
		u.parent.right = v
	}
	v.parent = u.parent
}

func (t *RBTree[K, V]) minimum(x *Node[K, V]) *Node[K, V] {
	for x.left != t.nilNode {
		x = x.left
	}
	return x
}

func (t *RBTree[K, V]) fixDelete(x *Node[K, V]) {
	for x != t.root && x.color == Black {
		if x == x.parent.left {
			w := x.parent.right
			if w.color == Red {
				w.color = Black
				x.parent.color = Red
				t.leftRotate(x.parent)
				w = x.parent.right
			}
			if w.left.color == Black && w.right.color == Black {
				w.color = Red
				x = x.parent
			} else {
				if w.right.color == Black {
					w.left.color = Black
					w.color = Red
					t.rightRotate(w)
					w = x.parent.right
				}
				w.color = x.parent.color
				x.parent.color = Black
				w.right.color = Black
				t.leftRotate(x.parent)
				x = t.root
			}
		} else {
			w := x.parent.left
			if w.color == Red {
				w.color = Black
				x.parent.color = Red
				t.rightRotate(x.parent)
				w = x.parent.left
			}
			if w.right.color == Black && w.left.color == Black {
				w.color = Red
				x = x.parent
			} else {
				if w.left.color == Black {
					w.right.color = Black
					w.color = Red
					t.leftRotate(w)
					w = x.parent.left
				}
				w.color = x.parent.color
				x.parent.color = Black
				w.left.color = Black
				t.rightRotate(x.parent)
				x = t.root
			}
		}
	}
	x.color = Black
}

func (t *RBTree[K, V]) Find(key K) (V, bool) {
	node := t.findNode(key)
	if node == t.nilNode {
		return t.nilNode.Val, false
	}
	return node.Val, true
}

func (t *RBTree[K, V]) findNode(key K) *Node[K, V] {
	current := t.root
	for current != t.nilNode {
		if key == current.Key {
			return current
		} else if key < current.Key {
			current = current.left
		} else {
			current = current.right
		}
	}
	return t.nilNode
}

// Exists checks if a key is present in the tree.
func (t *RBTree[K, V]) Exists(key K) bool {
	return t.findNode(key) != t.nilNode
}

// VerifyTreeProperties validates Red-Black Tree invariants:
// 1. Root is always black
// 2. Red nodes must have black children
// 3. All paths from node to leaves have same black node count
// Returns true if all properties are satisfied
func (t *RBTree[K, V]) VerifyTreeProperties() bool {
	if t.root.color != Black {
		return false
	}

	_, ok := t.checkSubtreeProperties(t.root)
	return ok
}

func (t *RBTree[K, V]) checkSubtreeProperties(node *Node[K, V]) (int, bool) {
	if node == t.nilNode {
		return 1, true
	}

	if node.color == Red && (node.left.color == Red || node.right.color == Red) {
		return 0, false
	}

	leftCount, leftOk := t.checkSubtreeProperties(node.left)
	rightCount, rightOk := t.checkSubtreeProperties(node.right)

	if !leftOk || !rightOk || leftCount != rightCount {
		return 0, false
	}

	if node.color == Black {
		leftCount++
	}
	return leftCount, true
}
