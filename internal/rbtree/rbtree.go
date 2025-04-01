// Package rbtree implements a Red-Black Tree data structure
// with insertion, deletion and search operations.
//
// Red-Black Tree is a self-balancing binary search tree that guarantees
// O(log n) time complexity for basic operations.
package rbtree

import (
	"errors"
)

type color bool

const (
	red   color = true
	black color = false
)

type Node struct {
	Key                 string
	Val                 string
	color               color
	left, right, parent *Node
}

// RBTree represents a Red-Black Tree instance.
// Use New() to create a new tree instance.
type RBTree struct {
	root    *Node
	nilNode *Node // Sentinel node
}

// New creates and returns a new empty Red-Black Tree.
func New() *RBTree {
	nilNode := &Node{color: black}
	return &RBTree{
		root:    nilNode,
		nilNode: nilNode,
	}
}

// Insert adds a new key to the tree while maintaining
// Red-Black Tree properties.
func (t *RBTree) Insert(key string, val string) {
	newNode := &Node{
		Key:    key,
		Val:    val,
		color:  red,
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

func (t *RBTree) fixInsert(x *Node) {
	for x.parent.color == red {
		if x.parent == x.parent.parent.left {
			y := x.parent.parent.right
			if y.color == red {
				x.parent.color = black
				y.color = black
				x.parent.parent.color = red
				x = x.parent.parent
			} else {
				if x == x.parent.right {
					x = x.parent
					t.leftRotate(x)
				}
				x.parent.color = black
				x.parent.parent.color = red
				t.rightRotate(x.parent.parent)
			}
		} else {
			y := x.parent.parent.left
			if y.color == red {
				x.parent.color = black
				y.color = black
				x.parent.parent.color = red
				x = x.parent.parent
			} else {
				if x == x.parent.left {
					x = x.parent
					t.rightRotate(x)
				}
				x.parent.color = black
				x.parent.parent.color = red
				t.leftRotate(x.parent.parent)
			}
		}
	}
	t.root.color = black
}

func (t *RBTree) leftRotate(x *Node) {
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

func (t *RBTree) rightRotate(y *Node) {
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
func (t *RBTree) Delete(key string) (string, error) {
	z := t.findNode(key)
	var zero string

	if z == t.nilNode {
		return zero, errors.New("Key not found")
	}

	var x *Node
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

	if yOriginalColor == black {
		t.fixDelete(x)
	}

	return z.Val, nil
}

func (t *RBTree) transplant(u, v *Node) {
	if u.parent == t.nilNode {
		t.root = v
	} else if u == u.parent.left {
		u.parent.left = v
	} else {
		u.parent.right = v
	}
	v.parent = u.parent
}

func (t *RBTree) minimum(x *Node) *Node {
	for x.left != t.nilNode {
		x = x.left
	}
	return x
}

func (t *RBTree) fixDelete(x *Node) {
	for x != t.root && x.color == black {
		if x == x.parent.left {
			w := x.parent.right
			if w.color == red {
				w.color = black
				x.parent.color = red
				t.leftRotate(x.parent)
				w = x.parent.right
			}
			if w.left.color == black && w.right.color == black {
				w.color = red
				x = x.parent
			} else {
				if w.right.color == black {
					w.left.color = black
					w.color = red
					t.rightRotate(w)
					w = x.parent.right
				}
				w.color = x.parent.color
				x.parent.color = black
				w.right.color = black
				t.leftRotate(x.parent)
				x = t.root
			}
		} else {
			w := x.parent.left
			if w.color == red {
				w.color = black
				x.parent.color = red
				t.rightRotate(x.parent)
				w = x.parent.left
			}
			if w.right.color == black && w.left.color == black {
				w.color = red
				x = x.parent
			} else {
				if w.left.color == black {
					w.right.color = black
					w.color = red
					t.leftRotate(w)
					w = x.parent.left
				}
				w.color = x.parent.color
				x.parent.color = black
				w.left.color = black
				t.rightRotate(x.parent)
				x = t.root
			}
		}
	}
	x.color = black
}

func (t *RBTree) Find(key string) (string, bool) {
	node := t.findNode(key)
	if node == t.nilNode {
		return t.nilNode.Val, false
	}
	return node.Val, true
}

func (t *RBTree) findNode(key string) *Node {
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
func (t *RBTree) Exists(key string) bool {
	return t.findNode(key) != t.nilNode
}

// VerifyTreeProperties validates Red-Black Tree invariants:
// 1. Root is always black
// 2. Red nodes must have black children
// 3. All paths from node to leaves have same black node count
// Returns true if all properties are satisfied
func (t *RBTree) VerifyTreeProperties() bool {
	if t.root.color != black {
		return false
	}

	_, ok := t.checkSubtreeProperties(t.root)
	return ok
}

func (t *RBTree) checkSubtreeProperties(node *Node) (int, bool) {
	if node == t.nilNode {
		return 1, true
	}

	if node.color == red && (node.left.color == red || node.right.color == red) {
		return 0, false
	}

	leftCount, leftOk := t.checkSubtreeProperties(node.left)
	rightCount, rightOk := t.checkSubtreeProperties(node.right)

	if !leftOk || !rightOk || leftCount != rightCount {
		return 0, false
	}

	if node.color == black {
		leftCount++
	}
	return leftCount, true
}
