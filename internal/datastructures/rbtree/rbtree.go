// Package rbtree implements a Red-Black Tree data structure
// with insertion, deletion and search operations.
//
// Red-Black Tree is a self-balancing binary search tree that guarantees
// O(log n) time complexity for basic operations.
package rbtree

type Color bool

const (
	Red   Color = true
	Black Color = false
)

type Node struct {
	key    int
	color  Color
	left   *Node
	right  *Node
	parent *Node
}

// RBTree represents a Red-Black Tree instance.
// Use New() to create a new tree instance.
type RBTree struct {
	root    *Node
	nilNode *Node // Sentinel node
}

// New creates and returns a new empty Red-Black Tree.
func NewRBTree() *RBTree {
	nilNode := &Node{color: Black}
	return &RBTree{
		root:    nilNode,
		nilNode: nilNode,
	}
}

// Insert adds a new key to the tree while maintaining
// Red-Black Tree properties.
func (t *RBTree) Insert(key int) {
	newNode := &Node{
		key:    key,
		color:  Red,
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
	}

	parent := t.nilNode
	current := t.root

	for current != t.nilNode {
		parent = current
		if newNode.key < current.key {
			current = current.left
		} else {
			current = current.right
		}
	}

	newNode.parent = parent
	if parent == t.nilNode {
		t.root = newNode
	} else if newNode.key < parent.key {
		parent.left = newNode
	} else {
		parent.right = newNode
	}

	t.fixInsert(newNode)
}

func (t *RBTree) fixInsert(x *Node) {
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

func (t *RBTree) leftRotate(x *Node) {
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
func (t *RBTree) Delete(key int) {
	z := t.findNode(key)
	if z == t.nilNode {
		return
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

	if yOriginalColor == Black {
		t.fixDelete(x)
	}
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

func (t *RBTree) findNode(key int) *Node {
	current := t.root
	for current != t.nilNode {
		if key == current.key {
			return current
		} else if key < current.key {
			current = current.left
		} else {
			current = current.right
		}
	}
	return t.nilNode
}

// Exists checks if a key is present in the tree.
func (t *RBTree) Exists(key int) bool {
	return t.findNode(key) != t.nilNode
}
