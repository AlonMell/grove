package types

import "cmp"

type Pair[K cmp.Ordered, V any] struct {
	Key K
	Val V
}
