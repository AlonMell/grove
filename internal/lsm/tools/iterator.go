// Package lsm implements a Log-Structured Merge Tree database.
package tools

import (
	"container/heap"

	"github.com/AlonMell/grove/internal/lsm/memtable"
)

// Iterator is the common interface for all iterators in the LSM-Tree.
type Iterator interface {
	// Next advances the iterator to the next key-value pair.
	// Returns false when no more items exist or when an error occurs.
	Next() bool

	// Key returns the current key.
	Key() string

	// Value returns the current value.
	Value() string

	// IsTombstone returns true if the current entry is a tombstone marker.
	IsTombstone() bool

	// Close releases resources associated with the iterator.
	Close() error
}

// Item represents an element in the priority queue used by MergeIterator.
type Item struct {
	iterator Iterator
	key      string
	value    string
	index    int // Used by heap.Interface
}

// priorityQueue implements heap.Interface and holds Items.
type priorityQueue []*Item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// We want to pop items with the smallest key first
	return pq[i].key < pq[j].key
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// MergeIterator merges multiple iterators in sorted order.
// It handles duplicate keys by always preferring the value from
// the iterator that appears earlier in the iterators list (newer data).
type MergeIterator struct {
	iterators []Iterator
	pq        priorityQueue
	current   *Item
	skipList  map[string]struct{} // For tracking keys we've already returned
}

// NewMergeIterator creates a new MergeIterator from multiple iterators.
// The first iterator in the list has the highest priority (newest data).
func NewMergeIterator(iterators []Iterator) *MergeIterator {
	m := &MergeIterator{
		iterators: iterators,
		skipList:  make(map[string]struct{}),
	}

	// Initialize the priority queue
	m.pq = make(priorityQueue, 0, len(iterators))

	// Add the first item from each iterator to the priority queue
	for _, it := range iterators {
		if it.Next() {
			item := &Item{
				iterator: it,
				key:      it.Key(),
				value:    it.Value(),
			}
			heap.Push(&m.pq, item)
		}
	}

	return m
}

// Next advances the iterator to the next key.
func (m *MergeIterator) Next() bool {
	// Skip keys that we've already processed
	for m.pq.Len() > 0 {
		item := heap.Pop(&m.pq).(*Item)
		key := item.key

		// Skip if we've already seen this key
		if _, ok := m.skipList[key]; ok {
			// Advance this iterator and push it back if it has more data
			if item.iterator.Next() {
				item.key = item.iterator.Key()
				item.value = item.iterator.Value()
				heap.Push(&m.pq, item)
			}
			continue
		}

		// Mark this key as seen
		m.skipList[key] = struct{}{}

		// Save current item
		m.current = item

		// Advance this iterator and push it back if it has more data
		if item.iterator.Next() {
			nextItem := &Item{
				iterator: item.iterator,
				key:      item.iterator.Key(),
				value:    item.iterator.Value(),
			}
			heap.Push(&m.pq, nextItem)
		}

		return true
	}

	m.current = nil
	return false
}

// Key returns the current key.
func (m *MergeIterator) Key() string {
	if m.current == nil {
		return ""
	}
	return m.current.key
}

// Value returns the current value.
func (m *MergeIterator) Value() string {
	if m.current == nil {
		return ""
	}
	return m.current.value
}

// IsTombstone returns whether the current entry is a tombstone.
func (m *MergeIterator) IsTombstone() bool {
	if m.current == nil {
		return false
	}
	return m.current.value == memtable.Tombstone
}

// Close closes all iterators.
func (m *MergeIterator) Close() error {
	var err error
	for _, it := range m.iterators {
		if closeErr := it.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// KeyRange represents a range of keys.
type KeyRange struct {
	Start string
	End   string
}

// RangeIterator wraps an Iterator and filters keys to a specific range.
type RangeIterator struct {
	Iterator
	rang      KeyRange
	exhausted bool
}

// NewRangeIterator creates a new range-limited iterator.
func NewRangeIterator(it Iterator, rang KeyRange) *RangeIterator {
	return &RangeIterator{
		Iterator: it,
		rang:     rang,
	}
}

// Next advances to the next key in the range.
func (r *RangeIterator) Next() bool {
	if r.exhausted {
		return false
	}

	// Advance until we find a key in range or run out of keys
	for r.Iterator.Next() {
		key := r.Key()

		// Stop if we've passed the end of the range
		if r.rang.End != "" && key > r.rang.End {
			r.exhausted = true
			return false
		}

		// Skip keys before the start of the range
		if r.rang.Start != "" && key < r.rang.Start {
			continue
		}

		// Key is in range
		return true
	}

	// No more keys
	r.exhausted = true
	return false
}
