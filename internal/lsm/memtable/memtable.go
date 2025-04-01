// Package lsm implements a Log-Structured Merge Tree database.
package memtable

import (
	"sync"

	"github.com/AlonMell/grove/internal/rbtree"
)

// Tombstone is a special marker for deleted keys
const Tombstone = "__TOMBSTONE__"

// MemTable represents an in-memory table using RBTree.
type MemTable struct {
	tree       *rbtree.RBTree
	tombstones map[string]struct{} // Хранит удаленные ключи
	size       int                 // Approximate size in bytes
	count      int                 // Number of entries
	mu         sync.RWMutex
}

// NewMemTable creates a new MemTable instance.
func NewMemTable() *MemTable {
	return &MemTable{
		tree:       rbtree.New(),
		tombstones: make(map[string]struct{}),
		size:       0,
		count:      0,
	}
}

// Put adds or updates a key-value pair in the MemTable.
func (m *MemTable) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if key already exists
	oldVal, exists := m.tree.Find(key)

	// Calculate size delta
	sizeDelta := len(key) + len(value)
	if exists {
		sizeDelta -= len(oldVal)
	} else {
		m.count++
	}

	// Update tree
	m.tree.Insert(key, value)

	// Update size
	m.size += sizeDelta
}

// Get retrieves a value by key from the MemTable.
func (m *MemTable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.tree.Find(key)
	if !exists {
		return "", false
	}

	// Check for tombstone
	if value == Tombstone {
		return "", false
	}

	return value, true
}

// GetWithTombstone retrieves a value by key from the MemTable with tombstone information.
// Returns value, found flag, tombstone flag.
func (m *MemTable) GetWithTombstone(key string) (string, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.tree.Find(key)
	if !exists {
		return "", false, false
	}

	if value == Tombstone {
		return "", true, true
	}

	return value, true, false
}

// HasTombstone checks if a key has a tombstone marker in the MemTable.
func (m *MemTable) HasTombstone(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.tree.Find(key)
	return exists && value == Tombstone
}

// Delete marks a key as deleted in the MemTable by adding a tombstone.
func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if key already exists
	oldVal, exists := m.tree.Find(key)

	// Calculate size delta
	sizeDelta := len(key) + len(Tombstone)
	if exists {
		sizeDelta -= len(oldVal)
	} else {
		m.count++
	}

	// Insert tombstone
	m.tree.Insert(key, Tombstone)

	// Update size
	m.size += sizeDelta
}

// Size returns the approximate size of the MemTable in bytes.
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size
}

// Count returns the number of entries in the MemTable.
func (m *MemTable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.count
}

// ForEach iterates over all entries in the MemTable in sorted order.
func (m *MemTable) ForEach(fn func(key, value string) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for key, value := range m.tree.InOrder() {
		if !fn(key, value) {
			break
		}
	}
}

// Iterator returns an iterator over the entries in the MemTable.
type MemTableIterator struct {
	table     *MemTable
	entries   []struct{ key, value string }
	currIndex int
	current   struct {
		key   string
		value string
	}
	mu     sync.Mutex
	closed bool
}

// Iterator creates a new MemTableIterator.
func (m *MemTable) Iterator() *MemTableIterator {
	m.mu.RLock() // Will be released when Close() is called

	// Collect all entries from the RBTree using InOrder iterator
	var entries []struct{ key, value string }

	// Use the iter.Seq2 generator correctly
	iterFn := m.tree.InOrder()
	iterFn(func(key, value string) bool {
		entries = append(entries, struct{ key, value string }{key, value})
		return true // Continue iteration
	})

	return &MemTableIterator{
		table:     m,
		entries:   entries,
		currIndex: -1, // Start before the first element
	}
}

// Next advances the iterator to the next entry.
func (it *MemTableIterator) Next() bool {
	if it.closed || it.currIndex >= len(it.entries)-1 {
		return false
	}

	it.currIndex++
	entry := it.entries[it.currIndex]
	it.current.key = entry.key
	it.current.value = entry.value
	return true
}

// Key returns the current key.
func (it *MemTableIterator) Key() string {
	return it.current.key
}

// Value returns the current value.
func (it *MemTableIterator) Value() string {
	return it.current.value
}

// IsTombstone checks if the current entry is a tombstone.
func (it *MemTableIterator) IsTombstone() bool {
	return it.current.value == Tombstone
}

// Close releases resources held by the iterator.
func (it *MemTableIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.closed {
		it.closed = true
		it.table.mu.RUnlock() // Release lock acquired in Iterator()
	}

	return nil
}
