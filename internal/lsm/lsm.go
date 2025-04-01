// Package lsm implements a Log-Structured Merge Tree database engine.
//
// The LSM-Tree is a write-optimized data structure that maintains
// sorted data by first buffering writes in memory and then
// systematically flushing and compacting data to disk.
package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlonMell/grove/internal/lsm/memtable"
	"github.com/AlonMell/grove/internal/lsm/sstable"
	"github.com/AlonMell/grove/internal/lsm/tools"
	"github.com/AlonMell/grove/internal/lsm/wal"
)

// Common errors returned by the database.
var (
	ErrDBClosed    = errors.New("database is closed")
	ErrKeyNotFound = errors.New("key not found")
)

// Config holds configuration options for the LSM-Tree database.
type Config struct {
	// Directory where database files will be stored
	Dir string

	// Maximum size of MemTable in bytes before flushing to disk
	MemTableSize int

	// Number of SSTables to merge during a single compaction
	CompactionFactor int

	// Number of bits per key for Bloom filters (higher = lower false positive rate)
	BloomFilterBits int

	// Whether to sync WAL after each write (safer but slower)
	SyncWrites bool

	// Maximum number of MemTables to keep in memory while flushing
	MaxImmutableMemTables int
}

// DefaultConfig returns a default database configuration.
func DefaultConfig() *Config {
	return &Config{
		Dir:                   "data",
		MemTableSize:          4 * 1024 * 1024, // 4MB
		CompactionFactor:      4,
		BloomFilterBits:       10,
		SyncWrites:            true,
		MaxImmutableMemTables: 2,
	}
}

// DB represents an LSM-Tree database instance.
type DB struct {
	config      *Config
	wal         *wal.WAL
	memTable    *memtable.MemTable
	immutables  []*memtable.MemTable
	sstables    []*sstable.SSTable
	compactChan chan struct{}
	bgCompact   int32 // Atomic flag for background compaction
	mu          sync.RWMutex
	closed      bool
}

// Open opens or creates a new LSM-Tree database at the specified path.
func Open(path string, config *Config) (*DB, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Set database directory
	config.Dir = path

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.Dir, 0o644); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Open WAL
	walPath := filepath.Join(config.Dir, "wal")
	wal, err := wal.NewWAL(walPath, config.SyncWrites)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	db := &DB{
		config:      config,
		wal:         wal,
		memTable:    memtable.NewMemTable(),
		compactChan: make(chan struct{}, 1),
		bgCompact:   0,
	}

	// Load existing SSTables
	if err := db.loadSSTables(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Replay WAL
	if err := db.replayWAL(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	// Start background compaction goroutine
	go db.backgroundCompaction()

	return db, nil
}

// Put stores a key-value pair in the database.
func (db *DB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	// Write to WAL first
	entry := &wal.Entry{
		Operation: wal.OpPut,
		Key:       key,
		Value:     value,
	}

	if err := db.wal.Append(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Add to MemTable
	db.memTable.Put(key, value)

	// Check if MemTable is full and needs to be flushed
	if db.memTable.Size() >= db.config.MemTableSize {
		if err := db.flushMemTableLocked(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	return nil
}

// Get retrieves a value by key from the database.
func (db *DB) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return "", ErrDBClosed
	}

	//foundTombstone := false

	// Check MemTable first
	value, found, isTombstone := db.memTable.GetWithTombstone(key)
	if found {
		if isTombstone {
			return "", ErrKeyNotFound
		}
		return value, nil
	}

	// Check immutable MemTables (newest to oldest)
	for i := len(db.immutables) - 1; i >= 0; i-- {
		value, found, isTombstone := db.immutables[i].GetWithTombstone(key)
		if found {
			if isTombstone {
				return "", ErrKeyNotFound
			}
			return value, nil
		}
	}

	// Check SSTables from newest to oldest
	for i := len(db.sstables) - 1; i >= 0; i-- {
		// Skip SSTable if Bloom Filter suggests key is not present
		if !db.sstables[i].MayContain(key) {
			continue
		}

		value, found, isTombstone, err := db.sstables[i].GetWithTombstone(key)
		if err != nil {
			return "", fmt.Errorf("failed to read from SSTable: %w", err)
		}

		if found {
			if isTombstone {
				return "", ErrKeyNotFound
			}
			return value, nil
		}
	}

	return "", ErrKeyNotFound
}

// Delete removes a key-value pair from the database.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	// Write to WAL first
	entry := &wal.Entry{
		Operation: wal.OpDelete,
		Key:       key,
	}

	if err := db.wal.Append(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Add tombstone to MemTable
	db.memTable.Delete(key)

	// Check if MemTable is full and needs to be flushed
	if db.memTable.Size() >= db.config.MemTableSize {
		if err := db.flushMemTableLocked(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	return nil
}

// Close closes the database, flushing any pending data.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true

	// Signal background compaction to stop
	close(db.compactChan)

	// Flush MemTable if it's not empty
	if db.memTable.Size() > 0 {
		if err := db.flushMemTableLocked(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	// Flush all immutable MemTables
	for len(db.immutables) > 0 {
		if err := db.flushNextImmutableMemTable(); err != nil {
			return fmt.Errorf("failed to flush immutable MemTable: %w", err)
		}
	}

	// Close WAL
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Close all SSTables
	for _, sstable := range db.sstables {
		if err := sstable.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable: %w", err)
		}
	}

	return nil
}

// Compact forces a compaction of the database.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	return db.compaction()
}

// flushMemTableLocked moves the current MemTable to immutables and creates a new MemTable.
// This method assumes the mutex is already locked.
func (db *DB) flushMemTableLocked() error {
	// Move current MemTable to immutables
	db.immutables = append(db.immutables, db.memTable)

	// Create new MemTable
	db.memTable = memtable.NewMemTable()

	// Check if we have too many immutable MemTables
	if len(db.immutables) > db.config.MaxImmutableMemTables {
		if err := db.flushNextImmutableMemTable(); err != nil {
			return fmt.Errorf("failed to flush immutable MemTable: %w", err)
		}
	}

	// Create a new WAL file
	walPath := filepath.Join(db.config.Dir, fmt.Sprintf("wal.%d", time.Now().UnixNano()))
	wal, err := wal.NewWAL(walPath, db.config.SyncWrites)
	if err != nil {
		return fmt.Errorf("failed to create new WAL: %w", err)
	}

	// Close and remove old WAL
	oldWAL := db.wal
	db.wal = wal

	if err := oldWAL.Close(); err != nil {
		return fmt.Errorf("failed to close old WAL: %w", err)
	}

	// Remove old WAL file (optional, could archive instead)
	if err := os.Remove(oldWAL.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove old WAL: %w", err)
	}

	return nil
}

// flushNextImmutableMemTable flushes the oldest immutable MemTable to disk.
func (db *DB) flushNextImmutableMemTable() error {
	if len(db.immutables) == 0 {
		return nil
	}

	// Get oldest immutable MemTable
	immutable := db.immutables[0]
	db.immutables = db.immutables[1:]

	// Create SSTable from immutable MemTable
	sstablePath := filepath.Join(db.config.Dir, fmt.Sprintf("sstable-L0-%d.db", time.Now().UnixNano()))
	sstable, err := sstable.NewSSTable(sstablePath, immutable, 0, db.config.BloomFilterBits)
	if err != nil {
		return fmt.Errorf("failed to create SSTable: %w", err)
	}

	// Add SSTable to list
	db.sstables = append(db.sstables, sstable)

	// Trigger compaction if needed
	if db.shouldCompact() {
		select {
		case db.compactChan <- struct{}{}:
			// Signal sent to compaction goroutine
		default:
			// Channel is full, compaction already scheduled
		}
	}

	return nil
}

// shouldCompact determines if compaction should be triggered.
func (db *DB) shouldCompact() bool {
	// Count SSTables at level 0
	l0Count := 0
	for _, sstable := range db.sstables {
		if sstable.Level == 0 {
			l0Count++
		}
	}

	return l0Count >= db.config.CompactionFactor
}

// replayWAL replays the WAL to reconstruct the MemTable.
func (db *DB) replayWAL() error {
	return db.wal.Replay(func(entry *wal.Entry) error {
		switch entry.Operation {
		case wal.OpPut:
			db.memTable.Put(entry.Key, entry.Value)
		case wal.OpDelete:
			db.memTable.Delete(entry.Key)
		}
		return nil
	})
}

// loadSSTables loads existing SSTables from disk.
func (db *DB) loadSSTables() error {
	// Find all SSTable files in data directory
	pattern := filepath.Join(db.config.Dir, "sstable-*.db")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list SSTable files: %w", err)
	}

	// Sort files by name (which includes timestamp)
	sort.Strings(files)

	// Open each SSTable
	for _, file := range files {
		sstable, err := sstable.OpenSSTable(file)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", file, err)
		}

		db.sstables = append(db.sstables, sstable)
	}

	return nil
}

// backgroundCompaction performs compaction in the background.
func (db *DB) backgroundCompaction() {
	for {
		select {
		case _, ok := <-db.compactChan:
			if !ok {
				// Channel closed, exit goroutine
				return
			}

			// Set compaction flag
			if !atomic.CompareAndSwapInt32(&db.bgCompact, 0, 1) {
				// Another compaction is already running
				continue
			}

			// Run compaction
			db.mu.Lock()
			err := db.compaction()
			db.mu.Unlock()

			// Reset compaction flag
			atomic.StoreInt32(&db.bgCompact, 0)

			if err != nil {
				// Log error, but continue
				fmt.Printf("Background compaction error: %v\n", err)
			}
		}
	}
}

// compaction performs the compaction process.
func (db *DB) compaction() error {
	// Identify SSTables to compact at each level
	levels := make(map[int][]*sstable.SSTable)
	for _, sstable := range db.sstables {
		levels[sstable.Level] = append(levels[sstable.Level], sstable)
	}

	// Start with level 0
	for level := range 10 { // arbitrary upper limit
		tablesToCompact := levels[level]

		// If we don't have enough tables at this level, move on
		if len(tablesToCompact) < db.config.CompactionFactor {
			continue
		}

		// Sort tables by creation time (using the filename)
		sort.Slice(tablesToCompact, func(i, j int) bool {
			return tablesToCompact[i].Path < tablesToCompact[j].Path
		})

		// Select oldest tables for compaction
		selected := tablesToCompact[:db.config.CompactionFactor]

		// Create iterators for each table
		var iterators []tools.Iterator
		for _, table := range selected {
			iterators = append(iterators, table.Iterator())
		}

		// Create merge iterator
		merger := tools.NewMergeIterator(iterators)
		defer merger.Close()

		// Create new SSTable for next level
		nextLevel := level + 1
		sstablePath := filepath.Join(db.config.Dir, fmt.Sprintf("sstable-L%d-%d.db", nextLevel, time.Now().UnixNano()))

		// Create temporary memory table to hold merged data
		mergedTable := memtable.NewMemTable()

		// Process all entries
		for merger.Next() {
			key := merger.Key()
			value := merger.Value()

			if merger.IsTombstone() {
				// If it's a tombstone at the highest level, we can discard it completely
				if level > 0 {
					continue
				}
				// Otherwise, propagate the tombstone
				mergedTable.Delete(key)
			} else {
				mergedTable.Put(key, value)
			}
		}

		// Skip if no data to write
		if mergedTable.Count() == 0 {
			continue
		}

		// Create new SSTable
		newTable, err := sstable.NewSSTable(sstablePath, mergedTable, nextLevel, db.config.BloomFilterBits)
		if err != nil {
			return fmt.Errorf("failed to create compacted SSTable: %w", err)
		}

		// Update SSTables list
		var updatedTables []*sstable.SSTable
		for _, table := range db.sstables {
			inCompaction := false
			for _, selected := range selected {
				if table == selected {
					inCompaction = true
					break
				}
			}

			if !inCompaction {
				updatedTables = append(updatedTables, table)
			} else {
				// Close and remove files for compacted tables
				table.Close()
				os.Remove(table.Path)
			}
		}

		// Add new table
		updatedTables = append(updatedTables, newTable)
		db.sstables = updatedTables
	}

	return nil
}
