// Package lsm implements a Log-Structured Merge Tree database.
package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/AlonMell/grove/internal/lsm/memtable"
	"github.com/AlonMell/grove/internal/lsm/sstable/bloom"
	"github.com/AlonMell/grove/internal/lsm/tools"
)

// SSTable file format:
// - Header: Version, Count, IndexOffset, BloomOffset, BloomFilterBits
// - Data: Sequence of (KeyLength, Key, ValueLength, Value)
// - Index: Sequence of (KeyLength, Key, Offset)
// - Bloom Filter: Serialized bloom filter

// SSTableHeader contains metadata about the SSTable.
type SSTableHeader struct {
	Version         uint32
	Count           uint32
	IndexOffset     int64
	BloomOffset     int64
	BloomFilterBits uint32
}

// SSTable represents a Sorted String Table file on disk.
type SSTable struct {
	Path  string
	file  *os.File
	bloom *bloom.BloomFilter
	index map[string]int64 // Maps keys to file offsets
	Level int
	mu    sync.RWMutex
}

// NewSSTable creates a new SSTable from a MemTable.
func NewSSTable(path string, memTable *memtable.MemTable, level int, bloomBits int) (*SSTable, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer func() {
		if err != nil {
			file.Close()
			os.Remove(path)
		}
	}()

	// Create Bloom filter
	count := memTable.Count()
	bloom := bloom.New(count, bloomBits)

	// Create index
	index := make(map[string]int64)

	// Write header placeholder (we'll fill it in at the end)
	header := SSTableHeader{
		Version: 1,
		Count:   uint32(count),
	}

	headerSize := 24 // Size of header struct in bytes
	headerBytes := make([]byte, headerSize)
	if _, err := file.Write(headerBytes); err != nil {
		return nil, fmt.Errorf("failed to write header placeholder: %w", err)
	}

	// Write data entries
	dataOffset := int64(headerSize)
	writer := bufio.NewWriter(file)

	// Process entries in sorted order
	memTable.ForEach(func(key, value string) bool {
		// Add key to Bloom filter
		bloom.Add(key)

		// Store key offset in index
		index[key] = dataOffset

		// Write key length and key
		keyLen := uint32(len(key))
		if err := binary.Write(writer, binary.LittleEndian, keyLen); err != nil {
			return false
		}
		if _, err := writer.Write([]byte(key)); err != nil {
			return false
		}

		// Write value length and value
		valueLen := uint32(len(value))
		if err := binary.Write(writer, binary.LittleEndian, valueLen); err != nil {
			return false
		}
		if _, err := writer.Write([]byte(value)); err != nil {
			return false
		}

		// Update offset
		dataOffset += 8 + int64(keyLen) + int64(valueLen) // 8 bytes for lengths

		return true
	})

	// Flush data
	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush data: %w", err)
	}

	// Write index
	indexOffset := dataOffset

	// Sort keys for sequential index
	var keys []string
	for key := range index {
		keys = append(keys, key)
	}

	// Write sorted index
	for _, key := range keys {
		offset := index[key]

		// Write key length and key
		keyLen := uint32(len(key))
		if err := binary.Write(file, binary.LittleEndian, keyLen); err != nil {
			return nil, fmt.Errorf("failed to write index key length: %w", err)
		}
		if _, err := file.Write([]byte(key)); err != nil {
			return nil, fmt.Errorf("failed to write index key: %w", err)
		}

		// Write offset
		if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
			return nil, fmt.Errorf("failed to write index offset: %w", err)
		}
	}

	// Write Bloom filter
	bloomOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to seek current position: %w", err)
	}
	bloomBytes := bloom.Encode()
	if _, err := file.Write(bloomBytes); err != nil {
		return nil, fmt.Errorf("failed to write Bloom filter: %w", err)
	}

	// Update and write header
	header.IndexOffset = indexOffset
	header.BloomOffset = bloomOffset
	header.BloomFilterBits = uint32(len(bloomBytes) * 8)

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Sync file to ensure durability
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync file: %w", err)
	}

	// Create SSTable instance
	sstable := &SSTable{
		Path:  path,
		file:  file,
		bloom: bloom,
		index: index,
		Level: level,
	}

	return sstable, nil
}

// OpenSSTable opens an existing SSTable from disk.
func OpenSSTable(path string) (*SSTable, error) {
	// Open file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}

	// Read header
	var header SSTableHeader
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Extract level from path
	level := 0
	re := regexp.MustCompile(`sstable-L(\d+)-`)
	matches := re.FindStringSubmatch(filepath.Base(path))
	if len(matches) >= 2 {
		level, _ = strconv.Atoi(matches[1])
	}

	// Read index
	if _, err := file.Seek(header.IndexOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to index: %w", err)
	}

	index := make(map[string]int64)
	for i := uint32(0); i < header.Count; i++ {
		// Read key length and key
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index key length: %w", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index key: %w", err)
		}

		// Read offset
		var offset int64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read index offset: %w", err)
		}

		index[string(keyBytes)] = offset
	}

	// Read Bloom filter
	if _, err := file.Seek(header.BloomOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to Bloom filter: %w", err)
	}

	bloomBytes := make([]byte, header.BloomFilterBits/8)
	if _, err := io.ReadFull(file, bloomBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read Bloom filter: %w", err)
	}

	bloom, err := bloom.Decode(bloomBytes, int(header.Count))
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to decode Bloom filter: %w", err)
	}

	// Create SSTable
	sstable := &SSTable{
		Path:  path,
		file:  file,
		bloom: bloom,
		index: index,
		Level: level,
	}

	return sstable, nil
}

// Get retrieves a value by key from the SSTable.
// Returns value, found flag, and error.
func (s *SSTable) Get(key string) (string, bool, error) {
	value, found, isTombstone, err := s.GetWithTombstone(key)
	if err != nil {
		return "", false, err
	}

	if !found || isTombstone {
		return "", false, nil
	}

	return value, true, nil
}

// GetWithTombstone retrieves a value by key from the SSTable with tombstone information.
// Returns value, found flag, tombstone flag, and error.
func (s *SSTable) GetWithTombstone(key string) (string, bool, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check Bloom filter first
	if !s.bloom.MayContain(key) {
		return "", false, false, nil
	}

	// Check index
	offset, exists := s.index[key]
	if !exists {
		return "", false, false, nil
	}

	// Seek to data position
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return "", false, false, fmt.Errorf("failed to seek to data: %w", err)
	}

	// Read key length and key
	var keyLen uint32
	if err := binary.Read(s.file, binary.LittleEndian, &keyLen); err != nil {
		return "", false, false, fmt.Errorf("failed to read key length: %w", err)
	}

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(s.file, keyBytes); err != nil {
		return "", false, false, fmt.Errorf("failed to read key: %w", err)
	}

	// Verify key (in case of hash collision in Bloom filter)
	if string(keyBytes) != key {
		return "", false, false, nil
	}

	// Read value length and value
	var valueLen uint32
	if err := binary.Read(s.file, binary.LittleEndian, &valueLen); err != nil {
		return "", false, false, fmt.Errorf("failed to read value length: %w", err)
	}

	valueBytes := make([]byte, valueLen)
	if _, err := io.ReadFull(s.file, valueBytes); err != nil {
		return "", false, false, fmt.Errorf("failed to read value: %w", err)
	}

	value := string(valueBytes)

	// Check for tombstone
	if value == memtable.Tombstone {
		return "", true, true, nil
	}

	return value, true, false, nil
}

// MayContain checks if the SSTable might contain a key using Bloom Filter.
func (s *SSTable) MayContain(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.bloom.MayContain(key)
}

// Close closes the SSTable.
func (s *SSTable) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
		s.file = nil
	}

	return nil
}

// Iterator returns an iterator over the SSTable entries.
func (s *SSTable) Iterator() tools.Iterator {
	return NewSSTableIterator(s)
}

// SSTableIterator provides iteration over SSTable entries.
type SSTableIterator struct {
	sstable *SSTable
	file    *os.File
	reader  *bufio.Reader
	current struct {
		key   string
		value string
	}
	mu     sync.Mutex
	closed bool
}

// NewSSTableIterator creates a new iterator for an SSTable.
func NewSSTableIterator(sstable *SSTable) *SSTableIterator {
	sstable.mu.RLock() // Will be released on Close()

	// Open a separate file handle for iteration
	file, err := os.Open(sstable.Path)
	if err != nil {
		sstable.mu.RUnlock()
		return nil
	}

	// Skip header to get to data section
	headerSize := 24
	file.Seek(int64(headerSize), io.SeekStart)

	return &SSTableIterator{
		sstable: sstable,
		file:    file,
		reader:  bufio.NewReader(file),
	}
}

// Next advances the iterator to the next entry.
func (it *SSTableIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	// Read key length
	var keyLen uint32
	if err := binary.Read(it.reader, binary.LittleEndian, &keyLen); err != nil {
		if err == io.EOF {
			it.Close()
			return false
		}
		return false
	}

	// Read key
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(it.reader, keyBytes); err != nil {
		it.Close()
		return false
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(it.reader, binary.LittleEndian, &valueLen); err != nil {
		it.Close()
		return false
	}

	// Read value
	valueBytes := make([]byte, valueLen)
	if _, err := io.ReadFull(it.reader, valueBytes); err != nil {
		it.Close()
		return false
	}

	it.current.key = string(keyBytes)
	it.current.value = string(valueBytes)
	return true
}

// Key returns the current key.
func (it *SSTableIterator) Key() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.current.key
}

// Value returns the current value.
func (it *SSTableIterator) Value() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.current.value
}

// IsTombstone checks if the current entry is a tombstone.
func (it *SSTableIterator) IsTombstone() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.current.value == memtable.Tombstone
}

// Close releases resources held by the iterator.
func (it *SSTableIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.closed {
		it.closed = true
		if it.file != nil {
			it.file.Close()
			it.file = nil
		}
		it.sstable.mu.RUnlock() // Release lock acquired in NewSSTableIterator
	}

	return nil
}
