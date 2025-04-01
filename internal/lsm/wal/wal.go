// Package lsm implements a Log-Structured Merge Tree database.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Operation types for WAL entries
const (
	OpPut    byte = 0
	OpDelete byte = 1
)

// Entry represents a single WAL entry.
type Entry struct {
	Operation byte // 0 = Put, 1 = Delete
	Key       string
	Value     string
}

// WAL represents a Write-Ahead Log for the LSM-Tree database.
type WAL struct {
	Path       string
	file       *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	syncWrites bool
}

// NewWAL creates a new WAL instance.
func NewWAL(path string, syncWrites bool) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		Path:       path,
		file:       file,
		writer:     bufio.NewWriter(file),
		syncWrites: syncWrites,
	}, nil
}

// Append adds a new entry to the WAL.
func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write operation type
	if err := binary.Write(w.writer, binary.LittleEndian, entry.Operation); err != nil {
		return fmt.Errorf("failed to write operation: %w", err)
	}

	// Write key length and key
	keyLen := uint32(len(entry.Key))
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}

	if _, err := w.writer.Write([]byte(entry.Key)); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	// For Put operations, write value length and value
	if entry.Operation == OpPut {
		valueLen := uint32(len(entry.Value))
		if err := binary.Write(w.writer, binary.LittleEndian, valueLen); err != nil {
			return fmt.Errorf("failed to write value length: %w", err)
		}

		if _, err := w.writer.Write([]byte(entry.Value)); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}

	// Flush buffer to file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	// Sync file if configured
	if w.syncWrites {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
	}

	return nil
}

// Replay replays the WAL, calling the provided function for each entry.
func (w *WAL) Replay(fn func(*Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any pending writes
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	// Seek to beginning of file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of WAL: %w", err)
	}

	reader := bufio.NewReader(w.file)

	for {
		// Read operation type
		opByte, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read operation: %w", err)
		}

		// Read key length
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				return fmt.Errorf("incomplete WAL entry: %w", err)
			}
			return fmt.Errorf("failed to read key length: %w", err)
		}

		// Read key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}

		entry := &Entry{
			Operation: opByte,
			Key:       string(keyBytes),
		}

		// For Put operations, read value length and value
		if opByte == OpPut {
			var valueLen uint32
			if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
				return fmt.Errorf("failed to read value length: %w", err)
			}

			valueBytes := make([]byte, valueLen)
			if _, err := io.ReadFull(reader, valueBytes); err != nil {
				return fmt.Errorf("failed to read value: %w", err)
			}

			entry.Value = string(valueBytes)
		}

		// Call function with entry
		if err := fn(entry); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	return nil
}
