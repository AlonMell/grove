package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/grove/internal/datastruct/rbtree"
)

const (
	memTableLimit = 1 << 20 // 1MB
	dataDir       = "data"
)

type LSMTree struct {
	memTable    *rbtree.RBTree[string, []byte]
	diskTables  []string // SSTable files
	dir         string
	mu          sync.RWMutex
	currentSize int
}

func NewLSMTree(dir string) (*LSMTree, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &LSMTree{
		memTable:   rbtree.New[string, []byte](),
		dir:        dir,
		diskTables: loadSSTables(dir),
	}, nil
}

func loadSSTables(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	type sstFile struct {
		path string
		num  int
	}

	var files []sstFile
	re := regexp.MustCompile(`^sst-(\d+)\.dat$`)

	// Собираем подходящие файлы
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		matches := re.FindStringSubmatch(entry.Name())
		if len(matches) != 2 {
			continue
		}

		num, err := strconv.Atoi(matches[1])
		if err != nil {
			continue
		}

		files = append(files, sstFile{
			path: filepath.Join(dir, entry.Name()),
			num:  num,
		})
	}

	// Сортируем по возрастанию номера
	sort.Slice(files, func(i, j int) bool {
		return files[i].num < files[j].num
	})

	// Формируем итоговый список путей
	result := make([]string, 0, len(files))
	for _, f := range files {
		result = append(result, f.path)
	}

	return result
}

func (lsm *LSMTree) Set(key string, value []byte) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	lsm.memTable.Insert(key, value)
	lsm.currentSize += len(key) + len(value)

	if lsm.currentSize >= memTableLimit {
		if err := lsm.flushMemTable(); err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
		lsm.currentSize = 0
		lsm.memTable = rbtree.New[string, []byte]()
	}
	return nil
}

func (lsm *LSMTree) Get(key string) ([]byte, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Проверяем MemTable
	if value, exists := lsm.memTable.Find(key); exists {
		return value, true
	}

	// Проверяем SSTables (в обратном порядке)
	for i := len(lsm.diskTables) - 1; i >= 0; i-- {
		if value, found := readSSTable(lsm.diskTables[i], key); found {
			return value, true
		}
	}
	return nil, false
}

func (lsm *LSMTree) flushMemTable() error {
	sstName := filepath.Join(lsm.dir, fmt.Sprintf("sst-%d.dat", len(lsm.diskTables)))
	f, err := os.Create(sstName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Простой формат: [key_len(4)|key|value_len(4)|value]...
	it := lsm.memTable.Iterator() // Нужно реализовать итератор в RBTree
	for it.Next() {
		key, value := it.Key(), it.Value()
		if err := writeBinary(f, uint32(len(key))); err != nil {
			return err
		}
		if _, err := f.WriteString(key); err != nil {
			return err
		}
		if err := writeBinary(f, uint32(len(value))); err != nil {
			return err
		}
		if _, err := f.Write(value); err != nil {
			return err
		}
	}

	lsm.diskTables = append(lsm.diskTables, sstName)
	return nil
}

func readSSTable(path, target string) ([]byte, bool) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	for {
		var keyLen uint32
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			break
		}

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBuf); err != nil {
			break
		}
		key := string(keyBuf)

		var valueLen uint32
		if err := binary.Read(f, binary.LittleEndian, &valueLen); err != nil {
			break
		}

		value := make([]byte, valueLen)
		if _, err := io.ReadFull(f, value); err != nil {
			break
		}

		if key == target {
			return value, true
		}
	}
	return nil, false
}

func writeBinary(w io.Writer, data any) error {
	return binary.Write(w, binary.LittleEndian, data)
}
