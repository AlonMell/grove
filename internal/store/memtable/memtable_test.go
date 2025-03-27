package memtable_test

import (
	"bytes"
	"cmp"
	"encoding/gob"
	"testing"

	"github.com/grove/internal/datastruct/types"
	"github.com/grove/internal/store/memtable"
)

// Функция для расчета ожидаемого размера пары, аналогичная логике newGob
func calcExpectedSize[K cmp.Ordered, V any](key K, val V) int {
	var buf bytes.Buffer

	// Имитируем инициализацию энкодера из newGob
	gob.Register(types.Pair[K, V]{})
	enc := gob.NewEncoder(&buf)
	enc.Encode(types.Pair[K, V]{}) // Пустая пара для регистрации типа
	buf.Reset()                    // Сброс, как в newGob

	// Кодируем реальные данные
	enc.Encode(types.Pair[K, V]{Key: key, Val: val})
	return buf.Len()
}

func TestInsertIncreasesSize(t *testing.T) {
	tests := []struct {
		name     string
		key      int
		val      string
		wantSize int
	}{
		{"simple int key", 1, "test1", calcExpectedSize(1, "test1")},
		{"zero value key", 0, "zero", calcExpectedSize(0, "zero")},
		{"negative key", -5, "negative", calcExpectedSize(-5, "negative")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := memtable.New[int, string]()
			mt.Insert(tt.key, tt.val)

			if mt.Size() != uint64(tt.wantSize) {
				t.Errorf("Expected size %d, got %d", tt.wantSize, mt.Size())
			}

			if _, found := mt.Search(tt.key); !found {
				t.Error("Inserted element not found")
			}
		})
	}
}

func TestDeleteDecreasesSize(t *testing.T) {
	mt := memtable.New[string, []byte]()
	expectedSize := 0
	tests := []struct {
		key string
		val []byte
	}{
		{"abc", []byte("ssdafasfdfdsa")},
		{"def", []byte("fsdgfgdsdfgsfsdg")},
		{"ghi", []byte("ssdafafdsdfgsdfgdfgssfdfdsa")},
	}

	for _, tt := range tests {
		mt.Insert(tt.key, tt.val)

		expectedSize += calcExpectedSize(tt.key, tt.val)

		if _, found := mt.Search(tt.key); !found {
			t.Error("Inserted element not found")
		}
	}

	for _, tt := range tests {
		if err := mt.Delete(tt.key); err != nil {
			t.Fatal("Delete failed:", err)
		}

		expectedSize -= calcExpectedSize(tt.key, tt.val)

		if _, found := mt.Search(tt.key); found {
			t.Error("Deleted element was founded")
		}
	}

	if mt.Size() != uint64(expectedSize) || mt.Size() != 0 {
		t.Errorf("Expected size %d, got %d", expectedSize, mt.Size())
	}
}

func TestMultipleOperations(t *testing.T) {
	mt := memtable.New[int, string]()
	expectedSize := 0
	keys := []int{1, 2, 3}

	// Вставка
	for _, key := range keys {
		val := "val"
		increase := calcExpectedSize(key, val)
		expectedSize += increase
		mt.Insert(key, val)
	}

	if mt.Size() != uint64(expectedSize) {
		t.Errorf("After insert: expected %d, got %d", expectedSize, mt.Size())
	}

	// Удаление двух ключей
	for _, key := range keys[:2] {
		val := "val"
		decrease := calcExpectedSize(key, val)
		expectedSize -= decrease
		if err := mt.Delete(key); err != nil {
			t.Fatal("Delete failed:", err)
		}
	}

	if mt.Size() != uint64(expectedSize) {
		t.Errorf("After delete: expected %d, got %d", expectedSize, mt.Size())
	}

	key := keys[len(keys)-1]
	val := "val"
	decrease := calcExpectedSize(key, val)
	expectedSize -= decrease
	if err := mt.Delete(key); err != nil {
		t.Fatal("Delete failed:", err)
	}
	if mt.Size() != uint64(0) {
		t.Errorf("After delete: expected %d, got %d", expectedSize, mt.Size())
	}
}

func TestClean(t *testing.T) {
	mt := memtable.New[string, float64]()
	mt.Insert("a", 1.5)
	mt.Insert("b", 2.8)

	mt.Clean()

	if mt.Size() != 0 {
		t.Error("Size not zero after Clean")
	}

	mt.Insert("c", 3.0) // Проверяем работоспособность после очистки
	if mt.Size() != uint64(calcExpectedSize("c", 3.0)) {
		t.Error("MemTable not reset properly")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	mt := memtable.New[int, struct{}]()

	if err := mt.Delete(999); err == nil {
		t.Error("Expected error for non-existent key")
	}

	mt.Insert(999, struct{}{})
	if err := mt.Delete(999); err != nil {
		t.Error("Valid delete failed")
	}

	if err := mt.Delete(999); err == nil { // Повторное удаление
		t.Error("Expected error for already deleted key")
	}
}
