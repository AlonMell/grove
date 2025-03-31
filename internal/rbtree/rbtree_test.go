package rbtree_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/grove/internal/rbtree"
)

const (
	BENCH_SIZE = 10_000
	SIZE       = 10_000
	SOURCE     = 42
)

func TestRBInsert(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	random := generateRandomStrings(r, SIZE)
	sorted := make([]string, len(random))
	copy(sorted, random)
	reversed := make([]string, len(random))
	copy(reversed, random)
	shuffled := make([]string, len(random))
	copy(shuffled, random)

	// Сортируем для тестов
	sortStringSlice(sorted)
	sortStringSlice(reversed)
	reverseStringSlice(reversed)

	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	tests := []struct {
		name  string
		input []string
	}{
		{"Empty", []string{}},
		{"Single", []string{"1"}},
		{"Shuffled", shuffled},
		{"Sorted", sorted},
		{"Reversed", reversed},
		{"LargeRandom", random},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := rbtree.New()
			for _, v := range tt.input {
				rb.Insert(v, v) // Используем значение = ключу
			}

			if !rb.VerifyTreeProperties() {
				t.Errorf("RBTree properties violated for input %v", tt.name)
			}
		})
	}
}

func TestRBDelete(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	t.Run("LargeDataset", func(t *testing.T) {
		rb := rbtree.New()
		keys := generateRandomStrings(r, SIZE)

		// Insert with dummy values
		for _, k := range keys {
			rb.Insert(k, k)
		}

		for _, k := range keys {
			if !rb.Exists(k) {
				t.Errorf("Key %s should exist before deletion", k)
			}
		}

		// Delete in random order
		r.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		for i, k := range keys {
			_, err := rb.Delete(k)
			if err != nil {
				t.Fatalf("Error deleting key %s: %v", k, err)
			}
			if rb.Exists(k) {
				t.Fatalf("Key %s still exists after deletion (iteration %d)", k, i)
			}
			if !rb.VerifyTreeProperties() {
				t.Fatalf("RBTree properties violated after deleting %s (iteration %d)", k, i)
			}
		}
	})

	t.Run("KeyDoesntExist", func(t *testing.T) {
		rb := rbtree.New()
		_, err := rb.Delete("nonexistent")
		if err == nil {
			t.Error("Expected error when deleting nonexistent key, got nil")
		}
	})
}

func TestRBFind(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	rb := rbtree.New()
	keys := generateRandomStrings(r, SIZE)

	for _, k := range keys {
		rb.Insert(k, k)
	}

	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	t.Run("KeyExists", func(t *testing.T) {
		for _, k := range keys {
			if value, exists := rb.Find(k); !exists {
				t.Errorf("Key %s not found", k)
			} else if value != k {
				t.Errorf("Key %s found with wrong value %s", k, value)
			}
		}
	})

	for _, k := range keys {
		_, err := rb.Delete(k)
		if err != nil {
			t.Fatalf("Error deleting key %s: %v", k, err)
		}
	}

	t.Run("KeyDoesntExist", func(t *testing.T) {
		for _, k := range keys {
			if _, exists := rb.Find(k); exists {
				t.Errorf("Key %s found but it's deleted", k)
			}
		}
	})
}

// Helpers
func generateRandomStrings(r *rand.Rand, size int) []string {
	arr := make([]string, size)
	for i := range arr {
		arr[i] = strconv.Itoa(i)
	}

	r.Shuffle(size, func(i, j int) {
		arr[i], arr[j] = arr[j], arr[i]
	})

	return arr
}

func sortStringSlice(slice []string) {
	for i := 0; i < len(slice)-1; i++ {
		for j := i + 1; j < len(slice); j++ {
			if slice[i] > slice[j] {
				slice[i], slice[j] = slice[j], slice[i]
			}
		}
	}
}

func reverseStringSlice(slice []string) {
	for i, j := 0, len(slice)-1; i < j; i, j = i+1, j-1 {
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func BenchmarkInsert(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		name := "Size-" + strconv.Itoa(size)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rb := rbtree.New()
				for n := 0; n < size; n++ {
					rb.Insert(strconv.Itoa(n), strconv.Itoa(n))
				}
			}
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	rb := rbtree.New()
	for n := 0; n < BENCH_SIZE; n++ {
		rb.Insert(strconv.Itoa(n), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Delete(strconv.Itoa(i % BENCH_SIZE))
	}
}

func BenchmarkSearch(b *testing.B) {
	rb := rbtree.New()
	for n := 0; n < BENCH_SIZE; n++ {
		rb.Insert(strconv.Itoa(n), strconv.Itoa(n))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Exists(strconv.Itoa(i % BENCH_SIZE))
	}
}
