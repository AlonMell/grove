package rbtree_test

import (
	"math/rand"
	"slices"
	"strconv"
	"testing"

	"github.com/grove/internal/datastruct/rbtree"
)

const (
	BENCH_SIZE = 10_000
	SIZE       = 10_000
	SOURCE     = 42
)

func TestRBInsert(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	random := generateRandomArray(r, SIZE)
	sorted := slices.Clone(random)
	reversed := slices.Clone(random)
	shuffled := slices.Clone(random)

	slices.Sort(sorted)
	slices.Sort(reversed)
	slices.Reverse(reversed)

	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	tests := []struct {
		name  string
		input []int
	}{
		{"Empty", []int{}},
		{"Single", []int{1}},
		{"Shuffled", shuffled},
		{"Sorted", sorted},
		{"Reversed", reversed},
		{"LargeRandom", random},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := rbtree.New[int, int]()
			for _, v := range tt.input {
				rb.Insert(v, v) // Используем значение = ключу
			}

			if !rb.VerifyTreeProperties() {
				t.Errorf("RBTree properties violated for input %v", tt.name)
			}
		})
	}
}

func TestRBIterator(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	rb := rbtree.New[int, float64]()
	keys := generateRandomArray(r, 1000)

	for _, k := range keys {
		rb.Insert(k, float64(k))
	}

	res := make([]int, 0, 1000)
	for k, _ := range rb.InOrder() {
		res = append(res, k)
	}

	if len(res) != len(keys) {
		t.Errorf("Expected %d elements, got %d", len(keys), len(res))
	}

	for _, k := range keys {
		if !slices.Contains(res, k) {
			t.Errorf("Key %d not found in iterator result", k)
		}
	}
}

func TestRBDelete(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	t.Run("LargeDataset", func(t *testing.T) {
		rb := rbtree.New[int, float64]()
		keys := generateRandomArray(r, SIZE)

		// Insert with dummy values
		for _, k := range keys {
			rb.Insert(k, float64(k))
		}

		for _, k := range keys {
			if !rb.Exists(k) {
				t.Errorf("Key %d should exist before deletion", k)
			}
		}

		// Delete in random order
		r.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		for i, k := range keys {
			rb.Delete(k)
			if rb.Exists(k) {
				t.Fatalf("Key %d still exists after deletion (iteration %d)", k, i)
			}
			if !rb.VerifyTreeProperties() {
				t.Fatalf("RBTree properties violated after deleting %d (iteration %d)", k, i)
			}
		}
	})

	t.Run("KeyDoesntExist", func(t *testing.T) {
		rb := rbtree.New[string, any]()
		rb.Delete("nonexistent")
	})
}

func TestRBFind(t *testing.T) {
	r := rand.New(rand.NewSource(SOURCE))
	rb := rbtree.New[int, int]()
	keys := generateRandomArray(r, SIZE)

	for _, k := range keys {
		rb.Insert(k, k)
	}

	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	t.Run("KeyExists", func(t *testing.T) {
		for _, k := range keys {
			if value, exists := rb.Find(k); !exists {
				t.Errorf("Key %d not found", k)
			} else if value != k {
				t.Errorf("Key %d found with wrong value %d", k, value)
			}
		}
	})

	for _, k := range keys {
		rb.Delete(k)
	}

	t.Run("KeyDoesntExist", func(t *testing.T) {
		for _, k := range keys {
			if _, exists := rb.Find(k); exists {
				t.Errorf("Key %d found but it's deleted", k)
			}
		}
	})

}

// Helpers
func generateRandomArray(r *rand.Rand, size int) []int {
	arr := make([]int, size)
	for i := range arr {
		arr[i] = i
	}

	r.Shuffle(size, func(i, j int) {
		arr[i], arr[j] = arr[j], arr[i]
	})

	return arr
}

func BenchmarkInsert(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		name := "Size-" + strconv.Itoa(size)
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				rb := rbtree.New[int, int]()
				for n := range size {
					rb.Insert(n, n)
				}
			}
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	rb := rbtree.New[int, string]()
	for n := range BENCH_SIZE {
		rb.Insert(n, "value")
	}

	for i := 0; b.Loop(); i++ {
		rb.Delete(i % BENCH_SIZE)
	}
}

func BenchmarkSearch(b *testing.B) {
	rb := rbtree.New[int, float64]()
	for n := range BENCH_SIZE {
		rb.Insert(n, float64(n))
	}

	for i := 0; b.Loop(); i++ {
		rb.Exists(i % BENCH_SIZE)
	}
}
