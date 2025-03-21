package rbtree_test

import (
	"math/rand"
	"slices"
	"strconv"
	"testing"

	"github.com/grove/internal/datastructures/rbtree"
)

const SIZE = 10_000

func TestRBTreeProperties(t *testing.T) {
	random := generateRandomArray(SIZE)
	sorted := slices.Clone(random)
	reversed := slices.Clone(random)
	shuffled := slices.Clone(random)

	slices.Sort(sorted)
	slices.Sort(reversed)
	slices.Reverse(reversed)

	r := rand.New(rand.NewSource(42))
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

func TestDelete(t *testing.T) {
	t.Run("LargeDataset", func(t *testing.T) {
		rb := rbtree.New[int, float64]()
		keys := generateRandomArray(SIZE)

		// Insert with dummy values
		for _, k := range keys {
			rb.Insert(k, float64(k))
		}

		for _, k := range keys {
			if !rb.Exists(k) {
				t.Errorf("Key %d should exist before deletion", k)
			}
		}

		r := rand.New(rand.NewSource(42))
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

// Helpers
func generateRandomArray(size int) []int {
	arr := make([]int, size)
	for i := range arr {
		arr[i] = i
	}

	r := rand.New(rand.NewSource(42))
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
	for n := range 10000 {
		rb.Insert(n, "value")
	}

	for i := 0; b.Loop(); i++ {
		rb.Delete(i % 10000)
	}
}

func BenchmarkSearch(b *testing.B) {
	rb := rbtree.New[int, float64]()
	for n := range 10000 {
		rb.Insert(n, float64(n))
	}

	for i := 0; b.Loop(); i++ {
		rb.Exists(i % 10000)
	}
}
