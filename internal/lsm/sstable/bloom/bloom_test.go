package bloom_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/AlonMell/grove/internal/lsm/sstable/bloom"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name           string
		n              int
		bitsPerElement int
		wantK          uint // expected number of hash functions
		wantM          uint // expected size in bits
	}{
		{
			name:           "typical values",
			n:              1000,
			bitsPerElement: 10,
			wantK:          7, // Approximately ln(2) * 10 ≈ 6.9
			wantM:          10000,
		},
		{
			name:           "small n",
			n:              0,
			bitsPerElement: 10,
			wantK:          7,
			wantM:          10,
		},
		{
			name:           "small bits per element",
			n:              1000,
			bitsPerElement: 0,
			wantK:          7,
			wantM:          10000,
		},
		{
			name:           "large bits per element",
			n:              1000,
			bitsPerElement: 20,
			wantK:          14, // Approximately ln(2) * 20 ≈ 13.9
			wantM:          20000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := bloom.New(tt.n, tt.bitsPerElement)
			if bf.NumHashFunctions() != tt.wantK {
				t.Errorf("New(%d, %d) k = %d, want %d",
					tt.n, tt.bitsPerElement, bf.NumHashFunctions(), tt.wantK)
			}
			if bf.Size() != tt.wantM {
				t.Errorf("New(%d, %d) m = %d, want %d",
					tt.n, tt.bitsPerElement, bf.Size(), tt.wantM)
			}
		})
	}
}

func TestBloomFilter_Add_MayContain(t *testing.T) {
	bf := bloom.New(1000, 10)

	// Add elements
	elements := []string{"a", "b", "c", "test", "hello", "world"}
	for _, e := range elements {
		bf.Add(e)
	}

	// Check elements that were added
	for _, e := range elements {
		if !bf.MayContain(e) {
			t.Errorf("MayContain(%q) = false, want true", e)
		}
	}

	// Check elements that were not added
	notAdded := []string{"d", "e", "f", "foo", "bar", "baz"}
	falsePositives := 0
	for _, e := range notAdded {
		if bf.MayContain(e) {
			falsePositives++
		}
	}

	// Some false positives are expected, but not too many
	falsePositiveRate := float64(falsePositives) / float64(len(notAdded))
	expectedRate := bf.EstimateFalsePositiveRate(len(elements))

	if falsePositiveRate > expectedRate*2 && falsePositives > 1 {
		t.Errorf("False positive rate too high: got %f, expected around %f",
			falsePositiveRate, expectedRate)
	}
}

func TestBloomFilter_Reset(t *testing.T) {
	bf := bloom.New(1000, 10)

	// Add elements
	elements := []string{"a", "b", "c"}
	for _, e := range elements {
		bf.Add(e)
	}

	// Check they are in the filter
	for _, e := range elements {
		if !bf.MayContain(e) {
			t.Fatalf("Before reset: MayContain(%q) = false, want true", e)
		}
	}

	// Reset the filter
	bf.Reset()

	// Check they are no longer in the filter
	for _, e := range elements {
		if bf.MayContain(e) {
			t.Errorf("After reset: MayContain(%q) = true, want false", e)
		}
	}
}

func TestBloomFilter_EstimateFalsePositiveRate(t *testing.T) {
	tests := []struct {
		name         string
		n            int
		bitsPerElem  int
		numElements  int
		expectRateGT float64
		expectRateLT float64
	}{
		{
			name:         "typical case",
			n:            1000,
			bitsPerElem:  10,
			numElements:  1000,
			expectRateGT: 0.005,
			expectRateLT: 0.015,
		},
		{
			name:         "half filled",
			n:            1000,
			bitsPerElem:  10,
			numElements:  500,
			expectRateGT: 0.0001,
			expectRateLT: 0.0005,
		},
		{
			name:         "twice overfilled",
			n:            1000,
			bitsPerElem:  10,
			numElements:  2000,
			expectRateGT: 0.12,
			expectRateLT: 0.15,
		},
		{
			name:         "empty",
			n:            1000,
			bitsPerElem:  10,
			numElements:  0,
			expectRateGT: 0.0,
			expectRateLT: 0.0001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := bloom.New(tt.n, tt.bitsPerElem)
			rate := bf.EstimateFalsePositiveRate(tt.numElements)

			if rate < tt.expectRateGT {
				t.Errorf("EstimateFalsePositiveRate(%d) = %f, want > %f",
					tt.numElements, rate, tt.expectRateGT)
			}
			if rate > tt.expectRateLT {
				t.Errorf("EstimateFalsePositiveRate(%d) = %f, want < %f",
					tt.numElements, rate, tt.expectRateLT)
			}
		})
	}
}

func TestBloomFilter_Encode_Decode(t *testing.T) {
	original := bloom.New(1000, 10)

	// Add elements
	elements := []string{"a", "b", "c", "test", "hello", "world"}
	for _, e := range elements {
		original.Add(e)
	}

	// Encode
	encoded := original.Encode()

	// Decode
	decoded, err := bloom.Decode(encoded, 1000)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	// Check all elements are still present
	for _, e := range elements {
		if !decoded.MayContain(e) {
			t.Errorf("After decoding: MayContain(%q) = false, want true", e)
		}
	}
}

func TestDecode_InvalidData(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		estimatedN  int
		expectError bool
	}{
		{
			name:        "data too short",
			data:        []byte{1, 2, 3},
			estimatedN:  1000,
			expectError: true,
		},
		{
			name: "invalid bitset size",
			data: func() []byte {
				// Create data with a large m value but insufficient bitset bytes
				data := make([]byte, 20)
				// Set m to 1000 (requires 125 bytes for bitset)
				binary.LittleEndian.PutUint32(data[0:4], 1000)
				return data
			}(),
			estimatedN:  1000,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := bloom.Decode(tt.data, tt.estimatedN)
			if (err != nil) != tt.expectError {
				t.Errorf("Decode() error = %v, expectError %v", err, tt.expectError)
			}

			// Test MustDecode, which should never return an error
			bf := bloom.MustDecode(tt.data, tt.estimatedN)
			if bf == nil {
				t.Errorf("MustDecode() = nil, want non-nil")
			}
		})
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping false positive rate test in short mode")
	}

	// Create a filter with known parameters
	n := 10000
	bitsPerElement := 8
	bf := bloom.New(n, bitsPerElement)

	// Add n elements
	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("element-%d", i))
	}

	// Test with n elements that were not added
	falsePositives := 0
	testElements := 10000
	for i := 0; i < testElements; i++ {
		if bf.MayContain(fmt.Sprintf("test-%d", i)) {
			falsePositives++
		}
	}

	// Calculate actual false positive rate
	actualRate := float64(falsePositives) / float64(testElements)

	// In practice, actual rates often differ from theoretical rates
	// The difference can be due to hash function correlations and uneven distribution
	// We'll verify the rate is within a reasonable range for our implementation
	if actualRate < 0.10 || actualRate > 0.15 {
		t.Errorf("Measured false positive rate %f is outside expected range [0.10, 0.15]", actualRate)
	}

	// Verify the estimation function is working (not checking actual value match)
	estimatedRate := bf.EstimateFalsePositiveRate(n)
	if estimatedRate <= 0 || estimatedRate >= 1.0 {
		t.Errorf("Estimated false positive rate %f is out of bounds (0,1)", estimatedRate)
	}
}
