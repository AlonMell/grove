// Package bloom implements a Bloom filter for membership testing.
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic data structure for testing set membership.
// It allows false positives but not false negatives.
type BloomFilter struct {
	bitset   []byte // Bit array
	k        uint   // Number of hash functions
	n        uint   // Expected number of elements
	m        uint   // Size of the filter in bits
	hashSeed uint32 // Seed for hash functions
}

// New creates a Bloom filter optimized for the expected number of elements.
// The bitsPerElement parameter affects the false positive rate.
func New(n, bitsPerElement int) *BloomFilter {
	if n <= 0 {
		n = 1
	}
	if bitsPerElement <= 0 {
		bitsPerElement = 10
	}

	m := max(uint(n*bitsPerElement), 8)

	// k = (m/n) * ln(2)
	k := uint(math.Ceil(float64(m) / float64(n) * math.Log(2)))
	k = min(max(k, 1), 30)

	return &BloomFilter{
		bitset:   make([]byte, (m+7)/8),
		k:        k,
		n:        uint(n),
		m:        m,
		hashSeed: 0x9747b28c,
	}
}

// hash generates a hash value for the key with the given seed.
func (b *BloomFilter) hash(key string, seed uint32) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{
		byte(seed),
		byte(seed >> 8),
		byte(seed >> 16),
		byte(seed >> 24),
	})
	return h.Sum32()
}

// locations returns the bit positions in the filter for the given key.
func (b *BloomFilter) locations(key string) []uint {
	locs := make([]uint, b.k)
	for i := uint(0); i < b.k; i++ {
		hash := b.hash(key, b.hashSeed+uint32(i))
		locs[i] = uint(hash) % b.m
	}
	return locs
}

// Add adds a key to the Bloom filter.
func (b *BloomFilter) Add(key string) {
	for _, loc := range b.locations(key) {
		byteIndex := loc / 8
		bitMask := byte(1 << (loc % 8))
		b.bitset[byteIndex] |= bitMask
	}
}

// MayContain tests if a key may be in the Bloom filter.
// Returns true if the key might be in the set, false if it definitely is not.
func (b *BloomFilter) MayContain(key string) bool {
	for _, loc := range b.locations(key) {
		byteIndex := loc / 8
		bitMask := byte(1 << (loc % 8))
		if (b.bitset[byteIndex] & bitMask) == 0 {
			return false
		}
	}
	return true
}

// EstimateFalsePositiveRate estimates the false positive rate.
func (b *BloomFilter) EstimateFalsePositiveRate(numElements int) float64 {
	if numElements <= 0 {
		return 0.0
	}

	// P = (1 - e^(-k*n/m))^k
	exponent := -float64(b.k) * float64(numElements) / float64(b.m)
	return math.Pow(1.0-math.Exp(exponent), float64(b.k))
}

// Reset clears the Bloom filter.
func (b *BloomFilter) Reset() {
	for i := range b.bitset {
		b.bitset[i] = 0
	}
}

// Size returns the size of the filter in bits.
func (b *BloomFilter) Size() uint {
	return b.m
}

// NumHashFunctions returns the number of hash functions.
func (b *BloomFilter) NumHashFunctions() uint {
	return b.k
}

// ExpectedElements returns the expected number of elements.
func (b *BloomFilter) ExpectedElements() uint {
	return b.n
}

// Encode serializes the Bloom filter to a byte slice.
func (b *BloomFilter) Encode() []byte {
	bufSize := 16 + len(b.bitset)
	buffer := make([]byte, bufSize)

	binary.LittleEndian.PutUint32(buffer[0:4], uint32(b.m))
	binary.LittleEndian.PutUint32(buffer[4:8], uint32(b.k))
	binary.LittleEndian.PutUint32(buffer[8:12], uint32(b.n))
	binary.LittleEndian.PutUint32(buffer[12:16], b.hashSeed)
	copy(buffer[16:], b.bitset)

	return buffer
}

// Decode deserializes a Bloom filter from a byte slice.
func Decode(data []byte, estimatedN int) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("invalid bloom filter data: too short (got %d bytes, need at least 16)", len(data))
	}

	m := binary.LittleEndian.Uint32(data[0:4])
	k := binary.LittleEndian.Uint32(data[4:8])
	n := binary.LittleEndian.Uint32(data[8:12])
	hashSeed := binary.LittleEndian.Uint32(data[12:16])

	bitsetSize := (m + 7) / 8
	if uint32(len(data)-16) < bitsetSize {
		return nil, fmt.Errorf("invalid bloom filter data: bitset too short (got %d bytes, need %d)",
			len(data)-16, bitsetSize)
	}

	bf := &BloomFilter{
		bitset:   make([]byte, bitsetSize),
		k:        uint(k),
		n:        uint(n),
		m:        uint(m),
		hashSeed: hashSeed,
	}

	copy(bf.bitset, data[16:16+bitsetSize])
	return bf, nil
}

// MustDecode deserializes a Bloom filter from a byte slice.
// Returns a new filter if the data is invalid.
func MustDecode(data []byte, estimatedN int) *BloomFilter {
	bf, err := Decode(data, estimatedN)
	if err != nil {
		return New(estimatedN, 10)
	}
	return bf
}
