// Example of using the LSM-Tree database
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/AlonMell/grove/internal/lsm"
)

func main() {
	// Create a temporary directory for our database
	dbDir, err := os.MkdirTemp("", "lsmdb-example")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dbDir) // Clean up

	// Configure the database
	config := lsm.DefaultConfig()
	config.MemTableSize = 1024 * 1024 // 1MB
	config.SyncWrites = true

	// Open the database
	db, err := lsm.Open(dbDir, config)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Println("LSM-Tree Database Example")
	fmt.Println("=========================")
	fmt.Printf("Database directory: %s\n\n", dbDir)

	// Insert some data
	fmt.Println("Inserting data...")
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := fmt.Sprintf("value-%04d", i)

		if err := db.Put(key, value); err != nil {
			log.Fatalf("Failed to put %s: %v", key, err)
		}

		// Log progress
		if i%100 == 0 {
			fmt.Printf("  Inserted %d records\n", i)
		}
	}

	// Retrieve some values
	fmt.Println("\nRetrieving data...")
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("key-%04d", i*100)
		value, err := db.Get(key)

		if err != nil {
			fmt.Printf("  Failed to get %s: %v\n", key, err)
		} else {
			fmt.Printf("  %s => %s\n", key, value)
		}
	}

	// Delete some values
	fmt.Println("\nDeleting data...")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key-%04d", i*100)
		if err := db.Delete(key); err != nil {
			fmt.Printf("  Failed to delete %s: %v\n", key, err)
		} else {
			fmt.Printf("  Deleted %s\n", key)
		}
	}

	// Verify deletion
	fmt.Println("\nVerifying deletion...")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key-%04d", i*100)
		value, err := db.Get(key)

		if err == lsm.ErrKeyNotFound {
			fmt.Printf("  %s correctly not found\n", key)
		} else if err != nil {
			fmt.Printf("  Error fetching %s: %v\n", key, err)
		} else {
			fmt.Printf("  Error: %s should have been deleted, but got value: %s\n", key, value)
		}
	}

	// Force compaction
	fmt.Println("\nForcing compaction...")
	if err := db.Compact(); err != nil {
		fmt.Printf("Compaction error: %v\n", err)
	} else {
		fmt.Println("Compaction completed successfully")
	}

	// Benchmark
	fmt.Println("\nRunning simple benchmark...")
	benchmarkWrites(db, 10000)
	benchmarkReads(db, 1000)

	fmt.Println("\nDatabase closed successfully")
}

func benchmarkWrites(db *lsm.DB, count int) {
	fmt.Printf("Writing %d records...\n", count)

	start := time.Now()

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("bench-key-%05d", i)
		value := fmt.Sprintf("bench-value-%05d-%s", i, strconv.FormatInt(time.Now().UnixNano(), 10))

		if err := db.Put(key, value); err != nil {
			log.Fatalf("Write error: %v", err)
		}
	}

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()

	fmt.Printf("  Time: %.2f seconds\n", duration.Seconds())
	fmt.Printf("  Throughput: %.0f ops/sec\n", opsPerSec)
}

func benchmarkReads(db *lsm.DB, count int) {
	fmt.Printf("Reading %d random records...\n", count)

	start := time.Now()
	hits := 0

	for i := 0; i < count; i++ {
		keyNum := (i * 17) % 10000 // Simple pseudo-random distribution
		key := fmt.Sprintf("bench-key-%05d", keyNum)

		_, err := db.Get(key)
		if err == nil {
			hits++
		}
	}

	duration := time.Since(start)
	opsPerSec := float64(count) / duration.Seconds()
	hitRate := float64(hits) / float64(count) * 100

	fmt.Printf("  Time: %.2f seconds\n", duration.Seconds())
	fmt.Printf("  Throughput: %.0f ops/sec\n", opsPerSec)
	fmt.Printf("  Hit rate: %.1f%%\n", hitRate)
}
