package main

import (
	"flag"
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

func main() {
	// Parse command line arguments
	dbPath := flag.String("db", "", "Path to boltdb file")
	bucketName := flag.String("bucket", "", "Name of bucket to view")
	limit := flag.Int("limit", 0, "Number of latest rows to display")
	flag.Parse()

	// Open the boltdb file
	db, err := bolt.Open(*dbPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// If no bucket name is specified, list all the buckets in the boltdb file
	if *bucketName == "" {
		err = db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
				fmt.Println(string(name))
				return nil
			})
		})
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	// Get the specified bucket
	var bucket *bolt.Bucket
	err = db.View(func(tx *bolt.Tx) error {
		bucket = tx.Bucket([]byte(*bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found", *bucketName)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Iterate over the key-value pairs in the bucket and store them in a slice
	var rows [][]byte
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(*bucketName))
		if b == nil {
			return fmt.Errorf("Bucket %q not found", *bucketName)
		}
		return b.ForEach(func(k, v []byte) error {
			row := []byte(fmt.Sprintf("key: %s\nvalue: %s\n===", k, v))
			rows = append(rows, row)
			return nil
		})
	})
	if err != nil {
		log.Fatal(err)
	}

	// Limit the number of rows based on the `limit` flag. show latest rows
	if *limit > 0 && *limit < len(rows) {
		rows = rows[(len(rows) - *limit):]
	}

	// Iterate over the key-value pairs in the slice and print them to the console
	err = db.View(func(tx *bolt.Tx) error {
		for _, row := range rows {
			fmt.Println(string(row))
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
