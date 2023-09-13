package csv

import (
	"encoding/json"
	"fmt"
	"os"
	"sink/env"
	"sync"
	"time"
)

type IndexEntry struct {
	// The json file.
	File string `json:"file"`
	// The time when the sink was last updated.
	LastUpdated time.Time `json:"lastUpdated"`
}

// The lock that must be used when writing or reading the index file.
// This is to gobally protect concurrent access to the same file.
var indexFileLock = &sync.Mutex{}

// Lookup all .json sink files in the static path and write them into a json file.
// This serves as an index for the later analysis.
func UpdateSinkIndex() {
	entries := make([]IndexEntry, 0)
	filePaths.Range(func(key, value interface{}) bool {
		path := key.(string)
		lastUpdated := value.(time.Time)

		// Add the history to the index.
		entries = append(entries, IndexEntry{
			File:        path,
			LastUpdated: lastUpdated,
		})

		return true
	})

	// Write the json files into a json file (without ioutil).
	jsonBytes, err := json.Marshal(entries)
	if err != nil {
		panic(err)
	}
	// Acquire the file locks for additional safety.
	indexFileLock.Lock()
	defer indexFileLock.Unlock()
	indexFile, err := os.Create(fmt.Sprintf("%s/index.json", env.StaticPath))
	if err != nil {
		panic(err)
	}
	defer indexFile.Close()
	_, err = indexFile.Write(jsonBytes)
	if err != nil {
		panic(err)
	}
}

// Build the index file periodically.
func UpdateSinkIndexPeriodically() {
	for {
		time.Sleep(60 * time.Second)
		UpdateSinkIndex()
	}
}
