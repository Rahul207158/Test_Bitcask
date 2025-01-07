package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// IndexEntry represents an entry in the in-memory index
type IndexEntry struct {
	FileName string
	Offsets  []int64
}

// Bitcask represents the key-value store
type Bitcask struct {
	dataDir         string
	index           sync.Map    // Concurrent map for the index
	currentFile     *os.File    // Active file for writing
	currentFileSize int64       // Size of the current active file
	maxFileSize     int64       // Max size for a single file
	activeFileMu    sync.Mutex  // Lock for managing the active file
}

// NewBitcask initializes a new Bitcask instance
func NewBitcask(dataDir string, maxFileSize int64) (*Bitcask, error) {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filepath.Join(dataDir, "segment.log"), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Bitcask{
		dataDir:     dataDir,
		currentFile: file,
		maxFileSize: maxFileSize,
	}, nil
}

// rotateFile creates a new segment file when the current file exceeds the size limit
func (bc *Bitcask) rotateFile() error {
	
	if err := bc.currentFile.Close(); err != nil {
		return err
	}

	newFileName := fmt.Sprintf("segment-%d.log", time.Now().UnixMicro())
	newFilePath := filepath.Join(bc.dataDir, newFileName)

	file, err := os.OpenFile(newFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	bc.currentFile = file
	bc.currentFileSize = 0
	return nil
}

// Put stores a key-value pair in the active file
func (bc *Bitcask) Put(key, value string) error {
	bc.activeFileMu.Lock()
	defer bc.activeFileMu.Unlock()

	// Serialize the data
	timestamp := time.Now().Unix()
	CRC := calculateCRC(key + value)

	var buffer bytes.Buffer
	binary.Write(&buffer, binary.LittleEndian, CRC)
	binary.Write(&buffer, binary.LittleEndian, timestamp)
	binary.Write(&buffer, binary.LittleEndian, int32(len(key)))
	binary.Write(&buffer, binary.LittleEndian, int32(len(value)))
	buffer.WriteString(key)
	buffer.WriteString(value)

	data := buffer.Bytes()

	offset, err := bc.currentFile.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	_, err = bc.currentFile.Write(data)
	if err != nil {
		return err
	}

	bc.currentFileSize += int64(len(data))
	if bc.currentFileSize > bc.maxFileSize {
		//fmt.Print("in maxfilesize")
		if err := bc.rotateFile(); err != nil {
			return err
		}
	}

	// Update in-memory index
	indexEntry, _ := bc.index.LoadOrStore(key, IndexEntry{FileName: bc.currentFile.Name(), Offsets: []int64{}})
	entry := indexEntry.(IndexEntry)
	entry.Offsets = append(entry.Offsets, offset)
	bc.index.Store(key, entry)

	return nil
}

// Get retrieves all values associated with a key
func (bc *Bitcask) Get(key string) ([]string, bool) {
	// Retrieve index entry
	indexEntryInterface, exists := bc.index.Load(key)
	if !exists {
		return nil, false
	}
	indexEntry := indexEntryInterface.(IndexEntry)

	var values []string
	for _, offset := range indexEntry.Offsets {
		file, err := os.Open(filepath.Join(indexEntry.FileName))
		if err != nil {
			log.Printf("Error opening file %s: %v", indexEntry.FileName, err)
			continue
		}

		_, err = file.Seek(offset, os.SEEK_SET)
		if err != nil {
			log.Printf("Error seeking offset %d in file %s: %v", offset, indexEntry.FileName, err)
			file.Close()
			continue
		}

		var CRC int32
		var timestamp int64
		var keyLen, valueLen int32
		binary.Read(file, binary.LittleEndian, &CRC)
		binary.Read(file, binary.LittleEndian, &timestamp)
		binary.Read(file, binary.LittleEndian, &keyLen)
		binary.Read(file, binary.LittleEndian, &valueLen)

		keyBytes := make([]byte, keyLen)
		file.Read(keyBytes)
		valueBytes := make([]byte, valueLen)
		file.Read(valueBytes)

		calculatedCRC := calculateCRC(string(keyBytes) + string(valueBytes))
		if calculatedCRC != CRC {
			log.Println("CRC mismatch detected")
			file.Close()
			continue
		}

		values = append(values, string(valueBytes))
		file.Close()
	}

	return values, len(values) > 0
}

// calculateCRC calculates a checksum for a string
func calculateCRC(data string) int32 {
	var crc int32
	for _, c := range data {
		crc += int32(c)
	}
	return crc
}

// HTTP Handlers
func (bc *Bitcask) putHandler(w http.ResponseWriter, r *http.Request) {
	type Request struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := bc.Put(req.Key, req.Value); err != nil {
		http.Error(w, "Failed to store key-value", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Key-Value stored successfully")
}

func (bc *Bitcask) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	values, found := bc.Get(key)
	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(values)
}

func main() {
	// Initialize Bitcask store
	store, err := NewBitcask("data/", 1024*900) // 300 KB max file size
	if err != nil {
		log.Fatalf("Error initializing Bitcask: %v", err)
	}

	// Set up HTTP server
	http.HandleFunc("/put", store.putHandler)
	http.HandleFunc("/get", store.getHandler)

	fmt.Println("Server running on :8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
