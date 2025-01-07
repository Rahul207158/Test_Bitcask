package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	
)

const (
	writeConcurrency = 100  // Number of concurrent writers
	getConcurrency   = 100    // Number of concurrent readers
	writeRequests    = 3000 // Total write requests
	readRequests     = 300  // Total read requests
	serverURL        = "http://localhost:8081"
)


// TestData generates test key-value pairs
func TestData(n int) []struct {
	Key   string
	Value string
} {
	data := make([]struct {
		Key   string
		Value string
	}, n)
	for i := 0; i < n; i++ {
		data[i] = struct {
			Key   string
			Value string
		}{
			Key:   fmt.Sprintf("key-%d", i),
			Value: fmt.Sprintf("value-%d", i),
		}
	}
	return data
}

// PerformWrite sends a PUT request to the server
func PerformWrite(key, value string) error {
	payload := map[string]string{
		"key":   key,
		"value": value,
	}
	data, _ := json.Marshal(payload)

	resp, err := http.Post(serverURL+"/put", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to write key: %s, status: %d", key, resp.StatusCode)
	}
	return nil
}

// PerformRead sends a GET request to the server
func PerformRead(key string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("%s/get?key=%s", serverURL, key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	//fmt.Println("Response from GET status code:", resp.StatusCode)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to read key: %s, status: %d", key, resp.StatusCode)
	}

	var values []string
	if err := json.NewDecoder(resp.Body).Decode(&values); err != nil {
		return "", fmt.Errorf("failed to decode response for key: %s", key)
	}

	// Assuming you only need the first value from the response
	if len(values) > 0 {
		return values[0], nil
	}

	return "", fmt.Errorf("no value found for key: %s", key)
}

// TestBitcask tests the Bitcask store with concurrent writes and reads
func TestBitcask() {
	data := TestData(writeRequests)

	var wg sync.WaitGroup
	var writeSuccessCount int32
	var readSuccessCount int32

	// Start write operations
	startTime := time.Now()
	for i := 0; i < writeConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < writeRequests; j += writeConcurrency {
				err := PerformWrite(data[j].Key, data[j].Value)
				if err != nil {
					log.Printf("Write error: %v", err)
				} else {
					atomic.AddInt32(&writeSuccessCount, 1)
				}
			}
		}(i)
	}
	wg.Wait()
	endTime := time.Now()
	// Start read operations
	startTime2 := time.Now()
	for i := 0; i < getConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < readRequests; j += getConcurrency {
				_, err := PerformRead(data[j].Key) // Assuming PerformRead returns (value, error)
				if err != nil {
					log.Printf("Read error: %v", err)
				} else {
					//log.Printf("Successfully read value: %s", value)
					atomic.AddInt32(&readSuccessCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	endTime2 := time.Now()

	// Print test results
	duration := endTime.Sub(startTime).Seconds()
	duration2 := endTime2.Sub(startTime2).Seconds()
	fmt.Printf("Total time: %.2fs\n", duration)
	fmt.Printf("Total time 2: %.2fs\n", duration2)
	fmt.Printf("Write throughput: %.2f ops/sec\n", float64(writeRequests)/duration)
	fmt.Printf("Read throughput: %.2f ops/sec\n", float64(readRequests)/duration2)
	fmt.Printf("Successful writes: %d\n", writeSuccessCount)
	fmt.Printf("Successful reads: %d\n", readSuccessCount)
}

func main() {
	
	
	fmt.Println("Starting Bitcask test...")
	TestBitcask()
}
