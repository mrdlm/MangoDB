package main

import (
	"bufio"
	"fmt"
	_ "index/suffixarray"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	addr = "localhost:8080"
)

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func main() {
	fmt.Println("Starting benchmark with random keys and values...\n\n")
	runMultiConnectionBenchmark(200, 1000) // 10 connections, 1000 requests per connection
}

func runMultiConnectionBenchmark(numConnections, numRequestsPerConnection int) {
	fmt.Printf("Starting multi-connection benchmark with %d connection, and %d requests per connection\n\n",
		numConnections, numRequestsPerConnection)

	var wg sync.WaitGroup
	start := time.Now()

	var failedConnection, successfulWrites int
	var countMutex sync.Mutex

	for i := 0; i < numConnections; i++ {
		wg.Add(1)

		go func(connID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fmt.Println("Connection failed:", err)

				// thread safe counting
				countMutex.Lock()
				failedConnection++
				countMutex.Unlock()

				return
			}

			defer conn.Close()

			writer := bufio.NewWriter(conn)
			reader := bufio.NewReader(conn)

			localSuccessCount := 0
			const pipelineSize = 100

			for j := 0; j < numRequestsPerConnection; j += pipelineSize {

				requestsInBatch := pipelineSize
				if j+pipelineSize > numRequestsPerConnection {
					requestsInBatch = numRequestsPerConnection - j
				}

				batchKeys := make([]string, requestsInBatch)

				for k := 0; k < requestsInBatch; k++ {
					key := generateRandomString(5)
					value := generateRandomString(100)

					batchKeys[k] = key
					cmd := fmt.Sprintf("PUT %s %s\n", key, value)
					_, err := writer.WriteString(cmd)

					if err != nil {
						fmt.Println("Write failed:", err)
						return
					}
				}

				err = writer.Flush()
				if err != nil {
					fmt.Println("Flush failed: ", err)
					return
				}

				for k := 0; k < requestsInBatch; k++ {
					response, err := reader.ReadString('\n')

					if err != nil {
						fmt.Println("Read failed on connection after successful write:", err)
						break
					}

					response = response[:len(response)-1]
					if response != batchKeys[k] {
						fmt.Printf("Read values mismatch %s, %s\n", batchKeys[k], response)
						continue
					}

					localSuccessCount++
				}
			}

			countMutex.Lock()
			successfulWrites += localSuccessCount
			countMutex.Unlock()
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("Benchmarking completed in %s\n", time.Since(start))
	fmt.Printf("Total successful writes: %d out of %d attempted\n",
		successfulWrites, numRequestsPerConnection*numConnections)
	fmt.Printf("Write Throughput: %v ops/s\n\n",
		float64(successfulWrites)/duration.Seconds())
}

func runSingleConnectionBenchmark() {
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	runWriteBenchmark(writer, reader)
}

func runWriteBenchmark(writer *bufio.Writer, reader *bufio.Reader) {
	start := time.Now()
	numReqs := 10000
	for i := 0; i < numReqs; i++ {
		key := generateRandomString(5)
		value := generateRandomString(100)

		cmd := fmt.Sprintf("PUT %s %s\n", key, value)
		_, err := writer.WriteString(cmd)

		if err != nil {
			panic(err)
		}
		err = writer.Flush()
		if err != nil {
			return
		}
		_, err = reader.ReadString('\n')
		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)
	fmt.Printf("Total time: %v, for %v records\n", duration, numReqs)
	fmt.Printf("Write Throughput: %v ops/s\n\n", float64(numReqs)/duration.Seconds())
}

func runReadBenchmark(writer *bufio.Writer, reader *bufio.Reader) {
	start := time.Now()
	numReqs := 1000
	for i := 0; i < numReqs; i++ {
		key := "hello"

		cmd := fmt.Sprintf("GET %s \n", key)
		_, err := writer.WriteString(cmd)

		if err != nil {
			panic(err)
		}
		err = writer.Flush()

		if err != nil {
			return
		}
		_, err = reader.ReadString('\n')
		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)
	fmt.Printf("Total time: %v, for %v records\n", duration, numReqs)
	fmt.Printf("Read throughput: %v ops/s\n\n", float64(numReqs)/duration.Seconds())
}
