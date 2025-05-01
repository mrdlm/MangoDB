package main

import (
	"bufio"
	"fmt"
	_ "index/suffixarray"
	"net"
	"sync"
	"time"
)

const (
	addr = "localhost:8080"
)

func main() {
	fmt.Println("Starting benchmark with random keys and values...\n\n")
	runMultiConnectionReadBenchmark(100, 5000) // 10 connections, 1000 requests per connection
}

func runMultiConnectionReadBenchmark(numConnections, numRequestsPerConnection int) {
	fmt.Printf("Starting multi-connection read benchmark with %d connection, and %d requests per connection\n\n",
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

				for k := 0; k < requestsInBatch; k++ {
					cmd := fmt.Sprintf("GET Hey\n")
					_, err := writer.WriteString(cmd)

					if err != nil {
						fmt.Println("Read failed:", err)
						break
					}
				}

				err = writer.Flush()
				if err != nil {
					fmt.Println("Flush failed: ", err)
					break
				}

				for k := 0; k < requestsInBatch; k++ {
					_, err := reader.ReadString('\n')

					/*
						if response != "there\n" {
							fmt.Printf("Read value mismatch from expectation: %s", response)
							continue
						}
					*/

					if err != nil {
						fmt.Println("Read failed on connection after successful write:", err)
						continue
					}

					// _, err = reader.ReadString('\n') // read empty line away
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
	fmt.Printf("Read Throughput: %v ops/s\n\n",
		float64(successfulWrites)/duration.Seconds())
}
