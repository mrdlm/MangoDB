package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
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
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	runWriteBenchmark(writer, reader)
	// runReadBenchmark(writer, reader)

	defer conn.Close()
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
