package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	S3_ENDPOINT   = "http://storage-san-test-0:9000"
	S3_REGION     = "us-east-1"
	S3_BUCKET     = "test-bucket"
	S3_ACCESS_KEY = "minioadmin"
	S3_SECRET_KEY = "minio-strong-secret"
)

func main() {
	workers := flag.Int("workers", 0, "Number of parallel workers")
	smallStart := flag.Int("small-start", 0, "Start of small file range")
	smallEnd := flag.Int("small-end", 0, "End of small file range")
	cycles := flag.Int("cycles", 10, "Number of cycles per worker")
	flag.Parse()

	if *workers <= 0 || *smallStart <= 0 || *smallEnd <= 0 {
		fmt.Println("usage: -workers=N -small-start=N -small-end=N [-cycles=N]")
		os.Exit(1)
	}

	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	randSeed := time.Now().UnixNano()
	log.Printf("Rand seed: %v", randSeed)
	rand.Seed(randSeed)

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(S3_REGION),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(S3_ACCESS_KEY, S3_SECRET_KEY, "")),
		config.WithHTTPClient(&http.Client{Timeout: 10 * time.Second}),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               S3_ENDPOINT,
					SigningRegion:     S3_REGION,
					HostnameImmutable: true,
				}, nil
			}),
		),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	var wg sync.WaitGroup
	wg.Add(*workers)
	for w := 0; w < *workers; w++ {
		go func(id int) {
			defer wg.Done()
			runWorker(&wg, id, s3client, *smallStart, *smallEnd, *cycles)
		}(w)
	}
	wg.Wait()
}

func runWorker(wg *sync.WaitGroup, id int, client *s3.Client, smallStart, smallEnd, cycles int) {
	largeMaxIndex := 50000
	largeSize := 100 * 1024 * 1024
	for c := 1; c <= cycles; c++ {
		// Read 1 random small file
		smallIdx := rand.Intn(smallEnd-smallStart+1) + smallStart
		smallFile := fmt.Sprintf("small/file_%0*d.txt", smallFilenameWidth(smallIdx), smallIdx)
		readSmallFile(client, id, c, smallFile)

		// Read 1000 random small files in parallel
		smallFiles := make([]string, 1000)
		for i := range smallFiles {
			idx := rand.Intn(smallEnd-smallStart+1) + smallStart
			smallFiles[i] = fmt.Sprintf("small/file_%0*d.txt", smallFilenameWidth(idx), idx)
		}
		wg.Add(len(smallFiles))
		for _, f := range smallFiles {
			go func(smallFile string) {
				defer wg.Done()
				readSmallFile(client, id, c, smallFile)

				fileIdx := rand.Intn(largeMaxIndex) + 1
				start := rand.Intn(largeSize - 1024*1024)
				end := start + 1024*1024 - 1
				file := fmt.Sprintf("large/largefile_%05d.bin", fileIdx)
				wg.Add(1)
				go func(f string, s, e int) {
					defer wg.Done()
					readLargeRange(client, id, c, f, s, e)
				}(file, start, end)
			}(f)
		}
	}
}

func smallFilenameWidth(n int) int {
	if n < 10000000 {
		return 7
	}
	return 8
}

func readSmallFile(client *s3.Client, wid, cycle int, key string) {
	start := time.Now()
	_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(S3_BUCKET),
		Key:    aws.String(key),
	})
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("Failed to read %s: %v", key, err)
	} else {
		log.Printf("[W%d] Cycle %d: small %s in %s", wid, cycle, key, elapsed)
	}
}

func readLargeRange(client *s3.Client, wid, cycle int, key string, startByte, endByte int) {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	start := time.Now()
	out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(S3_BUCKET),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("Failed to read %s: %v", key, err)
		log.Printf("[W%d] Cycle %d: range %s (0 bytes) in %s (NaN MB/s)", wid, cycle, key, elapsed)
		return
	}
	defer out.Body.Close()
	readBytes, _ := io.Copy(io.Discard, out.Body)
	speed := float64(readBytes) / elapsed.Seconds() / 1024 / 1024
	log.Printf("[W%d] Cycle %d: range %s (%d bytes) in %s (%.2f MB/s)", wid, cycle, key, readBytes, elapsed, speed)
}
