package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	endpoint   = "http://localhost:9000"
	region     = "us-east-1"
	bucketName = "test-bucket"
)

func main() {
	// Logging to file
	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		os.Exit(1)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Flags
	workers := flag.Int("workers", 0, "Number of parallel workers")
	smallStart := flag.Int("small-start", 0, "Start of small file index range")
	smallEnd := flag.Int("small-end", 0, "End of small file index range")
	cycles := flag.Int("cycles", 10, "Number of read cycles (default: 10)")
	flag.Parse()

	if *workers <= 0 || *smallStart <= 0 || *smallEnd <= 0 {
		fmt.Println("usage: -workers=N -small-start=FROM -small-end=TO [-cycles=N]")
		os.Exit(1)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Load AWS S3 config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					SigningRegion:     region,
					HostnameImmutable: true,
				}, nil
			}),
		),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Important for MinIO
	})

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for c := 1; c <= *cycles; c++ {
				// Small file
				smallIdx := r.Intn(*smallEnd-*smallStart+1) + *smallStart
				var smallKey string
				if smallIdx < 5_000_000 {
					smallKey = fmt.Sprintf("small/file_%07d.txt", smallIdx)
				} else {
					smallKey = fmt.Sprintf("small/file_%08d.txt", smallIdx)
				}
				start := time.Now()
				_, err := readObject(context.TODO(), client, smallKey, nil)
				dur := time.Since(start)
				if err != nil {
					log.Printf("Failed to read %s: %v", smallKey, err)
				}
				log.Printf("[W%d] Cycle %d: small %s in %s", workerID, c, smallKey, dur.Truncate(time.Millisecond))

				// Large file with byte range
				largeIdx := r.Intn(*smallEnd-*smallStart+1) + *smallStart
				largeFileNum := largeIdx / 1000
				rangeOffset := int64(r.Intn(100*1024*1024 - 1024*1024)) // up to ~100MB file
				rangeBytes := int64(1024 * 1024)                        // read 1MB
				rangeHeader := fmt.Sprintf("bytes=%d-%d", rangeOffset, rangeOffset+rangeBytes-1)
				largeKey := fmt.Sprintf("large/largefile_%05d.bin", largeFileNum)
				start = time.Now()
				size, err := readObject(context.TODO(), client, largeKey, &rangeHeader)
				dur = time.Since(start)
				mbps := float64(*size) / dur.Seconds() / 1024 / 1024
				if err != nil {
					log.Printf("Failed to read %s: %v", largeKey, err)
				}
				log.Printf("[W%d] Cycle %d: range %s (%d bytes) in %s (%.2f MB/s)", workerID, c, largeKey, *size, dur.Truncate(time.Millisecond), mbps)
			}
		}(i)
	}
	wg.Wait()
}

func readObject(ctx context.Context, client *s3.Client, key string, byteRange *string) (*int64, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	if byteRange != nil {
		input.Range = aws.String(*byteRange)
	}

	resp, err := client.GetObject(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return aws.Int64(0), nil // skip missing file silently
		}
		return aws.Int64(0), err
	}
	defer resp.Body.Close()

	var n int64
	buf := make([]byte, 32*1024)
	for {
		read, err := resp.Body.Read(buf)
		n += int64(read)
		if err != nil {
			break
		}
	}
	return aws.Int64(n), nil
}
