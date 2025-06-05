package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	endpoint  = "http://storage-san-test-0:9000"
	accessKey = "minioadmin"
	secretKey = "strong-minio-secret"
	bucket    = "test-bucket"
	rangeSize = 100 * 1024 // 100KB
)

var (
	smallStart, smallEnd int
	largeStart, largeEnd int
)

func main() {
	var (
		numWorkers int
		cycles     int
	)

	flag.IntVar(&numWorkers, "workers", 0, "Number of parallel workers (required)")
	flag.IntVar(&smallStart, "small-start", -1, "Start index of small file range (required)")
	flag.IntVar(&smallEnd, "small-end", -1, "End index of small file range (required)")
	flag.IntVar(&cycles, "cycles", 10, "Number of read cycles per worker (default: 10)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: s3load -workers=N -small-start=N -small-end=N [-cycles=N]

Parameters:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if numWorkers == 0 || smallStart < 0 || smallEnd < 0 {
		flag.Usage()
		os.Exit(1)
	}
	if smallStart > smallEnd {
		log.Fatalf("Invalid small file range: start %d > end %d", smallStart, smallEnd)
	}

	// init logger
	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// compute large file range
	largeStart = smallStart / 1000
	largeEnd = smallEnd / 1000
	if largeStart == 0 {
		largeStart = 1
	}

	client := newS3Client()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			workerLoop(client, id, rng, cycles)
		}(i)
	}
	wg.Wait()
}

type customEndpointResolver struct{}

func (r customEndpointResolver) ResolveEndpoint(service, region string, _ ...interface{}) (aws.Endpoint, error) {
	return aws.Endpoint{
		URL:           endpoint,
		SigningRegion: "us-east-1",
	}, nil
}

func newS3Client() *s3.Client {
	resolver := customEndpointResolver{}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 3
			})
		}),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func workerLoop(client *s3.Client, id int, rng *rand.Rand, cycles int) {
	log.Printf("Worker %d started with %d cycles", id, cycles)

	for c := 0; c < cycles; c++ {
		ctx := context.Background()

		// Step 1: single small file
		smallKey := randomSmallFilename(rng)
		if smallKey == "" {
			continue
		}
		_, dur := readObject(ctx, client, smallKey, "")
		log.Printf("[W%d] Cycle %d: small %s in %.2fms", id, c+1, smallKey, dur.Seconds()*1000)

		// Step 2: 100 small files
		var wg sync.WaitGroup
		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			keys[i] = randomSmallFilename(rng)
		}
		for _, key := range keys {
			if key == "" {
				continue
			}
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				_, d := readObject(ctx, client, k, "")
				log.Printf("[W%d] Cycle %d: small %s in %.2fms", id, c+1, k, d.Seconds()*1000)
			}(key)
		}
		wg.Wait()

		// Step 3: 100 range reads
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				largeKey := randomLargeFilename(rng)
				if largeKey == "" {
					return
				}
				start := rng.Intn(10*1024*1024*1024 - rangeSize)
				rangeHeader := fmt.Sprintf("bytes=%d-%d", start, start+rangeSize-1)
				log.Printf("DEBUG: reading %s with range %s", largeKey, rangeHeader)

				n, d := readObject(ctx, client, largeKey, rangeHeader)
				mbps := float64(n) / (d.Seconds() * 1024 * 1024)
				log.Printf("[W%d] Cycle %d: range %s (%d bytes) in %.2fms (%.2f MB/s)", id, c+1, largeKey, n, d.Seconds()*1000, mbps)
			}()
		}
		wg.Wait()

		time.Sleep(200 * time.Millisecond)
	}
}

func readObject(ctx context.Context, client *s3.Client, key, rangeHeader string) (int64, time.Duration) {
	start := time.Now()

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if rangeHeader != "" {
		input.Range = aws.String(rangeHeader)
	}

	resp, err := client.GetObject(ctx, input)
	if err != nil {
		log.Printf("Failed to read %s: %v", key, err)
		return 0, 0
	}
	defer resp.Body.Close()

	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		log.Printf("Failed to read body of %s: %v", key, err)
		return 0, 0
	}

	return n, time.Since(start)
}

func randomSmallFilename(rng *rand.Rand) string {
	if smallEnd < smallStart {
		return ""
	}
	n := smallStart + rng.Intn(smallEnd-smallStart+1)
	return fmt.Sprintf("small/file_%05d.txt", n)
}

func randomLargeFilename(rng *rand.Rand) string {
	if largeEnd < largeStart {
		return ""
	}
	n := largeStart + rng.Intn(largeEnd-largeStart+1)
	return fmt.Sprintf("large/largefile_%d.bin", n)
}
