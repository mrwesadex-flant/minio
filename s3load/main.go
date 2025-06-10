package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	// "github.com/aws/aws-sdk-go-v2/aws/config"

	"github.com/aws/aws-sdk-go-v2/credentials"
)

var (
	readSmallRunning int64 = 0
	readLargeRunning int64 = 0
	errorLogger      *log.Logger
)

const (
	S3_ENDPOINT   = "http://10.210.0.70:9000"
	S3_REGION     = "us-east-1"
	S3_BUCKET     = "test-bucket"
	S3_ACCESS_KEY = "minioadmin"
	S3_SECRET_KEY = "minio-strong-secret"
)

func main() {
	workers := flag.Int("workers", 10, "Number of parallel workers")
	smallStart := flag.Int("small-start", 0, "Start of small file range")
	smallEnd := flag.Int("small-end", 0, "End of small file range")
	cycles := flag.Int("cycles", 0, "Number of cycles per worker")
	flag.Parse()

	if *workers <= 0 || *smallStart < 0 || *smallEnd <= 0 || *smallStart >= *smallEnd {
		fmt.Println(`Parameter(s) error!
		
		usage: -workers=N -small-start=N -small-end=N [-cycles=N]
		Where:
		- workers = amount of simultaneously running workers. Must be > 0.
		- small-start = start of small file range (inclusive). Must be >= 0.
		- small-end = end of small file range (inclusive). Must be > small-start and > 0.
		- cycles = number of cycles per worker (default 0 - infinite)`)
		os.Exit(1)
	}

	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY, 0644)
	//logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open log file: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()

	errorLogger = log.New(logFile, "ERROR: ", log.LstdFlags|log.Lmicroseconds)

	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Setup signal handling for Ctrl+C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signalChan:
			fmt.Println("\nReceived shutdown signal. Cancelling context...")
			cancelFunc() // Cancel the context when signal is received
		case <-ctx.Done():
			// Context cancelled by other means
		}
	}()

	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	randSeed := time.Now().UnixNano()
	log.Printf("Rand seed: %v", randSeed)
	rand.Seed(randSeed)

	cfg, err := config.LoadDefaultConfig(ctx,
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
		errorLogger.Printf("Failed to load AWS config: %v", err)
		os.Exit(1)
	}

	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	var wg sync.WaitGroup
	wg.Add(*workers)
	for w := range *workers {
		go func(id int) {
			defer wg.Done()
			runWorker(ctx, id, s3client, *smallStart, *smallEnd, *cycles)
		}(w)
	}
	wg.Wait()
}

func runWorker(ctx context.Context, id int, client *s3.Client, smallStart, smallEnd, cycles int) {
	largeMaxIndex := 50000
	largeSize := 100 * 1024 * 1024
	if cycles == 0 {
		cycles = 9223372036854775807
		// Set to a very large number to simulate infinite cycles
	}

	for c := 1; c <= cycles; c++ {
		select {
		case <-ctx.Done():
			log.Printf("[W%d] Context canceled. Exiting worker loop.", id)
			return
		default:
			// Proceed with the current cycle
		}

		var wg sync.WaitGroup
		// Read 1 random small file
		smallIdx := rand.Intn(smallEnd-smallStart+1) + smallStart
		smallFile := fmt.Sprintf("small/file_%0*d.txt", smallFilenameWidth(smallIdx), smallIdx)
		readSmallFile(ctx, client, id, c, smallFile)

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
				select {
				case <-ctx.Done():
					log.Printf("[W%d] Context canceled. Exiting small file read.", id)
					return
				default:
					readSmallFile(ctx, client, id, c, smallFile)
				}

				fileIdx := rand.Intn(largeMaxIndex) + 1
				start := rand.Intn(largeSize - 1024*1024)
				end := start + 1024*1024 - 1
				file := fmt.Sprintf("large/largefile_%05d.bin", fileIdx)
				wg.Add(1)
				go func(f string, s, e int) {
					defer wg.Done()
					select {
					case <-ctx.Done():
						log.Printf("[W%d] Context canceled. Exiting large range read.", id)
						return
					default:
						readLargeRange(ctx, client, id, c, f, s, e)
					}
				}(file, start, end)
			}(f)
		}
		wg.Wait()
	}
}

func smallFilenameWidth(n int) int {
	if n < 10000000 {
		return 7
	}
	return 8
}

func readSmallFile(ctx context.Context, client *s3.Client, wid, cycle int, key string) {
	_ = atomic.AddInt64(&readSmallRunning, 1)
	start := time.Now()
	for {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
		})

		elapsed := time.Since(start)
		var err1 ratelimit.QuotaExceededError
		var err2 *retry.MaxAttemptsError
		if errors.As(err, &err1) || errors.As(err, &err2) {
			errorLogger.Printf("Failed to read small %s: %v in %s. Retrying", key, err, elapsed)
			continue
		} else if err == nil {
			break
		} else {
			errorLogger.Printf("Failed to read small %s: %v", key, err)
			return
		}
	}
	elapsed := time.Since(start)
	val := atomic.AddInt64(&readSmallRunning, -1)
	log.Printf("[W%d] Cycle %d: small %s in %s simultaneous %v", wid, cycle, key, elapsed, val)
}

func readLargeRange(ctx context.Context, client *s3.Client, wid, cycle int, key string, startByte, endByte int) {
	_ = atomic.AddInt64(&readLargeRunning, 1)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	var out *s3.GetObjectOutput
	var err error
	start := time.Now()
	for {
		out, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
			Range:  aws.String(rangeHeader),
		})

		elapsed := time.Since(start)
		var err1 ratelimit.QuotaExceededError
		var err2 *retry.MaxAttemptsError
		if errors.As(err, &err1) || errors.As(err, &err2) {
			errorLogger.Printf("Failed to read large %s: %v in %s. Retrying", key, err, elapsed)
			continue
		} else if err == nil {
			break
		} else {
			errorLogger.Printf("Failed to read large %s: %v", key, err)
			errorLogger.Printf("[W%d] Cycle %d: range %s (0 bytes) in %s (N/A MB/s)", wid, cycle, key, elapsed)
			return
		}
	}

	elapsed := time.Since(start)

	defer out.Body.Close()
	readBytes, _ := io.Copy(io.Discard, out.Body)
	speed := float64(readBytes) / elapsed.Seconds() / 1024 / 1024
	ret := atomic.AddInt64(&readLargeRunning, -1)
	log.Printf("[W%d] Cycle %d: range %s (%d bytes) in %s (%.2f MB/s) simultaneous %v", wid, cycle, key, readBytes, elapsed, speed, ret)
}
