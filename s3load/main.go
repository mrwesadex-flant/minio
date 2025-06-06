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
	"sync"
	"sync/atomic"
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
)

const (
	S3_ENDPOINT   = "http://172.17.1.70:9000"
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

	// // Create a Service Quotas client
	// client := servicequotas.NewFromConfig(cfg)

	// // Get S3 Quota (example - you'll need to find the correct QuotaCode)
	// input := &servicequotas.GetServiceQuotaInput{
	// 	QuotaCode:   aws.String("L-88118542-8E17-4C83-9634-761945DBA865"), // Replace with the correct QuotaCode
	// 	ServiceCode: aws.String("s3"),
	// }

	// output, err := client.GetServiceQuota(context.TODO(), input)
	// if err != nil {
	// 	panic("failed to get service quota: " + err.Error())
	// }

	// // Print the quota value
	// fmt.Printf("S3 Quota Value: %v\n", *output.Quota.Value)

	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	var wg sync.WaitGroup
	wg.Add(*workers)
	for w := range *workers {
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
	_ = atomic.AddInt64(&readSmallRunning, 1)
	start := time.Now()
	for {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
		})

		elapsed := time.Since(start)
		var err1 ratelimit.QuotaExceededError
		var err2 *retry.MaxAttemptsError
		if errors.As(err, &err1) || errors.As(err, &err2) {
			log.Printf("Failed to read small %s: %v in %s. Retrying", key, err, elapsed)
			continue
		} else if err == nil {
			break
		} else {
			log.Printf("Failed to read small %s: %v", key, err)
			return
		}
	}
	elapsed := time.Since(start)
	val := atomic.AddInt64(&readSmallRunning, -1)
	log.Printf("[W%d] Cycle %d: small %s in %s simultaneous %v", wid, cycle, key, elapsed, val)
}

func readLargeRange(client *s3.Client, wid, cycle int, key string, startByte, endByte int) {
	_ = atomic.AddInt64(&readLargeRunning, 1)
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	var out *s3.GetObjectOutput
	var err error
	start := time.Now()
	for {
		out, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(S3_BUCKET),
			Key:    aws.String(key),
			Range:  aws.String(rangeHeader),
		})

		elapsed := time.Since(start)
		var err1 ratelimit.QuotaExceededError
		var err2 *retry.MaxAttemptsError
		if errors.As(err, &err1) || errors.As(err, &err2) {
			log.Printf("Failed to read large %s: %v in %s. Retrying", key, err, elapsed)
			continue
		} else if err == nil {
			break
		} else {
			log.Printf("Failed to read large %s: %v", key, err)
			log.Printf("[W%d] Cycle %d: range %s (0 bytes) in %s (N/A MB/s)", wid, cycle, key, elapsed)
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
