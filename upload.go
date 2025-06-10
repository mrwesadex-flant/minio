package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	endpoint        = "localhost:9000"
	accessKeyID     = "minioadmin"
	secretAccessKey = "minio-strong-secret"
	useSSL          = false
	bucketName      = "test-bucket"
	prefix          = "small/"
	workers         = 100
	progressStep    = 1000 // How often to update the percentage output
)

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	mult := int64(1)
	unit := sizeStr[len(sizeStr)-1]
	switch unit {
	case 'K', 'k':
		mult = 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'M', 'm':
		mult = 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'G', 'g':
		mult = 1024 * 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	}
	val, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return val * mult, nil
}

func uploadWorker(minioClient *minio.Client, jobs <-chan int, wg *sync.WaitGroup, fileSize int64, logWriter io.Writer, counter *int64, total int, startTime time.Time, mu *sync.Mutex, force bool) {
	defer wg.Done()

	for i := range jobs {
		objectName := fmt.Sprintf("%sfile_%08d.txt", prefix, i)

		// Check if file exists
		_, err := minioClient.StatObject(context.Background(), bucketName, objectName, minio.StatObjectOptions{})
		if err == nil {
			logLine := fmt.Sprintf("[WARNING] %s already exists", objectName)
			fmt.Fprint(logWriter, logLine)
			if force {
				fmt.Fprint(logWriter, "... overwriting\n")
			} else {
				fmt.Fprint(logWriter, "... skipping\n")
				continue
			}

		}

		content := make([]byte, fileSize)
		if _, err := io.ReadFull(rand.Reader, content); err != nil {
			fmt.Fprintf(logWriter, "[ERROR] generating %s: %v\n", objectName, err)
			continue
		}

		_, err = minioClient.PutObject(
			context.Background(),
			bucketName,
			objectName,
			bytes.NewReader(content),
			fileSize,
			minio.PutObjectOptions{},
		)
		if err != nil {
			fmt.Fprintf(logWriter, "[ERROR] uploading %s: %v\n", objectName, err)
		} else {
			fmt.Fprintf(logWriter, "[OK] uploaded %s (%d bytes)\n", objectName, fileSize)
		}

		// Update and print progress
		mu.Lock()
		*counter++
		done := *counter
		if done%progressStep == 0 || done == int64(total) {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(done) / elapsed
			remaining := float64(total) - float64(done)
			eta := time.Duration(remaining/rate) * time.Second
			percent := float64(done) / float64(total) * 100
			fmt.Fprintf(logWriter, "[PROGRESS] %.2f%% (%d/%d), ETA: %s\n", percent, done, total, eta.Truncate(time.Second))
		}
		mu.Unlock()
	}
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println(`Usage: <program> <start> <end> <size> [--force]
Example: 0 10000 1K
- <start>: Starting index of files to upload
- <end>: Ending index of files to upload
- <size>: Size of each file (e.g., 1K, 10M, 1G)
- [--force]: Optional flag to overwrite existing files`)
		os.Exit(1)
	}

	start, err := strconv.Atoi(os.Args[1])
	if err != nil || start < 0 {
		log.Fatalf("Invalid start index: %v", err)
	}

	end, err := strconv.Atoi(os.Args[2])
	if err != nil || end > 50000000 || end <= start {
		log.Fatalf("Invalid end index: %v", err)
	}

	sizeBytes, err := parseSize(os.Args[3])
	if err != nil || sizeBytes <= 0 {
		log.Fatalf("Invalid size: %v", err)
	}

	var force bool
	overwriteFlag := os.Args[4]
	if overwriteFlag != "--force" && overwriteFlag != "" {
		log.Fatalf("Invalid key. Must be --force: %v", overwriteFlag)
	} else {
		force = true
	}

	// Logging
	logFile, err := os.OpenFile(filepath.Join(".", "log.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log.log: %v", err)
	}
	defer logFile.Close()
	logWriter := io.MultiWriter(os.Stdout, logFile)

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalf("MinIO connection error: %v", err)
	}

	// Ensure bucket exists
	ctx := context.Background()
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		log.Fatalf("BucketExists check failed: %v", err)
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			log.Fatalf("MakeBucket failed: %v", err)
		}
	}

	total := end - start
	jobs := make(chan int, workers*10)
	var wg sync.WaitGroup
	var counter int64 = 0
	var mu sync.Mutex
	startTime := time.Now()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go uploadWorker(minioClient, jobs, &wg, sizeBytes, logWriter, &counter, total, startTime, &mu, force)
	}

	for i := start; i < end; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	fmt.Fprintln(logWriter, "Upload completed.")
}
