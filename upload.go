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
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	endpoint        = "storage-san-test-3:9000"
	accessKeyID     = "minioadmin"
	secretAccessKey = "minio-strong-secret"
	useSSL          = false
	bucketName      = "test-bucket"
	prefix          = "small/"
	workers         = 100
)

func parseSize(sizeStr string) (int64, error) {
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

func uploadWorker(minioClient *minio.Client, jobs <-chan int, wg *sync.WaitGroup, fileSize int64, logWriter io.Writer) {
	defer wg.Done()

	for i := range jobs {
		objectName := fmt.Sprintf("%sfile_%08d.txt", prefix, i)

		// Check if file exists
		_, err := minioClient.StatObject(context.Background(), bucketName, objectName, minio.StatObjectOptions{})
		if err == nil {
			fmt.Fprintf(logWriter, "[SKIP] %s already exists\n", objectName)
			continue
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
	}
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: <program> <start> <end> <size> (e.g. 0 10000 1K)")
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

	// Setup logging to stdout + log file
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

	jobs := make(chan int, workers*10)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go uploadWorker(minioClient, jobs, &wg, sizeBytes, logWriter)
	}

	for i := start; i < end; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	fmt.Fprintln(logWriter, "Upload completed.")
}
