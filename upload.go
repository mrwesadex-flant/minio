package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

const (
	endpoint           = "localhost:9000"
	accessKeyID        = "minioadmin"
	secretAccessKey    = "minio-strong-secret"
	useSSL             = false
	bucketName         = "test-bucket"
	prefix             = "small/"
	workers            = 100
	printProgressEvery = 1000 // How often to update the percentage output
)

func parseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	multiplier := int64(1)
	unit := sizeStr[len(sizeStr)-1]
	switch unit {
	case 'K', 'k':
		multiplier = 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		sizeStr = sizeStr[:len(sizeStr)-1]
	}
	val, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func uploadWorker(
	ctx context.Context,
	minioClient *minio.Client,
	jobs <-chan int,
	wg *sync.WaitGroup,
	contentLength int64,
	log *zap.Logger,
	counter *int64,
	total int64,
	startTime time.Time,
	force bool,
) {
	defer wg.Done()

	for job := range jobs {

		objectName := fmt.Sprintf("%sfile_%08d.txt", prefix, job)
		log := log.With(
			zap.String("name", objectName),
			zap.Int("job", job),
			zap.String("bucket", bucketName),
		)

		select {
		case <-ctx.Done():
			log.Debug("ctx is done", zap.Error(ctx.Err()))
			return
		default:
		}

		// Check if file exists
		if !force {
			_, err := minioClient.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
			if err == nil {
				log.Warn("already exists. Skipping")
				continue
			}
		}

		_, err := minioClient.PutObject(
			ctx,
			bucketName,
			objectName,
			io.LimitReader(rand.Reader, contentLength),
			contentLength,
			minio.PutObjectOptions{},
		)
		if err != nil {
			log.Error("upload error", zap.Error(err))
		} else {
			// Update and print progress
			done := atomic.AddInt64(counter, 1)
			elapsed := time.Since(startTime).Seconds()
			percent := float64(done) / float64(total) * 100
			log.Debug(
				"uploaded",
				zap.Int64("contentLength", contentLength),
				zap.Float64("percent", percent),
				zap.Int64("done", done),
				zap.Int64("total", total),
			)
			if done%printProgressEvery == 0 || done == int64(total) {
				rate := float64(done) / elapsed
				remaining := float64(total) - float64(done)
				eta := time.Duration(remaining/rate) * time.Second
				log.Sugar().Infof("[PROGRESS] %.2f%% (%d/%d), ETA: %s\n", percent, done, total, eta.Truncate(time.Second))
			}
		}

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

	log := zap.Must(zap.Config{Level: zap.NewAtomicLevelAt(zap.DebugLevel), OutputPaths: []string{
		"stderr", filepath.Join(".", "log.log"),
	}}.Build())

	start, err := strconv.Atoi(os.Args[1])
	if err != nil || start < 0 {
		log.Fatal("Invalid start index", zap.Error(err))
	}

	end, err := strconv.Atoi(os.Args[2])
	if err != nil || end > 50000000 || end <= start {
		log.Fatal("Invalid end index", zap.Error(err), zap.Int("start", start), zap.Int("end", end))
	}

	sizeBytes, err := parseSize(os.Args[3])
	if err != nil || sizeBytes <= 0 {
		log.Fatal("Invalid size", zap.Error(err), zap.Int64("size", sizeBytes))
	}

	var force bool
	overwriteFlag := os.Args[4]
	if overwriteFlag != "--force" && overwriteFlag != "" {
		log.Fatal("Invalid key. Only --force supported for third argument", zap.String("have", overwriteFlag))
	} else {
		force = true
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatal("MinIO connection error", zap.Error(err))
	}

	// Ensure bucket exists
	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	// Setup signal handling for Ctrl+C
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signalChan:
			log.Debug("Received shutdown signal. Cancelling context...")
			cancelFunc()
		case <-ctx.Done():
			// Context cancelled by other means
		}
	}()

	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		log.Fatal("BucketExists check failed", zap.Error(err))
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			log.Fatal("MakeBucket failed", zap.Error(err))
		}
	}

	total := int64(end - start)
	jobs := make(chan int, workers*10)
	var wg sync.WaitGroup
	var counter int64 = 0
	startTime := time.Now()

	for worker := range workers {
		wg.Add(1)
		go uploadWorker(ctx, minioClient, jobs, &wg, sizeBytes, log.With(zap.Int("worker", worker)), &counter, total, startTime, force)
	}

	for i := start; i < end; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	log.Info("Upload completed")
}
