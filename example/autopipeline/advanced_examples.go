package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Example: Rate-limited API with AutoPipeline
func ExampleRateLimitedAPI() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 20,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Simulate API that processes requests with rate limiting
	const requestsPerSecond = 10000
	const duration = 1 * time.Second

	ticker := time.NewTicker(duration / time.Duration(requestsPerSecond))
	defer ticker.Stop()

	var wg sync.WaitGroup
	count := 0
	start := time.Now()

	for range ticker.C {
		if count >= requestsPerSecond {
			break
		}
		wg.Add(1)
		idx := count
		go func() {
			defer wg.Done()
			// Each API request increments counter and logs
			ap.Incr(ctx, "api:requests")
			ap.LPush(ctx, "api:log", fmt.Sprintf("request-%d", idx))
		}()
		count++
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Rate-limited API: %d requests in %v (%.0f req/sec)\n",
		requestsPerSecond, elapsed, float64(requestsPerSecond)/elapsed.Seconds())
}

// Example: Worker pool with AutoPipeline
func ExampleWorkerPool() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         200,
			MaxConcurrentBatches: 50,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Worker pool pattern
	const numWorkers = 50
	const jobsPerWorker = 200
	const totalJobs = numWorkers * jobsPerWorker

	jobs := make(chan int, numWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for jobID := range jobs {
				// Process job: update status and increment counter
				ap.Set(ctx, fmt.Sprintf("job:%d:status", jobID), "completed", 0)
				ap.Incr(ctx, fmt.Sprintf("worker:%d:processed", workerID))
			}
		}(w)
	}

	// Send jobs
	for i := 0; i < totalJobs; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Worker pool: %d workers processed %d jobs in %v (%.0f jobs/sec)\n",
		numWorkers, totalJobs, elapsed, float64(totalJobs)/elapsed.Seconds())
}

// Example: Batch processing with size-based flushing
func ExampleBatchProcessing() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         500, // Flush every 500 commands
			MaxConcurrentBatches: 10,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Process data in batches
	const totalRecords = 10000
	const batchSize = 500

	start := time.Now()
	var wg sync.WaitGroup

	for batch := 0; batch < totalRecords/batchSize; batch++ {
		wg.Add(1)
		go func(batchID int) {
			defer wg.Done()
			for i := 0; i < batchSize; i++ {
				recordID := batchID*batchSize + i
				ap.HSet(ctx, fmt.Sprintf("record:%d", recordID),
					"batch", batchID,
					"index", i,
					"timestamp", time.Now().Unix())
			}
		}(batch)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Batch processing: %d records in %v (%.0f records/sec)\n",
		totalRecords, elapsed, float64(totalRecords)/elapsed.Seconds())
}

// Example: High-throughput metrics collection
func ExampleMetricsCollection() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:             "localhost:6379",
		PipelinePoolSize: 30,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         1000,
			MaxConcurrentBatches: 100,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Simulate high-throughput metrics collection
	const numMetrics = 100000
	const numSources = 100

	var wg sync.WaitGroup
	var processed atomic.Int64

	start := time.Now()

	// Each source sends metrics continuously
	for source := 0; source < numSources; source++ {
		wg.Add(1)
		go func(sourceID int) {
			defer wg.Done()
			metricsPerSource := numMetrics / numSources
			for i := 0; i < metricsPerSource; i++ {
				timestamp := time.Now().Unix()
				ap.ZAdd(ctx, fmt.Sprintf("metrics:source:%d", sourceID),
					redis.Z{Score: float64(timestamp), Member: fmt.Sprintf("value-%d", i)})
				ap.Incr(ctx, "metrics:total")
				processed.Add(1)
			}
		}(source)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Metrics collection: %d metrics from %d sources in %v (%.0f metrics/sec)\n",
		processed.Load(), numSources, elapsed, float64(processed.Load())/elapsed.Seconds())
}

// Example: Session management with AutoPipeline
func ExampleSessionManagement() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 50,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Simulate session creation/update from web server
	const numSessions = 10000

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numSessions; i++ {
		wg.Add(1)
		go func(sessionID int) {
			defer wg.Done()
			sessionKey := fmt.Sprintf("session:%d", sessionID)

			// Create session with multiple fields
			ap.HSet(ctx, sessionKey,
				"user_id", sessionID,
				"created_at", time.Now().Unix(),
				"last_seen", time.Now().Unix(),
				"ip", "192.168.1.1")

			// Set expiration
			ap.Expire(ctx, sessionKey, 24*time.Hour)

			// Track active sessions
			ap.SAdd(ctx, "sessions:active", sessionID)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Session management: %d sessions created in %v (%.0f sessions/sec)\n",
		numSessions, elapsed, float64(numSessions)/elapsed.Seconds())
}

// Example: Cache warming with AutoPipeline
func ExampleCacheWarming() {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:                    "localhost:6379",
		PipelinePoolSize:        50,
		PipelineReadBufferSize:  512 * 1024,
		PipelineWriteBufferSize: 512 * 1024,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         1000,
			MaxConcurrentBatches: 50,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Warm cache with product data
	const numProducts = 50000

	var wg sync.WaitGroup
	start := time.Now()

	// Use worker pool for cache warming
	const numWorkers = 100
	products := make(chan int, numWorkers*2)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for productID := range products {
				// Cache product data
				ap.HSet(ctx, fmt.Sprintf("product:%d", productID),
					"name", fmt.Sprintf("Product %d", productID),
					"price", productID*10,
					"stock", productID%100,
					"category", productID%10)

				// Add to category index
				ap.SAdd(ctx, fmt.Sprintf("category:%d:products", productID%10), productID)

				// Add to price-sorted set
				ap.ZAdd(ctx, "products:by_price",
					redis.Z{Score: float64(productID * 10), Member: productID})
			}
		}()
	}

	// Send products to workers
	for i := 0; i < numProducts; i++ {
		products <- i
	}
	close(products)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Cache warming: %d products cached in %v (%.0f products/sec)\n",
		numProducts, elapsed, float64(numProducts)/elapsed.Seconds())
}

