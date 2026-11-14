package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"news-fetcher/internal/dedup"
	"news-fetcher/internal/fetcher"
	"news-fetcher/internal/model"
	"news-fetcher/internal/provider"
	"news-fetcher/internal/publisher"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	dao "dao/golang"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found or error reading it. Relying on system environment variables.")
	}

	configs, err := dao.LoadNewsProviderConfigs()
	if err != nil {
		panic(err)
	}

	// Initialize durable RabbitMQ publisher (with Dead-Letter Exchange & DLQ)
	rmqCfg := fetchRabbitMQConfig()
	pub, err := publisher.NewRabbitMQPublisher(rmqCfg)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ publisher: %v", err)
	}
	defer pub.Close()

	// Initialize L1 Hybrid Deduplicator (SHA-256 + 64-bit SimHash with 24h TTL)
	deduplicator := initDeduplicator()

	// Graceful OS signal shutdown context for continuous streaming workers
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	normalisedNewsfeed := make(chan *model.Article) // channel for normalised articles

	var wg sync.WaitGroup
	// 1. Start zero-cost RSS feed providers
	fetchFromRSSFeeds(ctx, configs, normalisedNewsfeed, &wg)
	// 2. Start NewsDataIO providers (if configured and API key is available)
	fetchFromNewsDataIO(ctx, configs, normalisedNewsfeed, &wg)

	// Close the channel when all workers (fetchers) have exited on ctx cancellation.
	go func() {
		wg.Wait()
		close(normalisedNewsfeed)
	}()

	for article := range normalisedNewsfeed {
		fmt.Println("Received:", article.Title)

		// L1 Deduplication check (Exact SHA-256 + 64-bit SimHash near-duplicate)
		dedupCtx, dedupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		isDup, reason, dedupErr := deduplicator.IsArticleDuplicate(dedupCtx, article)
		dedupCancel()
		if dedupErr != nil {
			log.Printf("Warning: L1 deduplication check failed (proceeding to publish): %v", dedupErr)
		} else if isDup {
			fmt.Printf("Skipping duplicate article [L1 %s hit]: %s\n", reason, article.Title)
			continue
		}

		pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pub.Publish(pubCtx, article)
		pubCancel()
		if err != nil {
			log.Printf("Failed to publish article: %v", err)
			continue
		}
		fmt.Printf("Published article: %s (trace_id=%s)\n", article.Title, article.TraceId)
	}

	fmt.Println("All articles processed. Exiting gracefully.")
}

func initDeduplicator() dedup.Deduplicator {
	ttlSeconds := 86400 // Default 24 hours
	if rawTTL := os.Getenv("L1_DEDUP_TTL_SECONDS"); rawTTL != "" {
		if parsed, err := strconv.Atoi(rawTTL); err == nil && parsed > 0 {
			ttlSeconds = parsed
		}
	}
	ttl := time.Duration(ttlSeconds) * time.Second

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisDedup := dedup.NewRedisDeduplicator(redisHost, redisPort, ttl)

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := redisDedup.Ping(pingCtx); err != nil {
		log.Printf("Redis unavailable (%v); falling back to InMemoryDeduplicator (TTL=%s)", err, ttl)
		return dedup.NewInMemoryDeduplicator(ttl)
	}

	log.Printf("Connected to Redis L1 Deduplicator (SHA-256 + SimHash, TTL=%s)", ttl)
	return redisDedup
}

func fetchRabbitMQConfig() publisher.RabbitMQConfig {
	configDir := os.Getenv("CONFIG_DIR")
	if configDir == "" {
		configDir = "../configs"
	}
	filePath := fmt.Sprintf("%s/rabbit_mq.json", configDir)

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var data map[string]string
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		panic(err)
	}

	rmqUser := os.Getenv("RABBITMQ_USER")
	if rmqUser == "" {
		rmqUser = "guest"
	}
	rmqPass := os.Getenv("RABBITMQ_PASSWORD")
	if rmqPass == "" {
		rmqPass = "guest"
	}
	rmqHost := os.Getenv("RABBITMQ_HOST")
	if rmqHost == "" {
		rmqHost = "localhost"
	}
	rmqPort := os.Getenv("RABBITMQ_PORT")
	if rmqPort == "" {
		rmqPort = "5672"
	}

	connectionString := fmt.Sprintf("amqp://%s:%s@%s:%s/", rmqUser, rmqPass, rmqHost, rmqPort)

	queueName := data["queue_name"]
	if queueName == "" {
		queueName = "news_articles_queue"
	}
	dlxName := data["dlx_name"]
	if dlxName == "" {
		dlxName = publisher.DefaultDLXName
	}
	dlqName := data["dlq_name"]
	if dlqName == "" {
		dlqName = publisher.DefaultDLQName
	}

	return publisher.RabbitMQConfig{
		ConnectionURL: connectionString,
		QueueName:     queueName,
		DLXName:       dlxName,
		DLQName:       dlqName,
	}
}

func fetchFromRSSFeeds(ctx context.Context, configs *dao.NewsProviderConfigs, normalisedNewsfeed chan<- *model.Article, wg *sync.WaitGroup) {
	for _, feed := range configs.GetRSSFeedConfigs() {
		fmt.Printf("Starting RSS provider: %s (%s)\n", feed.Name, feed.URL)
		p := &provider.RSSProvider{
			FeedName: feed.Name,
			FeedURL:  feed.URL,
		}
		wg.Add(1)
		go fetcher.StartFetching(ctx, p, 30*time.Second, normalisedNewsfeed, wg)
	}
}

func fetchFromNewsDataIO(ctx context.Context, configs *dao.NewsProviderConfigs, normalisedNewsfeed chan<- *model.Article, wg *sync.WaitGroup) {
	for _, config := range configs.GetNewsdataIOConfigs() {
		fmt.Printf("Processing config: %+v\n", config)
		apiKey, err := dao.GetAPIKey(config.APIKeyReference)
		if err != nil {
			fmt.Printf("Skipping NewsDataIO provider (missing API key): %v\n", err)
			continue
		}
		p := &provider.NewsDataIO{
			APIKey: apiKey,
			URL:    config.Endpoint,
			QUERY:  fmt.Sprintf("country=%s&language=%s&category=%s", config.QueryParams.Country, config.QueryParams.Language, config.QueryParams.Category),
		}
		wg.Add(1)
		go fetcher.StartFetching(ctx, p, 10*time.Second, normalisedNewsfeed, wg)
	}
}
