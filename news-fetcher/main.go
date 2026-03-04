package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"news-fetcher/internal/fetcher"
	"news-fetcher/internal/provider"
	"sync"
	"time"

	dao "dao/golang"
	"news-fetcher/internal/model"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	configs, err := dao.LoadNewsProviderConfigs()
	if err != nil {
		panic(err)
	}

	// Initialize RabbitMQ connection, channel, and queue
	conn, ch, q := queueInit()
	defer conn.Close()
	defer ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	normalisedNewsfeed := make(chan *model.Article) // channel for normalised articles

	var wg sync.WaitGroup
	// Handle news from NewsDataIO
	fetchFromNewsDataIO(configs, normalisedNewsfeed, &wg)

	// Close the channel when all workers (fetchers) are done.
	go func() {
		wg.Wait()
		// Close the channel to signal that no more articles will be sent. Receive is opened until all articles are processed.
		close(normalisedNewsfeed)
	}()

	// This loop will stay active as long as fetchers are sending data.
	// It will only exit once the channel is closed by the goroutine above.
	// This range on news channel is making the main goroutine wait for all articles to be processed before exiting.
	// In golang, ranging over a channel will block until the channel is closed, so this ensures we process all articles before exiting.
	for article := range normalisedNewsfeed {
		fmt.Println("Received:", article.Title)

		// Publish article to queue
		articleJSON, err := json.Marshal(article)
		if err != nil {
			log.Printf("Failed to marshal article: %v", err)
			continue
		}

		err = ch.PublishWithContext(ctx,
			"",     // exchange (default)
			q.Name, // routing key (queue name)
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        articleJSON,
			})
		if err != nil {
			log.Printf("Failed to publish article: %v", err)
			continue
		}
		fmt.Printf("Published article: %s\n", article.Title)
	}

	// 3. Only after the loop finishes is the program truly done.
	fmt.Println("All articles processed. Exiting.")
}

// queueInit initializes RabbitMQ connection, channel, and declares the articles queue
func queueInit() (*amqp.Connection, *amqp.Channel, amqp.Queue) {
	// 1. Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// 2. Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	// 3. Declare the articles queue
	q, err := ch.QueueDeclare(
		"articles_queue", // name - descriptive queue name
		false,            // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare articles queue: %v", err)
	}

	return conn, ch, q
}

func fetchFromNewsDataIO(configs *dao.NewsProviderConfigs, normalisedNewsfeed chan<- *model.Article, wg *sync.WaitGroup) {
	// Handle news from NewsDataIO
	for _, config := range configs.GetNewsdataIOConfigs() {
		fmt.Printf("Processing config: %+v\n", config)
		// Here you would initialize your provider and start fetching
		apiKey, err := dao.GetAPIKey(config.APIKeyReference)
		if err != nil {
			fmt.Printf("Failed to get API key: %v\n", err)
			continue
		}
		p := &provider.NewsDataIO{
			APIKey: apiKey.(string), // Type assertion to string
			URL:    config.Endpoint,
			QUERY:  fmt.Sprintf("country=%s&language=%s&category=%s", config.QueryParams.Country, config.QueryParams.Language, config.QueryParams.Category),
		}
		wg.Add(1)
		// Start fetcher in a goroutine with a 10-second rate limit
		go fetcher.StartFetching(context.Background(), p, 10*time.Second, normalisedNewsfeed, wg)
	}
}
