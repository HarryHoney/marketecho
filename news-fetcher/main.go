package main

import (
	"context"
	"fmt"
	"news-fetcher/internal/fetcher"
	"news-fetcher/internal/provider"
	"sync"
	"time"

	dao "dao/golang"
	"news-fetcher/internal/model"
)

func main() {
	configs, err := dao.LoadNewsProviderConfigs()
	if err != nil {
		panic(err)
	}

	normalisedNewsfeed := make(chan *model.Article) // channel for normalised articles

	var wg sync.WaitGroup
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
		go fetcher.StartFetching(context.Background(), p, 10*time.Second, normalisedNewsfeed, &wg)
	}

	// Close the channel when all workers (fetchers) are done.
	go func() {
		wg.Wait()
		close(normalisedNewsfeed)
	}()

	// This loop will stay active as long as fetchers are sending data.
	// It will only exit once the channel is closed by the goroutine above.
	for article := range normalisedNewsfeed {
		fmt.Println("Received:", article.Title)
	}

	// 3. Only after the loop finishes is the program truly done.
	fmt.Println("All articles processed. Exiting.")
}
