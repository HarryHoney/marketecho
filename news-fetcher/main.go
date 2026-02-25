package main

import (
	"context"
	"encoding/json"
	"fmt"
	"news-fetcher/internal/fetcher"
	"news-fetcher/internal/provider"
	"os"
	"time"
)

func main() {
	apiKey, err := loadAPIKey()
	if err != nil {
		panic(err)
	}
	// Initialize Provider (Easy to swap in future)
	p := &provider.NewsDataIO{
		APIKey: apiKey.(string),
		URL:    "https://newsdata.io/api/1/latest",
	}

	// Start fetcher in a goroutine with a 10-second rate limit
	fetcher.StartFetching(context.Background(), p, 10*time.Second)
}

func loadAPIKey() (any, any) {
	// 1. Open the file
	file, err := os.Open("../creds/api_keys.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 2. Decode into a map
	var data map[string]any
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		panic(err)
	}

	// 3. Extract your specific attribute (e.g., "target_id")
	// We use "type assertion" to ensure it's the type we expect
	if val, ok := data["newsdata_api_key"]; ok {
		return val, nil
	} else {
		return nil, fmt.Errorf("key not found")
	}
}
