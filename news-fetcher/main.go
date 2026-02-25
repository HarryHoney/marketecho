package main

import (
	"context"
	"news-fetcher/internal/fetcher"
	"news-fetcher/internal/provider"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize Provider (Easy to swap in future)
	p := &provider.NewsDataIO{
		APIKey: "YOUR_API_KEY",
		URL:    "https://newsdata.io/api/1/latest",
	}

	// Start fetcher in a goroutine with a 10-second rate limit
	fetcher.StartFetching(ctx, p, 10*time.Second)

	// Keep main alive until interrupted
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	cancel()
}
