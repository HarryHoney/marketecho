package fetcher

import (
	"context"
	"fmt"
	"log"
	"news-fetcher/internal/provider"
	"time"
)

func StartFetching(ctx context.Context, p provider.NewsProvider, interval time.Duration) {
	// Rate limiting: ticker ensures we only call every X seconds
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				articles, err := p.FetchLatest(ctx)
				if err != nil {
					log.Printf("Error or Rate Limit: %v", err)
					continue
				}

				fmt.Printf("Fetched %d normalized articles\n", len(articles))
				for _, a := range articles {
					log.Printf("Processed: %s", a.Title)
				}
			}
		}
	}()
}
