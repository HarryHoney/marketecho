package fetcher

import (
	"context"
	"fmt"
	"log"
	"news-fetcher/internal/provider"
	"time"
)

func StartFetching(ctx context.Context, p provider.NewsProvider, interval time.Duration) {
	articles, err := p.FetchLatest(ctx)
	if err != nil {
		log.Printf("Error or Rate Limit: %v", err)
	}

	fmt.Printf("Fetched %d normalized articles\n", len(articles))
	for _, a := range articles {
		log.Printf("Processed: %s", a.Title)
	}
}
