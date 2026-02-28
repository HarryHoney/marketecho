package fetcher

import (
	"context"
	"fmt"
	"log"
	"news-fetcher/internal/model"
	"news-fetcher/internal/provider"
	"sync"
	"time"
)

func StartFetching(ctx context.Context, p provider.NewsProvider, interval time.Duration, normalisedNewsfeed chan<- *model.Article, wg *sync.WaitGroup) {
	defer wg.Done()
	articles, err := p.FetchLatest(ctx)
	if err != nil {
		log.Printf("Error or Rate Limit: %v", err)
	}

	fmt.Printf("Fetched %d normalized articles\n", len(articles))
	for _, a := range articles {
		select {
		case normalisedNewsfeed <- a:
		case <-ctx.Done():
			log.Printf("Context cancelled: %v", ctx.Err())
			return
		}
	}
}
