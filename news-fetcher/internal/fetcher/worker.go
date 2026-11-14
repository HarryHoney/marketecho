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

	if interval <= 0 {
		interval = 10 * time.Second
	}

	fetchOnce := func() {
		articles, err := p.FetchLatest(ctx)
		if err != nil {
			log.Printf("[%s] Error or Rate Limit: %v", p.Name(), err)
			return
		}

		fmt.Printf("[%s] Fetched %d normalized articles\n", p.Name(), len(articles))
		for _, a := range articles {
			select {
			case normalisedNewsfeed <- a:
			case <-ctx.Done():
				log.Printf("[%s] Context cancelled while sending article: %v", p.Name(), ctx.Err())
				return
			}
		}
	}

	// Perform initial fetch immediately on startup
	fetchOnce()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Stopping worker: %v", p.Name(), ctx.Err())
			return
		case <-ticker.C:
			fetchOnce()
		}
	}
}
