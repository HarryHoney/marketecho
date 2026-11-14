package fetcher

import (
	"context"
	"news-fetcher/internal/model"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockProvider struct {
	calls atomic.Int32
}

func (m *mockProvider) Name() string {
	return "MockProvider"
}

func (m *mockProvider) FetchLatest(ctx context.Context) ([]*model.Article, error) {
	c := m.calls.Add(1)
	return []*model.Article{
		{
			Id:      "mock-id",
			Title:   "Mock Article",
			Content: "Mock Content",
			Source:  "MockSource",
			PublishedAtUnix: int64(c),
		},
	}, nil
}

func TestStartFetching_TickerLoopAndContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mp := &mockProvider{}
	ch := make(chan *model.Article, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go StartFetching(ctx, mp, 25*time.Millisecond, ch, &wg)

	// Wait enough time for initial fetch + at least 2 ticker ticks
	time.Sleep(70 * time.Millisecond)
	cancel()
	wg.Wait()
	close(ch)

	var count int
	for range ch {
		count++
	}

	if count < 2 {
		t.Fatalf("expected continuous fetching (>= 2 fetches), got %d", count)
	}
}
