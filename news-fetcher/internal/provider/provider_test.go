package provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newHandlerClient(handler http.HandlerFunc) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if err := req.Context().Err(); err != nil {
				return nil, err
			}
			rec := httptest.NewRecorder()
			handler(rec, req)
			resp := rec.Result()
			resp.Request = req
			return resp, nil
		}),
	}
}

func TestRSSProvider_FetchLatest(t *testing.T) {
	mockRSS := `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Yahoo Finance</title>
    <item>
      <title>NVIDIA Announces Next-Gen AI Chips</title>
      <link>https://finance.yahoo.com/news/nvidia-ai-chips</link>
      <description>NVIDIA unveiled its latest GPU architecture for data centers.</description>
      <guid>nvda-guid-001</guid>
      <pubDate>Sun, 25 Oct 2026 10:00:00 GMT</pubDate>
    </item>
    <item>
      <title>   </title>
      <description>Empty title item should be skipped</description>
    </item>
  </channel>
</rss>`

	client := newHandlerClient(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, mockRSS)
	})

	rss := &RSSProvider{
		FeedName: "Yahoo Finance",
		FeedURL:  "https://finance.yahoo.com/news/rssindex",
		Client:   client,
	}

	if rss.Name() != "RSS:Yahoo Finance" {
		t.Fatalf("expected Name() 'RSS:Yahoo Finance', got %q", rss.Name())
	}

	articles, err := rss.FetchLatest(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(articles) != 1 {
		t.Fatalf("expected 1 valid article, got %d", len(articles))
	}

	got := articles[0]
	if got.Id != "nvda-guid-001" {
		t.Errorf("expected Id 'nvda-guid-001', got %q", got.Id)
	}
	if got.Title != "NVIDIA Announces Next-Gen AI Chips" {
		t.Errorf("unexpected Title: %q", got.Title)
	}
	if got.Url != "https://finance.yahoo.com/news/nvidia-ai-chips" {
		t.Errorf("unexpected Url: %q", got.Url)
	}
	if got.Source != "Yahoo Finance" {
		t.Errorf("expected Source 'Yahoo Finance', got %q", got.Source)
	}
	if got.PublishedAtUnix <= 0 {
		t.Errorf("expected positive PublishedAtUnix, got %d", got.PublishedAtUnix)
	}
}

func TestNewsDataIO_FetchLatest_AndTimeout(t *testing.T) {
	mockJSON := `{
		"results": [
			{
				"article_id": "nd-101",
				"title": "Fed Holds Interest Rates Steady",
				"description": "Federal Reserve signals cautious approach.",
				"content": "Full FOMC statement analysis and market reaction.",
				"creator": ["Reuters"],
				"link": "https://example.com/fed-rates",
				"pubDate": "2026-10-25 12:00:00"
			},
			{
				"article_id": "nd-102",
				"title": "   ",
				"description": "Missing title should be skipped",
				"content": "Some content"
			}
		]
	}`

	client := newHandlerClient(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "apikey=test-key") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, mockJSON)
	})

	provider := &NewsDataIO{
		APIKey: "test-key",
		URL:    "https://newsdata.io/api/1/latest",
		QUERY:  "country=us&language=en",
		Client: client,
	}

	articles, err := provider.FetchLatest(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(articles) != 1 {
		t.Fatalf("expected 1 normalized article, got %d", len(articles))
	}
	if articles[0].Id != "nd-101" || articles[0].Url != "https://example.com/fed-rates" {
		t.Errorf("unexpected article fields: %+v", articles[0])
	}

	// Verify context cancellation / timeout is respected
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	cancel()
	_, err = provider.FetchLatest(ctx)
	if err == nil {
		t.Fatalf("expected error on cancelled context, got nil")
	}
}
