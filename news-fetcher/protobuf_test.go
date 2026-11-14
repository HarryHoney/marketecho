package main

import (
	"news-fetcher/internal/model"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestProtobufSerialization(t *testing.T) {
	// 1. Create a sample article with evolved schema fields
	originalArticle := &model.Article{
		Id:              "123",
		Title:           "Test News Title",
		Description:     "This is a test description.",
		Content:         "This is the full content of the test article.",
		Source:          "TestSource",
		Url:             "https://finance.yahoo.com/news/test-123",
		PublishedAtUnix: 1761386400,
		TraceId:         "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	// 2. Marshal (Serialize) to protobuf binary
	data, err := proto.Marshal(originalArticle)
	if err != nil {
		t.Fatalf("Failed to marshal article: %v", err)
	}

	if len(data) == 0 {
		t.Fatalf("Marshaled data is empty")
	}

	// 3. Unmarshal (Deserialize) back to a struct to verify integrity
	unmarshaledArticle := &model.Article{}
	err = proto.Unmarshal(data, unmarshaledArticle)
	if err != nil {
		t.Fatalf("Failed to unmarshal article: %v", err)
	}

	// 4. Validate fields match
	if unmarshaledArticle.Title != originalArticle.Title {
		t.Errorf("Expected Title %q, got %q", originalArticle.Title, unmarshaledArticle.Title)
	}
	if unmarshaledArticle.Content != originalArticle.Content {
		t.Errorf("Expected Content %q, got %q", originalArticle.Content, unmarshaledArticle.Content)
	}
	if unmarshaledArticle.Url != originalArticle.Url {
		t.Errorf("Expected Url %q, got %q", originalArticle.Url, unmarshaledArticle.Url)
	}
	if unmarshaledArticle.PublishedAtUnix != originalArticle.PublishedAtUnix {
		t.Errorf("Expected PublishedAtUnix %d, got %d", originalArticle.PublishedAtUnix, unmarshaledArticle.PublishedAtUnix)
	}
	if unmarshaledArticle.TraceId != originalArticle.TraceId {
		t.Errorf("Expected TraceId %q, got %q", originalArticle.TraceId, unmarshaledArticle.TraceId)
	}
}

func TestMarketSignalProtobufSerialization(t *testing.T) {
	signal := &model.MarketSignal{
		ArticleId:        "123",
		Tickers:          []string{"NVDA", "TSM"},
		Sentiment:        model.SentimentLabel_BULLISH,
		SentimentScore:   0.85,
		Confidence:       0.92,
		Reasoning:        "Strong datacenter GPU demand.",
		SemanticCacheHit: false,
		ProcessedAtUnix:  1761386410,
	}

	data, err := proto.Marshal(signal)
	if err != nil {
		t.Fatalf("Failed to marshal MarketSignal: %v", err)
	}

	decoded := &model.MarketSignal{}
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatalf("Failed to unmarshal MarketSignal: %v", err)
	}

	if decoded.ArticleId != "123" || decoded.Sentiment != model.SentimentLabel_BULLISH || len(decoded.Tickers) != 2 {
		t.Errorf("Decoded MarketSignal mismatch: %+v", decoded)
	}
}
