package publisher

import (
	"news-fetcher/internal/model"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestPrepareArticleForPublish_GeneratesW3CTraceID(t *testing.T) {
	article := &model.Article{
		Id:      "art-789",
		Title:   "TSMC Expands Arizona Fab Capacity",
		Content: "Taiwan Semiconductor announced phase 3 expansion.",
		Source:  "Yahoo Finance",
		Url:     "https://finance.yahoo.com/news/tsmc-arizona",
	}

	payload, err := PrepareArticleForPublish(article)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(payload) == 0 {
		t.Fatalf("expected non-empty protobuf payload")
	}

	if !strings.HasPrefix(article.TraceId, "00-") || !strings.HasSuffix(article.TraceId, "-01") {
		t.Errorf("expected W3C traceparent format '00-<trace>-<span>-01', got %q", article.TraceId)
	}
	if article.PublishedAtUnix <= 0 {
		t.Errorf("expected PublishedAtUnix to be populated, got %d", article.PublishedAtUnix)
	}

	decoded := &model.Article{}
	if err := proto.Unmarshal(payload, decoded); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}
	if decoded.TraceId != article.TraceId {
		t.Errorf("expected decoded TraceId %q, got %q", article.TraceId, decoded.TraceId)
	}
}
