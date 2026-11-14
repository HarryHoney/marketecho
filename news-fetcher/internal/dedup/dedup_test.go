package dedup

import (
	"context"
	"news-fetcher/internal/model"
	"testing"
	"time"
)

func TestSimHash_NearDuplicateAndDistinct(t *testing.T) {
	// Syndicated article across Yahoo Finance and CNBC with minor attribution differences
	textSourceA := "NVIDIA Corporation reported record quarterly revenue of $35.1 billion on Wednesday, driven by surging data center artificial intelligence GPU demand from hyperscale cloud providers."
	textSourceB := "NVIDIA Corporation reported record quarterly revenue of $35.1 billion on Wednesday, driven by surging data center artificial intelligence GPU demand from hyperscale cloud providers, Reuters reported."

	hA := SimHash(textSourceA)
	hB := SimHash(textSourceB)
	distNear := HammingDistance(hA, hB)

	if distNear > DefaultSimHashMaxHammingDistance {
		t.Fatalf("expected syndicated near-duplicate Hamming distance <= %d, got %d", DefaultSimHashMaxHammingDistance, distNear)
	}

	// Completely different market story
	textDistinct := "Federal Reserve policymakers voted unanimously to hold benchmark interest rates steady at 4.25% while monitoring consumer inflation and labor market cooling."
	hC := SimHash(textDistinct)
	distFar := HammingDistance(hA, hC)

	if distFar <= DefaultSimHashMaxHammingDistance {
		t.Fatalf("expected distinct article Hamming distance > %d, got %d", DefaultSimHashMaxHammingDistance, distFar)
	}
}

func TestInMemoryDeduplicator_HybridExactAndSimHash(t *testing.T) {
	ctx := context.Background()
	d := NewInMemoryDeduplicator(100 * time.Millisecond)

	art1 := &model.Article{
		Id:          "yahoo-1",
		Title:       "Apple Unveils M5 Chip Architecture for MacBook Pro Lineup",
		Description: "Apple announced next-generation silicon with enhanced neural engine cores.",
		Content:     "Apple Inc. unveiled its new M5 processor architecture on Tuesday featuring 3nm fabrication and faster unified memory bandwidth for AI workloads.",
		Url:         "https://finance.yahoo.com/news/apple-m5-chip",
		Source:      "Yahoo Finance",
	}

	// 1. First ingestion -> should NOT be duplicate
	isDup, reason, err := d.IsArticleDuplicate(ctx, art1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isDup {
		t.Fatalf("expected first article to be unique, got duplicate (reason=%s)", reason)
	}

	// 2. Second ingestion of exact same Title + URL -> should hit SHA-256
	isDup, reason, err = d.IsArticleDuplicate(ctx, art1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isDup || reason != "sha256" {
		t.Fatalf("expected exact duplicate with reason 'sha256', got isDup=%v reason=%q", isDup, reason)
	}

	// 3. Syndicated copy on CNBC (different URL, nearly identical text) -> should hit SimHash
	artSyndicated := &model.Article{
		Id:          "cnbc-99",
		Title:       "Apple Unveils M5 Chip Architecture for MacBook Pro Lineup",
		Description: "Apple announced next-generation silicon with enhanced neural engine cores.",
		Content:     "Apple Inc. unveiled its new M5 processor architecture on Tuesday featuring 3nm fabrication and faster unified memory bandwidth for AI workloads, CNBC reports.",
		Url:         "https://cnbc.com/2026/10/25/apple-m5-macbook.html",
		Source:      "CNBC",
	}
	isDup, reason, err = d.IsArticleDuplicate(ctx, artSyndicated)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isDup || reason != "simhash" {
		t.Fatalf("expected syndicated near-duplicate with reason 'simhash', got isDup=%v reason=%q", isDup, reason)
	}

	// 4. Wait for TTL expiration -> should allow re-ingestion
	time.Sleep(130 * time.Millisecond)
	isDup, _, err = d.IsArticleDuplicate(ctx, art1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isDup {
		t.Fatalf("expected article to expire after TTL, but still marked duplicate")
	}
}
