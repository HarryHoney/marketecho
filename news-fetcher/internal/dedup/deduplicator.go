package dedup

import (
	"context"
	"news-fetcher/internal/model"
)

// DefaultSimHashMaxHammingDistance is the maximum bit difference (out of 64 bits)
// for two article SimHash fingerprints to be considered near-duplicates.
const DefaultSimHashMaxHammingDistance = 3

// Deduplicator defines the L1 ingestion deduplication port.
type Deduplicator interface {
	// IsDuplicate checks and records a single exact key (e.g., SHA-256 hash).
	IsDuplicate(ctx context.Context, key string) (bool, error)

	// IsArticleDuplicate runs the hybrid L1 check:
	// 1. Exact SHA-256 on normalized Title + URL.
	// 2. 64-bit SimHash near-duplicate check (Hamming distance <= 3) on Title + Content.
	// Returns (isDup, reason, error) where reason is "sha256", "simhash", or "".
	IsArticleDuplicate(ctx context.Context, article *model.Article) (bool, string, error)
}
