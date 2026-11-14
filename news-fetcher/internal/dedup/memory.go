package dedup

import (
	"context"
	"news-fetcher/internal/model"
	"sync"
	"time"
)

type simhashEntry struct {
	hash      uint64
	expiresAt time.Time
}

// InMemoryDeduplicator is a thread-safe in-memory implementation of Deduplicator
// supporting both exact SHA-256 keys and 64-bit SimHash near-duplicate detection with TTL.
type InMemoryDeduplicator struct {
	mu          sync.Mutex
	ttl         time.Duration
	maxDistance int
	exactKeys   map[string]time.Time
	simhashes   []simhashEntry
}

func NewInMemoryDeduplicator(ttl time.Duration) *InMemoryDeduplicator {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &InMemoryDeduplicator{
		ttl:         ttl,
		maxDistance: DefaultSimHashMaxHammingDistance,
		exactKeys:   make(map[string]time.Time),
	}
}

func (m *InMemoryDeduplicator) IsDuplicate(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.evictExpiredLocked(now)

	if exp, exists := m.exactKeys[key]; exists && now.Before(exp) {
		return true, nil
	}
	m.exactKeys[key] = now.Add(m.ttl)
	return false, nil
}

func (m *InMemoryDeduplicator) IsArticleDuplicate(ctx context.Context, article *model.Article) (bool, string, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	now := time.Now()
	shaKey := ComputeSHA256Key(article.GetTitle(), article.GetUrl())
	sim := SimHash(article.GetTitle() + " " + article.GetDescription() + " " + article.GetContent())

	m.mu.Lock()
	defer m.mu.Unlock()

	m.evictExpiredLocked(now)

	// 1. Exact SHA-256 check
	if exp, exists := m.exactKeys[shaKey]; exists && now.Before(exp) {
		return true, "sha256", nil
	}

	// 2. 64-bit SimHash near-duplicate check
	if sim != 0 {
		for _, entry := range m.simhashes {
			if HammingDistance(sim, entry.hash) <= m.maxDistance {
				// Also record the exact key so future identical polls hit O(1) exact cache
				m.exactKeys[shaKey] = now.Add(m.ttl)
				return true, "simhash", nil
			}
		}
		m.simhashes = append(m.simhashes, simhashEntry{
			hash:      sim,
			expiresAt: now.Add(m.ttl),
		})
	}

	m.exactKeys[shaKey] = now.Add(m.ttl)
	return false, "", nil
}

func (m *InMemoryDeduplicator) evictExpiredLocked(now time.Time) {
	for k, exp := range m.exactKeys {
		if !now.Before(exp) {
			delete(m.exactKeys, k)
		}
	}
	valid := m.simhashes[:0]
	for _, entry := range m.simhashes {
		if now.Before(entry.expiresAt) {
			valid = append(valid, entry)
		}
	}
	m.simhashes = valid
}
