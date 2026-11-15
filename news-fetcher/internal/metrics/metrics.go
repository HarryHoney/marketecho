package metrics

import (
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
)

var (
	articlesIngestedTotal atomic.Uint64
	l1Sha256HitsTotal     atomic.Uint64
	l1SimhashHitsTotal    atomic.Uint64
	publishErrorsTotal    atomic.Uint64
)

func IncArticlesIngested() {
	articlesIngestedTotal.Add(1)
}

func IncL1DedupHit(reason string) {
	switch reason {
	case "simhash":
		l1SimhashHitsTotal.Add(1)
	default:
		l1Sha256HitsTotal.Add(1)
	}
}

func IncPublishErrors() {
	publishErrorsTotal.Add(1)
}

func GetArticlesIngested() uint64 {
	return articlesIngestedTotal.Load()
}

func GetL1DedupHits(reason string) uint64 {
	if reason == "simhash" {
		return l1SimhashHitsTotal.Load()
	}
	return l1Sha256HitsTotal.Load()
}

// Handler returns an http.Handler serving Prometheus text exposition format metrics.
func Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		ingested := articlesIngestedTotal.Load()
		shaHits := l1Sha256HitsTotal.Load()
		simHits := l1SimhashHitsTotal.Load()
		pubErrs := publishErrorsTotal.Load()

		fmt.Fprintf(w, "# HELP articles_ingested_total Total number of unique articles published to RabbitMQ.\n")
		fmt.Fprintf(w, "# TYPE articles_ingested_total counter\n")
		fmt.Fprintf(w, "articles_ingested_total %d\n", ingested)

		fmt.Fprintf(w, "# HELP l1_dedup_hits_total Total number of duplicate articles dropped at L1 cache.\n")
		fmt.Fprintf(w, "# TYPE l1_dedup_hits_total counter\n")
		fmt.Fprintf(w, "l1_dedup_hits_total{reason=\"sha256\"} %d\n", shaHits)
		fmt.Fprintf(w, "l1_dedup_hits_total{reason=\"simhash\"} %d\n", simHits)

		fmt.Fprintf(w, "# HELP publish_errors_total Total number of RabbitMQ publish errors.\n")
		fmt.Fprintf(w, "# TYPE publish_errors_total counter\n")
		fmt.Fprintf(w, "publish_errors_total %d\n", pubErrs)
	}
}

// StartServer starts a background HTTP server exposing /metrics on the given address (e.g. ":2112").
func StartServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", Handler())
	go func() {
		log.Printf("Prometheus metrics server listening on http://localhost%s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
			log.Printf("Warning: Prometheus metrics server stopped: %v", err)
		}
	}()
}
