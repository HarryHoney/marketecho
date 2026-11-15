package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsHandler(t *testing.T) {
	IncArticlesIngested()
	IncL1DedupHit("sha256")
	IncL1DedupHit("simhash")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"articles_ingested_total",
		"l1_dedup_hits_total{reason=\"sha256\"}",
		"l1_dedup_hits_total{reason=\"simhash\"}",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("expected /metrics output to contain %q, got:\n%s", expected, body)
		}
	}
}
