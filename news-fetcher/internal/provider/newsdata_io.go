package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"news-fetcher/internal/model"
	"strings"
	"time"
)

type NewsDataIO struct {
	APIKey string
	URL    string
	QUERY  string
	Client *http.Client
}

// Internal struct to match the JSON from NewsData.io
type newsDataResponse struct {
	Results []struct {
		ArticleID   string   `json:"article_id"`
		Title       string   `json:"title"`
		Description string   `json:"description"`
		Content     string   `json:"content"`
		Creator     []string `json:"creator"`
		Link        string   `json:"link"`
		PubDate     string   `json:"pubDate"`
	} `json:"results"`
}

func (n *NewsDataIO) Name() string {
	return "NewsDataIO"
}

func (n *NewsDataIO) httpClient() *http.Client {
	if n.Client != nil {
		return n.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (n *NewsDataIO) FetchLatest(ctx context.Context) ([]*model.Article, error) {
	reqURL := fmt.Sprintf("%s?apikey=%s&%s", n.URL, n.APIKey, n.QUERY)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := n.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error while contacting API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests { // 429
		return nil, fmt.Errorf("rate limited by NewsData API")
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("newsData API is down or returned an error (HTTP %d)", resp.StatusCode)
	}

	var data newsDataResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var normalized []*model.Article
	for _, res := range data.Results {
		title := strings.TrimSpace(res.Title)
		desc := strings.TrimSpace(res.Description)
		content := strings.TrimSpace(res.Content)

		// On NewsData.io free plans, 'content' is often empty or "ONLY AVAILABLE IN PAID PLANS".
		// Fall back cleanly to description or title so valid news is never dropped.
		if content == "" || strings.EqualFold(content, "ONLY AVAILABLE IN PAID PLANS") {
			content = desc
		}
		if content == "" {
			content = title
		}

		// Data Validation: Skip articles missing critical information
		if title == "" {
			fmt.Printf("Warning: Skipping article %s due to missing title\n", res.ArticleID)
			continue
		}

		source := strings.Join(res.Creator, ", ")
		if source == "" {
			source = n.Name()
		}

		publishedUnix := parsePubDate(res.PubDate)

		normalized = append(normalized, &model.Article{
			Id:              res.ArticleID,
			Title:           title,
			Description:     desc,
			Content:         content,
			Source:          source,
			Url:             res.Link,
			PublishedAtUnix: publishedUnix,
		})
	}
	return normalized, nil
}
