package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"news-fetcher/internal/model"
	"strings"
)

type NewsDataIO struct {
	APIKey string
	URL    string
}

// Internal struct to match the JSON you provided
type newsDataResponse struct {
	Results []struct {
		ArticleID   string   `json:"article_id"`
		Title       string   `json:"title"`
		Description string   `json:"description"`
		Content     string   `json:"content"`
		Creator     []string `json:"creator"`
	} `json:"results"`
}

func (n *NewsDataIO) FetchLatest(ctx context.Context) ([]*model.Article, error) {
	resp, err := http.Get(fmt.Sprintf("%s?apikey=%s", n.URL, n.APIKey))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited")
	}

	var data newsDataResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var normalized []*model.Article
	for _, res := range data.Results {
		normalized = append(normalized, &model.Article{
			Id:          res.ArticleID,
			Title:       res.Title,
			Description: res.Description,
			Content:     res.Content,
			Source:      strings.Join(res.Creator, ", "), // Handle array to string
		})
	}
	return normalized, nil
}
