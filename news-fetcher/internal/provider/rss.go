package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"news-fetcher/internal/model"
	"strings"
	"time"
)

type RSSProvider struct {
	FeedName string
	FeedURL  string
	Client   *http.Client
}

type rssDocument struct {
	Channel struct {
		Title string    `xml:"title"`
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	Content     string `xml:"encoded"`
	GUID        string `xml:"guid"`
	PubDate     string `xml:"pubDate"`
}

func (r *RSSProvider) Name() string {
	if r.FeedName != "" {
		return fmt.Sprintf("RSS:%s", r.FeedName)
	}
	return "RSSProvider"
}

func (r *RSSProvider) httpClient() *http.Client {
	if r.Client != nil {
		return r.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (r *RSSProvider) FetchLatest(ctx context.Context) ([]*model.Article, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.FeedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create RSS request for %s: %w", r.Name(), err)
	}
	req.Header.Set("User-Agent", "MarketEcho-Fetcher/1.0")

	resp, err := r.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error fetching RSS feed %s: %w", r.Name(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("RSS feed %s returned HTTP %d", r.Name(), resp.StatusCode)
	}

	var doc rssDocument
	if err := xml.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("failed to decode RSS XML from %s: %w", r.Name(), err)
	}

	sourceName := r.FeedName
	if sourceName == "" {
		sourceName = strings.TrimSpace(doc.Channel.Title)
	}
	if sourceName == "" {
		sourceName = "RSS"
	}

	var articles []*model.Article
	for _, item := range doc.Channel.Items {
		title := strings.TrimSpace(item.Title)
		desc := strings.TrimSpace(item.Description)
		content := strings.TrimSpace(item.Content)
		if content == "" {
			content = desc
		}
		if content == "" {
			content = title
		}

		if title == "" {
			continue
		}

		link := strings.TrimSpace(item.Link)
		id := strings.TrimSpace(item.GUID)
		if id == "" {
			hash := sha256.Sum256([]byte(title + "|" + link))
			id = hex.EncodeToString(hash[:16])
		}

		articles = append(articles, &model.Article{
			Id:              id,
			Title:           title,
			Description:     desc,
			Content:         content,
			Source:          sourceName,
			Url:             link,
			PublishedAtUnix: parsePubDate(item.PubDate),
		})
	}

	return articles, nil
}

func parsePubDate(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Now().Unix()
	}
	layouts := []string{
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"Mon, 2 Jan 2006 15:04:05 MST",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.Unix()
		}
	}
	return time.Now().Unix()
}
