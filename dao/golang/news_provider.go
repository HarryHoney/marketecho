package golang

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// QueryParams represents the query parameters for a news provider
type QueryParams struct {
	Country  string `json:"country"`
	Language string `json:"language"`
	Category string `json:"category"`
}

// ProviderConfig represents a single newsdata.io provider configuration
type ProviderConfig struct {
	APIKeyReference string      `json:"api_key_reference"`
	Endpoint        string      `json:"endpoint"`
	QueryParams     QueryParams `json:"query_params"`
}

// RSSFeedConfig represents a free RSS feed source configuration
type RSSFeedConfig struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// NewsProviderConfigs represents the root structure of the config file
type NewsProviderConfigs struct {
	RSSFeeds   []RSSFeedConfig  `json:"rss_feeds"`
	NewsdataIO []ProviderConfig `json:"newsdata_io"`
}

// LoadNewsProviderConfigs loads and parses the news provider configurations from JSON file
func LoadNewsProviderConfigs() (*NewsProviderConfigs, error) {
	configDir := os.Getenv("CONFIG_DIR")
	if configDir == "" {
		configDir = "../configs"
	}
	filePath := fmt.Sprintf("%s/news_provider_configs.json", configDir)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var configs NewsProviderConfigs
	if err := json.NewDecoder(file).Decode(&configs); err != nil {
		return nil, err
	}

	return &configs, nil
}

// GetRSSFeedConfigs returns all configured RSS feeds
func (c *NewsProviderConfigs) GetRSSFeedConfigs() []RSSFeedConfig {
	return c.RSSFeeds
}

// GetNewsdataIOConfigs returns all newsdata.io provider configurations
func (c *NewsProviderConfigs) GetNewsdataIOConfigs() []ProviderConfig {
	return c.NewsdataIO
}

// GetNewsdataIOConfigByIndex returns a specific newsdata.io configuration by index
func (c *NewsProviderConfigs) GetNewsdataIOConfigByIndex(index int) *ProviderConfig {
	if index >= 0 && index < len(c.NewsdataIO) {
		return &c.NewsdataIO[index]
	}
	return nil
}

func GetAPIKey(apiKeyRef string) (string, error) {
	// Extract your specific attribute from environment variables
	// Converting the reference to uppercase to match .env conventions (e.g. newsdata_api_key -> NEWSDATA_API_KEY)
	envKey := strings.ToUpper(apiKeyRef)
	val := os.Getenv(envKey)
	if val != "" {
		return val, nil
	}
	return "", fmt.Errorf("key not found in environment for reference: %s", envKey)
}
