package golang

import (
	"encoding/json"
	"fmt"
	"os"
)

// QueryParams represents the query parameters for a news provider
type QueryParams struct {
	Country  string `json:"country"`
	Language string `json:"language"`
	Category string `json:"category"`
}

// ProviderConfig represents a single news provider configuration
type ProviderConfig struct {
	APIKeyReference string      `json:"api_key_reference"`
	Endpoint        string      `json:"endpoint"`
	QueryParams     QueryParams `json:"query_params"`
}

// NewsProviderConfigs represents the root structure of the config file
type NewsProviderConfigs struct {
	NewsdataIO []ProviderConfig `json:"newsdata_io"`
}

// LoadNewsProviderConfigs loads and parses the news provider configurations from JSON file
func LoadNewsProviderConfigs() (*NewsProviderConfigs, error) {
	file, err := os.Open("../configs/news_provider_configs.json")
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

func GetAPIKey(api_key string) (any, any) {
	// 1. Open the file
	file, err := os.Open("../creds/api_keys.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 2. Decode into a map
	var data map[string]any
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		panic(err)
	}

	// 3. Extract your specific attribute (e.g., "target_id")
	// We use "type assertion" to ensure it's the type we expect
	if val, ok := data[api_key]; ok {
		return val, nil
	} else {
		return nil, fmt.Errorf("key not found")
	}
}
