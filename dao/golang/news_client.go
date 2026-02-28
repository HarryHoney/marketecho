package golang

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"os"
// 	"sync"

// 	supabase "github.com/supabase-community/supabase-go"
// )

// type NewsClient struct {
// 	ID          int    `json:"id"`
// 	ClientName  string `json:"client_name"`
// 	APIEndpoint string `json:"api_endpoint"`
// 	Query       string `json:"query"`
// 	RateLimit   int    `json:"rate_limit"`
// }

// type SupabaseDAO struct {
// 	client *supabase.Client
// }

// var (
// 	instance *SupabaseDAO
// 	once     sync.Once
// )

// // GetSupabaseDAO returns the singleton instance of SupabaseDAO
// func GetSupabaseDAO() *SupabaseDAO {
// 	key, err := loadSupabaseAPIKey()
// 	if err != nil {
// 		log.Fatalf("Failed to load Supabase API key: %v", err)
// 	}
// 	once.Do(func() {
// 		client, err := supabase.NewClient(
// 			"https://cijuzozcejdzpqrizkia.supabase.co",
// 			key.(string),
// 			nil,
// 		)
// 		if err != nil {
// 			log.Fatalf("Failed to create Supabase client: %v", err)
// 		}
// 		instance = &SupabaseDAO{client: client}
// 	})
// 	return instance
// }

// // GetAllNewsClients fetches all rows from the NewsClient table
// func (dao *SupabaseDAO) GetAllNewsClients() ([]NewsClient, error) {
// 	var results []NewsClient
// 	_, _, err := dao.client.From("NewsClient").Select("*", "", false).Execute()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return results, nil
// }

// func loadSupabaseAPIKey() (any, any) {
// 	// 1. Open the file
// 	file, err := os.Open(".../creds/api_keys.json")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()

// 	// 2. Decode into a map
// 	var data map[string]any
// 	if err := json.NewDecoder(file).Decode(&data); err != nil {
// 		panic(err)
// 	}

// 	// 3. Extract your specific attribute (e.g., "target_id")
// 	// We use "type assertion" to ensure it's the type we expect
// 	if val, ok := data["supabase_api_key"]; ok {
// 		return val, nil
// 	} else {
// 		return nil, fmt.Errorf("key not found")
// 	}
// }
