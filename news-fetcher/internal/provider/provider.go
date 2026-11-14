package provider

import (
	"context"
	"news-fetcher/internal/model"
)

type NewsProvider interface {
	Name() string
	FetchLatest(ctx context.Context) ([]*model.Article, error)
}
