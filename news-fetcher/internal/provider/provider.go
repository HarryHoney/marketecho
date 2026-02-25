package provider

import (
	"context"
	"news-fetcher/internal/model"
)

type NewsProvider interface {
	FetchLatest(ctx context.Context) ([]*model.Article, error)
}
