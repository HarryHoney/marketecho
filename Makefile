PROTO_DIR = schema/proto
PROTO_SRC = article.proto
GOPATH_BIN := $(shell go env GOPATH)/bin

.PHONY: proto
proto:
	@echo "Updating shared Protobuf files (Go & Python)..."
	PATH="$(GOPATH_BIN):$$PATH" protoc -I=$(PROTO_DIR) \
		--go_out=./news-fetcher/internal/model \
		--go_opt=paths=source_relative \
		--python_out=./news-consumer/schema \
		$(PROTO_DIR)/$(PROTO_SRC)

.PHONY: run-news-fetcher
run-news-fetcher: proto
	@echo "Launching Go News Fetcher..."
	cd news-fetcher && go run main.go

.PHONY: test
test:
	cd news-fetcher && go test -v ./...
	cd news-consumer && .venv/bin/pytest -v test_protobuf.py test_inference_and_sink.py test_semantic_cache_and_metrics.py test_eod_evaluator.py

N ?= 10
.PHONY: signals
signals:
	python3 news-consumer/query_signals.py -n $(N)

.PHONY: evaluate-eod
evaluate-eod:
	cd news-consumer && .venv/bin/python evaluate_eod.py -n $(N)
