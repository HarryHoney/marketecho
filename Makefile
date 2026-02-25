PROTO_DIR = schema/proto
PROTO_SRC = article.proto

.PHONY: proto
proto:
	@echo "Updating shared Protobuf files..."
	# We use -I to set the base directory, so the output doesn't include the folder path
	protoc -I=$(PROTO_DIR) \
	--go_out=./news-fetcher/internal/model \
	--go_opt=paths=source_relative \
	$(PROTO_DIR)/$(PROTO_SRC)
#	protoc -I=$(PROTO_DIR) --cpp_out=./cpp-subscriber/include $(PROTO_DIR)/$(PROTO_SRC)

.PHONY: run-news-fetcher
run-news-fetcher: proto
	@echo "Launching Go News Fetcher..."
	cd news-fetcher && go run main.go