FROM golang:1.23-alpine

WORKDIR /app

# Copy the entire project because the fetcher depends on the dao module at the root level.
COPY . .

# Set working directory to the fetcher
WORKDIR /app/news-fetcher

# Build the Go application
RUN go build -o main .

# Run the binary
CMD ["./main"]