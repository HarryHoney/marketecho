module news-fetcher

go 1.25.0

require (
	dao/golang v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.11
)

require github.com/rabbitmq/amqp091-go v1.10.0

require github.com/joho/godotenv v1.5.1

replace example.com/dao => ../dao

replace dao/golang => ../dao/golang
