What it takes to run this project so far.
1. Go should be installed - brew install go
2. protobuf should be installed - brew install protobuf
3. Golang protobuf installed - go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
4. Path setup:
~ export PATH=$PATH:$(go env GOPATH)/bin

Dependencies for go:
1. go get google.golang.org/protobuf
2. go get github.com/rabbitmq/amqp091-go
3. go get github.com/joho/godotenv

## Configuration (Environment Variables)
Before running the project, you must set up your environment variables. 
1. Copy the example file: `cp .env.example .env`
2. Open `.env` and fill in your real `NEWSDATA_API_KEY` and `GEMINI_API_KEY`.

------------------------------------------------------------------------
To Run the docker image for RabbitMQ. Just run the following command:
```
docker run -d \
  --name local-rabbit \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```
Open http://localhost:15672 to access the dashboard.
User: guest
Password: guest
