package publisher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"news-fetcher/internal/model"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultDLXName = "marketecho.dlx"
	DefaultDLQName = "news_articles_dlq"
)

// EventPublisher defines the port for publishing normalized Article events to the message broker.
type EventPublisher interface {
	Publish(ctx context.Context, article *model.Article) error
	Close() error
}

type RabbitMQConfig struct {
	ConnectionURL string
	QueueName     string
	DLXName       string
	DLQName       string
}

type RabbitMQPublisher struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
}

// NewRabbitMQPublisher connects to RabbitMQ and declares the durable topology:
// 1. Durable Direct Dead-Letter Exchange (marketecho.dlx)
// 2. Durable Dead-Letter Queue (news_articles_dlq) bound to marketecho.dlx
// 3. Durable primary queue (news_articles_queue) with x-dead-letter-exchange and x-dead-letter-routing-key
func NewRabbitMQPublisher(cfg RabbitMQConfig) (*RabbitMQPublisher, error) {
	if cfg.DLXName == "" {
		cfg.DLXName = DefaultDLXName
	}
	if cfg.DLQName == "" {
		cfg.DLQName = DefaultDLQName
	}

	conn, err := amqp.Dial(cfg.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}

	// 1. Declare Dead-Letter Exchange (DLX)
	if err := ch.ExchangeDeclare(
		cfg.DLXName, // name
		"direct",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare dead-letter exchange %s: %w", cfg.DLXName, err)
	}

	// 2. Declare Dead-Letter Queue (DLQ)
	if _, err := ch.QueueDeclare(
		cfg.DLQName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare dead-letter queue %s: %w", cfg.DLQName, err)
	}

	// 3. Bind DLQ to DLX
	if err := ch.QueueBind(
		cfg.DLQName, // queue name
		cfg.DLQName, // routing key
		cfg.DLXName, // exchange
		false,       // no-wait
		nil,         // arguments
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to bind DLQ %s to DLX %s: %w", cfg.DLQName, cfg.DLXName, err)
	}

	// 4. Declare Durable Main Articles Queue wired to DLX
	mainQueueArgs := amqp.Table{
		"x-dead-letter-exchange":    cfg.DLXName,
		"x-dead-letter-routing-key": cfg.DLQName,
	}
	q, err := ch.QueueDeclare(
		cfg.QueueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		mainQueueArgs, // arguments
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to declare durable queue %s: %w", cfg.QueueName, err)
	}

	return &RabbitMQPublisher{
		conn:      conn,
		ch:        ch,
		queueName: q.Name,
	}, nil
}

// PrepareArticleForPublish populates missing TraceId and PublishedAtUnix fields and marshals to Protobuf.
func PrepareArticleForPublish(article *model.Article) ([]byte, error) {
	if article == nil {
		return nil, fmt.Errorf("cannot publish nil article")
	}
	if article.TraceId == "" {
		article.TraceId = GenerateW3CTraceID()
	}
	if article.PublishedAtUnix == 0 {
		article.PublishedAtUnix = time.Now().Unix()
	}
	return proto.Marshal(article)
}

// GenerateW3CTraceID generates a W3C Traceparent-formatted string (00-<32hex>-<16hex>-01).
func GenerateW3CTraceID() string {
	traceBytes := make([]byte, 16)
	spanBytes := make([]byte, 8)
	_, _ = rand.Read(traceBytes)
	_, _ = rand.Read(spanBytes)
	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceBytes), hex.EncodeToString(spanBytes))
}

func (p *RabbitMQPublisher) Publish(ctx context.Context, article *model.Article) error {
	payload, err := PrepareArticleForPublish(article)
	if err != nil {
		return fmt.Errorf("failed to marshal article to protobuf: %w", err)
	}

	return p.ch.PublishWithContext(
		ctx,
		"",          // default exchange
		p.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:  "application/protobuf",
			DeliveryMode: amqp.Persistent, // Persistent (2)
			Timestamp:    time.Now(),
			MessageId:    article.GetId(),
			Headers: amqp.Table{
				"traceparent": article.GetTraceId(),
			},
			Body: payload,
		},
	)
}

func (p *RabbitMQPublisher) Close() error {
	var firstErr error
	if p.ch != nil {
		if err := p.ch.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
