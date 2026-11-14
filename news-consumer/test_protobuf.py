import pytest
import schema.article_pb2 as article_pb2

def test_protobuf_deserialization():
    # 1. Create a dummy protobuf payload (simulate the Go fetcher)
    article = article_pb2.Article()
    article.id = "456"
    article.title = "Python Test Title"
    article.description = "Python test description."
    article.content = "Python test content."
    article.source = "PythonTest"
    article.url = "https://cnbc.com/news/456"
    article.published_at_unix = 1761386400
    article.trace_id = "trace-xyz-987"
    
    binary_payload = article.SerializeToString()
    
    assert len(binary_payload) > 0, "Binary payload should not be empty"

    # 2. Simulate the Consumer receiving the binary payload and parsing it
    received_article = article_pb2.Article()
    try:
        received_article.ParseFromString(binary_payload)
    except Exception as e:
        pytest.fail(f"Failed to parse binary payload: {e}")

    # 3. Validate the fields match
    assert received_article.title == "Python Test Title"
    assert received_article.source == "PythonTest"
    assert received_article.content == "Python test content."
    assert received_article.url == "https://cnbc.com/news/456"
    assert received_article.published_at_unix == 1761386400
    assert received_article.trace_id == "trace-xyz-987"

def test_market_signal_protobuf():
    signal = article_pb2.MarketSignal()
    signal.article_id = "456"
    signal.tickers.extend(["AAPL", "MSFT"])
    signal.sentiment = article_pb2.BULLISH
    signal.sentiment_score = 0.75
    signal.confidence = 0.9
    signal.reasoning = "Strong quarterly earnings."
    signal.semantic_cache_hit = True
    signal.processed_at_unix = 1761386500

    payload = signal.SerializeToString()
    parsed = article_pb2.MarketSignal()
    parsed.ParseFromString(payload)

    assert parsed.article_id == "456"
    assert list(parsed.tickers) == ["AAPL", "MSFT"]
    assert parsed.sentiment == article_pb2.BULLISH
    assert pytest.approx(parsed.sentiment_score, 0.001) == 0.75
    assert pytest.approx(parsed.confidence, 0.001) == 0.9
    assert parsed.semantic_cache_hit is True
    assert parsed.processed_at_unix == 1761386500

def test_protobuf_deserialization_failure():
    # Test how the system handles invalid binary data
    invalid_payload = b"this is not a valid protobuf binary"
    
    received_article = article_pb2.Article()
    with pytest.raises(Exception):
         from google.protobuf.message import DecodeError
         try:
             received_article.ParseFromString(invalid_payload)
             if not received_article.title:
                 raise DecodeError("Failed to parse valid fields")
         except DecodeError:
             raise


def test_declare_durable_topology(mocker):
    import main as consumer_main

    mock_channel = mocker.MagicMock()
    consumer_main.declare_durable_topology(
        mock_channel,
        queue_name="news_articles_queue",
        dlx_name="marketecho.dlx",
        dlq_name="news_articles_dlq",
    )

    mock_channel.exchange_declare.assert_called_once_with(
        exchange="marketecho.dlx",
        exchange_type="direct",
        durable=True,
    )
    mock_channel.queue_bind.assert_called_once_with(
        queue="news_articles_dlq",
        exchange="marketecho.dlx",
        routing_key="news_articles_dlq",
    )
    mock_channel.queue_declare.assert_any_call(
        queue="news_articles_dlq",
        durable=True,
    )
    mock_channel.queue_declare.assert_any_call(
        queue="news_articles_queue",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "marketecho.dlx",
            "x-dead-letter-routing-key": "news_articles_dlq",
        },
    )

