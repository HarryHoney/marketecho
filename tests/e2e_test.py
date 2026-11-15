import os
import json
import time
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


# 1. MOCK HTTP SERVER (Simulates NewsData.io API)
class MockNewsDataHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        # Mock JSON response matching newsDataResponse struct
        mock_response = {
            "results": [
                {
                    "article_id": "e2e_test_123",
                    "title": "E2E Test Title: NVIDIA Joins S&P 500 Rally",
                    "description": "This is a test description flowing through the system.",
                    "content": "This is the full test content for NVIDIA Corporation.",
                    "creator": ["E2E_Test_Author"],
                    "link": "https://example.com/e2e-nvidia",
                },
                {
                    "article_id": "e2e_test_bad_456",
                    "title": "",
                    "description": "",
                    "content": "",
                    "creator": ["Bad_Author"],
                },
            ]
        }
        self.wfile.write(json.dumps(mock_response).encode("utf-8"))

    def log_message(self, format, *args):
        pass  # Suppress logs


def run_mock_server():
    server_address = ("127.0.0.1", 8080)
    httpd = HTTPServer(server_address, MockNewsDataHandler)
    httpd.serve_forever()


if __name__ == "__main__":
    print("Starting E2E Integration Test...")

    mock_server_thread = threading.Thread(target=run_mock_server, daemon=True)
    mock_server_thread.start()

    test_config_dir = os.path.abspath("tests/mock_configs")
    os.makedirs(test_config_dir, exist_ok=True)

    provider_config = {
        "rss_feeds": [],
        "newsdata_io": [
            {
                "api_key_reference": "NEWSDATA_API_KEY",
                "endpoint": "http://127.0.0.1:8080/latest",
                "query_params": {
                    "country": "us",
                    "language": "en",
                    "category": "breaking",
                },
            }
        ],
    }
    with open(os.path.join(test_config_dir, "news_provider_configs.json"), "w") as f:
        json.dump(provider_config, f)

    with open(os.path.join(test_config_dir, "rabbit_mq.json"), "w") as f:
        json.dump(
            {
                "queue_name": "e2e_test_queue",
                "dlx_name": "marketecho.e2e.dlx",
                "dlq_name": "e2e_test_dlq",
            },
            f,
        )

    env = os.environ.copy()
    env["CONFIG_DIR"] = test_config_dir
    env["NEWSDATA_API_KEY"] = "mock_key"
    env["GEMINI_API_KEY"] = "mock_key"
    env["RABBITMQ_HOST"] = "localhost"

    print("Running Go Fetcher to push mock data to RabbitMQ...")
    go_process = subprocess.Popen(
        ["go", "run", "main.go"],
        cwd="news-fetcher",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(3)
    go_process.terminate()

    print("Go Fetcher finished. Mock data is in RabbitMQ 'e2e_test_queue'.")
    print("E2E Test harness setup complete. You can now test the consumer side.")
