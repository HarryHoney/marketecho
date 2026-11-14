import os
import json
import pytest
import threading
import pika
import sys

# Add the directory containing main.py to the path so we can import it
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../news-consumer')))
import main as consumer_main

def test_full_e2e_consumer_flow(mocker):
    """
    This test assumes that `e2e_test.py` has already run and populated 
    the 'e2e_test_queue' in the local RabbitMQ instance with a mock protobuf message.
    """
    # 1. Setup Environment Variables for the test
    test_config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'mock_configs'))
    os.environ["CONFIG_DIR"] = test_config_dir
    os.environ["GEMINI_API_KEY"] = "mock_key_for_test"
    os.environ["RABBITMQ_HOST"] = "localhost"
    
    # 2. Mock the Gemini API call so we don't actually hit Google's servers
    mock_gemini = mocker.patch('main.call_gemini_api')
    
    # 3. Setup a timeout to stop consuming so the test doesn't run forever
    def stop_consuming(channel):
        channel.stop_consuming()
    
    # We patch the rabbit_mq_listener_setup to inject the timeout
    original_start = pika.adapters.blocking_connection.BlockingChannel.start_consuming
    
    def mock_start_consuming(self):
        # Stop consuming after 2 seconds
        timer = threading.Timer(2.0, stop_consuming, args=[self])
        timer.start()
        original_start(self)
        
    mocker.patch('pika.adapters.blocking_connection.BlockingChannel.start_consuming', mock_start_consuming)
    
    # 4. Run the consumer setup!
    try:
        consumer_main.rabbit_mq_listener_setup("mock_key_for_test")
    except Exception as e:
        pytest.fail(f"Consumer threw an error: {e}")
        
    # 5. Assert the mock data flowed all the way through
    # If e2e_test.py ran correctly, there should be exactly one message in the queue
    # because the second mocked article had empty content and should have been dropped.
    assert mock_gemini.call_count == 1, "The Gemini API should have been called exactly once."
    
    # Check the contents of the last call
    last_call_args = mock_gemini.call_args[0] # (api_key, prompt, news_data)
    news_data_passed = last_call_args[2]
    
    assert "E2E Test Title" in news_data_passed, "The mock title from Go did not reach Python"
    assert "E2E_Test_Author" in news_data_passed, "The mock author from Go did not reach Python"
