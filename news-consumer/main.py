import json
import pika
import sys
import os
from google import genai
from google.genai import types  # Added this import

def get_gemini_api_key(file_path="../creds/api_keys.json"):
    """Reads the API key from a local JSON file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found at {file_path}")
    
    with open(file_path, "r") as f:
        data = json.load(f)
        return data.get("gemini_api_key")

def rabbit_mq_listener_setup(gemini_api_key):
    # Load RabbitMQ config from JSON
    with open("../configs/rabbit_mq.json", "r") as f:
        config = json.load(f)
    
    queue_name = config.get("queue_name", "task_queue")
    # 1. Connect to the local RabbitMQ node
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # 2. Ensure the queue exists
    channel.queue_declare(queue=queue_name)

    # 3. SET PREFETCH: The "Fair Dispatch" setting
    # This tells RabbitMQ not to give more than 1 message to a worker at a time.
    channel.basic_qos(prefetch_count=1)

    # 4. Define the "Push" Callback
    # This function is triggered automatically when RabbitMQ pushes a message.
    def callback(ch, method, properties, body):
        print(f" [x] Received: {body.decode()}")
        
        # Call the Gemini API with the news data
        call_gemini_api(gemini_api_key, "Summarize the following news data:", body.decode()) 
        
        # 5. Send Acknowledgment back to RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # 6. Start consuming (The "Push" starts here)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(' [*] Waiting for news data. To exit press CTRL+C')
    channel.start_consuming()

def call_gemini_api(gemini_api_key, prompt, news_data):

    # Initialize client with the key from JSON
    client = genai.Client(api_key=gemini_api_key)
    
    # Updated call with config to reduce latency(Takes about 1-2 seconds for simple queries, 20-30 seconds for complex ones)
    response = client.models.generate_content(
        model="gemini-3-flash-preview",
        contents=f"{prompt}\n\nNews Data: {news_data}",
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_level="LOW"  # Stops the "stuck" feeling for simple queries
            ),
            temperature=1.0
        )
    )
    print(response.text)
    # TODO: Add analysis to some DB or file.
def main():
    # Load the key
    api_key = get_gemini_api_key()
    if not api_key:
        print("Error: 'gemini_api_key' not found in JSON.")
        return

    # Start RabbitMQ listener
    rabbit_mq_listener_setup(api_key)

if __name__ == "__main__":
    main()