import json
import os
from google import genai
from google.genai import types  # Added this import

def get_api_key(file_path="../creds/api_keys.json"):
    """Reads the API key from a local JSON file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found at {file_path}")
    
    with open(file_path, "r") as f:
        data = json.load(f)
        return data.get("gemini_api_key")

def main():
    # Load the key
    api_key = get_api_key()
    
    if not api_key:
        print("Error: 'gemini_api_key' not found in JSON.")
        return

    # Initialize client with the key from JSON
    client = genai.Client(api_key=api_key)
    
    # Updated call with config to reduce latency(Takes about 1-2 seconds for simple queries, 20-30 seconds for complex ones)
    response = client.models.generate_content(
        model="gemini-3-flash-preview",
        contents="Explain how AI works in a few words",
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_level="LOW"  # Stops the "stuck" feeling for simple queries
            ),
            temperature=1.0
        )
    )
    print(response.text)

if __name__ == "__main__":
    main()