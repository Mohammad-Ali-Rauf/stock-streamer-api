import os
from dotenv import load_dotenv
import requests
from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Read Alpha Vantage API key from environment variables
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
if not ALPHA_VANTAGE_API_KEY:
    raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables")

# Alpha Vantage API endpoint
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE"

# Kafka producer configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock_prices"

# Create Kafka producer instance
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# Endpoint to retrieve the latest stock price for a specific symbol
@app.get("/stock/{symbol}")
async def get_stock_price(symbol: str):
    # Construct the URL with the symbol and API key
    url = f"{ALPHA_VANTAGE_URL}&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
    
    # Send a request to Alpha Vantage API
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Check if the symbol is found in the response
        if "Global Quote" in data:
            # Extract the latest price from the response
            latest_price = float(data["Global Quote"]["05. price"])
            
            # Publish the stock price data to Kafka topic
            producer.produce(KAFKA_TOPIC, key=symbol.encode(), value=str(latest_price).encode())
            producer.flush()

            return {"symbol": symbol, "price": latest_price}
        else:
            raise HTTPException(status_code=404, detail="Stock symbol not found")
    else:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch stock data")
