import asyncio 
from alpaca_trade_api.stream import Stream
from quixstreams import Application
import os 
import datetime

KAFKA_BROKER =  os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-trades-raw")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
print(f"Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}, Key: {API_KEY}, Secret: {API_SECRET}")

#class AlpacaKafkaProducer:
 #   def __init__(self):
        # Initialize Kafka connection
        # Initialize Alpaca connection
        
  #  async def trade_callback(self, trade):
        # Transform trade object to your schema
        # Serialize to JSON
        # Produce to Kafka
        # Handle errors
        
   # async def run(self):
        # Subscribe to symbols
        # Start stream
        # Handle Ctrl+C gracefully