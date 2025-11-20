import asyncio 
from alpaca_trade_api.stream import Stream
from quixstreams import Application
import os 
import datetime
import logging
import json

# Getting env vars, printing to confirm successful access
KAFKA_BROKER =  os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-trades-raw")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
print(f"Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}, Key: {API_KEY}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',)

logger = logging.getLogger(__name__)


class AlpacaKafkaProducer:
    # Initialize Kafka connection
    def __init__(self):
        self.app = Application(
            broker_address = KAFKA_BROKER
        )
        self.producer = None

    # getting producer when opening connection
    def __enter__(self):
        self.producer = self.app.get_producer()
        return self
    
    # closing connection
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.producer:
            self.producer.close()

    def send_event(self, trade):
        try:
            # Extract key from the event (using id if available, otherwise generate one)
            key = str(trade.timestamp)
            timestamp = trade.timestamp
            trade_symbol = trade.symbol
            price = trade.price
            size = trade.size
            data = {"timestamp": timestamp, "trade_symbol": trade_symbol,
            "price":price, "size": size}
                
            # Send to Kafka
            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=key,
                value=json.dumps(data),
            )
                
            # Log event being sent
            logger.info(f"Sent event to Kafka at: {key}")
                
                
        except json.JSONDecodeError:
            logger.error("Unable to parse JSON message")
        except Exception as e:
            logger.exception(f"Error processing message: {e}")

    async def trade_callback(self,trade):
        """Called every time a trade happens"""
        self.send_event(trade)
        logger.info(f"[{trade.timestamp}] {trade.symbol}: ${trade.price:.2f} (size: {trade.size})")
    
    
    async def main(self):
        # Initialize stream
        stream = Stream(
            key_id=API_KEY,
            secret_key=API_SECRET,
            data_feed='iex',  # Free IEX data feed
            raw_data=False
        )
        
        # Subscribe to trade updates for tech stocks
        stream.subscribe_trades(
            self.trade_callback,
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'
        )
        
        
        print("Connecting to Alpaca stream...")
        print("Watching: AAPL, MSFT, GOOGL, AMZN, META")
        print("Press Ctrl+C to stop\n")
        
        # Start the stream
        await stream._run_forever()

if __name__ == "__main__":
    producer = AlpacaKafkaProducer()
    with producer:
        asyncio.run(producer.main())