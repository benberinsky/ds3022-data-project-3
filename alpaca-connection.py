#!/usr/bin/env python3
"""
Test Alpaca WebSocket Connection
"""
import asyncio
from alpaca_trade_api.stream import Stream

# Your API credentials(SAVE AS envvars)
API_KEY = "PKWD33QEAAQG56MAEZIIPQ3SQS"
API_SECRET = "C7EWUy1dTBT7U7TathyAZxVYDx4g8b9hmB29PjbE5wLr"

async def trade_callback(trade):
    """Called every time a trade happens"""
    print(f"[{trade.timestamp}] {trade.symbol}: ${trade.price:.2f} (size: {trade.size})")

async def quote_callback(quote):
    """Called every time bid/ask changes"""
    print(f"[QUOTE] {quote.symbol}: bid=${quote.bid_price:.2f} ask=${quote.ask_price:.2f}")

async def main():
    # Initialize stream
    stream = Stream(
        key_id=API_KEY,
        secret_key=API_SECRET,
        data_feed='iex',  # Free IEX data feed
        raw_data=False
    )
    
    # Subscribe to trade updates for tech stocks
    stream.subscribe_trades(
        trade_callback,
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'
    )
    
    
    print("Connecting to Alpaca stream...")
    print("Watching: AAPL, MSFT, GOOGL, AMZN, META")
    print("Press Ctrl+C to stop\n")
    
    # Start the stream
    await stream._run_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped")