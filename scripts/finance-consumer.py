from quixstreams import Application
import json
import os
from datetime import datetime
import duckdb
import logging

# Configuring logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',)

# Creating instance of logger
logger = logging.getLogger(__name__)

# Getting env variables
KAFKA_BROKER =  os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-trades-raw")

# Creating duckdb connection to send data to
conn = duckdb.connect(database='trades_info.duckdb', read_only=False)
print("DuckDB connection successful")
logger.info("DuckDb connection successful")

# Creating sequence for trade ids, track progress
conn.execute("CREATE SEQUENCE trade_id_seq START 1")
# Making table with defined data types
conn.execute("""CREATE TABLE trades(
            id INTEGER PRIMARY KEY DEFAULT nextval('trade_id_seq'),
            time TIMESTAMP,
            trade_symbol VARCHAR,
            price DOUBLE,
            size INTEGER)
            """)
logger.info("DuckDB schema initialized")

# Batch storage 
batch = []
BATCH_SIZE = 100

def insert_trade_record(conn, key, offset, value):
    """Insert a trade record into DuckDB (batched)"""
    global batch
    try:
        # Pulling individual variables from value
        timestamp = datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        trade_symbol = value['trade_symbol']
        price = value['price']
        size = value['size']

        # Add to batch (id auto-increments)
        batch.append((timestamp, trade_symbol, price, size))

        # When batch is full, insert all at once
        if len(batch) >= BATCH_SIZE:
            conn.executemany(
                "INSERT INTO trades (time, trade_symbol, price, size) VALUES (?, ?, ?, ?)", 
                batch
            )
            conn.commit()
            logger.info(f"Inserted batch of {len(batch)} records (last offset: {offset})")
            batch.clear()
             
        return True
    except Exception as e:
        logger.error(f"Error inserting record at offset {offset}: {e}")
        return False

def flush_batch(conn):
    """Flush any remaining records in the batch"""
    global batch
    if batch:
        try:
            conn.executemany(
                "INSERT INTO trades (time, trade_symbol, price, size) VALUES (?, ?, ?, ?)", 
                batch
            )
            conn.commit()
            logger.info(f"Flushed final batch of {len(batch)} records")
            batch.clear()
        except Exception as e:
            logger.error(f"Error flushing batch: {e}")

# Configuring Kafka settings
def main():
    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="INFO",
        consumer_group="stock-trades-consumer",
        auto_offset_reset="earliest",
    )

    logger.info(f"Starting Kafka consumer: broker={KAFKA_BROKER}, topic={KAFKA_TOPIC}")

    with app.get_consumer() as consumer:
        # Subscribe to the Kafka topic containing raw stock trade data
        consumer.subscribe(["stock-trades-raw"])
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
        # Polls for new messages continuously
        while True:
            msg = consumer.poll(5)
            # Wait if no messages present, print to console
            if msg is None:
                print("Waiting...")
            # Handle error if present
            elif msg.error() is not None:
                logger.error(f"Kafka error: {msg.error()}")
                raise Exception(msg.error())
            # Message received 
            else:
                # Extract message components
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()
                
                # Insert into DuckDB (batched)
                if insert_trade_record(conn, key, offset, value):
                    print(f"Inserted record {offset} into DuckDB")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                
                consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        logger.info("Stock Trades Consumer Starting")
        main()
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
    finally:
        logger.info("Shutting down gracefully...")
        flush_batch(conn)  # Flush any remaining records
        conn.commit()
        conn.close()
        logger.info("DuckDB connection closed. Consumer stopped.")

