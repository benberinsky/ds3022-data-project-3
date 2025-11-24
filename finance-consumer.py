from quixstreams import Application
import json
import os
from datetime import datetime
import duckdb

KAFKA_BROKER =  os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-trades-raw")

conn = duckdb.connect(database='trades_info.duckdb', read_only=False)
print("DuckDB connection successful")

conn.execute("CREATE SEQUENCE trade_id_seq START 1")
#conn.execute("DROP TABLE IF EXISTS trades")
conn.execute("""CREATE TABLE trades(
            id INTEGER PRIMARY KEY DEFAULT nextval('trade_id_seq'),
            time TIMESTAMP,
            trade_symbol VARCHAR,
            price DOUBLE,
            size INTEGER)
            """)

# Batch storage - persists across function calls
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

        # Add to batch (id will auto-increment)
        batch.append((timestamp, trade_symbol, price, size))

        # When batch is full, insert all at once
        if len(batch) >= BATCH_SIZE:
            conn.executemany(
                "INSERT INTO trades (time, trade_symbol, price, size) VALUES (?, ?, ?, ?)", 
                batch
            )
            conn.commit()
            print(f"Inserted batch of {len(batch)} records")
            batch.clear()
             
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
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
            print(f"Flushed final batch of {len(batch)} records")
            batch.clear()
        except Exception as e:
            print(f"Error flushing batch: {e}")

def main():
    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="INFO",
        consumer_group="stock-trades-consumer",
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["stock-trades-raw"])
        while True:
            msg = consumer.poll(5)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                #print(f"{offset} {key} {value}")
                
                # Insert into DuckDB (batched)
                if insert_trade_record(conn, key, offset, value):
                    print(f"Inserted record {offset} into DuckDB")
                else:
                    print(f"Failed to insert record {offset}")
                
                consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    finally:
        flush_batch(conn)  # Flush any remaining records
        conn.commit()
        conn.close()
        print("DuckDB connection closed")

