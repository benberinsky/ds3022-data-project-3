from quixstreams import Application
import json
import time
import os
import pandas as pd
from datetime import datetime
import duckdb

KAFKA_BROKER =  os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-trades-raw")

conn = duckdb.connect(database='trades_info.duckdb', read_only=False)
print("DuckDB connection successful")

conn.execute("""CREATE TABLE IF NOT EXISTS trades(
            id INTEGER PRIMARY KEY,
            time TIMESTAMP,
            trade_symbol VARCHAR,
            price DOUBLE,
            size INTEGER)
            """)

def insert_trade_record(conn, key, offset, value):
    """Insert a trade record into DuckDB"""
    try:
        action = value.get('action', 'unknown')

        # Pulling individual variables from value
        timestamp = datetime.strptime(value["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        trade_symbol = value['trade_symbol']
        price = value['price']
        size = value['size']

        #Write out to DuckDB
        #conn.execute(
       #"INSERT INTO trades (time, trade_symbol, price, size) VALUES (?, ?, ?, ?)",
       #(timestamp, trade_symbol, price, size),)
        #conn.commit()

        batch = []
        batch_size = 100

        # Collect messages
        batch.append((timestamp, trade_symbol, price, size))

        # When batch is full
        if len(batch) >= batch_size:
            conn.executemany(
                "INSERT INTO trades VALUES (?, ?, ?, ?)", 
                batch
            )
            conn.commit()
            batch.clear()
             
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False

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
                
                # Insert into MySQL
                if insert_trade_record(conn, key, offset, value):
                    print(f"Inserted record {offset} into DuckDB")
                else:
                    print(f"Failed to insert record {offset}")
                
                consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        conn.commit()
        conn.close()

