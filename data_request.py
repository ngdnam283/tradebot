import sqlalchemy
from sqlalchemy import text
import websocket
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
db_url = os.getenv('DATABASE_URL', 'sqlite:///TradingData.db')

# Initialize the database engine
engine = sqlalchemy.create_engine(db_url)

def create_table(engine, pair, buffer_size):
    """
    Creates a table for the circular buffer with a fixed number of slots.
    """
    with engine.connect() as conn:
        query = text(f"""
        CREATE TABLE IF NOT EXISTS {pair} (
            slot INTEGER PRIMARY KEY,
            timestamp DATETIME,
            close REAL
        );
        """)
        conn.execute(query)

        index_query = text(f"CREATE INDEX IF NOT EXISTS idx_timestamp_{pair} ON {pair} (timestamp);")
        conn.execute(index_query)
        
        count_query = text(f"SELECT COUNT(*) FROM {pair};")
        current_slots = conn.execute(count_query).scalar()
        if current_slots < buffer_size:
            for slot in range(current_slots + 1, buffer_size + 1):
                query_fill = text(f"INSERT INTO {pair} (slot) VALUES ({slot});")
                conn.execute(query_fill)

        conn.commit()
        print(f"Table for {pair} with {buffer_size} slots is ready.")

def get_next_slot(conn, pair):
    """
    Get the next available slot in the circular buffer.
    """
    query = text(f"""
    SELECT slot
    FROM {pair}
    WHERE timestamp IS NULL
    ORDER BY slot ASC
    LIMIT 1;
    """)
    result = conn.execute(query)
    slot = result.scalar()

    if slot is not None:
        return slot
    else:
        query = text(f"""
        SELECT slot
        FROM {pair}
        ORDER BY timestamp ASC
        LIMIT 1;
        """)
        result = conn.execute(query)
        slot = result.scalar()
        return slot

def insert_data(engine, pair, timestamp, close, buffer_size):
    """
    Inserts data into the circular buffer, overwriting the oldest slot when full.
    """
    with engine.connect() as conn:
        next_slot = get_next_slot(conn, pair)
        insert_query = text(f"""
        UPDATE {pair}
        SET timestamp = :timestamp, close = :close
        WHERE slot = :slot;
        """)
        conn.execute(insert_query, {"timestamp": timestamp, "close": close, "slot": next_slot})
        conn.commit()
        print(f"Inserted into slot {next_slot}: close={close}, timestamp={timestamp}")

def start_websocket(pair, buffer_size, time_interval):
    """
    Start a WebSocket connection to Binance and insert data into the circular buffer.
    """
    try:
        ws = websocket.create_connection(f"wss://stream.binance.com:9443/ws/{pair.lower()}@ticker")
        print("WebSocket connected successfully!")
    except websocket.WebSocketException as e:
        print(f"WebSocketException: {e}")
        return
    except Exception as e:
        print(f"Other error: {e}")
        return
    
    try:
        last_minute = None
        last_close_price = None
        while True:
            response = json.loads(ws.recv())
            current_price = float(response["c"])
            current_time = datetime.now()
            current_minute = current_time.replace(second=0, microsecond=0)

            if last_minute is None:
                last_minute = current_minute
                last_close_price = current_price
                continue
    
            if current_minute - last_minute >= timedelta(minutes=time_interval):
                insert_data(engine, pair.upper(), last_minute, last_close_price, buffer_size)
                last_minute = current_minute

            last_close_price = current_price

    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        ws.close()

def run(pair):
    """
    Run the WebSocket for the given trading pair.
    """
    buffer_size = 200
    time_interval = 1
    create_table(engine, pair, buffer_size)
    start_websocket(pair, buffer_size, time_interval)
    print("WebSocket started for the pair:", pair)

if __name__ == "__main__":
    trading_pair = 'BTCUSDT'
    run(trading_pair)
    print("WebSocket version:", websocket.__version__)




