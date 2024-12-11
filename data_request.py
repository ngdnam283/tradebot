# ## Step 1: Create table




import sqlalchemy
from sqlalchemy import text


# In[3]:


engine = sqlalchemy.create_engine('sqlite:///TradingData.db')


# In[4]:


# Function to create a table dynamically based on the pair
def create_table_for_pair(engine, pair, buffer_size):
    """
    Creates a table for the circular buffer with a fixed number of slots.
    """
    with engine.connect() as conn:
        # Create the table
        query = text(f"""
        CREATE TABLE IF NOT EXISTS {pair} (
            slot INTEGER PRIMARY KEY, -- Slot position (1 to buffer_size)
            timestamp DATETIME,
            close REAL
        );
        """)
        conn.execute(query)

        index_query = text(f"CREATE INDEX IF NOT EXISTS idx_timestamp_{pair} ON {pair} (timestamp);")
        conn.execute(index_query)
        
        # Pre-fill the table with empty slots if not already filled
        count_query = text(f"SELECT COUNT(*) FROM {pair};")
        current_slots = conn.execute(count_query).scalar()
        if current_slots < buffer_size:
            for slot in range(current_slots + 1, buffer_size + 1):
                query_fill = text(f"INSERT INTO {pair} (slot) VALUES ({slot});")
                conn.execute(query_fill)

        conn.commit()
        print(f"Table for {pair} with {buffer_size} slots is ready.")



# ## Step 2: Insert data into table


def get_next_slot(conn, pair):
    # First, try to find a slot where the timestamp is NULL
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
        # If there's a slot with NULL timestamp, return it
        return slot
    else:
        # Otherwise, find the slot with the earliest timestamp
        query = text(f"""
        SELECT slot
        FROM {pair}
        ORDER BY timestamp ASC
        LIMIT 1;
        """)
        result = conn.execute(query)
        slot = result.scalar()
        return slot


# In[7]:


def insert_into_circular_buffer(engine, pair, timestamp, close, buffer_size):
    """
    Inserts data into the circular buffer, overwriting the oldest slot when full.
    """
    with engine.connect() as conn:
        # Get the next slot to overwrite
        next_slot = get_next_slot(conn, pair)

        # Insert/Update the data in the slot
        insert_query = text(f"""
        UPDATE {pair}
        SET timestamp = :timestamp, close = :close
        WHERE slot = :slot;
        """)
        conn.execute(insert_query, {"timestamp": timestamp, "close": close, "slot": next_slot})
        conn.commit()
        print(f"Inserted into slot {next_slot}: close={close}, timestamp={timestamp}")


# ## Step 3: Request data from Websocket

# In[9]:


import websocket
import json
from datetime import datetime, timedelta


# In[10]:


def binance_websocket(pair, buffer_size, time_interval):    
    # ws = websocket.create_connection(f"wss://stream.binance.com:9443/ws/{pair.lower()}@ticker")
    # last_minute = None
    # last_close_price = None
    try:
        ws = websocket.create_connection(f"wss://stream.binance.com:9443/ws/{pair.lower()}@ticker")
        print("WebSocket connected successfully!")
    except websocket.WebSocketException as e:
        print(f"WebSocketException: {e}")
    except Exception as e:
        print(f"Other error: {e}")
    
    try:
        ws = websocket.create_connection(f"wss://stream.binance.com:9443/ws/{pair.lower()}@ticker")
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
                continue  # Skip to the next iteration
    
            if current_minute - last_minute >= timedelta(minutes=time_interval):
                insert_into_circular_buffer(engine, pair.upper(), last_minute, last_close_price, buffer_size)
                last_minute = current_minute

            last_close_price = current_price

            
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        ws.close()
    


# ## Step 4: Run stage

# In[12]:


def run_websocket_for_pairs(pair):

    buffer_size = 200
    time_interval = 1
    # Create tables pair and start a websocket
    create_table_for_pair(engine, pair, buffer_size)  # Assuming create_table_for_pair is defined elsewhere
    binance_websocket(pair, buffer_size, time_interval)

    print("Websockets started for all pairs.")


# In[13]:


if __name__ == "__main__":
    trading_pair = 'BTCUSDT'
    run_websocket_for_pairs(trading_pair)
    print("websocket version:", websocket.__version__)






# In[ ]:




