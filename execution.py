#!/usr/bin/env python
# coding: utf-8

# ## Step 1: Fetch data

# In[2]:


import pandas as pd
import sqlalchemy


# In[3]:


engine = sqlalchemy.create_engine('sqlite:///TradingData.db')


# In[4]:


from sqlalchemy import text


# In[5]:


def fetch_closest_rows(engine, pair='BTCUSDT', num_rows=100):
    query = text(f"""
    SELECT * FROM {pair}
    ORDER BY timestamp DESC
    LIMIT :num_rows;
    """)  # Fetch the rows in reverse chronological order
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"num_rows": num_rows})  # Use params to pass the LIMIT
    # Reverse the DataFrame to get the rows in chronological order
    df = df.iloc[::-1].reset_index(drop=True)
    df.dropna(inplace=True)
    df.set_index('slot', inplace=True)
    return df


# In[6]:


def fetch_latest_timestamp(engine, pair='BTCUSDT'):
    query = text(f"""
    SELECT timestamp FROM {pair}
    ORDER BY timestamp DESC LIMIT 1;
    """)
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    if not result.empty:
        return result['timestamp'].iloc[0]
    return None


# ## Step 2: Function to place an order

# In[8]:


# !pip install python-dotenv


# In[9]:


from dotenv import load_dotenv
import os


# In[10]:


load_dotenv()
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_SECRET_KEY')


# In[11]:


from binance.client import Client


# In[12]:


client = Client(api_key, api_secret, testnet=True)


# ## Step 3: Strategy

# ### Step 3.1: Manipulate data

# In[15]:


def calculate_MA(data, short_window=9, long_windows=[20, 50, 100]):
    data['MA9'] = data['close'].rolling(window=short_window).mean()
    for window in long_windows:
        data[f'MA{window}'] = data['close'].rolling(window=window).mean()
    return data


# ### Step 3.2: Check for a signal

# In[17]:


def check_for_buy_signal(data):
    """
    Check if the MA9 crosses over any of the longer MAs (MA20, MA50, MA100).
    """
    ma9 = data['MA9'].iloc[-1]
    ma9_prev = data['MA9'].iloc[-2]

    for window in [20, 50]:
        ma_long = data[f'MA{window}'].iloc[-1]
        ma_long_prev = data[f'MA{window}'].iloc[-2]
        
        # Check for crossover
        if ma9_prev < ma_long_prev and ma9 > ma_long:
            return True
    return False


# In[18]:


def check_for_sell_signal(data, entry_price, current_price):
    """
    Check if the sell condition is met based on:
    - Short MA crossing below Long MA
    - Profit target (1.3%)
    - Stop loss (1%)
    """
    if not data.empty and 'MA9' in data.columns:
        ma9 = data['MA9'].iloc[-1]
        if pd.notna(ma9):
            print(f"MA9 value: {ma9}")
        else:
            print("Last value in 'MA9' is NaN.")
    else:
        print("DataFrame is empty or 'MA9' column not found.")
        
    ma9_prev = data['MA9'].iloc[-2]

    # Check for crossover (short MA crossing below long MA)
    for window in [50, 100]:  # MA50, MA100
        ma_long = data[f'MA{window}'].iloc[-1]
        ma_long_prev = data[f'MA{window}'].iloc[-2]
        
        if ma9_prev > ma_long_prev and ma9 < ma_long:
            print("Sell signal due to MA crossover: MA9 crossed below MA50 or MA100")
            return True
    
    # Check for profit target (1.3% profit)
    profit_threshold = 1.013  # 1.3% profit
    if current_price >= entry_price * profit_threshold:
        print("Profit target reached: 1.3% profit!")
        return True
    
    # Check for stop loss (1% loss)
    loss_threshold = 0.99  # 1% loss
    if current_price <= entry_price * loss_threshold:
        print("Stop loss triggered: 1% loss!")
        return True
    
    return False


# ### Step 3.3: Calculate suitable quantity for trade

# In[20]:


import math


# In[21]:


# Get current price of a symbol
def get_price(symbol):
    """
    Fetches the current price for the given symbol from Binance.
    """
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker['price'])


# In[22]:


# Get symbol's trading info (including min quantity and step size)
def get_symbol_info(symbol):
    """
    Retrieves the symbol's trading info, including minimum quantity and step size.
    """
    symbol_info = client.get_symbol_info(symbol)
    return symbol_info


# In[23]:


# Calculate reasonable trade quantity based on price and amount range
def get_trade_quantity(symbol, amount_range=(10, 20)):
    """
    Calculates the trade quantity within the given amount range, ensuring it's above the minimum.
    """
    price = get_price(symbol)
    symbol_info = get_symbol_info(symbol)

    # Extract filters to get minQty, stepSize
    min_quantity = None
    step_size = None
    for filter in symbol_info['filters']:
        if filter['filterType'] == 'LOT_SIZE':
            required_quantity = float(filter['minQty'])
            step_size = float(filter['stepSize'])
            break
    
    if not required_quantity or not step_size:
        raise ValueError("Minimum quantity or step size not found for this symbol.")

    
    # Calculate the maximum allowable quantity within the amount range
    max_quantity = amount_range[1] / price

    # Round the required quantity to the nearest step size
    rounded_quantity = math.floor(max_quantity / step_size) * step_size

    # Ensure the quantity doesn't exceed the maximum allowable quantity within the amount range
    if rounded_quantity < required_quantity:
        rounded_quantity = required_quantity

    
    # Return the final rounded quantity
    return rounded_quantity


# ## Step 4: Execute order

# In[25]:


import time
from datetime import datetime
from performance import insert_trade_performance 


# In[26]:


def execute_strategy(engine, pair='BTCUSDT', interval_seconds=20):
    position = False  # Set initial position state

    
    last_timestamp = None  # Initialize the last timestamp to track changes

    while True:
        df = fetch_closest_rows(engine, pair)
        # Skip if the DataFrame is empty
        if df.empty:
            print(f"No data available for {pair}. Skipping iteration...")
            time.sleep(interval_seconds)  # Wait for the next iteration
            continue

        # Extract the latest timestamp from the DataFrame
        new_timestamp = df['timestamp'].iloc[-1]

        # If the timestamp has changed (new data added), fetch the data and execute the strategy
        if new_timestamp != last_timestamp:
            last_timestamp = new_timestamp  # Update last_timestamp to the latest one
            print(f"New data detected at {new_timestamp}, executing strategy...")
            
            # Calculate moving averages
            df = calculate_MA(df)
            
            # Check for buy signal
            if not position and check_for_buy_signal(df):
                print("Buy signal detected! Placing order...")
                # Example buy order:
                quantity = get_trade_quantity(pair)  # calculate resonable quantity
                try:
                    order = client.order_market_buy(
                            symbol=pair,
                            quantity=quantity
                        )
                    entry_stamp = datetime.now().replace(second=0, microsecond=0)
                    position = True  # Update position state
                    entry_price = df['close'].iloc[-1]  # Store the entry price
                    print("Order placed successfully:", order)
                
                except Exception as e:
                    print("Error placing order:", e)

            elif position:
                current_price = df['close'].iloc[-1]  # Get the latest price
                
                if check_for_sell_signal(df, entry_price, current_price):
                    print("Sell signal triggered based on crossover, profit/loss conditions. Placing sell order...")
                    try:
                        order = client.order_market_sell(
                                symbol=pair,
                                quantity=quantity
                            )
                        
                        position = False  # Update position state to False (sold)
                        print("Sell order placed successfully:", order)

                        #insert that trade into the performance table
                        insert_trade_performance(engine, pair, entry_stamp, entry_price, current_price, quantity)
                    
                    except Exception as e:
                        print("Error placing sell order:", e)

        # Wait briefly before checking again (polling every 1 second)
        time.sleep(interval_seconds)

# In[27]:


def strategy_execute(pair):
    time.sleep(300)
    execute_strategy(engine, pair, 20)



if __name__ == "__main__":
    trading_pair = 'BTCUSDT'
    strategy_execute(trading_pair)
    print(engine)


# In[ ]:




