#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
import os
from binance.client import Client
import math
import time
from datetime import datetime
from performance import insert_trade_performance

# Load environment variables from .env file
load_dotenv()
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_SECRET_KEY')

# Initialize the Binance client
client = Client(api_key, api_secret, testnet=True)

# Initialize the database engine
engine = sqlalchemy.create_engine('sqlite:///TradingData.db')

def fetch_closest_rows(engine, pair='BTCUSDT', num_rows=100):
    """
    Fetch the closest rows from the database for the given trading pair.
    """
    query = text(f"""
    SELECT * FROM {pair}
    ORDER BY timestamp DESC
    LIMIT :num_rows;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"num_rows": num_rows})
    df = df.iloc[::-1].reset_index(drop=True)
    df.dropna(inplace=True)
    df.set_index('slot', inplace=True)
    return df

def fetch_latest_timestamp(engine, pair='BTCUSDT'):
    """
    Fetch the latest timestamp from the database for the given trading pair.
    """
    query = text(f"""
    SELECT timestamp FROM {pair}
    ORDER BY timestamp DESC LIMIT 1;
    """)
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    if not result.empty:
        return result['timestamp'].iloc[0]
    return None

def calculate_MA(data, short_window=9, long_windows=[20, 50, 100]):
    """
    Calculate moving averages (MA) for the given data.
    """
    data['MA9'] = data['close'].rolling(window=short_window).mean()
    for window in long_windows:
        data[f'MA{window}'] = data['close'].rolling(window=window).mean()
    return data

def check_for_buy_signal(data):
    """
    Check if the MA9 crosses over any of the longer MAs (MA20, MA50, MA100).
    """
    ma9 = data['MA9'].iloc[-1]
    ma9_prev = data['MA9'].iloc[-2]

    for window in [20, 50, 100]:
        ma_long = data[f'MA{window}'].iloc[-1]
        ma_long_prev = data[f'MA{window}'].iloc[-2]
        
        # Check for crossover
        if ma9_prev < ma_long_prev and ma9 > ma_long:
            return True
    return False

def check_for_sell_signal(data, entry_price, current_price):
    """
    Check if the sell condition is met based on:
    - Short MA crossing below Long MA
    - Profit target (1.3%)
    - Stop loss (1%)
    """
    ma9 = data['MA9'].iloc[-1]
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

def get_price(symbol):
    """
    Fetch the current price for the given symbol from Binance.
    """
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker['price'])

def get_symbol_info(symbol):
    """
    Retrieve the symbol's trading info, including minimum quantity and step size.
    """
    symbol_info = client.get_symbol_info(symbol)
    return symbol_info

def get_trade_quantity(symbol, amount_range=(10, 20)):
    """
    Calculate the trade quantity within the given amount range, ensuring it's above the minimum.
    """
    price = get_price(symbol)
    symbol_info = get_symbol_info(symbol)

    # Extract filters to get minQty, stepSize
    min_quantity = None
    step_size = None
    for filter in symbol_info['filters']:
        if filter['filterType'] == 'LOT_SIZE':
            min_quantity = float(filter['minQty'])
            step_size = float(filter['stepSize'])
            break
    
    if not min_quantity or not step_size:
        raise ValueError("Minimum quantity or step size not found for this symbol.")

    # Calculate the maximum allowable quantity within the amount range
    max_quantity = amount_range[1] / price

    # Round the required quantity to the nearest step size
    rounded_quantity = math.floor(max_quantity / step_size) * step_size

    # Ensure the quantity doesn't exceed the maximum allowable quantity within the amount range
    if rounded_quantity < min_quantity:
        rounded_quantity = min_quantity

    return rounded_quantity

def execute_strategy(engine, pair='BTCUSDT', interval_seconds=20):
    """
    Execute the trading strategy by checking for buy/sell signals and placing orders.
    """
    position = False  # Set initial position state
    last_timestamp = None  # Initialize the last timestamp to track changes

    while True:
        df = fetch_closest_rows(engine, pair)
        if df.empty:
            print(f"No data available for {pair}. Skipping iteration...")
            time.sleep(interval_seconds)
            continue

        new_timestamp = df['timestamp'].iloc[-1]
        if new_timestamp != last_timestamp:
            last_timestamp = new_timestamp
            print(f"New data detected at {new_timestamp}, executing strategy...")
            
            df = calculate_MA(df)
            
            if not position and check_for_buy_signal(df):
                print("Buy signal detected! Placing order...")
                quantity = get_trade_quantity(pair)
                try:
                    order = client.order_market_buy(symbol=pair, quantity=quantity)
                    entry_stamp = datetime.now().replace(second=0, microsecond=0)
                    position = True
                    entry_price = df['close'].iloc[-1]
                    print("Order placed successfully:", order)
                except Exception as e:
                    print("Error placing order:", e)

            elif position:
                current_price = df['close'].iloc[-1]
                if check_for_sell_signal(df, entry_price, current_price):
                    print("Sell signal triggered based on crossover, profit/loss conditions. Placing sell order...")
                    try:
                        order = client.order_market_sell(symbol=pair, quantity=quantity)
                        position = False
                        print("Sell order placed successfully:", order)
                        insert_trade_performance(engine, pair, entry_stamp, entry_price, current_price, quantity)
                    except Exception as e:
                        print("Error placing sell order:", e)

        time.sleep(interval_seconds)

def strategy_executor(pair):
    """
    Execute the trading strategy for the given pair after a delay.
    """
    time.sleep(300)
    execute_strategy(engine, pair, 20)

if __name__ == "__main__":
    trading_pair = 'BTCUSDT'
    strategy_executor(trading_pair)
    print(engine)