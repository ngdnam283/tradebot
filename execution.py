import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
import os
from binance.client import Client
import math
import time
from datetime import datetime, timedelta
from performance import insert_trade_performance

# Load environment variables from .env file
load_dotenv()
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_SECRET_KEY')

# Initialize the Binance client
client = Client(api_key, api_secret, testnet=True)

# Initialize the database engine
engine = sqlalchemy.create_engine('sqlite:///TradingData.db')

def fetch_recent_rows(engine, pair='BTCUSDT', num_rows=100):
    """
    Fetch the most recent rows from the database for the given trading pair.
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

def calculate_last_two_MA(data, short_window=9, long_windows=[20, 50, 100]):
    """
    Calculate the last two values of moving averages (MA) for the given data.
    
    Parameters:
    data (pd.DataFrame): DataFrame containing the 'close' prices.
    short_window (int): The window size for the short-term MA, default is 9.
    long_windows (list): List of window sizes for the long-term MAs, default is [20, 50, 100].
    
    Returns:
    dict: A dictionary containing the last two values of the short-term MA and each long-term MA.
    """
    ma_values = {}
    
    # Calculate the last two values of the short-term MA
    ma_values['MA9'] = data['close'].rolling(window=short_window).mean().iloc[-2:].values
    
    # Calculate the last two values of each long-term MA
    for window in long_windows:
        ma_values[f'MA{window}'] = data['close'].rolling(window=window).mean().iloc[-2:].values
    
    return ma_values


def check_for_buy_signal(data):
    """
    Check if the buy signal occurs based on:
    - The MA9 crosses over any of the longer MAs (MA20, MA50, MA100).
    """
    ma_values = calculate_last_two_MA(data)
    ma9_last, ma9_prev = ma_values['MA9']

    for window in [20, 50, 100]:
        ma_long_last, ma_long_prev = ma_values[f'MA{window}']
        
        # Check for crossover
        if ma9_prev < ma_long_prev and ma9_last > ma_long_last:
            return True
    return False

def check_for_sell_signal(current_price, entry_price, highest_price):
    """
    Check if the sell condition is met based on:
    - Fall 1.3% from the highest price.
    - 1% profit target reached.
    """
    # Check for profit target (1% profit)
    profit_threshold = 1.01  # 1% profit
    if current_price >= entry_price * profit_threshold:
        print("Profit target reached: 1% profit!")
        return True
    
    # Check for fall from highest price (1.3% fall)
    loss_threshold = 0.987  # 1.3% fall
    if current_price <= highest_price * loss_threshold:
        print("Sell signal due to 1.3% fall from highest price!")
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

def get_trade_quantity(symbol, amount_range=(10, 50)):
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
    rounded_quantity = int(math.floor(max_quantity / step_size)) * step_size

    # Ensure the quantity doesn't exceed the maximum allowable quantity within the amount range
    if rounded_quantity < min_quantity:
        rounded_quantity = min_quantity

    return rounded_quantity

def execute_trading_strategy(engine, pair='BTCUSDT', interval_seconds=20):
    """
    Execute the trading strategy by checking for buy/sell signals and placing orders.
    """
    position = False  # Set initial position state
    last_timestamp = None  # Initialize the last timestamp to track changes
    highest_price = None  # Track the highest price since the buy order

    while True:
        df = fetch_recent_rows(engine, pair)
        if df.empty:
            print(f"No data available for {pair}. Skipping iteration...")
            time.sleep(interval_seconds)
            continue

        new_timestamp = df['timestamp'].iloc[-1]
        if new_timestamp != last_timestamp:
            last_timestamp = new_timestamp
            print(f"New data detected at {new_timestamp}, executing strategy...")

            current_price = df['close'].iloc[-1]

            if not position and check_for_buy_signal(df):
                print("Buy signal detected! Placing order...")
                quantity = get_trade_quantity(pair)
                print(f"Trade quantity for {pair}: {quantity}")
                try:
                    order = client.order_market_buy(symbol=pair, quantity=quantity)
                    entry_stamp = datetime.now().replace(second=(datetime.now().second // interval_seconds) * interval_seconds, microsecond=0)
                    position = True
                    entry_price = float(order['fills'][0]['price'])
                    highest_price = entry_price  # Initialize highest price
                    print("Order placed successfully:", order)
                except Exception as e:
                    print("Error placing order:", e)

            elif position:
                highest_price = max(highest_price, current_price)  # Update highest price
                if check_for_sell_signal(current_price, entry_price, highest_price):
                    print("Sell signal triggered based on profit/loss conditions. Placing sell order...")
                    try:
                        order = client.order_market_sell(symbol=pair, quantity=quantity)
                        position = False
                        print("Sell order placed successfully:", order)
                        sell_price = float(order['fills'][0]['price'])
                        insert_trade_performance(engine, pair, entry_stamp, entry_price, sell_price, quantity)
                    except Exception as e:
                        print("Error placing sell order:", e)

        time.sleep(interval_seconds)

def start_trading_strategy(pair):
    """
    Start the trading strategy for the given pair after a delay.
    """
    time.sleep(300)
    execute_trading_strategy(engine, pair, 20)

if __name__ == "__main__":
    trading_pair = 'BTCUSDT'
    start_trading_strategy(trading_pair)
    print(engine)