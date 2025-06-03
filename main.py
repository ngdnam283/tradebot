#!/usr/bin/env python
# coding: utf-8

import threading
from execution import start_trading_strategy  # Import the execution logic from execution.py
from data_request import run as data_request_run  # Import data fetching from data_request.py
from performance import performance_table_create  # Import performance table creation from performance.py

def main(pair):
    """
    Main function to start data fetching and trading execution threads.
    """
    # Create threads for data fetching and trading execution
    data_thread = threading.Thread(target=data_request_run, args=(pair,))
    trade_thread = threading.Thread(target=start_trading_strategy, args=(pair,))

    # Start the threads
    data_thread.start()
    trade_thread.start()

    # Join the threads (wait for them to finish, but they will run indefinitely)
    data_thread.join()
    trade_thread.join()

if __name__ == "__main__":
    performance_table_create()
    pairs = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    for pair in pairs:
        threading.Thread(target=main, args=(pair,)).start()



