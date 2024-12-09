#!/usr/bin/env python
# coding: utf-8

# In[5]:


import threading
import time
from execution import execute_strategy  # Import the execution logic from your execution.py
from data_request import run_websocket_for_pairs  # Import data fetching from your data_request.py
from performance import performance_table_create


# In[7]:


def main(pair):
    # Create threads for data fetching and trading execution
    data_thread = threading.Thread(target=run_websocket_for_pairs, args=(pair,))
    trade_thread = threading.Thread(target=execute_strategy, args=(pair,))

    # Start the threads
    data_thread.start()
    trade_thread.start()

    # Join the threads (wait for them to finish, but they will run indefinitely)
    data_thread.join()
    trade_thread.join()


# In[ ]:


if __name__ == "__main__":
    trading_pair = "BTCUSDT"  # Replace with the desired pair
    performance_table_create()
    main(trading_pair)


# In[ ]:




