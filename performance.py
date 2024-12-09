#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlalchemy
from sqlalchemy import text


# ## Step 1: Create table

# In[3]:

engine = sqlalchemy.create_engine('sqlite:///TradingData.db')

def create_performance_table(engine):
    with engine.connect() as conn:
        query = text("""
        CREATE TABLE IF NOT EXISTS bot_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,    -- Unique identifier for each record
            timestamp DATETIME,                      -- When the performance record was created
            pair TEXT,                               -- The trading pair (e.g., BTCUSDT)
            entry_price REAL,                        -- Entry price of the trade
            exit_price REAL,                         -- Exit price of the trade
            profit_loss REAL,                        -- Profit or loss from the trade
            total_profit_loss REAL,                  -- Cumulative profit/loss up to that point
            trade_count INTEGER,                     -- Number of trades up to that point
            win_count INTEGER,                       -- Number of winning trades
            loss_count INTEGER,                      -- Number of losing trades
            pct_change REAL,                         -- Percentage of interest for the trade
            cumulative_pct_change REAL DEFAULT 0,    -- Cumulative percentage change up to this trade
            trade_duration REAL                      -- Average trade duration (optional)
        );
        """)
        conn.execute(query)
        print("Performance table is ready.")


# ## Step 2: Functions

# In[5]:


def get_latest_row(engine, pair):
    """
    Fetches the row with the latest timestamp (or largest id) and returns it as a Pandas Series.
    """
    with engine.connect() as conn:
        # Select the latest row based on the largest id (which corresponds to the most recent timestamp)
        query = text(f"""
        SELECT * FROM bot_performance
        ORDER BY id DESC
        LIMIT 1;
        """)
        result = conn.execute(query).fetchone()

        if result:
            # Fetch column names using the table schema from SQLAlchemy's inspector
            inspector = sqlalchemy.inspect(engine)
            columns = [col['name'] for col in inspector.get_columns('bot_performance')]
            return pd.Series(result, index=columns)
        else:
            return None


# ### 2.1: Insert data

# In[7]:


def insert_performance_record(engine, pair, entry_price, exit_price, profit_loss, total_profit_loss, trade_count, win_count, loss_count, pct_change, cumulative_pct_change, trade_duration):
    with engine.connect() as conn:
        query = text("""
        INSERT INTO bot_performance (timestamp, pair, entry_price, exit_price, profit_loss, total_profit_loss, trade_count, win_count, loss_count, pct_change, cumulative_pct_change, trade_duration)
        VALUES (CURRENT_TIMESTAMP, :pair, :entry_price, :exit_price, :profit_loss, :total_profit_loss, :trade_count, :win_count, :loss_count, :pct_change, :cumulative_pct_change, :trade_duration);
        """)
        conn.execute(query, {
            "pair": pair,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "profit_loss": profit_loss,
            "total_profit_loss": total_profit_loss,
            "trade_count": trade_count,
            "win_count": win_count,
            "loss_count": loss_count,
            "pct_change": pct_change,
            "cumulative_pct_change": cumulative_pct_change,
            "trade_duration": trade_duration
        })
        conn.commit()
        print(f"Performance data for {pair} inserted successfully.")


# In[8]:

from datetime import datetime

def insert_trade_performance(engine, pair, entry_time, entry_price, current_price, quantity):
    
    exit_price = current_price
    profit_loss = (current_price - entry_price) * quantity
    pct_change = (current_price - entry_price) / entry_price * 100
    # Fetch the latest row from the performance table
    last_row = get_latest_row(engine, 'bot_performance')

    # If no previous record exists, initialize cumulative values
    if last_row is None:
        total_profit_loss = profit_loss
        trade_count = 1
        win_count = 1 if profit_loss > 0 else 0
        loss_count = 1 - win_count
        cumulative_pct_change = pct_change
        
    else:
        total_profit_loss = last_row['total_profit_loss'] + profit_loss
        trade_count = last_row['trade_count'] + 1
        win_count = last_row['win_count'] + (1 if profit_loss > 0 else 0)
        loss_count = trade_count - win_count
        cumulative_pct_change = ((last_row['cumulative_pct_change']/100 + 1)*(pct_change / 100 + 1) - 1) * 100
        

    current_time = datetime.now().replace(second=0, microsecond=0)
    trade_duration = (current_time - entry_time).total_seconds() / 60

    insert_performance_record(engine, pair, entry_price, exit_price, profit_loss, total_profit_loss, trade_count, win_count, loss_count, pct_change, cumulative_pct_change, trade_duration)
    
    


# ### 2.2: Total profit/loss

# In[10]:


def get_total_profit_loss(engine):
    query = text("""
    SELECT total_profit_loss
    FROM bot_performance
    ORDER BY id DESC
    LIMIT 1;
    """)
    
    # Execute the query and fetch the result
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
    
    if result:
        return result[0]  # result[0] contains the value of total_profit_loss from the last row
    else:
        return None  # If no rows are found


# ### 2.3: Win Rate

# In[12]:


def get_win_rate(engine):
    with engine.connect() as conn:
        query = text("""
        SELECT (win_count * 1.0) / (trade_count * 1.0) AS win_rate
        FROM bot_performance
        ORDER BY timestamp DESC
        LIMIT 1;
        """)
        result = conn.execute(query)
        win_rate = result.scalar()
        return win_rate


# ## Step 3:

# In[14]:

def performance_table_create():
    create_performance_table(engine)

if __name__ == "__main__":
    performance_table_create()


# In[ ]:




