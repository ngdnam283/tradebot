#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime

# Initialize the database engine
engine = create_engine('sqlite:///TradingData.db')

def create_performance_table(engine):
    with engine.connect() as conn:
        query = text("""
        CREATE TABLE IF NOT EXISTS bot_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            pair TEXT,
            entry_price REAL,
            exit_price REAL,
            profit_loss REAL,
            total_profit_loss REAL,
            trade_count INTEGER,
            win_count INTEGER,
            loss_count INTEGER,
            pct_change REAL,
            cumulative_pct_change REAL DEFAULT 0,
            trade_duration REAL
        );
        """)
        conn.execute(query)
        print("Performance table is ready.")

def get_latest_row(engine):
    with engine.connect() as conn:
        query = text("""
        SELECT * FROM bot_performance
        ORDER BY id DESC
        LIMIT 1;
        """)
        result = conn.execute(query).fetchone()
        if result:
            inspector = sqlalchemy.inspect(engine)
            columns = [col['name'] for col in inspector.get_columns('bot_performance')]
            return pd.Series(result, index=columns)
        return None

def insert_performance_record(engine, **kwargs):
    with engine.connect() as conn:
        query = text("""
        INSERT INTO bot_performance (timestamp, pair, entry_price, exit_price, profit_loss, total_profit_loss, trade_count, win_count, loss_count, pct_change, cumulative_pct_change, trade_duration)
        VALUES (DATETIME(CURRENT_TIMESTAMP, '+7 hours'), :pair, :entry_price, :exit_price, :profit_loss, :total_profit_loss, :trade_count, :win_count, :loss_count, :pct_change, :cumulative_pct_change, :trade_duration);
        """)
        conn.execute(query, kwargs)
        conn.commit()
        print(f"Performance data for {kwargs['pair']} inserted successfully.")

def insert_trade_performance(engine, pair, entry_time, entry_price, current_price, quantity):
    exit_price = current_price
    profit_loss = (current_price - entry_price) * quantity
    pct_change = (current_price - entry_price) / entry_price * 100
    last_row = get_latest_row(engine)

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
        cumulative_pct_change = ((last_row['cumulative_pct_change'] / 100 + 1) * (pct_change / 100 + 1) - 1) * 100

    current_time = datetime.now().replace(second=0, microsecond=0)
    trade_duration = (current_time - entry_time).total_seconds() / 60

    insert_performance_record(engine, pair=pair, entry_price=entry_price, exit_price=exit_price, profit_loss=profit_loss, total_profit_loss=total_profit_loss, trade_count=trade_count, win_count=win_count, loss_count=loss_count, pct_change=pct_change, cumulative_pct_change=cumulative_pct_change, trade_duration=trade_duration)

def get_total_profit_loss(engine):
    query = text("""
    SELECT total_profit_loss
    FROM bot_performance
    ORDER BY id DESC
    LIMIT 1;
    """)
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        return result[0] if result else None

def get_win_rate(engine):
    query = text("""
    SELECT (win_count * 1.0) / (trade_count * 1.0) AS win_rate
    FROM bot_performance
    ORDER BY timestamp DESC
    LIMIT 1;
    """)
    with engine.connect() as conn:
        return conn.execute(query).scalar()

def performance_table_create():
    create_performance_table(engine)

if __name__ == "__main__":
    performance_table_create()




