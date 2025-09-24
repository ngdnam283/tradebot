from flask import render_template, request, redirect, url_for, jsonify
from sqlalchemy import text
from datetime import datetime

def init_app(app, engine):

    @app.route('/')
    def index():
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM bot_performance ORDER BY timestamp DESC LIMIT 50"))
            performances = result.fetchall()
        return render_template('index.html', performances=performances)

    @app.route('/api/performance')
    def api_performance():
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM bot_performance ORDER BY timestamp DESC LIMIT 50"))
            rows = [dict(row._mapping) for row in result]
        return jsonify(rows)

    @app.route('/add', methods=['GET', 'POST'])
    def add_performance():
        if request.method == 'POST':
            # Get form data
            timestamp = request.form['timestamp']
            pair = request.form['pair']
            entry_price = float(request.form['entry_price'])
            exit_price = float(request.form['exit_price'])
            profit_loss = float(request.form['profit_loss'])
            total_profit_loss = float(request.form['total_profit_loss'])
            trade_count = int(request.form['trade_count'])
            win_count = int(request.form['win_count'])
            loss_count = int(request.form['loss_count'])
            pct_change = float(request.form['pct_change'])
            trade_duration = float(request.form['trade_duration'])
            commission_rate = float(request.form.get('commission_rate', 0.001))
            total_money_invested = float(request.form.get('total_money_invested', 0))
            total_profit = float(request.form.get('total_profit', 0))

            with engine.connect() as conn:
                query = text("""
                    INSERT INTO bot_performance 
                    (timestamp, pair, entry_price, exit_price, profit_loss, total_profit_loss, trade_count, win_count, loss_count, pct_change, trade_duration, commission_rate, total_money_invested, total_profit)
                    VALUES (:timestamp, :pair, :entry_price, :exit_price, :profit_loss, :total_profit_loss, :trade_count, :win_count, :loss_count, :pct_change, :trade_duration, :commission_rate, :total_money_invested, :total_profit)
                """)
                conn.execute(query, {
                    'timestamp': timestamp,
                    'pair': pair,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'profit_loss': profit_loss,
                    'total_profit_loss': total_profit_loss,
                    'trade_count': trade_count,
                    'win_count': win_count,
                    'loss_count': loss_count,
                    'pct_change': pct_change,
                    'trade_duration': trade_duration,
                    'commission_rate': commission_rate,
                    'total_money_invested': total_money_invested,
                    'total_profit': total_profit
                })

            return redirect(url_for('index'))

        return render_template('add.html')
