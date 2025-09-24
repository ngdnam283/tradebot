import os
from flask import Flask
from sqlalchemy import create_engine

def create_app():
    app = Flask(__name__)

    # Create DB engine; adjust relative path to your DB if needed
    basedir = os.path.abspath(os.path.dirname(__file__))
    db_path = os.path.join(basedir, '..', 'TradingData.db')
    engine = create_engine(f'sqlite:///{db_path}')
    # Import routes and initialize with app and engine
    from . import routes
    routes.init_app(app, engine)

    return app
