import sqlite3
import logging

DB_FILE = "stocks.db"

def create_connection():
    """Creates a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_FILE)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def create_tables(conn):
    """Creates the necessary tables if they don't exist."""
    try:
        with conn:
            # Table 1: Static company list from Wikipedia
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sp500_companies (
                    Symbol TEXT PRIMARY KEY,
                    Security TEXT,
                    "GICS Sector" TEXT
                );
            """)
            
            # Table 2: The main data table, populated by our two ETL sources
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    Symbol TEXT PRIMARY KEY,
                    
                    -- Data from Alpha Vantage (Slow-moving)
                    Company TEXT,
                    "Market Cap ($B)" REAL,
                    "P/E Ratio" REAL,
                    last_fundamentals_update TEXT,
                    
                    -- Data from yfinance (Fast-moving)
                    "YTD Return (%)" REAL,
                    last_performance_update TEXT,
                    
                    FOREIGN KEY (Symbol) REFERENCES sp500_companies (Symbol)
                );
            """)
        logging.info("Database tables created/verified.")
    except sqlite3.Error as e:
        logging.error(f"Error creating tables: {e}")