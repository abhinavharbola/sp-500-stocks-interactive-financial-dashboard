import logging
import pandas as pd
from datetime import datetime

# No longer need config
from database import create_connection, create_tables
from data_loader import (
    get_sp500_list, 
    get_performance_data, 
    get_fundamental_data_yfinance # Use new yfinance function
)

# --- CONFIG ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# No longer need batch size
DB_FILE = "stocks.db"

def update_sp500_list(conn, sp500_df):
    """Updates the sp500_companies table."""
    logging.info(f"Updating sp500_companies table with {len(sp500_df)} stocks.")
    sp500_df.to_sql("sp500_companies", conn, if_exists="replace", index=False)

def update_performance_db(conn, perf_df):
    """
    UPSERTS (Update or Insert) performance data into the stock_data table.
    """
    logging.info(f"Upserting {len(perf_df)} performance records...")
    sql_upsert = """
        INSERT INTO stock_data (Symbol, "YTD Return (%)", last_performance_update)
        VALUES (?, ?, ?)
        ON CONFLICT(Symbol) DO UPDATE SET
            "YTD Return (%)" = excluded."YTD Return (%)",
            last_performance_update = excluded.last_performance_update;
    """
    try:
        with conn:
            conn.executemany(sql_upsert, perf_df.to_records(index=False))
        logging.info("Performance data upsert complete.")
    except Exception as e:
        logging.error(f"Error upserting performance data: {e}")

def update_fundamentals_db(conn, funda_df):
    """
    UPSERTS (Update or Insert) fundamental data into the stock_data table.
    Now takes a DataFrame.
    """
    if funda_df.empty:
        logging.warning("No fundamental data to update.")
        return
        
    logging.info(f"Upserting {len(funda_df)} fundamental records...")
    sql_upsert = """
        INSERT INTO stock_data (Symbol, Company, "Market Cap ($B)", "P/E Ratio", last_fundamentals_update)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(Symbol) DO UPDATE SET
            Company = excluded.Company,
            "Market Cap ($B)" = excluded."Market Cap ($B)",
            "P/E Ratio" = excluded."P/E Ratio",
            last_fundamentals_update = excluded.last_fundamentals_update;
    """
    try:
        # Convert NaNs (like for P/E ratio) to None (NULL in SQL)
        funda_df_sql = funda_df.where(pd.notnull(funda_df), None)
        with conn:
            conn.executemany(sql_upsert, funda_df_sql.to_records(index=False))
        logging.info("Fundamental data upsert complete.")
    except Exception as e:
        logging.error(f"Error upserting fundamental data: {e}")

# No longer need get_stocks_to_update function

def run_etl():
    """Main ETL orchestration function (yfinance only)."""
    logging.info("Starting YFINANCE-ONLY ETL process...")
    conn = create_connection()
    if not conn:
        return
    
    create_tables(conn)
    
    # 1. Update S&P 500 Company List
    sp500_df = get_sp500_list()
    if sp500_df.empty:
        logging.error("ETL failed: Could not get S&P 500 list.")
        return
    update_sp500_list(conn, sp500_df)
    all_symbols = sp500_df['Symbol'].tolist()

    # 2. Get Performance Data (yfinance)
    perf_df = get_performance_data(all_symbols)
    if not perf_df.empty:
        update_performance_db(conn, perf_df)

    # 3. Get Fundamental Data (yfinance)
    logging.info("Starting yfinance fundamental data fetch for all stocks...")
    funda_df = get_fundamental_data_yfinance(all_symbols)
    if not funda_df.empty:
        update_fundamentals_db(conn, funda_df)
    
    conn.close()
    logging.info("Yfinance-only ETL process complete.")

if __name__ == "__main__":
    run_etl()