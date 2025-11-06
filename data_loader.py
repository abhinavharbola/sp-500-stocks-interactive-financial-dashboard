import pandas as pd
import yfinance as yf
import requests
from io import StringIO
import logging
from datetime import datetime
import time  # Import the time module

def get_sp500_list():
    """
    Scrapes the Wikipedia page for the S&P 500 company list.
    Returns a DataFrame with Symbol, Security, and GICS Sector.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        df_list = pd.read_html(StringIO(response.text))
        df = df_list[0]
        df = df[['Symbol', 'Security', 'GICS Sector']].copy()
        # Clean up symbols (e.g., 'BF.B' to 'BF-B')
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df
    except Exception as e:
        logging.error(f"Failed to scrape S&P 500 list: {e}")
        return pd.DataFrame()

def get_performance_data(symbols):
    """
    Gets daily-fresh YTD performance data for all symbols using yfinance.
    Downloads in batches for stability.
    Returns a DataFrame with Symbol, YTD Return (%), and timestamp.
    """
    logging.info(f"Fetching yfinance performance data for {len(symbols)} symbols in batches...")
    perf_data = []
    today = datetime.now().isoformat()
    batch_size = 100
    
    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i + batch_size]
        logging.info(f"Processing performance batch {i//batch_size + 1}/{(len(symbols)//batch_size) + 1} ({len(batch_symbols)} symbols)")
        
        try:
            # Batch download this group of symbols
            yf_data = yf.download(
                tickers=batch_symbols, 
                period="ytd", 
                interval="1d", 
                progress=False,
                auto_adjust=True  # Fixes the FutureWarning
            )
            
            if yf_data.empty:
                logging.warning(f"yfinance download returned no data for batch {i//batch_size + 1}.")
                continue

            for sym in batch_symbols:
                try:
                    # Access this symbol's 'Close' data
                    # yf.download with multiple tickers returns a multi-index column
                    data = yf_data['Close'][sym].dropna()
                    
                    if data.empty:
                        continue
                    
                    start_price = float(data.iloc[0])
                    end_price = float(data.iloc[-1])
                    ytd_return = ((end_price - start_price) / start_price) * 100
                    
                    perf_data.append({
                        "Symbol": sym,
                        "YTD Return (%)": round(ytd_return, 2),
                        "last_performance_update": today
                    })
                except (KeyError, IndexError):
                    # KeyError if ticker failed
                    # IndexError if it downloaded but had no YTD data
                    logging.warning(f"Could not process YTD for symbol {sym} in batch.")
                except Exception as e:
                    logging.error(f"Unexpected error processing {sym}: {e}")
            
            time.sleep(0.5) # Pause briefly between batches

        except Exception as e:
            logging.error(f"yfinance batch download failed for batch {i//batch_size + 1}: {e}")
            
    logging.info(f"Finished yfinance performance processing. Collected {len(perf_data)} records.")
    return pd.DataFrame(perf_data)

def get_fundamental_data_yfinance(symbols):
    """
    Gets fundamental data for all symbols using yfinance.
    This is slower than batch performance data, so it runs per-ticker.
    Returns a DataFrame.
    """
    logging.info(f"Fetching yfinance fundamental data for {len(symbols)} symbols...")
    funda_data = []
    today = datetime.now().isoformat()
    
    for i, symbol in enumerate(symbols):
        if i % 25 == 0 and i > 0:
            logging.info(f"Fundamental fetch progress: {i}/{len(symbols)}")
            
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            market_cap = info.get("marketCap", 0)
            # Get trailing P/E, fallback to forward P/E
            pe_ratio = info.get("trailingPE", info.get("forwardPE")) 

            # yfinance often returns string 'None' or just None, handle this
            if pe_ratio is None or not isinstance(pe_ratio, (int, float)):
                pe_ratio = None
            
            funda_data.append({
                "Symbol": symbol,
                "Company": info.get("longName"),
                "Market Cap ($B)": round(float(market_cap) / 1e9, 2) if market_cap else 0,
                "P/E Ratio": pe_ratio,
                "last_fundamentals_update": today
            })
            
            # yfinance can be rate-limited, add a small pause
            time.sleep(0.05) 

        except Exception as e:
            # yfinance often throws errors for delisted/problematic tickers
            logging.warning(f"Failed to get fundamental data for {symbol}: {e}")

    logging.info(f"Finished fundamental data processing. Collected {len(funda_data)} records.")
    return pd.DataFrame(funda_data)