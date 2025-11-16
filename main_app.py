import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- App Config ---
st.set_page_config(page_title="FinDash S&P 500", layout="wide", page_icon="🏦")
st.title('🏦 FinDash S&P 500 Analyzer')
st.markdown("A high-performance dashboard for the S&P 500, powered by a yfinance ETL pipeline.")

DB_FILE = "stocks.db"

# --- Data Loading ---
@st.cache_data(ttl=600)  # Cache the data for 10 minutes
def load_data():
    """Loads all data from the SQLite database."""
    try:
        conn = sqlite3.connect(DB_FILE)
        query = """
            SELECT
                c.Symbol, 
                c.Security, 
                c."GICS Sector",
                d.Company, 
                d."Market Cap ($B)", 
                d."P/E Ratio",
                d."YTD Return (%)",
                d.last_fundamentals_update,
                d.last_performance_update
            FROM sp500_companies c
            LEFT JOIN stock_data d ON c.Symbol = d.Symbol
        """
        df = pd.read_sql(query, conn)
        conn.close()
        df['last_performance_update'] = pd.to_datetime(df['last_performance_update'])
        df['last_fundamentals_update'] = pd.to_datetime(df['last_fundamentals_update'])
        return df
    except Exception as e:
        st.error(f"Failed to load data from database. Run `python etl.py` first. Error: {e}")
        return pd.DataFrame()

# --- Plotting Functions ---

def plot_comparison_chart(symbols):
    """Plots a normalized YTD performance chart for multiple stocks."""
    try:
        data = yf.download(symbols, period="ytd", auto_adjust=True, timeout=10)['Close']
        if data.empty:
            st.warning("No YTD data found for the selected tickers.")
            return
        if isinstance(data, pd.Series):
            data = data.to_frame(name=symbols[0])
        
        # Normalize data
        normalized_data = (data / data.iloc[0]) * 100
        
        fig = px.line(
            normalized_data,
            x=normalized_data.index,
            y=normalized_data.columns,
            title="Normalized YTD Performance (Starting at 100)"
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Normalized Price (Base 100)",
            legend_title="Ticker"
        )
        st.plotly_chart(fig, width='stretch')

    except Exception as e:
        st.error(f"Could not plot comparison chart: {e}")

def plot_stock_chart(symbol):
    """Plots an interactive Plotly Candlestick chart for a single stock."""
    try:
        data = yf.download(symbol, period="ytd", interval="1d", auto_adjust=True, timeout=10)
        if data.empty:
            st.warning(f"No YTD data found for {symbol}")
            return

        data['MA50'] = data['Close'].rolling(window=50).mean()
        data['MA200'] = data['Close'].rolling(window=200).mean()

        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=data.index, open=data['Open'], high=data['High'], low=data['Low'], close=data['Close'], name='Candlestick'
        ))
        fig.add_trace(go.Scatter(
            x=data.index, y=data['MA50'], mode='lines', name='50-Day MA', line=dict(color='orange', width=1.5)
        ))
        fig.add_trace(go.Scatter(
            x=data.index, y=data['MA200'], mode='lines', name='200-Day MA', line=dict(color='blue', width=1.5)
        ))
        fig.update_layout(
            title=f"{symbol} - YTD Candlestick Chart with Moving Averages",
            yaxis_title="Price ($)", xaxis_title="Date", legend_title="Legend", xaxis_rangeslider_visible=False
        )
        st.plotly_chart(fig, width='stretch')
        
    except Exception as e:
        st.error(f"Could not plot chart: {e}")

# --- Load Data ---
df = load_data()
if df.empty:
    st.stop()

# --- Sidebar Controls ---
st.sidebar.header('📊 Screener Controls')
sectors = sorted(df['GICS Sector'].dropna().unique())
selected_sectors = st.sidebar.multiselect("Select Sectors", sectors, default=sectors)

min_cap_val = df['Market Cap ($B)'].min()
max_cap_val = df['Market Cap ($B)'].max()
# Handle cases where data might be empty or NaN
if pd.isna(min_cap_val) or pd.isna(max_cap_val):
    min_cap_val, max_cap_val = 0.0, 5000.0
else:
    min_cap_val, max_cap_val = float(min_cap_val), float(max_cap_val)
selected_cap = st.sidebar.slider("Market Cap ($B)", min_cap_val, max_cap_val, (min_cap_val, max_cap_val))

min_pe_val = df['P/E Ratio'].min()
# Handle cases where min_pe might be NaN
if pd.isna(min_pe_val):
    min_pe_val = 0.0
else:
    min_pe_val = float(min_pe_val)

# Cap the max slider value for usability, though real data may go higher
selected_pe = st.sidebar.slider("P/E Ratio Range", min_pe_val, 150.0, (min_pe_val, 150.0))
# Checkbox to include companies without P/E data (often unprofitable or missing)
include_na_pe = st.sidebar.checkbox("Include companies with N/A P/E Ratio", value=True)

st.sidebar.header("📈 Live Stock Chart")
watch_ticker = st.sidebar.text_input("Enter one ticker (e.g., AAPL):")

st.sidebar.header("⚖️ Stock Comparison")
all_symbols = sorted(df['Symbol'].dropna().unique())
compare_tickers = st.sidebar.multiselect(
    "Select tickers to compare (YTD % Change):",
    all_symbols
)

st.sidebar.subheader("Data Freshness")
try:
    perf_time = df['last_performance_update'].max().strftime('%Y-%m-%d %H:%M')
    st.sidebar.info(f"Performance Data (YTD): \n{perf_time}")
    funda_time = df['last_fundamentals_update'].max().strftime('%Y-%m-%d %H:%M')
    st.sidebar.info(f"Fundamentals Data: \n{funda_time}")
except:
    st.sidebar.error("ETL data not found.")

# --- Main Page Filtering ---

# 1. Sector Filter
sector_filter = df['GICS Sector'].isin(selected_sectors)

# 2. Market Cap Filter
cap_filter = df['Market Cap ($B)'].between(selected_cap[0], selected_cap[1])

# 3. P/E Ratio Filter (Complex logic to handle N/A)
if include_na_pe:
    # Include stocks within range OR stocks with no P/E (NaN)
    pe_filter = (
        df['P/E Ratio'].between(selected_pe[0], selected_pe[1]) |
        df['P/E Ratio'].isna()
    )
else:
    # Strictly enforce range, excluding NaN
    pe_filter = df['P/E Ratio'].between(selected_pe[0], selected_pe[1])

# Apply all filters
filtered_df = df[sector_filter & cap_filter & pe_filter]

st.header(f"🏢 Displaying {len(filtered_df)} of {len(df)} Companies")
st.dataframe(filtered_df.drop(columns=['last_fundamentals_update', 'last_performance_update']))

# --- Main Page Charting ---

# Plot comparison chart first, if selected
if compare_tickers:
    st.header(f"⚖️ YTD Performance Comparison: {', '.join(compare_tickers)}")
    plot_comparison_chart(compare_tickers)

# Only show live chart if NOT comparing
if watch_ticker and not compare_tickers:
    st.header(f"📈 Live Chart: {watch_ticker.upper()}")
    plot_stock_chart(watch_ticker.upper())

# --- Dashboard Metrics ---
st.header("📈 Market Dashboard")
tabs = st.tabs(["Top Performers", "Sector Performance", "Correlation Analysis"])

with tabs[0]:
    st.subheader("🏆 Top & Bottom 10 Performers (YTD)")
    col1, col2 = st.columns(2)
    
    # Top 10
    top_10 = filtered_df.sort_values("YTD Return (%)", ascending=False).head(10)
    col1.dataframe(top_10[['Symbol', 'Company', 'YTD Return (%)']])
    
    # Bottom 10
    bottom_10 = filtered_df.sort_values("YTD Return (%)", ascending=True).head(10)
    col2.dataframe(bottom_10[['Symbol', 'Company', 'YTD Return (%)']])

with tabs[1]:
    st.subheader("📊 Average YTD Return by Sector")
    # Calculate mean return per sector based on filtered data
    sector_perf = filtered_df.groupby('GICS Sector')['YTD Return (%)'].mean().sort_values(ascending=False).reset_index()
    
    fig = px.bar(
        sector_perf, x="YTD Return (%)", y="GICS Sector", orientation='h',
        title="Average YTD Return by Sector", color="GICS Sector", template="plotly_white"
    )
    fig.update_layout(yaxis_title="Sector", xaxis_title="Average YTD Return (%)", showlegend=False)
    st.plotly_chart(fig, width='stretch')

with tabs[2]:
    st.subheader("Stock Correlation Analysis")
    st.markdown("This heatmap shows the correlation of daily returns between the stocks selected in the **'Stock Comparison'** sidebar menu.")
    
    # Use the comparison list for correlation
    symbols_list = compare_tickers
    
    if len(symbols_list) < 2:
        st.warning("Please select at least two tickers in the 'Stock Comparison' sidebar to display a correlation matrix.")
    else:
        try:
            # Download data for correlation
            all_data = yf.download(symbols_list, period="ytd", auto_adjust=True)['Close']
            
            # Calculate daily percentage change
            daily_returns = all_data.pct_change().dropna()
            
            # Create correlation matrix
            corr_matrix = daily_returns.corr()
            
            fig_corr = px.imshow(
                corr_matrix,
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                text_auto=".2f",
                title=f"Correlation Matrix of Daily Returns (YTD) for {len(symbols_list)} Stocks",
                color_continuous_scale='RdBu_r', zmin=-1, zmax=1
            )
            fig_corr.update_layout(height=700)
            st.plotly_chart(fig_corr, width='stretch')
        
        except Exception as e:
            st.error(f"Could not calculate correlation. One or more tickers may have failed. Error: {e}")
