# FinDash - S&P 500 Analyzer

**FinDash** is a high-performance, interactive dashboard for analyzing S&P 500 companies.  
Built on a robust ETL pipeline for **data freshness**, powered entirely by **yfinance** and **Streamlit**.

---

## Key Features

- **Blazing Fast UI** - Loads instantly using a pre-computed local SQLite database (no live API lag).  
- **Consolidated ETL Pipeline** - Fetches performance and fundamental data for all 500+ companies daily.  
- **Advanced Screener** - Filter companies by *Sector*, *Market Cap*, and *P/E Ratio*.  
- **Live Watchlist** - View up-to-date yfinance charts for any ticker.  
- **Modular Architecture** - Clean separation between ETL and UI layers for scalability.  

---

## System Architecture

FinDash isn’t a single script - it’s a small-scale **data product** that cleanly separates:
- **ETL (etl.py)** - Handles data collection, transformation, and storage.
- **App (app.py)** - Renders the dashboard UI and reads pre-computed data.

### ETL (`etl.py`)
- Scrapes the **S&P 500 company list** from Wikipedia.  
- Fetches **performance data** (e.g., YTD Return) using `yfinance`.  
- Fetches **fundamental data** (e.g., Market Cap, P/E).  
- Merges, cleans, and saves everything into `stocks.db`.  

### App (`app.py`)
- Streamlit-based frontend that queries `stocks.db`.  
- Displays company performance, metrics, and watchlists.  
- No external API calls = instant load time 🚀  

---

## Project Structure

```
FinDash-SP500/
├── app.py              # Streamlit dashboard (UI)
├── etl.py              # Data ingestion & transformation
├── stocks.db           # Local SQLite database (auto-generated)
├── requirements.txt    # Dependencies
└── README.md           # Documentation
```

---

## Installation & Setup

### 1. Clone the repository
```bash
git clone https://github.com/abhinavharbola/sp-500-stocks-interactive-financial-dashboard
cd sp-500-stocks-interactive-financial-dashboard
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the ETL script (first time setup)
This script creates the `stocks.db` SQLite database and populates it with data for all 500+ S&P companies.

```bash
python etl.py
```

> **Note:** The initial run may take several minutes as data is fetched sequentially.

To keep data fresh, re-run the ETL script daily or schedule it using a task scheduler (e.g., cron or Windows Task Scheduler).

### 4. Launch the dashboard
```bash
streamlit run app.py
```

---

## Example Workflow

```bash
# Step 1: Setup
git clone https://github.com/your-username/FinDash-SP500.git
cd FinDash-SP500

# Step 2: Install dependencies
pip install -r requirements.txt

# Step 3: Build the database
python etl.py

# Step 4: Run the dashboard
streamlit run app.py
```

---

## Data Flow Overview

```
Wikipedia  →  etl.py  →  yfinance  →  SQLite (stocks.db)  →  Streamlit App
```

---

## Potential Enhancements

- Automate ETL via GitHub Actions or cron job  
- Add sector-level summaries and aggregate metrics  
- Integrate news sentiment for each company  
- Export dashboards to PDF or Excel reports  
- Add visual analytics (e.g., moving averages, RSI)  

---

## Contribute & Support

If you find **FinDash** useful, please consider:
- Starring the repository  
- Reporting issues  
- Submitting a pull request  

---


**Author:** [Abhinav Harbola](https://github.com/abhinavharbola)


