"""
extract_yahoo.py — Extract 30-day OHLCV stock data from Yahoo Finance

Uses yfinance for the 8 main gaming-sector tickers.
Uploads: raw/yahoo/GamingStocks/YYYYMMDD/extract.json
"""

import logging
import os
from datetime import datetime, timedelta, timezone
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "yfinance"])

import yfinance as yf
from dotenv import load_dotenv

from dags.lib.s3_utils import s3_key, upload_json, download_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

load_dotenv()

logger = logging.getLogger(__name__)

# Gaming-sector tickers to track
GAMING_TICKERS = [
    "EA",       # Electronic Arts
    "TTWO",     # Take-Two Interactive
    "MSFT",     # Microsoft (Xbox + Activision)
    "SONY",     # Sony (PlayStation)
    "NTDOY",    # Nintendo
    "UBSFY",    # Ubisoft
    "RBLX",     # Roblox
    "OTGLY",    # CD Projekt (Cyberpunk / Witcher)
    "9697.T",   # Capcom (Monster Hunter / Resident Evil)
    "9684.T",   # Square Enix (Final Fantasy)
    "7832.T",   # Bandai Namco (Elden Ring / Tekken)
    "NTES",     # NetEase (Chinese gaming giant)
    "TCEHY",    # Tencent (League of Legends / Valorant)
    "SE",       # Sea Limited (Garena / Free Fire)
    "U",        # Unity Technologies (game engine)
    "CRSR",     # Corsair Gaming (peripherals + streaming)
    "DKNG",     # DraftKings (gaming-adjacent / esports betting)
]


def extract_yahoo(**kwargs) -> dict:
    """
    Fetch 30-day OHLCV data for all gaming tickers and upload to S3.

    Returns:
        dict with 'date', 'tickers', 's3_key' metadata
    """
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    if hasattr(execution_date, "strftime"):
        date_str = execution_date.strftime("%Y%m%d")
        end_date = execution_date
    else:
        end_date = datetime.now(timezone.utc)
        date_str = end_date.strftime("%Y%m%d")

    start_date = end_date - timedelta(days=30)

    logger.info(
        "Extracting Yahoo Finance data for %d tickers (%s → %s)",
        len(GAMING_TICKERS),
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    all_stocks = []
    failed_tickers = []

    for ticker_symbol in GAMING_TICKERS:
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=True,
            )
            logger.info("Ticker %s → shape=%s", ticker_symbol, hist.shape)
            logger.info("Ticker %s → head:\n%s", ticker_symbol, hist.head())
            if hist.empty:
                logger.warning("No data for ticker %s", ticker_symbol)
                failed_tickers.append(ticker_symbol)
                continue

            records = []
            for ts, row in hist.iterrows():
                open_price = float(row["Open"]) if row["Open"] == row["Open"] else None
                close_price = float(row["Close"]) if row["Close"] == row["Close"] else None
                high_price = float(row["High"]) if row["High"] == row["High"] else None
                low_price = float(row["Low"]) if row["Low"] == row["Low"] else None
                volume = int(row["Volume"]) if row["Volume"] == row["Volume"] else 0

                records.append(
                    {
                        "ticker": ticker_symbol,
                        "date": ts.strftime("%Y-%m-%d"),
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                    }
                )

            all_stocks.extend(records)
            logger.info("Fetched %d records for %s", len(records), ticker_symbol)

        except Exception as exc:
            logger.error("Failed to fetch %s: %s", ticker_symbol, exc)
            failed_tickers.append(ticker_symbol)

    payload = {
        "source": "yahoo_finance",
        "date": date_str,
        "tickers": GAMING_TICKERS,
        "failed_tickers": failed_tickers,
        "count": len(all_stocks),
        "records": all_stocks,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }

    key = s3_key("raw", "yahoo", "GamingStocks", date_str, "extract.json")
    upload_json(payload, key)

    logger.info(
        "Yahoo Finance extraction complete: %d records (%d tickers, %d failed) → s3://%s/%s",
        len(all_stocks),
        len(GAMING_TICKERS),
        len(failed_tickers),
        os.getenv("S3_BUCKET", "datalake"),
        key,
    )
    return {
        "date": date_str,
        "tickers": GAMING_TICKERS,
        "failed_tickers": failed_tickers,
        "count": len(all_stocks),
        "s3_key": key,
    }
