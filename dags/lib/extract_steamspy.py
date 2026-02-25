"""
extract_steamspy.py — Extract top games from SteamSpy (no API key required)

Endpoint: https://steamspy.com/api.php?request=top100in2weeks
Uploads: raw/steam/TopGames/YYYYMMDD/extract.json
"""

import logging
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from dags.lib.s3_utils import s3_key, upload_json

load_dotenv()

logger = logging.getLogger(__name__)

STEAMSPY_URL = "https://steamspy.com/api.php"

# Mapping of known game publishers / developers to their stock tickers
PUBLISHER_TICKER_MAP = {
    # Electronic Arts
    "Electronic Arts": "EA",
    "EA Sports": "EA",
    # Activision / Blizzard (now Microsoft)
    "Activision": "MSFT",
    "Blizzard Entertainment": "MSFT",
    "Activision Blizzard": "MSFT",
    # Take-Two Interactive
    "Rockstar Games": "TTWO",
    "2K Games": "TTWO",
    "2K Sports": "TTWO",
    "Take-Two Interactive": "TTWO",
    # Ubisoft
    "Ubisoft": "UBSFY",
    "Ubisoft Entertainment": "UBSFY",
    # Sony / PlayStation
    "Sony Interactive Entertainment": "SONY",
    "PlayStation Studios": "SONY",
    # Microsoft / Xbox
    "Microsoft Studios": "MSFT",
    "Xbox Game Studios": "MSFT",
    # Valve (private, use proxy or skip)
    "Valve": None,
    # Riot / Tencent (private/Chinese)
    "Riot Games": None,
    # CD Projekt
    "CD PROJEKT RED": "OTGLY",
    "CD Projekt": "OTGLY",
    # THQ Nordic / Embracer
    "THQ Nordic": "EMBRAC-B.ST",
    # Bandai Namco
    "Bandai Namco Entertainment": "7832.T",
    # Square Enix
    "Square Enix": "9684.T",
    # Capcom
    "Capcom": "9697.T",
    # Sega
    "Sega": "6460.T",
    # Konami
    "Konami": "9766.T",
}

# Default tickers to always include regardless of SteamSpy results
DEFAULT_TICKERS = ["EA", "MSFT", "TTWO", "UBSFY", "SONY", "NTDOY", "ATVI", "RBLX"]


def _lookup_ticker(publisher: str, developer: str) -> str | None:
    """Look up the stock ticker for a game based on publisher/developer."""
    for name in [publisher, developer]:
        if not name:
            continue
        # Exact match
        ticker = PUBLISHER_TICKER_MAP.get(name)
        if ticker:
            return ticker
        # Partial match
        for key, val in PUBLISHER_TICKER_MAP.items():
            if key.lower() in name.lower() or name.lower() in key.lower():
                return val
    return None


def extract_steamspy(**kwargs) -> dict:
    """
    Fetch top 100 games from SteamSpy and upload raw JSON to S3.

    Returns:
        dict with 'date', 'count', 's3_key' metadata
    """
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    # Support both Airflow datetime and plain datetime
    if hasattr(execution_date, "date"):
        date_str = execution_date.strftime("%Y%m%d")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    logger.info("Extracting SteamSpy top 100 games for date %s", date_str)

    try:
        response = requests.get(
            STEAMSPY_URL,
            params={"request": "top100in2weeks"},
            timeout=30,
            headers={"User-Agent": "DataLakePipeline/1.0"},
        )
        response.raise_for_status()
        raw_games = response.json()
        logger.info("SteamSpy returned %d games", len(raw_games))
    except requests.RequestException as exc:
        logger.error("SteamSpy API request failed: %s", exc)
        raise

    # Enrich with ticker mapping
    games_list = []
    for rank, (app_id, game) in enumerate(raw_games.items(), start=1):
        publisher = game.get("publisher", "")
        developer = game.get("developer", "")
        ticker = _lookup_ticker(publisher, developer)

        games_list.append(
            {
                "rank": rank,
                "app_id": app_id,
                "name": game.get("name", ""),
                "developer": developer,
                "publisher": publisher,
                "positive": game.get("positive", 0),
                "negative": game.get("negative", 0),
                "owners": game.get("owners", ""),
                "average_forever": game.get("average_forever", 0),
                "average_2weeks": game.get("average_2weeks", 0),
                "peak_ccu": game.get("peak_ccu", 0),
                "price": game.get("price", 0),
                "ticker": ticker,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "date": date_str,
            }
        )

    payload = {
        "source": "steamspy",
        "date": date_str,
        "count": len(games_list),
        "games": games_list,
    }

    key = s3_key("raw", "steam", "TopGames", date_str, "extract.json")
    upload_json(payload, key)

    logger.info(
        "SteamSpy extraction complete: %d games → s3://%s/%s",
        len(games_list),
        os.getenv("S3_BUCKET", "datalake"),
        key,
    )
    return {"date": date_str, "count": len(games_list), "s3_key": key}
