"""
extract_twitch.py — Extract top games & viewer counts from Twitch Helix API

OAuth2 Client Credentials flow → top 300 games (6 pages of 50) →
per-game top stream viewers → upload raw/twitch/TopGames/YYYYMMDD/extract.json
"""

import logging
import os
from datetime import datetime, timezone

import requests
import yaml
from dotenv import load_dotenv

from dags.lib.s3_utils import s3_key, upload_json
from airflow.hooks.base import BaseHook

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_TOP_GAMES_URL = "https://api.twitch.tv/helix/games/top"
TWITCH_STREAMS_URL = "https://api.twitch.tv/helix/streams"


def _load_credentials() -> tuple[str, str]:
    """
    Load Twitch credentials from (in priority order):
    1) Airflow Connection: twitch_default (login=client_id, password=client_secret)
    2) Environment variables TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET
    3) credentials/twitch_keys.yaml
    """
    # 1) Airflow Connection
    try:
        conn = BaseHook.get_connection("twitch_default")
        client_id = (conn.login or "").strip()
        client_secret = (conn.password or "").strip()
        if client_id and client_secret:
            logger.info("Loaded Twitch credentials from Airflow connection: twitch_default")
            return client_id, client_secret
        logger.warning("Airflow connection twitch_default found but missing login/password")
    except Exception as exc:
        logger.info("Airflow connection twitch_default not available: %s", exc)

    # 2) Environment variables
    client_id = (os.getenv("TWITCH_CLIENT_ID", "") or "").strip()
    client_secret = (os.getenv("TWITCH_CLIENT_SECRET", "") or "").strip()
    if client_id and client_secret:
        logger.info("Loaded Twitch credentials from environment")
        return client_id, client_secret

    # 3) YAML file
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    yaml_path = os.path.join(project_root, "credentials", "twitch_keys.yaml")

    if os.path.exists(yaml_path):
        with open(yaml_path) as fh:
            creds = yaml.safe_load(fh) or {}
        client_id = (creds.get("client_id", "") or "").strip()
        client_secret = (creds.get("client_secret", "") or "").strip()
        if client_id and client_secret:
            logger.info("Loaded Twitch credentials from %s", yaml_path)
            return client_id, client_secret

    raise FileNotFoundError(
        "Twitch credentials not found. "
        "Create Airflow connection 'twitch_default' (login=client_id, password=client_secret), "
        "or set TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET, "
        "or copy credentials/twitch_keys.yaml.example → credentials/twitch_keys.yaml"
    )


def _get_access_token(client_id: str, client_secret: str) -> str:
    """Obtain an OAuth2 app access token via Client Credentials flow."""
    response = requests.post(
        TWITCH_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
        timeout=15,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    logger.info("Twitch OAuth2 token obtained")
    return token


def _fetch_top_games(client_id: str, token: str, pages: int = 6) -> list[dict]:
    """Fetch top games (50 per page × pages)."""
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
    games = []
    cursor = None

    for page in range(pages):
        params = {"first": 50}
        if cursor:
            params["after"] = cursor

        resp = requests.get(TWITCH_TOP_GAMES_URL, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        body = resp.json()

        page_games = body.get("data", [])
        games.extend(page_games)
        cursor = body.get("pagination", {}).get("cursor")

        logger.info("Page %d: fetched %d games (total: %d)", page + 1, len(page_games), len(games))

        if not cursor or not page_games:
            break

    return games


def _fetch_top_viewers_for_game(game_id: str, client_id: str, token: str) -> int:
    """Get the viewer count of the top stream for a given game ID."""
    headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
    params = {"game_id": game_id, "first": 1, "type": "live"}

    try:
        resp = requests.get(TWITCH_STREAMS_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        streams = resp.json().get("data", [])
        if streams:
            return streams[0].get("viewer_count", 0)
    except requests.RequestException as exc:
        logger.warning("Failed to fetch streams for game_id=%s: %s", game_id, exc)

    return 0


def extract_twitch(**kwargs) -> dict:
    """
    Fetch top 100 Twitch games with viewer counts and upload to S3.

    Returns:
        dict with 'date', 'count', 's3_key' metadata
    """
    execution_date = kwargs.get("execution_date") or kwargs.get(
        "logical_date", datetime.now(timezone.utc)
    )
    if hasattr(execution_date, "strftime"):
        date_str = execution_date.strftime("%Y%m%d")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    logger.info("Extracting Twitch top games for date %s", date_str)

    client_id, client_secret = _load_credentials()
    token = _get_access_token(client_id, client_secret)

    games = _fetch_top_games(client_id, token, pages=6)

    # Enrich each game with viewer count from top stream
    enriched = []
    for rank, game in enumerate(games, start=1):
        game_id = game["id"]
        viewers = _fetch_top_viewers_for_game(game_id, client_id, token)
        enriched.append(
            {
                "rank": rank,
                "game_id": game_id,
                "game_name": game["name"],
                "box_art_url": game.get("box_art_url", ""),
                "top_stream_viewers": viewers,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "date": date_str,
            }
        )
    logger.info("data enriched successfully", enriched)
    payload = {
        "source": "twitch",
        "date": date_str,
        "count": len(enriched),
        "games": enriched,
    }

    key = s3_key("raw", "twitch", "TopGames", date_str, "extract.json")
    upload_json(payload, key)

    logger.info(
        "Twitch extraction complete: %d games → s3://%s/%s",
        len(enriched),
        os.getenv("S3_BUCKET", "datalake"),
        key,
    )
    return {"date": date_str, "count": len(enriched), "s3_key": key}
