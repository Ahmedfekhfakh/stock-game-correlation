# Data Lake — Game Popularity × Stock Price Correlation

A full Big Data pipeline correlating **Steam** game popularity and **Twitch** viewership with **gaming-sector stock prices** (Yahoo Finance).

**Stack:** Apache Airflow · Apache Spark (PySpark) · Elasticsearch 8.12 · Kibana 8.12 · LocalStack S3 · Postgres · Docker

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Setup](#setup)
5. [Running the Pipeline](#running-the-pipeline)
6. [Verification](#verification)
7. [Kibana Dashboards](#kibana-dashboards)
8. [DAGs Reference](#dags-reference)
9. [S3 Naming Convention](#s3-naming-convention)
10. [Stock Tickers Tracked](#stock-tickers-tracked)
11. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────┐   ┌──────────────┐   ┌──────────────┐
│  SteamSpy   │   │ Twitch Helix │   │ Yahoo Finance│
│  (no key)   │   │  (OAuth2)    │   │  (yfinance)  │
└──────┬──────┘   └──────┬───────┘   └──────┬───────┘
       │                 │                   │
       ▼                 ▼                   ▼
┌──────────────────────────────────────────────────┐
│              LocalStack S3  (raw/)               │
└──────────────────────────┬───────────────────────┘
                           │
                   ┌───────▼────────┐
                   │  Apache Spark  │  ← UTC normalise, KPIs, Parquet
                   └───────┬────────┘
                           │
┌──────────────────────────▼───────────────────────┐
│            LocalStack S3  (formatted/)           │
└──────────────────────────┬───────────────────────┘
                           │
          ┌────────────────▼─────────────────────┐
          │  Apache Spark  (Spark SQL JOIN)       │
          │  + 7-day rolling Pearson correlation  │
          │  + GradientBoosting ML (150 est.)     │
          └────────────────┬─────────────────────┘
                           │
┌──────────────────────────▼───────────────────────┐
│              LocalStack S3  (usage/)             │
└──────────────────────────┬───────────────────────┘
                           │
                  ┌────────▼─────────┐
                  │  Elasticsearch   │
                  │  8.12            │
                  └────────┬─────────┘
                           │
                  ┌────────▼─────────┐
                  │     Kibana       │
                  │  Dashboards      │
                  └──────────────────┘

Orchestration: Apache Airflow 2.9 (daily + 5-min DAGs)
```

### Data Flow

| Layer | Path pattern | Format |
|---|---|---|
| Raw | `raw/<source>/<entity>/YYYYMMDD/extract.json` | JSON |
| Formatted | `formatted/<source>/<entity>/YYYYMMDD/data.snappy.parquet` | Snappy Parquet |
| Usage | `usage/correlation/<entity>/YYYYMMDD/result.snappy.parquet` | Snappy Parquet |

---

## Project Structure

```
stock-game-correlation/
├── docker-compose.yml              # All services: Postgres, LocalStack, ES, Kibana, Airflow
├── Dockerfile                      # Airflow + PySpark + Java 17 image
├── requirements.txt                # Pinned Python dependencies
├── run.sh                          # One-command: build + start + import Kibana
├── export.ndjson                   # Kibana dashboard (Data Views + Dashboard)
├── .dockerignore
├── .gitignore
│
├── credentials/
│   └── twitch_keys.yaml            # Twitch OAuth2 credentials
│
├── dags/
│   ├── gaming_finance_dag.py       # Main daily DAG  (08:00 UTC)
│   ├── realtime_stock_dag.py       # Live price refresh DAG (every 5 min)
│   └── lib/
│       ├── __init__.py
│       ├── s3_utils.py             # boto3 S3 helpers + naming convention enforcer
│       ├── extract_steamspy.py     # SteamSpy top-100 games (retry + fallback)
│       ├── extract_twitch.py       # Twitch Helix OAuth2 — top games + viewer counts
│       ├── extract_yahoo.py        # yfinance 30-day OHLCV for 17 gaming tickers
│       ├── format_steamspy.py      # Spark: UTC normalise, rank_score, review_ratio
│       ├── format_twitch.py        # Spark: UTC normalise, viewer_rank_score
│       ├── format_yahoo.py         # Spark: daily_change_pct, daily_range KPIs
│       ├── combine_correlation.py  # Spark SQL UNION ALL + 7d Pearson + XGBoost
│       └── index_to_elastic.py     # ES bulk index with explicit mapping

```

---

## Prerequisites

| Tool | Minimum version | Notes |
|---|---|---|
| Docker Desktop | 4.x | WSL2 integration must be enabled |
| curl | any | For health checks |

> **No cloud account needed.** S3 is emulated locally by LocalStack.  
> **No Java/Python install needed.** Everything runs inside Docker.

---

## Setup & Running

### 1. Clone the project

```bash
cd /mnt/f/Projets/stock-game-correlation
```

### 2. Configure Twitch credentials (optional)

Register a free app at [dev.twitch.tv/console](https://dev.twitch.tv/console):

```bash
cp credentials/twitch_keys.yaml.example credentials/twitch_keys.yaml
# Edit and fill in client_id + client_secret
```

> Twitch data is optional — the pipeline works without it.

### 3. Launch everything

```bash
bash run.sh
```

This will:
1. **Build** the Docker image (Airflow + PySpark + Java 17)
2. **Start** all 8 containers (Postgres, pgAdmin, LocalStack, Elasticsearch, Kibana, Airflow init/webserver/scheduler)
3. **Wait** for Airflow to be healthy
4. **Import** Kibana dashboards from `export.ndjson`

### 4. Access the services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Kibana Dashboard | http://localhost:5601/app/dashboards | — |
| Elasticsearch | http://localhost:9200 | — |
| LocalStack S3 | http://localhost:4566 | — |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

### Stop everything

```bash
docker-compose down
```

---

## Verification

### Infrastructure

```bash
# LocalStack
curl http://localhost:4566/_localstack/health

# Elasticsearch
curl http://localhost:9200

# Kibana
curl http://localhost:5601/api/status
```

### S3 contents

```bash
# Using awslocal (if installed)
awslocal s3 ls s3://datalake/ --recursive

# Or via Python
python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
for p in s3.get_paginator('list_objects_v2').paginate(Bucket='datalake'):
    for o in p.get('Contents', []):
        print(o['Key'])
"
```

Expected output after a full pipeline run:

```
raw/steam/TopGames/YYYYMMDD/extract.json
raw/twitch/TopGames/YYYYMMDD/extract.json
raw/yahoo/GamingStocks/YYYYMMDD/extract.json
formatted/steam/TopGames/YYYYMMDD/data.snappy.parquet
formatted/twitch/TopGames/YYYYMMDD/data.snappy.parquet
formatted/yahoo/GamingStocks/YYYYMMDD/data.snappy.parquet
usage/correlation/GameStockCorrelation/YYYYMMDD/result.snappy.parquet
```

### Elasticsearch

```bash
# Document count
curl http://localhost:9200/game-stock-correlation/_count

# Sample documents
curl "http://localhost:9200/game-stock-correlation/_search?size=3&pretty"

# Signal distribution
curl -s -X POST http://localhost:9200/game-stock-correlation/_search \
  -H "Content-Type: application/json" \
  -d '{"size":0,"aggs":{"signals":{"terms":{"field":"signal"}}}}'
```

### Airflow UI

Open [http://localhost:8080](http://localhost:8080) — login with `admin` / `admin`

---

## Kibana Dashboards

The Kibana dashboard is defined in `export.ndjson` and imported automatically by `run.sh`.

Open the dashboard: [http://localhost:5601/app/dashboards](http://localhost:5601/app/dashboards)

Two **Data Views** are included:

| Data View | Index | Time field |
|---|---|---|
| Game Stock Correlation | `game-stock-correlation` | `date` |
| Game Stock Live | `game-stock-live` | `timestamp` |

### Updating the Kibana dashboard

> **No Docker rebuild needed!** Dashboard changes are independent from the code.

**To modify the dashboard:**
1. Edit it visually in Kibana at `http://localhost:5601/app/dashboards`
2. Export: go to `Management → Saved Objects → Export` and download your objects
3. Replace the `export.ndjson` file in the project

**To re-import after editing `export.ndjson`:**
```bash
curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" --form file=@export.ndjson
```

Or via the UI: `Management → Saved Objects → Import → select export.ndjson`

---

## DAGs Reference

### `gaming_finance_correlation` — Daily at 08:00 UTC

```
start
  ├── extract_steamspy   ──► format_steamspy ──┐
  ├── extract_twitch     ──► format_twitch   ──┼──► combine_correlation ──► index_to_elastic ──► end
  └── extract_yahoo      ──► format_yahoo    ──┘
```

| Setting | Value |
|---|---|
| Schedule | `0 8 * * *` |
| Max active runs | 1 |
| Retries | 2 (5 min delay) |
| Timeout per task | 2 hours |

### `realtime_stock_refresh` — Every 5 minutes

Single task: `refresh_prices` — fetches `yfinance.fast_info` for all 8 tickers and bulk-indexes to `game-stock-live`. No Spark required.

| Setting | Value |
|---|---|
| Schedule | `*/5 * * * *` |
| Max active runs | 1 |
| Retries | 1 (1 min delay) |

---

## S3 Naming Convention

All S3 keys follow a strict pattern enforced by `s3_key()` in `dags/lib/s3_utils.py`:

```
<layer>/<group>/<entity>/<YYYYMMDD>/<filename>
```

| Layer | Group | Entity | Example |
|---|---|---|---|
| `raw` | `steam` | `TopGames` | `raw/steam/TopGames/20240115/extract.json` |
| `raw` | `twitch` | `TopGames` | `raw/twitch/TopGames/20240115/extract.json` |
| `raw` | `yahoo` | `GamingStocks` | `raw/yahoo/GamingStocks/20240115/extract.json` |
| `formatted` | `steam` | `TopGames` | `formatted/steam/TopGames/20240115/data.snappy.parquet` |
| `formatted` | `twitch` | `TopGames` | `formatted/twitch/TopGames/20240115/data.snappy.parquet` |
| `formatted` | `yahoo` | `GamingStocks` | `formatted/yahoo/GamingStocks/20240115/data.snappy.parquet` |
| `usage` | `correlation` | `GameStockCorrelation` | `usage/correlation/GameStockCorrelation/20240115/result.snappy.parquet` |

---

## Stock Tickers Tracked

| Ticker | Company | Notes |
|---|---|---|
| `EA` | Electronic Arts | FIFA, Apex Legends, Battlefield |
| `TTWO` | Take-Two Interactive | GTA, NBA 2K, Red Dead |
| `MSFT` | Microsoft | Xbox, Activision, Call of Duty |
| `SONY` | Sony Group | PlayStation Studios |
| `NTDOY` | Nintendo | Switch, Mario, Zelda |
| `UBSFY` | Ubisoft | Assassin's Creed, Far Cry |
| `RBLX` | Roblox | Roblox platform |
| `OTGLY` | CD Projekt | Cyberpunk 2077, Witcher |
| `9697.T` | Capcom | Monster Hunter, Resident Evil |
| `9684.T` | Square Enix | Final Fantasy |
| `7832.T` | Bandai Namco | Elden Ring, Tekken |
| `NTES` | NetEase | Chinese gaming giant |
| `TCEHY` | Tencent | League of Legends, Valorant |
| `SE` | Sea Limited | Garena, Free Fire |
| `U` | Unity Technologies | Game engine |
| `CRSR` | Corsair Gaming | Peripherals + streaming |
| `DKNG` | DraftKings | Esports betting |

---

## Troubleshooting

### Docker not found in WSL2
Enable WSL2 integration in **Docker Desktop → Settings → Resources → WSL Integration**.

### Elasticsearch `compatible-with=9` error
```bash
pip install "elasticsearch==8.12.0" "elastic-transport==8.13.1"
```

### S3 bucket missing after restart
LocalStack data is persisted via Docker volume. If the volume was removed:
```bash
docker exec localstack awslocal s3 mb s3://datalake
```

### Kibana shows empty dashboard
Make sure `export.ndjson` was imported:
```bash
curl -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" --form file=@export.ndjson
```

### SteamSpy rate limit ("Too many connections")
The pipeline has built-in retry logic (3 attempts with exponential backoff) and fallback data for 20+ games. It will never fail due to SteamSpy limits.

### CRLF error in run.sh
If you get `invalid option namepipefail`, convert line endings:
```bash
sed -i 's/\r$//' run.sh
bash run.sh
```

---

## Pipeline Run Results

Results from a verified full pipeline execution (`2026-02-24`).

### Infrastructure

| Service | Status | URL |
|---|---|---|
| LocalStack S3 | healthy | http://localhost:4566 |
| Elasticsearch 8.12 | healthy | http://localhost:9200 |
| Kibana 8.12 | healthy | http://localhost:5601 |
| Airflow 2.9.0 | healthy | http://localhost:8080 |

### Data Extracted

| Source | Records | Destination |
|---|---|---|
| SteamSpy | 100 games | `raw/steam/TopGames/20260224/extract.json` |
| Twitch Helix | 100 games + viewer counts | `raw/twitch/TopGames/20260224/extract.json` |
| Yahoo Finance | 140 records · 7 tickers | `raw/yahoo/GamingStocks/20260224/extract.json` |

### Data Formatted (Spark → Snappy Parquet)

| Source | Rows | Destination |
|---|---|---|
| Steam | 100 | `formatted/steam/TopGames/20260224/data.snappy.parquet` |
| Twitch | 100 | `formatted/twitch/TopGames/20260224/data.snappy.parquet` |
| Yahoo | 140 | `formatted/yahoo/GamingStocks/20260224/data.snappy.parquet` |

### Correlation & ML Output

| Metric | Value |
|---|---|
| Total rows in usage layer | 280 |
| Tickers correlated | EA · TTWO · MSFT · UBSFY |
| Elasticsearch index | `game-stock-correlation` · **280 docs** |
| Live price index | `game-stock-live` · **7 docs** |
| Kibana Data Views | `game-stock-correlation` · `game-stock-live` |

### Signal Distribution (`corr_7d_popularity_price`)

| Signal | Threshold | Count |
|---|---|---|
| STRONG\_POSITIVE | r ≥ 0.5 | 21 |
| WEAK\_POSITIVE | 0.2 ≤ r < 0.5 | 80 |
| NEUTRAL | −0.2 ≤ r < 0.2 | 74 |
| WEAK\_NEGATIVE | −0.5 ≤ r < −0.2 | 84 |
| STRONG\_NEGATIVE | r < −0.5 | 21 |

---

## XGBoost Model Results

The ML model (`XGBClassifier`, 150 estimators) classifies each game-ticker pair into one of the 5 signal categories.

### 5-Fold Stratified Cross-Validation

| Metric | Mean | ± Std |
|---|---|---|
| Accuracy | 0.5500 | ± 0.0510 |
| F1 (weighted) | 0.5397 | ± 0.0446 |
| F1 (macro) | 0.4937 | ± 0.0464 |
| Precision (weighted) | 0.5642 | ± 0.0215 |
| Recall (weighted) | 0.5500 | ± 0.0510 |

> Cross-validation scores reflect generalisation on a single day of Steam data.
> Scores will improve as daily DAG runs accumulate multiple Steam snapshots over time.

### Training Set Evaluation

| Metric | Score |
|---|---|
| Accuracy | **0.9786** |
| F1 (weighted) | **0.9786** |
| F1 (macro) | **0.9850** |
| Precision (weighted) | **0.9786** |
| Recall (weighted) | **0.9786** |
| Avg confidence (all docs) | **0.7639** |

### Per-class Classification Report

| Class | Precision | Recall | F1-score | Support |
|---|---|---|---|---|
| NEUTRAL | 0.97 | 0.99 | 0.98 | 74 |
| STRONG\_NEGATIVE | 1.00 | 1.00 | 1.00 | 21 |
| STRONG\_POSITIVE | 1.00 | 1.00 | 1.00 | 21 |
| WEAK\_NEGATIVE | 0.98 | 0.96 | 0.97 | 84 |
| WEAK\_POSITIVE | 0.97 | 0.97 | 0.97 | 80 |
| **weighted avg** | **0.98** | **0.98** | **0.98** | **280** |

### Confusion Matrix

Rows = true label · Columns = predicted label · Order: NEUTRAL, STRONG\_NEG, STRONG\_POS, WEAK\_NEG, WEAK\_POS

```
           NEU  S_NEG  S_POS  W_NEG  W_POS
NEUTRAL   [ 73     0      0      1      0 ]
S_NEG     [  0    21      0      0      0 ]
S_POS     [  0     0     21      0      0 ]
W_NEG     [  1     0      0     81      2 ]
W_POS     [  1     0      0      1     78 ]
```

### Feature Importances

| Feature | Importance |
|---|---|
| `daily_change_pct` | 0.2145 |
| `avg_playtime_2weeks` | 0.1714 |
| `daily_range_pct` | 0.1640 |
| `popularity_score` | 0.1569 |
| `stock_volume` | 0.1506 |
| `steam_rank_score` | 0.1425 |
| `twitch_rank_score` | 0.0000 |
| `twitch_viewers` | 0.0000 |
| `peak_ccu` | 0.0000 |

> `twitch_rank_score`, `twitch_viewers`, and `peak_ccu` show zero importance for a single-day run.
> Their contribution grows with multi-day accumulation when Twitch ranks vary across dates.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Kibana Dashboard | http://localhost:5601/app/dashboards | — |
| Elasticsearch | http://localhost:9200 | — |
| LocalStack S3 | http://localhost:4566 | — |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |
