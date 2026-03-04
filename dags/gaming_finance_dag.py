"""
gaming_finance_dag.py — Main daily pipeline DAG

Schedule: 08:00 UTC every day
Tasks (parallel extract → sequential format → combine → index):

    extract_steamspy  ─┐
    extract_twitch    ─┤─► format_steam  ─┐
    extract_yahoo     ─┘─► format_twitch ─┤─► combine_correlation ─► index_to_elastic ─► end
                           format_yahoo   ─┘
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv

# Ensure project root is on PYTHONPATH so dags.lib imports work
# Use realpath to resolve symlinks (airflow_home/dags/ → dags/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logger = logging.getLogger(__name__)

# ── Default args ───────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-lake",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="gaming_finance_correlation",
    description="Game Popularity × Stock Price Correlation (Steam × Twitch × Yahoo → Spark → ES)",
    schedule="0 8 * * *",  # 08:00 UTC daily
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["data-lake", "gaming", "finance", "correlation"],
    doc_md="""
## Game-Finance Correlation Pipeline

Daily pipeline extracting game popularity (SteamSpy + Twitch) and
stock price data (Yahoo Finance), joining them via Spark, computing
7-day rolling Pearson correlations, running GradientBoosting ML,
and indexing results to Elasticsearch for Kibana dashboards.

### Data flow
1. **Extract** (parallel): SteamSpy API → S3 raw/, Twitch Helix API → S3 raw/, Yahoo Finance → S3 raw/
2. **Format** (Spark): JSON → Snappy Parquet with UTC normalization → S3 formatted/
3. **Combine**: Spark SQL JOIN + Window 7d Pearson + GradientBoosting → S3 usage/
4. **Index**: Bulk index to Elasticsearch `game-stock-correlation`

### Access
- Airflow UI: http://localhost:8080
- Kibana: http://localhost:5601
- ES: http://localhost:9200/game-stock-correlation/_count
    """,
) as dag:

    # ── Helper: lazy-import task functions ─────────────────────────────────────
    def _run_extract_steamspy(**kwargs):
        from dags.lib.extract_steamspy import extract_steamspy
        return extract_steamspy(**kwargs)

    def _run_extract_twitch(**kwargs):
        from dags.lib.extract_twitch import extract_twitch
        return extract_twitch(**kwargs)

    def _run_extract_yahoo(**kwargs):
        from dags.lib.extract_yahoo import extract_yahoo
        return extract_yahoo(**kwargs)

    def _run_format_steamspy(**kwargs):
        from dags.lib.format_steamspy import format_steamspy
        return format_steamspy(**kwargs)

    def _run_format_twitch(**kwargs):
        from dags.lib.format_twitch import format_twitch
        return format_twitch(**kwargs)

    def _run_format_yahoo(**kwargs):
        from dags.lib.format_yahoo import format_yahoo
        return format_yahoo(**kwargs)

    def _run_combine(**kwargs):
        from dags.lib.combine_correlation import combine_correlation
        return combine_correlation(**kwargs)

    def _run_index(**kwargs):
        from dags.lib.index_to_elastic import index_to_elastic
        return index_to_elastic(**kwargs)

    # ── Start sentinel ─────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Extract tasks (run in parallel) ───────────────────────────────────────
    t_extract_steam = PythonOperator(
        task_id="extract_steamspy",
        python_callable=_run_extract_steamspy,
    )

    t_extract_twitch = PythonOperator(
        task_id="extract_twitch",
        python_callable=_run_extract_twitch,
    )

    t_extract_yahoo = PythonOperator(
        task_id="extract_yahoo",
        python_callable=_run_extract_yahoo,
    )

    # ── Format tasks (each depends on its extractor) ───────────────────────────
    t_format_steam = PythonOperator(
        task_id="format_steamspy",
        python_callable=_run_format_steamspy,
    )

    t_format_twitch = PythonOperator(
        task_id="format_twitch",
        python_callable=_run_format_twitch,
    )

    t_format_yahoo = PythonOperator(
        task_id="format_yahoo",
        python_callable=_run_format_yahoo,
    )

    # ── Combine task ───────────────────────────────────────────────────────────
    t_combine = PythonOperator(
        task_id="combine_correlation",
        python_callable=_run_combine,
    )

    # ── Index task ─────────────────────────────────────────────────────────────
    t_index = PythonOperator(
        task_id="index_to_elastic",
        python_callable=_run_index,
    )

    # ── End sentinel ───────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── DAG wiring ─────────────────────────────────────────────────────────────
    start >> [t_extract_steam, t_extract_twitch, t_extract_yahoo]

    t_extract_steam >> t_format_steam
    t_extract_twitch >> t_format_twitch
    t_extract_yahoo >> t_format_yahoo

    [t_format_steam, t_format_twitch, t_format_yahoo] >> t_combine
    t_combine >> t_index >> end
