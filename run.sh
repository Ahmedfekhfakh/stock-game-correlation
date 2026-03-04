#!/usr/bin/env bash
# run.sh — One-command: start infra + trigger full pipeline
# Usage: bash run.sh [--no-docker]
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
NO_DOCKER=false

for arg in "$@"; do
    case $arg in
        --no-docker) NO_DOCKER=true ;;
    esac
done

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
fi

export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__WEBSERVER__SECRET_KEY="${AIRFLOW__WEBSERVER__SECRET_KEY:-supersecretkey}"


export JAVA_HOME="$PROJECT_DIR/.jdk"
export PYTHONPATH="$PROJECT_DIR:${PYTHONPATH:-}"
source "$VENV_DIR/bin/activate"

# ── 1. Start Docker infra ─────────────────────────────────────────────────────
if [ "$NO_DOCKER" = false ]; then
    echo "[INFO] Starting Docker infrastructure..."
    docker-compose -f "$PROJECT_DIR/docker-compose.yml" up -d

    echo "[INFO] Waiting for services to be healthy..."
    # Wait for LocalStack
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:4566/_localstack/health" >/dev/null 2>&1; then
            echo "[OK] LocalStack ready"
            break
        fi
        echo "  LocalStack: attempt $i/30..."
        sleep 5
    done

    # Wait for Elasticsearch
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:9200/_cluster/health" >/dev/null 2>&1; then
            echo "[OK] Elasticsearch ready"
            break
        fi
        echo "  Elasticsearch: attempt $i/30..."
        sleep 5
    done

    for i in $(seq 1 30); do
        if docker exec airflow-postgres pg_isready -U airflow >/dev/null 2>&1; then
            echo "[OK] PostgreSQL ready"
            break
        fi
        echo "  PostgreSQL: attempt $i/30..."
        sleep 5
    done

    # Ensure bucket exists
    awslocal s3 mb "s3://${S3_BUCKET:-datalake}" 2>/dev/null || true
    fi

# ── 1b. Initialize Airflow DB ─────────────────────────────────────────────────
echo "[INFO] Initializing Airflow database..."
airflow db migrate

# Create admin user (idempotent — ignores if already exists)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>/dev/null || true

# ── 2. Start Airflow webserver + scheduler (background) ───────────────────────
echo "[INFO] Starting Airflow webserver..."
airflow webserver --port 8080 --daemon \
    --log-file "$AIRFLOW_HOME/logs/webserver.log" \
    --pid "$AIRFLOW_HOME/airflow-webserver.pid" 2>/dev/null || true

echo "[INFO] Starting Airflow scheduler..."
airflow scheduler --daemon \
    --log-file "$AIRFLOW_HOME/logs/scheduler.log" \
    --pid "$AIRFLOW_HOME/airflow-scheduler.pid" 2>/dev/null || true

sleep 10

# ── 3. Trigger the main DAG ───────────────────────────────────────────────────
echo "[INFO] Triggering gaming_finance_correlation DAG..."
airflow dags trigger gaming_finance_correlation --conf '{}' || \
    echo "[WARN] Could not trigger DAG — check Airflow UI at http://localhost:8080"

# ── 4. Trigger realtime DAG ───────────────────────────────────────────────────
echo "[INFO] Unpausing realtime_stock_refresh DAG..."
airflow dags unpause realtime_stock_refresh 2>/dev/null || true

echo ""
echo "=== Pipeline triggered! ==="
echo ""
echo "  Airflow UI:          http://localhost:8080  (admin/admin)"
echo "  Kibana:              http://localhost:5601"
echo "  Elasticsearch:       http://localhost:9200"
echo "  LocalStack:          http://localhost:4566"
echo ""
echo "  Monitor S3:          awslocal s3 ls s3://${S3_BUCKET:-datalake}/ --recursive"
echo "  Check ES index:      curl http://localhost:9200/game-stock-correlation/_count"
echo ""
