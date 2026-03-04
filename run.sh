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

# ── Activate venv FIRST (so airflow points to right python) ───────────────────
source "$VENV_DIR/bin/activate"

# ── Airflow env ───────────────────────────────────────────────────────────────
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__WEBSERVER__SECRET_KEY="${AIRFLOW__WEBSERVER__SECRET_KEY:-supersecretkey}"

# Make 'lib' importable for DAG parsing + task runtime
export PYTHONPATH="$AIRFLOW_HOME/dags:${PYTHONPATH:-}"

# Java (for Spark)
export JAVA_HOME="$PROJECT_DIR/.jdk"
export PATH="$JAVA_HOME/bin:${PATH}"

mkdir -p "$AIRFLOW_HOME/logs"

# ── 1. Start Docker infra ─────────────────────────────────────────────────────
if [ "$NO_DOCKER" = false ]; then
  echo "[INFO] Starting Docker infrastructure..."
  docker-compose -f "$PROJECT_DIR/docker-compose.yml" up -d

  echo "[INFO] Waiting for services to be healthy..."
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:4566/_localstack/health" >/dev/null 2>&1; then
      echo "[OK] LocalStack ready"
      break
    fi
    echo "  LocalStack: attempt $i/30..."
    sleep 5
  done

  for i in $(seq 1 30); do
    if curl -sf "http://localhost:9200/_cluster/health" >/dev/null 2>&1; then
      echo "[OK] Elasticsearch ready"
      break
    fi
    echo "  Elasticsearch: attempt $i/30..."
    sleep 5
  done

  awslocal s3 mb "s3://${S3_BUCKET:-datalake}" 2>/dev/null || true
fi

# ── 2. Start Airflow ─────────────────────────────────────────────────────────
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

# ── 4. Unpause realtime DAG ───────────────────────────────────────────────────
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
