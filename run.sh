#!/usr/bin/env bash
# run.sh — One-command: build + start everything in Docker
# Usage: bash run.sh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "  Game × Stock Correlation Pipeline"
echo "============================================================"

# ── 1. Build the Airflow image ────────────────────────────────────────────────
echo ""
echo "[1/3] Building Docker images..."
docker-compose -f "$PROJECT_DIR/docker-compose.yml" build

# ── 2. Start all services ────────────────────────────────────────────────────
echo ""
echo "[2/3] Starting all services..."
docker-compose -f "$PROJECT_DIR/docker-compose.yml" up -d

# ── 3. Wait for Airflow to be ready ──────────────────────────────────────────
echo ""
echo "[3/3] Waiting for Airflow webserver..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:8080/health" >/dev/null 2>&1; then
        echo "[OK] Airflow is ready!"
        break
    fi
    echo "  Airflow: attempt $i/30..."
    sleep 10
done

echo ""
echo "============================================================"
echo "  All services running!"
echo "============================================================"
echo ""
echo "  Airflow UI:     http://localhost:8080  (admin/admin)"
echo "  Kibana:         http://localhost:5601"
echo "  Elasticsearch:  http://localhost:9200"
echo "  LocalStack S3:  http://localhost:4566"
echo "  pgAdmin:        http://localhost:5050  (admin@admin.com/admin)"
echo ""
echo "  Stop everything:  docker-compose down"
echo "  View logs:        docker-compose logs -f airflow-scheduler"
echo "============================================================"
echo ""
