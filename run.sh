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
echo "[1/4] Building Docker images..."
docker-compose -f "$PROJECT_DIR/docker-compose.yml" build

# ── 2. Start all services ────────────────────────────────────────────────────
echo ""
echo "[2/4] Starting all services..."
docker-compose -f "$PROJECT_DIR/docker-compose.yml" up -d

# ── 3. Wait for Airflow to be ready ──────────────────────────────────────────
echo ""
echo "[3/4] Waiting for Airflow webserver..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:8080/health" >/dev/null 2>&1; then
        echo "[OK] Airflow is ready!"
        break
    fi
    echo "  Airflow: attempt $i/30..."
    sleep 10
done

# ── 4. Import Kibana dashboards ──────────────────────────────────────────────
echo ""
echo "[4/4] Importing Kibana dashboards..."
for i in $(seq 1 15); do
    if curl -sf "http://localhost:5601/api/status" | grep -q '"level":"available"'; then
        curl -s -X POST "http://localhost:5601/api/saved_objects/_import?overwrite=true" \
          -H "kbn-xsrf: true" \
          --form file=@"$PROJECT_DIR/export.ndjson" > /dev/null
        echo "[OK] Kibana dashboards imported!"
        break
    fi
    echo "  Kibana: attempt $i/15..."
    sleep 10
done

echo ""
echo "============================================================"
echo "  All services running!"
echo "============================================================"
echo ""
echo "  Airflow UI:     http://localhost:8080  (admin/admin)"
echo "  Kibana:         http://localhost:5601/app/dashboards"
echo "  Elasticsearch:  http://localhost:9200"
echo "  LocalStack S3:  http://localhost:4566"
echo "  pgAdmin:        http://localhost:5050  (admin@admin.com/admin)"
echo ""
echo "  Stop everything:  docker-compose down"
echo "  View logs:        docker-compose logs -f airflow-scheduler"
echo ""
echo "  Update Kibana dashboard (no rebuild needed):"
echo "    curl -X POST 'http://localhost:5601/api/saved_objects/_import?overwrite=true' \\"
echo "      -H 'kbn-xsrf: true' --form file=@export.ndjson"
echo "============================================================"
echo ""
