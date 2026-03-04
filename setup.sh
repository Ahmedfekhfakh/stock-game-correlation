#!/usr/bin/env bash
# setup.sh — One-shot project bootstrap
# Usage: bash setup.sh
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"

echo "=== Data Lake Project Setup ==="
echo "Project dir: $PROJECT_DIR"

# ── 1. Load .env ─────────────────────────────────────────────────────────────
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
    echo "[OK] .env loaded"
else
    echo "[WARN] .env not found — copy .env.example and fill in secrets"
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
    echo "[INFO] .env created from template — edit it now and re-run setup.sh"
    exit 1
fi

# ── 2. Python virtual environment ────────────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    echo "[INFO] Creating Python venv..."
    python3 -m venv "$VENV_DIR"
    echo "[OK] venv created at $VENV_DIR"
else
    echo "[OK] venv already exists"
fi

source "$VENV_DIR/bin/activate"
echo "[OK] venv activated"

# ── 3. Java (required by PySpark) ────────────────────────────────────────────
JDK_DIR="$PROJECT_DIR/.jdk"
if [ ! -f "$JDK_DIR/bin/java" ]; then
    echo "[INFO] Downloading portable OpenJDK 17 (required by PySpark)..."
    mkdir -p "$JDK_DIR"
    JDK_URL="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.10%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.10_7.tar.gz"
    curl -sL "$JDK_URL" -o /tmp/jdk17.tar.gz
    tar -xzf /tmp/jdk17.tar.gz -C "$JDK_DIR" --strip-components=1
    rm /tmp/jdk17.tar.gz
    echo "[OK] OpenJDK 17 installed at $JDK_DIR"
else
    echo "[OK] Java already present at $JDK_DIR"
fi
export JAVA_HOME="$JDK_DIR"

# ── 4. Upgrade pip + install requirements ────────────────────────────────────
echo "[INFO] Installing Python requirements (this may take a few minutes)..."
pip install --quiet --upgrade pip setuptools wheel
pip install --quiet -r "$PROJECT_DIR/requirements.txt"
echo "[OK] Python requirements installed"

# ── 4. Airflow initialisation ────────────────────────────────────────────────
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__WEBSERVER__SECRET_KEY="${AIRFLOW__WEBSERVER__SECRET_KEY:-supersecretkey}"

mkdir -p "$AIRFLOW_HOME/dags"

# Symlink project dags into airflow dags folder
if [ ! -L "$AIRFLOW_HOME/dags/gaming_finance_dag.py" ]; then
    ln -sf "$PROJECT_DIR/dags/gaming_finance_dag.py" "$AIRFLOW_HOME/dags/gaming_finance_dag.py"
    ln -sf "$PROJECT_DIR/dags/realtime_stock_dag.py" "$AIRFLOW_HOME/dags/realtime_stock_dag.py"
    echo "[OK] DAG symlinks created"
fi

echo "[INFO] Initialising Airflow DB..."
airflow db migrate
echo "[OK] Airflow DB ready"

# Create admin user (ignore if already exists)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@datalake.local 2>/dev/null || echo "[OK] Admin user already exists"

echo "[OK] Airflow admin user: admin / admin"

# ── 5. LocalStack S3 bucket ───────────────────────────────────────────────────
echo "[INFO] Waiting for LocalStack to be ready..."
RETRIES=15
for i in $(seq 1 $RETRIES); do
    if curl -sf "http://localhost:4566/_localstack/health" >/dev/null 2>&1; then
        echo "[OK] LocalStack is ready"
        break
    fi
    echo "  Attempt $i/$RETRIES — waiting 5s..."
    sleep 5
done

BUCKET="${S3_BUCKET:-datalake}"
echo "[INFO] Creating S3 bucket: $BUCKET"
awslocal s3 mb "s3://$BUCKET" 2>/dev/null || echo "[OK] Bucket already exists"
awslocal s3api put-bucket-versioning \
    --bucket "$BUCKET" \
    --versioning-configuration Status=Enabled 2>/dev/null || true
echo "[OK] S3 bucket '$BUCKET' ready"

# ── 6. Directory structure ───────────────────────────────────────────────────
mkdir -p "$PROJECT_DIR/tests"
mkdir -p "$PROJECT_DIR/credentials"
mkdir -p "$PROJECT_DIR/dags/lib"
touch "$PROJECT_DIR/dags/lib/__init__.py"
echo "[OK] Directory structure ensured"

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "  1. Fill in Twitch credentials: cp credentials/twitch_keys.yaml.example credentials/twitch_keys.yaml"
echo "  2. Start infrastructure:       docker-compose up -d"
echo "  3. Run the pipeline:           bash run.sh"
echo "  4. Airflow UI:                 http://localhost:8080  (admin/admin)"
echo "  5. Kibana:                     http://localhost:5601"
echo ""
