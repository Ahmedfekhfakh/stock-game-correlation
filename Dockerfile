# ═══════════════════════════════════════════════════════════════════════════════
# Dockerfile — Airflow + PySpark + Java 17 (tout-en-un)
#
# Image complète pour exécuter Airflow (webserver/scheduler/init)
# avec PySpark et toutes les dépendances Python du projet.
# ═══════════════════════════════════════════════════════════════════════════════
FROM python:3.12-slim

# ── 1. Dépendances système : Java 17 (Adoptium) + outils ─────────────────────
#    PySpark 3.5 nécessite Java 17. Debian Trixie (python:3.12-slim) ne le
#    fournit plus dans apt, donc on télécharge Adoptium Temurin 17 directement.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        procps \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/java \
    && curl -fsSL "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz" \
       | tar xz --strip-components=1 -C /opt/java

# Dire à PySpark où trouver Java
ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── 2. Répertoire de travail ──────────────────────────────────────────────────
WORKDIR /app

# ── 3. Dépendances Python ────────────────────────────────────────────────────
#    On copie requirements.txt EN PREMIER pour que Docker cache cette couche.
#    Tant que requirements.txt ne change pas, Docker ne réinstalle pas les deps
#    → builds suivants ultra-rapides.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# ── 4. Copier le code ────────────────────────────────────────────────────────
#    Tout le projet est copié dans /app.
#    PYTHONPATH=/app pour que "from dags.lib.xxx import yyy" fonctionne.
COPY dags/ /app/dags/
COPY credentials/ /app/credentials/
COPY export.ndjson /app/export.ndjson

# ── 5. Variables d'environnement ──────────────────────────────────────────────
ENV PYTHONPATH="/app" \
    AIRFLOW_HOME="/app/airflow_home" \
    AIRFLOW__CORE__LOAD_EXAMPLES="False" \
    AIRFLOW__CORE__DAGS_FOLDER="/app/dags" \
    AIRFLOW__CORE__EXECUTOR="LocalExecutor" \
    PYSPARK_PYTHON="python3"

# ── 6. Créer le dossier Airflow ──────────────────────────────────────────────
RUN mkdir -p /app/airflow_home/logs

# Pas de CMD — chaque service docker-compose définit sa propre commande