# ── Base: official Airflow image (includes Python 3.12) ───────────────────
FROM apache/airflow:2.9.3-python3.12

# Non-interactive, no prompts, no cache for any pip install
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/airflow

USER root

# System dependencies needed by dbt-duckdb and pyarrow
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ── Install extra Python packages ─────────────────────────────────────────
COPY requirements.txt          /tmp/requirements.txt
COPY requirements-airflow.txt  /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir --upgrade pip --quiet && \
    pip install --no-cache-dir -r /tmp/requirements.txt --quiet && \
    pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
        --quiet

# ── Copy project source ───────────────────────────────────────────────────
COPY --chown=airflow:root standalone/analysis.py  /opt/airflow/standalone/analysis.py
COPY --chown=airflow:root etl/             /opt/airflow/etl/
COPY --chown=airflow:root dbt/             /opt/airflow/dbt/
COPY --chown=airflow:root airflow/dags/    /opt/airflow/dags/

# Pre-create writable directories for lake, warehouse, and results
RUN mkdir -p /opt/airflow/data/lake /opt/airflow/warehouse /opt/airflow/results

VOLUME ["/opt/airflow/data/lake", "/opt/airflow/warehouse", "/opt/airflow/results"]
