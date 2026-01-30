FROM apache/airflow:2.9.1-python3.10

USER root

# System deps
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt (Postgres adapter – change later if needed) and duckdb for local transformations/tests
RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-postgres==1.7.9 \
    dbt-duckdb==1.7.5 \
    duckdb
