#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/project/transformation/dbt/air_traffic_analytics"
PROFILES_DIR="/home/airflow/.dbt"

echo "[RUN] dbt deps"
dbt deps --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR"

echo "[RUN] dbt run staging"
dbt run --select staging --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR"

echo "[RUN] dbt run intermediate"
dbt run --select intermediate --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR"

echo "[RUN] dbt run marts"
dbt run --select marts --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR"

echo "[RUN] dbt test"
dbt test --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR"

echo "[OK] dbt pipeline finished"