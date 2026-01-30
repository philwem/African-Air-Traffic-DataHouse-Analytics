#!/usr/bin/env bash
set -euo pipefail

echo "[RUN] build_silver.py"
python /opt/project/ingestion/build_silver.py
echo "[OK] build_silver.py finished"