#!/usr/bin/env bash
set -euo pipefail

: "${PORT:=8001}"
: "${WORKERS:=2}"
: "${THREADS:=4}"
: "${LOG_LEVEL:=info}"
: "${DATA_DIR:=/data}"
: "${APP_DATA_DIR:=/app/data}"
: "${APP_DB_PATH:=/data/ota.db}"

# 1) assicura /data e permessi (siamo root qui)
mkdir -p "$DATA_DIR"
chown -R appuser:appuser "$DATA_DIR" || true

# 2) seed DB alla prima run
if [ ! -f "$APP_DB_PATH" ]; then
  echo "[init] Seeding DB in $APP_DB_PATH"
  cp -n /seed/ota.db "$APP_DB_PATH"
  chown appuser:appuser "$APP_DB_PATH" || true
fi

# 3) symlink compat
ln -sfn "$DATA_DIR" "$APP_DATA_DIR"

# 4) avvio gunicorn come appuser
exec gosu appuser:appuser gunicorn "wsgi:app" \
  --bind "0.0.0.0:${PORT}" \
  --workers "${WORKERS}" \
  --threads "${THREADS}" \
  --timeout 120 \
  --access-logfile - \
  --error-logfile - \
  --log-level "${LOG_LEVEL}"
