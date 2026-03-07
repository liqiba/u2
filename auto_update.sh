#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/opt/catch_magic_web"
LOG_FILE="/opt/catch_magic_web/logs/auto_update.log"
API_URL="http://127.0.0.1:18088/api/status"
SERVICE_NAME="catch-magic-web"

mkdir -p "$(dirname "$LOG_FILE")"
log(){ echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $*" >> "$LOG_FILE"; }

cd "$REPO_DIR"

# 1) 拉取最新代码（有更新才部署）
git fetch origin main >/dev/null 2>&1 || { log "git fetch failed"; exit 1; }
LOCAL_SHA="$(git rev-parse main 2>/dev/null || true)"
REMOTE_SHA="$(git rev-parse origin/main 2>/dev/null || true)"

if [[ -n "$REMOTE_SHA" && "$LOCAL_SHA" != "$REMOTE_SHA" ]]; then
  log "new commit detected: $LOCAL_SHA -> $REMOTE_SHA"
  if git pull --ff-only origin main >/dev/null 2>&1; then
    log "git pull ok"
    if docker-compose up -d --build >/dev/null 2>&1; then
      log "docker-compose up -d --build ok"
    else
      log "docker-compose build/deploy failed"
    fi
  else
    log "git pull failed"
  fi
fi

# 2) 健康检查
sleep 2
if curl -fsS "$API_URL" >/dev/null 2>&1; then
  log "health check ok"
  exit 0
fi

log "health check failed, trying self-heal restart"
docker-compose restart "$SERVICE_NAME" >/dev/null 2>&1 || true
sleep 4
if curl -fsS "$API_URL" >/dev/null 2>&1; then
  log "self-heal by restart ok"
  exit 0
fi

log "restart failed, trying rebuild"
docker-compose up -d --build >/dev/null 2>&1 || true
sleep 5
if curl -fsS "$API_URL" >/dev/null 2>&1; then
  log "self-heal by rebuild ok"
  exit 0
fi

log "self-heal failed, manual check required"
exit 1
