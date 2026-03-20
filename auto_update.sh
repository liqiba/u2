#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/opt/catch_magic_web"
LOG_FILE="/opt/catch_magic_web/logs/auto_update.log"
HEALTH_URL="http://127.0.0.1:18088/"
SERVICE_NAME="catch-magic-web"
REQUEST_FILE="/opt/catch_magic_web/data/upgrade.request.json"
STATUS_FILE="/opt/catch_magic_web/data/upgrade.status.json"
LOCK_FILE="/tmp/catch_magic_update.lock"
CONFIG_FILE="/opt/catch_magic_web/data/config.json"

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$STATUS_FILE")"
log(){ echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $*" >> "$LOG_FILE"; }
set_status(){
  local state="$1"; shift
  local msg="$*"
  python3 - <<'PY2' "$STATUS_FILE" "$state" "$msg"
import json,sys,datetime
path,state,msg=sys.argv[1],sys.argv[2],sys.argv[3]
now=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S CST')
with open(path,'w',encoding='utf-8') as f:
    json.dump({'state':state,'updated_at':now,'message':msg}, f, ensure_ascii=False)
PY2
}

tg_send(){
  [[ -f "$CONFIG_FILE" ]] || return 0
  python3 - <<'PY' "$CONFIG_FILE" "$1"
import json,sys,requests
cfg_path,text=sys.argv[1],sys.argv[2]
try:
    cfg=json.load(open(cfg_path,'r',encoding='utf-8'))
    if not cfg.get('tg_enabled'): raise SystemExit
    token=(cfg.get('tg_bot_token') or '').strip()
    chat=(str(cfg.get('tg_chat_id') or '')).strip()
    if not token or not chat: raise SystemExit
    requests.post(f'https://api.telegram.org/bot{token}/sendMessage',json={'chat_id':chat,'text':text},timeout=10)
except Exception:
    pass
PY
}

(
  flock -n 9 || exit 0

  cd "$REPO_DIR"

  # 默认健康巡检
  if [[ ! -f "$REQUEST_FILE" ]]; then
    sleep 1
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
      exit 0
    fi
    log "health check failed, trying restart"
    docker-compose restart "$SERVICE_NAME" >/dev/null 2>&1 || true
    sleep 4
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
      log "self-heal by restart ok"
      exit 0
    fi
    log "self-heal failed"
    exit 1
  fi

  set_status running "收到升级请求，开始执行"
  log "manual upgrade requested"
  tg_send "🚀 U2 开始一键升级容器"

  if ! git fetch origin main >/dev/null 2>&1; then
    set_status failed "git fetch 失败"
    tg_send "❌ U2 升级失败：git fetch 失败"
    rm -f "$REQUEST_FILE"
    exit 1
  fi

  LOCAL_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
  REMOTE_SHA="$(git rev-parse origin/main 2>/dev/null || true)"

  if [[ -n "$REMOTE_SHA" && "$LOCAL_SHA" == "$REMOTE_SHA" ]]; then
    # 代码仓库已最新，但容器可能仍在旧镜像（例如此前仅 git 更新、未重建）
    REPO_VER="$(python3 - <<'PYV'
import re
s=open('/opt/catch_magic_web/main.py','r',encoding='utf-8').read()
m=re.search(r"APP_VERSION\s*=\s*'([^']+)'", s)
print(m.group(1) if m else '')
PYV
)"
    RUN_VER="$(docker exec "$SERVICE_NAME" python -c "import main; print(main.APP_VERSION)" 2>/dev/null || true)"

    if [[ -n "$REPO_VER" && -n "$RUN_VER" && "$REPO_VER" != "$RUN_VER" ]]; then
      set_status running "检测到运行版本落后(${RUN_VER} -> ${REPO_VER})，开始重建容器"
      if ! docker-compose up -d --build >/dev/null 2>&1; then
        set_status failed "运行版本落后但重建失败"
        tg_send "❌ U2 升级失败：运行版本落后且重建失败"
        rm -f "$REQUEST_FILE"
        exit 1
      fi
      sleep 4
      if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
        set_status success "已同步运行版本：${RUN_VER} -> ${REPO_VER}"
        log "sync runtime version success: $RUN_VER -> $REPO_VER"
        tg_send "✅ U2 已同步运行版本
${RUN_VER} -> ${REPO_VER}"
      else
        set_status failed "运行版本同步后健康检查失败"
        log "sync runtime version failed: health check failed"
        tg_send "❌ U2 版本同步后健康检查失败"
        rm -f "$REQUEST_FILE"
        exit 1
      fi
      rm -f "$REQUEST_FILE"
      exit 0
    fi

    set_status latest "已经是最新版本，无需升级"
    log "already latest: $LOCAL_SHA"
    tg_send "✅ U2 已是最新版本，无需升级"
    rm -f "$REQUEST_FILE"
    exit 0
  fi

  if ! git reset --hard origin/main >/dev/null 2>&1; then
    set_status failed "git reset 失败"
    tg_send "❌ U2 升级失败：git reset 失败"
    rm -f "$REQUEST_FILE"
    exit 1
  fi

  set_status running "代码已更新，开始重建容器"
  if ! docker-compose up -d --build >/dev/null 2>&1; then
    set_status failed "docker-compose 重建失败"
    tg_send "❌ U2 升级失败：容器重建失败"
    rm -f "$REQUEST_FILE"
    exit 1
  fi

  sleep 4
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    set_status success "升级成功：${LOCAL_SHA:0:7} -> ${REMOTE_SHA:0:7}"
    log "upgrade success: $LOCAL_SHA -> $REMOTE_SHA"
    tg_send "✅ U2 升级成功\n版本: ${LOCAL_SHA:0:7} -> ${REMOTE_SHA:0:7}"
  else
    set_status failed "升级后健康检查失败"
    log "upgrade failed: health check failed"
    tg_send "❌ U2 升级失败：健康检查未通过"
    rm -f "$REQUEST_FILE"
    exit 1
  fi

  rm -f "$REQUEST_FILE"

) 9>"$LOCK_FILE"
