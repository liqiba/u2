import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

DATA_DIR = Path('/data')
LOG_DIR = Path('/logs')
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = DATA_DIR / 'config.json'
STATE_PATH = DATA_DIR / 'state.json'
APP_LOG = LOG_DIR / 'app.log'
APP_VERSION = '2026.3.30'
QB_TORRENT_UP_LIMIT_BYTES = 50 * 1024 * 1024  # default: 50 MB/s per torrent
LOCAL_TZ = ZoneInfo('Asia/Shanghai')

DEFAULT_CONFIG = {
    'enabled': False,
    'interval': 120,
    'u2_api_base': 'https://u2.kysdm.com/api/v2',
    'u2_api_token': '',
    'u2_passkey': '',
    'scope': 'public',
    'limit': 20,
    'max_seeders': 5,
    'download_non_free': False,
    'qb_up_limit_mb': 50,
    'qb_mode': 'round_robin',
    'tg_enabled': False,
    'tg_bot_token': '',
    'tg_chat_id': '',
    'tg_notify_new': True,
    'tg_notify_error': True,
    'qb_clients': [
        {
            'name': 'qb-1',
            'enabled': True,
            'qb_url': 'http://127.0.0.1:8080',
            'qb_username': '',
            'qb_password': '',
            'qb_category': '',
            'qb_savepath': '',
            'qb_paused': False,
        }
    ],
}


def now_iso():
    return datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S CST')


def log(msg: str):
    with open(APP_LOG, 'a', encoding='utf-8') as f:
        f.write(f'[{now_iso()}] {msg}\n')


def tg_notify(cfg: dict, text: str):
    if not cfg.get('tg_enabled'):
        return False, 'TG未启用'
    token = (cfg.get('tg_bot_token') or '').strip()
    chat_id = str(cfg.get('tg_chat_id') or '').strip()
    if not (token and chat_id):
        return False, 'TG配置不完整（缺少 Bot Token 或 Chat ID）'
    try:
        resp = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text},
            timeout=15,
        )
        if resp.status_code != 200:
            return False, f'HTTP {resp.status_code}: {resp.text[:120]}'
        data = resp.json() if resp.text else {}
        if not data.get('ok', False):
            return False, data.get('description', 'TG接口返回失败')
        return True, 'ok'
    except Exception as e:
        log(f'TG通知发送失败：{e}')
        return False, str(e)


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def load_config():
    cfg = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    merged = dict(DEFAULT_CONFIG)
    if isinstance(cfg, dict):
        merged.update(cfg)

    # 兼容老版本单 qB 字段
    if not merged.get('qb_clients'):
        merged['qb_clients'] = [{
            'name': 'qb-1',
            'enabled': True,
            'qb_url': merged.get('qb_url', 'http://127.0.0.1:8080'),
            'qb_username': merged.get('qb_username', ''),
            'qb_password': merged.get('qb_password', ''),
            'qb_category': merged.get('qb_category', ''),
            'qb_savepath': merged.get('qb_savepath', ''),
            'qb_paused': bool(merged.get('qb_paused', False)),
        }]

    return merged


def qb_login(client: dict):
    qb_url = (client.get('qb_url') or '').strip().rstrip('/')
    qb_username = (client.get('qb_username') or '').strip()
    qb_password = (client.get('qb_password') or '').strip()
    if not (qb_url and qb_username and qb_password):
        return None, qb_url, 'qB 配置不完整'

    sess = requests.Session()
    login_resp = sess.post(
        f'{qb_url}/api/v2/auth/login',
        data={'username': qb_username, 'password': qb_password},
        timeout=20,
    )
    if login_resp.status_code != 200 or 'Ok.' not in login_resp.text:
        return None, qb_url, f'qB 登录失败: {login_resp.status_code} {login_resp.text[:120]}'
    return sess, qb_url, None


def qb_add_torrent(client: dict, torrent_url: str, up_limit_bytes: int = QB_TORRENT_UP_LIMIT_BYTES):
    sess, qb_url, err = qb_login(client)
    if err:
        return False, err

    data = {
        'urls': torrent_url,
        'autoTMM': 'false',
        'paused': 'true' if client.get('qb_paused') else 'false',
        'upLimit': str(max(0, int(up_limit_bytes))),
    }
    if client.get('qb_category'):
        data['category'] = str(client.get('qb_category'))
    if client.get('qb_savepath'):
        data['savepath'] = str(client.get('qb_savepath'))

    add_resp = sess.post(f'{qb_url}/api/v2/torrents/add', data=data, timeout=20)
    if add_resp.status_code == 200:
        return True, 'ok'
    return False, f'qB 添加失败: {add_resp.status_code} {add_resp.text[:120]}'


def qb_fetch_stats(client: dict):
    name = client.get('name') or client.get('qb_url') or 'unknown'
    sess, qb_url, err = qb_login(client)
    if err:
        return {'name': name, 'ok': False, 'error': err}
    try:
        resp = sess.get(f'{qb_url}/api/v2/sync/maindata', timeout=20)
        resp.raise_for_status()
        data = resp.json()
        ss = data.get('server_state', {})
        torrents = data.get('torrents', {})
        return {
            'name': name,
            'ok': True,
            'enabled': bool(client.get('enabled', True)),
            'qb_url': qb_url,
            'task_count': len(torrents) if isinstance(torrents, dict) else 0,
            'dl_speed': int(ss.get('dl_info_speed', 0) or 0),
            'up_speed': int(ss.get('up_info_speed', 0) or 0),
            'dl_total': int(ss.get('alltime_dl', 0) or 0),
            'up_total': int(ss.get('alltime_ul', 0) or 0),
        }
    except Exception as e:
        return {'name': name, 'ok': False, 'error': str(e)}


def pick_qb_clients(cfg: dict, state: dict):
    clients = [c for c in (cfg.get('qb_clients') or []) if c.get('enabled', True)]
    if not clients:
        return []
    mode = (cfg.get('qb_mode') or 'round_robin').strip().lower()
    if mode == 'all':
        return clients

    idx = int(state.get('qb_rr_index', 0)) % len(clients)
    picked = clients[idx]
    state['qb_rr_index'] = (idx + 1) % len(clients)
    return [picked]


class Runner:
    def __init__(self):
        self._lock = threading.Lock()
        self._running = False
        self._stop = False
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        log('调度器已启动')

    def stop(self):
        self._stop = True
        log('调度器停止中')

    def _loop(self):
        while not self._stop:
            cfg = load_config()
            if cfg.get('enabled'):
                self.run_once()
            interval = max(10, int(cfg.get('interval', 120)))
            for _ in range(interval):
                if self._stop:
                    return
                time.sleep(1)

    def run_once(self):
        if not self._lock.acquire(blocking=False):
            return
        self._running = True
        try:
            cfg = load_config()
            state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0})

            token = cfg.get('u2_api_token', '').strip()
            if not token:
                state['last_run'] = now_iso()
                state['last_error'] = 'u2_api_token 为空，跳过执行'
                save_json(STATE_PATH, state)
                log('执行跳过：u2_api_token 为空')
                return

            url = f"{cfg.get('u2_api_base').rstrip('/')}/promotions"
            params = {'scope': cfg.get('scope', 'public'), 'limit': int(cfg.get('limit', 20))}
            headers = {'Authorization': f'Bearer {token}'}
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            raw_data = payload.get('data', [])
            if isinstance(raw_data, dict):
                if isinstance(raw_data.get('promotion'), list):
                    data = raw_data.get('promotion')
                elif isinstance(raw_data.get('items'), list):
                    data = raw_data.get('items')
                else:
                    data = []
            elif isinstance(raw_data, list):
                data = raw_data
            else:
                data = []

            # 兜底：过滤非对象项，避免结构变化导致报错
            data = [x for x in data if isinstance(x, dict)]

            last_seen = set(state.get('last_seen', []))
            new_items = []
            for p in data:
                pid = p.get('promotion_id') or p.get('id')
                if pid and str(pid) not in last_seen:
                    new_items.append(p)

            passkey = (cfg.get('u2_passkey') or '').strip()
            up_limit_mb = int(cfg.get('qb_up_limit_mb', 50) or 0)
            up_limit_bytes = max(0, up_limit_mb) * 1024 * 1024
            for p in new_items:
                pid = p.get('promotion_id') or p.get('id')
                tid = p.get('torrent_id')
                dr = p.get('download_ratio')
                seeders = p.get('seeders', '?')
                log(f'检测到新的推广：ID={pid}，线程号={tid}，dr={dr}，种子用户={seeders}')
                if cfg.get('tg_notify_new', True):
                    ok_tg, msg_tg = tg_notify(cfg, f'🆕 U2新优惠\nID: {pid}\nTID: {tid}\nDR: {dr}\nSeeders: {seeders}')
                    if not ok_tg:
                        log(f'TG通知未发送：{msg_tg}')

                if not tid:
                    log(f'跳过推送到 qB：推广号={pid} 缺少线程号')
                    continue
                if not passkey:
                    log('跳过推送到 qB：u2_passkey 未配置 (skip push to qB: u2_passkey missing)')
                    continue

                targets = pick_qb_clients(cfg, state)
                if not targets:
                    log('跳过推送到 qB：没有启用的客户端 (skip push to qB: no enabled clients)')
                    continue

                torrent_url = f'https://u2.dmhy.org/download.php?id={tid}&passkey={passkey}&https=1'
                for cli in targets:
                    cname = cli.get('name') or cli.get('qb_url') or 'unknown'
                    ok, msg = qb_add_torrent(cli, torrent_url, up_limit_bytes)
                    if ok:
                        log(f'已推送至 qB[{cname}]：推广号={pid}，线程号={tid}，上传限制={up_limit_bytes}B/s')
                    else:
                        log(f'推送到 qB[{cname}] 失败：推广号={pid}，线程号={tid}，原因={msg}')

            state['last_seen'] = [str((p.get('promotion_id') or p.get('id'))) for p in data if (p.get('promotion_id') or p.get('id'))][:200]
            state['last_run'] = now_iso()
            state['last_error'] = None
            save_json(STATE_PATH, state)
            log(f'运行完成：已获取={len(data)}，新增={len(new_items)}')
        except Exception as e:
            state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0})
            state['last_run'] = now_iso()
            state['last_error'] = str(e)
            save_json(STATE_PATH, state)
            log(f'运行失败：{e}')
            if cfg.get('tg_notify_error', True):
                ok_tg, msg_tg = tg_notify(cfg, f'❌ U2任务执行失败\n{e}')
                if not ok_tg:
                    log(f'TG通知未发送：{msg_tg}')
        finally:
            self._running = False
            self._lock.release()


app = FastAPI(title='Catch Magic Web')
runner = Runner()


class QBClientIn(BaseModel):
    name: str = ''
    enabled: bool = True
    qb_url: str = 'http://127.0.0.1:8080'
    qb_username: str = ''
    qb_password: str = ''
    qb_category: str = ''
    qb_savepath: str = ''
    qb_paused: bool = False


class ConfigIn(BaseModel):
    enabled: bool
    interval: int
    u2_api_base: str
    u2_api_token: str
    u2_passkey: str
    scope: str
    limit: int
    max_seeders: int
    download_non_free: bool
    qb_up_limit_mb: int = 50
    qb_mode: str = 'round_robin'
    tg_enabled: bool = False
    tg_bot_token: str = ''
    tg_chat_id: str = ''
    tg_notify_new: bool = True
    tg_notify_error: bool = True
    qb_clients: List[QBClientIn] = Field(default_factory=list)


@app.on_event('startup')
def startup_event():
    if not CONFIG_PATH.exists():
        save_json(CONFIG_PATH, DEFAULT_CONFIG)
    if not STATE_PATH.exists():
        save_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0})
    runner.start()


@app.on_event('shutdown')
def shutdown_event():
    runner.stop()


@app.get('/', response_class=HTMLResponse)
def index():
    return """
<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Catch Magic Web</title>
  <style>
    :root{--bg:#0b1020;--bg2:#131a2e;--card:#151d34;--line:#263150;--text:#e7ecff;--sub:#9eadcf;--ok:#22c55e;--warn:#f59e0b;--err:#ef4444;--pri:#4f7cff;--pri2:#335ed8;--module1:#162241;--module2:#0f1a35}
    body.theme-light{--bg:#f4f7ff;--bg2:#ffffff;--card:#ffffff;--line:#dce4ff;--text:#1a2440;--sub:#4a5f8f;--ok:#15803d;--warn:#b45309;--err:#b91c1c;--pri:#2563eb;--pri2:#1d4ed8;--module1:#f4f7ff;--module2:#ffffff}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(1200px 800px at 20% -10%,#1a2650 0%,var(--bg) 50%);color:var(--text);font-family:Inter,Segoe UI,Arial,sans-serif;transition:background .2s,color .2s}
    body.theme-light{background:linear-gradient(180deg,#f8faff 0%,#eef3ff 100%)}
    .wrap{max-width:1100px;margin:24px auto;padding:0 16px;width:100%}.title{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:14px}
    h2{margin:0;font-size:24px;line-height:1.2}.badge{display:inline-flex;align-items:center;height:36px;padding:0 12px;border-radius:999px;border:1px solid var(--line);font-size:12px;color:var(--sub);white-space:nowrap}
    .grid{display:grid;gap:14px}.card{background:linear-gradient(180deg,var(--card),var(--bg2));border:1px solid var(--line);border-radius:14px;padding:14px;overflow:hidden}
    .status,.form{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.form{grid-template-columns:1fr 1fr}.full{grid-column:1/-1}
    .status{grid-template-columns:repeat(3,minmax(0,1fr))}
    .k{font-size:12px;color:var(--sub);margin-bottom:4px}.v{font-size:14px;word-break:break-word;overflow-wrap:anywhere;min-width:0}.dot{display:inline-block;width:8px;height:8px;border-radius:999px;margin-right:6px}
    .ok{background:var(--ok)} .err{background:var(--err)} .warn{background:var(--warn)} label{font-size:12px;color:var(--sub);display:block;margin-bottom:6px}
    input,select{width:100%;max-width:100%;background:#0e1528;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:10px}\n    body.theme-light input,body.theme-light select{background:#ffffff;color:#12203f;border-color:#b8c8f5}\n    body.theme-light input::placeholder,body.theme-light select::placeholder{color:#6b7fae;opacity:1}
    input:focus,select:focus{outline:none;border-color:var(--pri)} .switch{display:flex;align-items:center;gap:10px}.switch input{width:auto}
    .actions{display:flex;gap:10px;flex-wrap:wrap} button{border:0;border-radius:10px;padding:10px 14px;color:white;background:var(--pri);cursor:pointer;font-weight:600}
    button:hover{background:var(--pri2)} button.ghost{background:transparent;border:1px solid var(--line);color:var(--text)} button.danger{background:#b4232a}
    pre{margin:0;background:#0a0f1f;color:#c7d2ff;border:1px solid var(--line);border-radius:10px;padding:12px;max-height:360px;overflow:auto;line-height:1.45}
    .tip{font-size:12px;color:var(--sub)} .qb-item{border:1px dashed var(--line);border-radius:12px;padding:10px;margin-top:10px}
    .modules{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
    .module-card{transition:.18s;border:1px solid var(--line);border-radius:12px;padding:9px;background:linear-gradient(180deg,var(--module1),var(--module2));box-shadow:0 4px 12px #00000010}
    .module-card:hover{border-color:var(--pri);transform:translateY(-1px)}
    .module-card.active{border-color:var(--pri);box-shadow:0 0 0 1px #6ea1ff33 inset}
    .module-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
    .editor-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
    @media (max-width:980px){
      .editor-grid{grid-template-columns:1fr}
      .modules{grid-template-columns:1fr}
      .status{grid-template-columns:1fr 1fr}
    }
    @media (max-width:680px){
      .wrap{margin:8px auto;padding:0 8px}
      h2{font-size:18px;line-height:1.2}
      .card{padding:8px;border-radius:10px}
      .form,.status,.modules{grid-template-columns:1fr}
      .actions{display:grid;grid-template-columns:1fr;gap:8px}
      .actions button{width:100%}
      input,select,button{font-size:16px}
      .module-head{gap:8px;align-items:flex-start;flex-direction:column}
      .k{font-size:11px}
      .v{font-size:13px}
      .badge{font-size:11px;padding:4px 8px}
    }
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='title'><h2>Catch Magic Web <span style='font-size:13px;color:var(--sub);font-weight:500'>v__APP_VERSION__</span></h2><div class='actions'><button class='ghost' type='button' onclick='openMainConfigModal()'>基础配置</button><button class='ghost' type='button' onclick='openTGModal()'>TG配置</button><button class='ghost' type='button' onclick='toggleTheme()'>🌗 主题切换</button><div class='badge' id='runBadge'>状态读取中...</div></div></div>
    <div id='app' class='grid'>loading...</div>
  </div>
<script>
let qbClients=[];
let qbStatsTimer=null;
let editingQbIndex=null;

async function j(u,o){const r=await fetch(u,o);return await r.json()}
function esc(t){return (t??'').toString().replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function fmtBytes(n){ n=Number(n||0); const u=['B','KB','MB','GB','TB']; let i=0; while(n>=1024&&i<u.length-1){n/=1024;i++;} return `${n.toFixed(i<=1?0:2)} ${u[i]}`; }
function fmtSpeed(n){ return `${fmtBytes(n)}/s`; }
function statusBadge(s){ if(s.running) return `<span class='dot warn'></span>执行中`; if(s.last_error) return `<span class='dot err'></span>异常`; return `<span class='dot ok'></span>正常`; }
function getTheme(){ return localStorage.getItem('cm_theme')||'dark'; }
function applyTheme(){ const t=getTheme(); document.body.classList.toggle('theme-light', t==='light'); }
function toggleTheme(){ localStorage.setItem('cm_theme', getTheme()==='light'?'dark':'light'); applyTheme(); }



function openMainConfigModal(){ const m=document.getElementById('mainCfgModal'); if(m) m.style.display='flex'; }
function closeMainConfigModal(){ const m=document.getElementById('mainCfgModal'); if(m) m.style.display='none'; }

function openTGModal(){
  const m=document.getElementById('tgModal');
  if(!m) return;
  m.style.display='flex';
}
function closeTGModal(){
  const m=document.getElementById('tgModal');
  if(m) m.style.display='none';
}
function setTGForm(c){
  const e=document.getElementById('tg_enabled'); if(!e) return;
  e.checked=!!c.tg_enabled;
  document.getElementById('tg_bot_token').value=c.tg_bot_token||'';
  document.getElementById('tg_chat_id').value=c.tg_chat_id||'';
  document.getElementById('tg_notify_new').checked=(c.tg_notify_new!==false);
  document.getElementById('tg_notify_error').checked=(c.tg_notify_error!==false);
}
function collectTG(){
  return {
    tg_enabled: document.getElementById('tg_enabled')?.checked||false,
    tg_bot_token: (document.getElementById('tg_bot_token')?.value||'').trim(),
    tg_chat_id: (document.getElementById('tg_chat_id')?.value||'').trim(),
    tg_notify_new: document.getElementById('tg_notify_new')?.checked!==false,
    tg_notify_error: document.getElementById('tg_notify_error')?.checked!==false,
  }
}
async function saveTGConfigOnly(){
  const c=await j('/api/config');
  const t=collectTG();
  const body={...c,...t, qb_clients: qbClients};
  await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  alert('TG配置已保存');
  closeTGModal();
}

function openQb(i){
  const q=qbClients[i]||{};
  if(q.qb_url) window.open(q.qb_url,'_blank');
}

function openQbConfig(i){
  editingQbIndex=i;
  const q=qbClients[i]||{};
  const modal=document.getElementById('qbModal');
  if(!modal) return;
  document.getElementById('m_qb_name').value=q.name||'';
  document.getElementById('m_qb_enabled').checked=q.enabled!==false;
  document.getElementById('m_qb_url').value=q.qb_url||'';
  document.getElementById('m_qb_username').value=q.qb_username||'';
  document.getElementById('m_qb_password').value=q.qb_password||'';
  document.getElementById('m_qb_category').value=q.qb_category||'';
  document.getElementById('m_qb_savepath').value=q.qb_savepath||'';
  document.getElementById('m_qb_paused').checked=!!q.qb_paused;
  modal.style.display='flex';
}

function closeQbConfig(){
  editingQbIndex=null;
  const modal=document.getElementById('qbModal');
  if(modal) modal.style.display='none';
}

function saveQbConfig(){
  if(editingQbIndex===null || editingQbIndex<0 || editingQbIndex>=qbClients.length) return;
  const q=qbClients[editingQbIndex];
  q.name=document.getElementById('m_qb_name').value.trim();
  q.enabled=document.getElementById('m_qb_enabled').checked;
  q.qb_url=document.getElementById('m_qb_url').value.trim();
  q.qb_username=document.getElementById('m_qb_username').value.trim();
  q.qb_password=document.getElementById('m_qb_password').value.trim();
  q.qb_category=document.getElementById('m_qb_category').value.trim();
  q.qb_savepath=document.getElementById('m_qb_savepath').value.trim();
  q.qb_paused=document.getElementById('m_qb_paused').checked;
  closeQbConfig();
  refreshQbStats();
}

function renderQbModules(items=[]){
  const el=document.getElementById('qbModules'); if(!el) return;
  if(!qbClients.length){ el.innerHTML = `<div class='tip'>暂无 qB 模块</div>`; return; }
  el.innerHTML = qbClients.map((q,i)=>{
    const it = items[i] || {};
    const ok = !!it.ok;
    return `
      <div class='module-card'>
        <div class='module-head'>
          <div style='font-weight:700'>${esc(q.name||('qb-'+(i+1)))}</div>
          <div class='badge'>${ok?"<span class='dot ok'></span>在线":"<span class='dot err'></span>离线"}</div>
        </div>
        <div class='status'>
          <div><div class='k'>任务数</div><div class='v'>${ok?(it.task_count??'-'):'-'}</div></div>
          <div><div class='k'>实时下载</div><div class='v'>${ok?fmtSpeed(it.dl_speed):'-'}</div></div>
          <div><div class='k'>实时上传</div><div class='v'>${ok?fmtSpeed(it.up_speed):'-'}</div></div>
          <div><div class='k'>总下载</div><div class='v'>${ok?fmtBytes(it.dl_total):'-'}</div></div>
          <div><div class='k'>总上传</div><div class='v'>${ok?fmtBytes(it.up_total):'-'}</div></div>
          <div><div class='k'>节点</div><div class='v'>${esc(q.qb_url||'-')}</div></div>
        </div>
        <div class='actions' style='margin-top:6px'>
          <button class='ghost' onclick='openQb(${i})'>打开 qB</button>
          <button onclick='openQbConfig(${i})'>配置</button>
          <button class='danger' onclick='removeQb(${i})'>删除</button>
        </div>
        ${ok?'':`<div class='tip' style='margin-top:8px;color:#ff9aa2'>${esc(it.error||'连接失败')}</div>`}
      </div>
    `;
  }).join('');
}

function addQb(){
  qbClients.push({name:`qb-${qbClients.length+1}`,enabled:true,qb_url:'http://127.0.0.1:8080',qb_username:'',qb_password:'',qb_category:'',qb_savepath:'',qb_paused:false});
  refreshQbStats();
}

function removeQb(i){
  qbClients.splice(i,1);
  refreshQbStats();
}

async function refreshQbStats(){
  try{
    const res=await j('/api/qb/stats');
    renderQbModules(res.items||[]);
  }catch(e){
    renderQbModules([]);
  }
}

async function load(){
 applyTheme();
 const c=await j('/api/config'); const s=await j('/api/status'); qbClients = JSON.parse(JSON.stringify(c.qb_clients || []));
 document.getElementById('runBadge').innerHTML=statusBadge(s);
 document.getElementById('app').innerHTML=`
 <div class='card'><div class='status'>
   <div><div class='k'>最后执行</div><div class='v'>${esc(s.last_run||'-')}</div></div>
   <div><div class='k'>运行状态</div><div class='v'>${s.running?'执行中':'空闲'}</div></div>
   <div><div class='k'>最近错误</div><div class='v'>${esc(s.last_error||'无')}</div></div>
 </div></div>
 <div class='card'>
   <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>
     <div class='k'>qB 模块看板（自动刷新）</div>
     <button type='button' onclick='addQb()'>+ 添加QB配置</button>
   </div>
   <div id='qbModules' class='modules'><div class='tip'>加载中...</div></div>
 </div>

 <div id='mainCfgModal' style='display:none;position:fixed;inset:0;background:#0008;z-index:1000;align-items:center;justify-content:center;padding:14px'>
   <div style='width:min(920px,100%);max-height:90vh;overflow:auto;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px'>
     <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>
       <div style='font-weight:700;color:var(--text)'>基础配置</div>
       <button class='ghost' onclick='closeMainConfigModal()'>关闭</button>
     </div>
 <div class='card'><div class='form'>
   <input style='display:none' type='text' name='fake_username' autocomplete='username'>
   <input style='display:none' type='password' name='fake_password' autocomplete='current-password'>
   <div class='full switch'><input id='enabled' type='checkbox' ${c.enabled?'checked':''}><label for='enabled' style='margin:0;color:var(--text)'>启用定时任务</label></div>
   <div><label>执行间隔（秒）</label><input id='interval' type='number' min='10' value='${c.interval}'></div>
   <div><label>抓取条数（limit）</label><input id='limit' type='number' min='1' max='60' value='${c.limit}'></div>
   <div class='full'><label>U2 API Base</label><input id='u2_api_base' value='${esc(c.u2_api_base)}'></div>
   <div class='full'><label>U2 API Token</label><input id='u2_api_token' name='u2_api_token_input' type='password' autocomplete='new-password' autocapitalize='off' autocorrect='off' spellcheck='false' value='${esc(c.u2_api_token||'')}'></div>
   <div class='full'><label>U2 Passkey</label><input id='u2_passkey' name='u2_passkey_input' type='password' autocomplete='new-password' autocapitalize='off' autocorrect='off' spellcheck='false' value='${esc(c.u2_passkey||'')}'></div>
   <div><label>魔法范围（scope）</label><select id='scope'><option value='public' ${c.scope==='public'?'selected':''}>公共魔法（public）</option><option value='all' ${c.scope==='all'?'selected':''}>全部魔法（all）</option><option value='private' ${c.scope==='private'?'selected':''}>私人魔法（private）</option><option value='global' ${c.scope==='global'?'selected':''}>全局魔法（global）</option></select></div>
   <div><label>qB 分发模式</label><select id='qb_mode'><option value='round_robin' ${c.qb_mode==='round_robin'?'selected':''}>轮询分发</option><option value='all' ${c.qb_mode==='all'?'selected':''}>全部推送</option></select></div>
   <div><label>单种上传限速(MB/s)</label><input id='qb_up_limit_mb' type='number' min='0' value='${c.qb_up_limit_mb??50}'></div>
 </div>
 <div class='actions' style='margin-top:12px'><button onclick='save()'>保存配置</button><button onclick='runNow()'>立即执行一次</button><button class='ghost' onclick='refreshLogs()'>刷新日志</button></div>
 <div class='tip' style='margin-top:10px'>点击模块上的“配置”按钮才会弹出配置窗口。</div>
 </div>

   </div>
 </div>
 <div class='card'><div class='k' style='margin-bottom:8px'>最近日志（最多 200 行）</div><pre id='logs'>loading logs...</pre></div>

 <div id='qbModal' style='display:none;position:fixed;inset:0;background:#0008;z-index:999;align-items:center;justify-content:center;padding:14px'>
   <div style='width:min(560px,100%);max-height:90vh;overflow:auto;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px'>
     <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>
       <div style='font-weight:700;color:var(--text)'>qB 配置</div>
       <button class='ghost' onclick='closeQbConfig()'>关闭</button>
     </div>
     <div class='editor-grid'>
       <div><label>名称</label><input id='m_qb_name'></div>
       <div class='switch'><input id='m_qb_enabled' type='checkbox'><label for='m_qb_enabled' style='margin:0;color:var(--text)'>启用</label></div>
       <div><label>URL</label><input id='m_qb_url' placeholder='http://127.0.0.1:8080'></div>
       <div><label>用户名</label><input id='m_qb_username'></div>
       <div><label>密码</label><input id='m_qb_password' type='password'></div>
       <div><label>分类</label><input id='m_qb_category'></div>
       <div class='full'><label>保存路径</label><input id='m_qb_savepath' placeholder='/downloads/u2'></div>
       <div class='switch full'><input id='m_qb_paused' type='checkbox'><label for='m_qb_paused' style='margin:0;color:var(--text)'>推送后暂停</label></div>
     </div>
     <div class='actions' style='margin-top:10px'>
       <button onclick='saveQbConfig()'>保存当前模块配置</button>
     </div>
   </div>
 </div>

 <div id='tgModal' style='display:none;position:fixed;inset:0;background:#0008;z-index:1000;align-items:center;justify-content:center;padding:14px'>
   <div style='width:min(560px,100%);max-height:90vh;overflow:auto;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px'>
     <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>
       <div style='font-weight:700;color:var(--text)'>Telegram 配置</div>
       <button class='ghost' onclick='closeTGModal()'>关闭</button>
     </div>
     <div class='editor-grid'>
       <div class='switch full'><input id='tg_enabled' type='checkbox'><label for='tg_enabled' style='margin:0;color:var(--text)'>启用TG通知</label></div>
       <div class='full'><label style='color:var(--text)'>Bot Token</label><input id='tg_bot_token' name='tg_bot_token_input' type='password' autocomplete='new-password' autocapitalize='off' autocorrect='off' spellcheck='false' placeholder='123456:ABC...'></div>
       <div class='full'><label style='color:var(--text)'>Chat ID</label><input id='tg_chat_id' placeholder='例如 1036463619'></div>
       <div class='switch'><input id='tg_notify_new' type='checkbox'><label for='tg_notify_new' style='margin:0;color:var(--text)'>新推广通知</label></div>
       <div class='switch'><input id='tg_notify_error' type='checkbox'><label for='tg_notify_error' style='margin:0;color:var(--text)'>失败告警通知</label></div>
     </div>
     <div class='actions' style='margin-top:10px'>
       <button onclick='saveTGConfigOnly()'>保存TG配置</button>
       <button onclick='testTG()'>测试通知</button>
     </div>
   </div>
 </div>`;
 setTGForm(c);
 await refreshQbStats();
 if(qbStatsTimer) clearInterval(qbStatsTimer);
 qbStatsTimer = setInterval(refreshQbStats, 5000);
 await refreshLogs();
}
async function save(){
 const body={
  enabled:document.getElementById('enabled').checked,
  interval:parseInt(document.getElementById('interval').value||'120'),
  u2_api_base:document.getElementById('u2_api_base').value.trim(),
  u2_api_token:document.getElementById('u2_api_token').value.trim(),
  u2_passkey:document.getElementById('u2_passkey').value.trim(),
  scope:document.getElementById('scope').value,
  limit:parseInt(document.getElementById('limit').value||'20'),
  max_seeders:5,
  download_non_free:false,
  qb_mode:document.getElementById('qb_mode').value,
  qb_up_limit_mb:parseInt(document.getElementById('qb_up_limit_mb').value||'50'),
  qb_clients:qbClients,
  ...collectTG(),
 };
 await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 alert('配置已保存'); load();
}
async function testTG(){ const r=await fetch('/api/tg/test',{method:'POST'}); const d=await r.json(); alert(d.ok?('测试通知已发送：'+(d.message||'')):('测试通知失败：'+(d.error||'unknown'))); }
async function runNow(){ await fetch('/api/run',{method:'POST'}); setTimeout(load, 900); }
async function refreshLogs(){ const t=await fetch('/api/logs').then(r=>r.text()); const node=document.getElementById('logs'); if(node) node.textContent=t||'(暂无日志)'; }
load();
</script>
</body></html>
""".replace('__APP_VERSION__', APP_VERSION)


@app.get('/api/version')
def get_version():
    return {'version': APP_VERSION}


@app.get('/api/config')
def get_config():
    return load_config()


@app.put('/api/config')
def put_config(cfg: ConfigIn):
    data = cfg.dict()
    data['interval'] = max(10, int(data['interval']))
    data['qb_up_limit_mb'] = max(0, int(data.get('qb_up_limit_mb', 50)))
    if not data.get('qb_clients'):
        data['qb_clients'] = []
    save_json(CONFIG_PATH, data)
    log('配置已更新')
    return {'ok': True}


@app.get('/api/status')
def status():
    st = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0})
    st['running'] = runner._running
    return st


@app.post('/api/run')
def run_now():
    threading.Thread(target=runner.run_once, daemon=True).start()
    return {'ok': True}


@app.get('/api/qb/stats')
def qb_stats():
    cfg = load_config()
    clients = cfg.get('qb_clients') or []
    stats = [qb_fetch_stats(c) for c in clients]
    return {'items': stats, 'mode': cfg.get('qb_mode', 'round_robin')}




@app.post('/api/tg/test')
def tg_test():
    cfg = load_config()
    try:
        ok, msg = tg_notify(cfg, f'✅ U2 TG通知测试\n时间: {now_iso()}\n版本: {APP_VERSION}')
        if ok:
            return {'ok': True, 'message': '已发送'}
        return {'ok': False, 'error': msg}
    except Exception as e:
        return {'ok': False, 'error': str(e)}

@app.get('/api/logs', response_class=PlainTextResponse)
def logs():
    if not APP_LOG.exists():
        return ''
    txt = APP_LOG.read_text(encoding='utf-8', errors='ignore')
    lines = txt.splitlines()[-200:]
    lines.reverse()
    return '\n'.join(lines)
