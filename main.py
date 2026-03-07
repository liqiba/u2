import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List

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
APP_VERSION = '2026.3.1'

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
    'qb_mode': 'round_robin',
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
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')


def log(msg: str):
    with open(APP_LOG, 'a', encoding='utf-8') as f:
        f.write(f'[{now_iso()}] {msg}\n')


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


def qb_add_torrent(client: dict, torrent_url: str):
    sess, qb_url, err = qb_login(client)
    if err:
        return False, err

    data = {
        'urls': torrent_url,
        'autoTMM': 'false',
        'paused': 'true' if client.get('qb_paused') else 'false',
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
        log('scheduler started')

    def stop(self):
        self._stop = True
        log('scheduler stopping')

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
                log('run skipped: empty u2_api_token')
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
            for p in new_items:
                pid = p.get('promotion_id') or p.get('id')
                tid = p.get('torrent_id')
                dr = p.get('download_ratio')
                seeders = p.get('seeders', '?')
                log(f'new promotion detected: id={pid}, tid={tid}, dr={dr}, seeders={seeders}')

                if not tid:
                    log(f'skip push to qB: missing tid for promotion {pid}')
                    continue
                if not passkey:
                    log('skip push to qB: u2_passkey 未配置')
                    continue

                targets = pick_qb_clients(cfg, state)
                if not targets:
                    log('skip push to qB: 没有启用的 qB 客户端')
                    continue

                torrent_url = f'https://u2.dmhy.org/download.php?id={tid}&passkey={passkey}&https=1'
                for cli in targets:
                    cname = cli.get('name') or cli.get('qb_url') or 'unknown'
                    ok, msg = qb_add_torrent(cli, torrent_url)
                    if ok:
                        log(f'pushed to qB[{cname}]: promotion={pid}, tid={tid}')
                    else:
                        log(f'push to qB[{cname}] failed: promotion={pid}, tid={tid}, err={msg}')

            state['last_seen'] = [str((p.get('promotion_id') or p.get('id'))) for p in data if (p.get('promotion_id') or p.get('id'))][:200]
            state['last_run'] = now_iso()
            state['last_error'] = None
            save_json(STATE_PATH, state)
            log(f'run done: fetched={len(data)}, new={len(new_items)}')
        except Exception as e:
            state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0})
            state['last_run'] = now_iso()
            state['last_error'] = str(e)
            save_json(STATE_PATH, state)
            log(f'run failed: {e}')
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
    qb_mode: str = 'round_robin'
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
    :root{--bg:#0b1020;--bg2:#131a2e;--card:#151d34;--line:#263150;--text:#e7ecff;--sub:#9eadcf;--ok:#22c55e;--warn:#f59e0b;--err:#ef4444;--pri:#4f7cff;--pri2:#335ed8}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(1200px 800px at 20% -10%,#1a2650 0%,var(--bg) 50%);color:var(--text);font-family:Inter,Segoe UI,Arial,sans-serif}
    .wrap{max-width:1100px;margin:24px auto;padding:0 16px}.title{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
    h2{margin:0;font-size:24px}.badge{padding:6px 10px;border-radius:999px;border:1px solid var(--line);font-size:12px;color:var(--sub)}
    .grid{display:grid;gap:14px}.card{background:linear-gradient(180deg,var(--card),var(--bg2));border:1px solid var(--line);border-radius:14px;padding:14px}
    .status,.form{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.form{grid-template-columns:1fr 1fr}.full{grid-column:1/-1}
    .k{font-size:12px;color:var(--sub);margin-bottom:4px}.v{font-size:14px;word-break:break-all}.dot{display:inline-block;width:8px;height:8px;border-radius:999px;margin-right:6px}
    .ok{background:var(--ok)} .err{background:var(--err)} .warn{background:var(--warn)} label{font-size:12px;color:var(--sub);display:block;margin-bottom:6px}
    input,select{width:100%;background:#0e1528;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:10px}
    input:focus,select:focus{outline:none;border-color:var(--pri)} .switch{display:flex;align-items:center;gap:10px}.switch input{width:auto}
    .actions{display:flex;gap:10px;flex-wrap:wrap} button{border:0;border-radius:10px;padding:10px 14px;color:white;background:var(--pri);cursor:pointer;font-weight:600}
    button:hover{background:var(--pri2)} button.ghost{background:transparent;border:1px solid var(--line);color:var(--text)} button.danger{background:#b4232a}
    pre{margin:0;background:#0a0f1f;color:#c7d2ff;border:1px solid var(--line);border-radius:10px;padding:12px;max-height:360px;overflow:auto;line-height:1.45}
    .tip{font-size:12px;color:var(--sub)} .qb-item{border:1px dashed var(--line);border-radius:12px;padding:10px;margin-top:10px}
    .qb-row{display:grid;grid-template-columns:1.1fr 1.2fr .8fr 1fr 1fr auto;gap:8px;align-items:center;margin-top:8px}
    .qb-head{font-size:12px;color:var(--sub);padding:0 4px}
    .modules{display:grid;grid-template-columns:1fr 1fr;gap:10px}
    @media (max-width:960px){.qb-row{grid-template-columns:1fr 1fr}.qb-row .fullm{grid-column:1/-1}}
    @media (max-width:760px){.form,.status,.modules{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='title'><h2>Catch Magic Web <span style='font-size:13px;color:var(--sub);font-weight:500'>v__APP_VERSION__</span></h2><div class='badge' id='runBadge'>状态读取中...</div></div>
    <div id='app' class='grid'>loading...</div>
  </div>
<script>
let qbClients=[];
let qbStatsTimer=null;
async function j(u,o){const r=await fetch(u,o);return await r.json()}
function esc(t){return (t??'').toString().replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function fmtBytes(n){
  n=Number(n||0);
  const u=['B','KB','MB','GB','TB'];
  let i=0; while(n>=1024&&i<u.length-1){n/=1024;i++;}
  return `${n.toFixed(i<=1?0:2)} ${u[i]}`;
}
function fmtSpeed(n){ return `${fmtBytes(n)}/s`; }
function statusBadge(s){ if(s.running) return `<span class='dot warn'></span>执行中`; if(s.last_error) return `<span class='dot err'></span>异常`; return `<span class='dot ok'></span>正常`; }
function renderQbModules(items=[]){
  const el=document.getElementById('qbModules'); if(!el) return;
  if(!items.length){ el.innerHTML = `<div class='tip'>暂无 qB 模块</div>`; return; }
  el.innerHTML = items.map(it=>`
    <div class='qb-item'>
      <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>
        <div style='font-weight:700'>${esc(it.name||'qB')}</div>
        <div class='badge'>${it.ok?"<span class='dot ok'></span>在线":"<span class='dot err'></span>离线"}</div>
      </div>
      <div class='status'>
        <div><div class='k'>任务数</div><div class='v'>${it.ok?it.task_count:'-'}</div></div>
        <div><div class='k'>实时下载</div><div class='v'>${it.ok?fmtSpeed(it.dl_speed):'-'}</div></div>
        <div><div class='k'>实时上传</div><div class='v'>${it.ok?fmtSpeed(it.up_speed):'-'}</div></div>
        <div><div class='k'>总下载</div><div class='v'>${it.ok?fmtBytes(it.dl_total):'-'}</div></div>
        <div><div class='k'>总上传</div><div class='v'>${it.ok?fmtBytes(it.up_total):'-'}</div></div>
        <div><div class='k'>节点</div><div class='v'>${esc(it.qb_url||'-')}</div></div>
      </div>
      ${it.ok?'':`<div class='tip' style='margin-top:8px;color:#ff9aa2'>${esc(it.error||'连接失败')}</div>`}
    </div>
  `).join('');
}
async function refreshQbStats(){
  try{
    const res=await j('/api/qb/stats');
    renderQbModules(res.items||[]);
  }catch(e){
    renderQbModules([]);
  }
}
function renderQbList(){
  const box=document.getElementById('qbList'); if(!box) return;
  if(!qbClients.length){ box.innerHTML = `<div class='tip'>还没有 qB，点上方“添加 qB 模块”</div>`; return; }
  box.innerHTML = `
    <div class='qb-row qb-head'>
      <div>名称</div><div>URL</div><div>用户名</div><div>分类</div><div>保存路径</div><div>操作</div>
    </div>
  ` + qbClients.map((q,i)=>`
    <div class='qb-row qb-item'>
      <div>
        <input id='qb_name_${i}' value='${esc(q.name||`qb-${i+1}`)}'>
        <div class='switch' style='margin-top:6px'><input id='qb_enabled_${i}' type='checkbox' ${q.enabled!==false?'checked':''}><label for='qb_enabled_${i}' style='margin:0;color:var(--text)'>启用</label></div>
      </div>
      <div>
        <input id='qb_url_${i}' value='${esc(q.qb_url||'')}' placeholder='http://127.0.0.1:8080'>
        <input id='qb_password_${i}' type='password' value='${esc(q.qb_password||'')}' placeholder='密码' style='margin-top:6px'>
      </div>
      <div><input id='qb_username_${i}' value='${esc(q.qb_username||'')}'></div>
      <div><input id='qb_category_${i}' value='${esc(q.qb_category||'')}'></div>
      <div>
        <input id='qb_savepath_${i}' value='${esc(q.qb_savepath||'')}' placeholder='/downloads/u2'>
        <div class='switch' style='margin-top:6px'><input id='qb_paused_${i}' type='checkbox' ${q.qb_paused?'checked':''}><label for='qb_paused_${i}' style='margin:0;color:var(--text)'>推送后暂停</label></div>
      </div>
      <div class='fullm'><button class='danger' onclick='removeQb(${i})'>删除</button></div>
    </div>
  `).join('');
}
function addQb(){ qbClients.push({name:`qb-${qbClients.length+1}`,enabled:true,qb_url:'http://127.0.0.1:8080',qb_username:'',qb_password:'',qb_category:'',qb_savepath:'',qb_paused:false}); renderQbList(); }
function removeQb(i){ qbClients.splice(i,1); renderQbList(); }
function collectQb(){
  return qbClients.map((_,i)=>({
    name:document.getElementById(`qb_name_${i}`).value.trim(),
    enabled:document.getElementById(`qb_enabled_${i}`).checked,
    qb_url:document.getElementById(`qb_url_${i}`).value.trim(),
    qb_username:document.getElementById(`qb_username_${i}`).value.trim(),
    qb_password:document.getElementById(`qb_password_${i}`).value.trim(),
    qb_category:document.getElementById(`qb_category_${i}`).value.trim(),
    qb_savepath:document.getElementById(`qb_savepath_${i}`).value.trim(),
    qb_paused:document.getElementById(`qb_paused_${i}`).checked,
  }));
}
async function load(){
 const c=await j('/api/config'); const s=await j('/api/status'); qbClients = c.qb_clients || [];
 document.getElementById('runBadge').innerHTML=statusBadge(s);
 document.getElementById('app').innerHTML=`
 <div class='card'><div class='status'>
   <div><div class='k'>最后执行</div><div class='v'>${esc(s.last_run||'-')}</div></div>
   <div><div class='k'>运行状态</div><div class='v'>${s.running?'执行中':'空闲'}</div></div>
   <div><div class='k'>最近错误</div><div class='v'>${esc(s.last_error||'无')}</div></div>
 </div></div>
 <div class='card'>
   <div class='k' style='margin-bottom:8px'>qB 模块看板（自动刷新）</div>
   <div id='qbModules' class='modules'><div class='tip'>加载中...</div></div>
 </div>
 <div class='card'><div class='form'>
   <div class='full switch'><input id='enabled' type='checkbox' ${c.enabled?'checked':''}><label for='enabled' style='margin:0;color:var(--text)'>启用定时任务</label></div>
   <div><label>执行间隔（秒）</label><input id='interval' type='number' min='10' value='${c.interval}'></div>
   <div><label>抓取条数（limit）</label><input id='limit' type='number' min='1' max='60' value='${c.limit}'></div>
   <div class='full'><label>U2 API Base</label><input id='u2_api_base' value='${esc(c.u2_api_base)}'></div>
   <div class='full'><label>U2 API Token</label><input id='u2_api_token' type='password' value='${esc(c.u2_api_token||'')}'></div>
   <div class='full'><label>U2 Passkey</label><input id='u2_passkey' type='password' value='${esc(c.u2_passkey||'')}'></div>
   <div><label>魔法范围（scope）</label><select id='scope'><option value='public' ${c.scope==='public'?'selected':''}>公共魔法（public）</option><option value='all' ${c.scope==='all'?'selected':''}>全部魔法（all）</option><option value='private' ${c.scope==='private'?'selected':''}>私人魔法（private）</option><option value='global' ${c.scope==='global'?'selected':''}>全局魔法（global）</option></select></div>
   <div><label>qB 分发模式</label><select id='qb_mode'><option value='round_robin' ${c.qb_mode==='round_robin'?'selected':''}>轮询分发</option><option value='all' ${c.qb_mode==='all'?'selected':''}>全部推送</option></select></div>
   <div class='full' style='margin-top:6px;padding-top:10px;border-top:1px dashed var(--line);color:var(--sub);font-size:12px'>多 qB 配置</div>
   <div class='full actions'><button type='button' onclick='addQb()'>+ 添加 qB 模块</button></div>
   <div class='full' id='qbList'></div>
 </div>
 <div class='actions' style='margin-top:12px'><button onclick='save()'>保存配置</button><button onclick='runNow()'>立即执行一次</button><button class='ghost' onclick='refreshLogs()'>刷新日志</button></div>
 <div class='tip' style='margin-top:10px'>建议使用“轮询分发”，可把任务均匀分配到多个 qB。</div>
 </div>
 <div class='card'><div class='k' style='margin-bottom:8px'>最近日志（最多 200 行）</div><pre id='logs'>loading logs...</pre></div>`;
 renderQbList();
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
  qb_clients:collectQb(),
 };
 await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
 alert('配置已保存'); load();
}
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
    if not data.get('qb_clients'):
        data['qb_clients'] = []
    save_json(CONFIG_PATH, data)
    log('config updated')
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


@app.get('/api/logs', response_class=PlainTextResponse)
def logs():
    if not APP_LOG.exists():
        return ''
    txt = APP_LOG.read_text(encoding='utf-8', errors='ignore')
    return '\n'.join(txt.splitlines()[-200:])
