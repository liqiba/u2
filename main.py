import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from urllib.parse import urlparse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

DATA_DIR = Path('/data')
LOG_DIR = Path('/logs')
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = DATA_DIR / 'config.json'
STATE_PATH = DATA_DIR / 'state.json'
UPGRADE_REQUEST_PATH = DATA_DIR / 'upgrade.request.json'
UPGRADE_STATUS_PATH = DATA_DIR / 'upgrade.status.json'
APP_LOG = LOG_DIR / 'app.log'
APP_VERSION = '2026.3.107'
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / 'templates'
STATIC_DIR = BASE_DIR / 'static'
QB_TORRENT_UP_LIMIT_BYTES = 50 * 1024 * 1024  # default: 50 MB/s per torrent
LOCAL_TZ = ZoneInfo('Asia/Shanghai')
SELF_MAGIC_LOCK = threading.Lock()

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
    'require_2x_free': True,
    'auto_self_magic_enabled': False,
    'auto_self_magic_min_upload_kib': 1024,
    'auto_self_magic_min_size_gb': 5,
    'auto_self_magic_max_size_gb': 0,
    'auto_self_magic_hours': 24,
    'auto_self_magic_interval': 60,
    'auto_self_magic_magic_downloading': True,
    'auto_self_magic_min_d': 180,
    'u2_uid': 0,
    'u2_cookie': '',
    'qb_up_limit_mb': 50,
    'qb_mode': 'round_robin',
    'tg_enabled': False,
    'tg_bot_token': '',
    'tg_chat_id': '',
    'tg_notify_new': True,
    'tg_notify_error': True,
    'failed_retry_interval_seconds': 300,
    'failed_retry_batch': 20,
    'failed_push_ttl_seconds': 1800,
    'web_auth_enabled': False,
    'web_password_hash': '',
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

AUTH_COOKIE_NAME = 'u2_auth_sid'
AUTH_SESSION_TTL_SEC = 7 * 24 * 3600
AUTH_MAX_FAILS = 5
AUTH_LOCK_SECONDS = 30 * 60
AUTH_WINDOW_SECONDS = 15 * 60
AUTH_FAIL_DELAY_SEC = 1.2

AUTH_SESSIONS = {}
AUTH_GUARD = {}


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


def tg_api(cfg: dict, method: str, payload: dict = None, timeout: int = 15):
    token = (cfg.get('tg_bot_token') or '').strip()
    if not token:
        return None, '缺少TG Bot Token'
    try:
        r = requests.post(
            f'https://api.telegram.org/bot{token}/{method}',
            json=(payload or {}),
            timeout=timeout,
        )
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}: {r.text[:120]}'
        d = r.json() if r.text else {}
        if not d.get('ok', False):
            return None, d.get('description', 'TG接口返回失败')
        return d.get('result'), None
    except Exception as e:
        return None, str(e)


def tg_set_upgrade_menu(cfg: dict):
    if not cfg.get('tg_enabled'):
        return False, 'TG未启用'
    commands = [
        {'command': 'upgrade', 'description': '一键升级容器'},
        {'command': 'upgradestatus', 'description': '查看升级状态'},
    ]
    _, err = tg_api(cfg, 'setMyCommands', {'commands': commands, 'scope': {'type': 'chat', 'chat_id': str(cfg.get('tg_chat_id') or '')}})
    if err:
        return False, err
    return True, 'ok'


def tg_poll_upgrade_commands(cfg: dict, state: dict):
    if not cfg.get('tg_enabled'):
        return
    chat_id = str(cfg.get('tg_chat_id') or '').strip()
    if not chat_id:
        return

    offset = int(state.get('tg_update_offset') or 0)
    result, err = tg_api(cfg, 'getUpdates', {'offset': offset, 'timeout': 0, 'limit': 30}, timeout=20)
    if err:
        return
    if not isinstance(result, list) or not result:
        return

    for up in result:
        try:
            uid = int(up.get('update_id') or 0)
            if uid >= offset:
                offset = uid + 1
            msg = up.get('message') or {}
            txt = str(msg.get('text') or '').strip().lower()
            cid = str(((msg.get('chat') or {}).get('id')))
            if cid != chat_id:
                continue
            if txt == '/upgrade':
                if UPGRADE_REQUEST_PATH.exists():
                    tg_notify(cfg, '⏳ 已有升级任务在队列中，请稍候。')
                    continue
                save_json(UPGRADE_REQUEST_PATH, {'requested_at': now_iso(), 'source': 'tg:/upgrade', 'note': 'telegram menu'})
                save_json(UPGRADE_STATUS_PATH, {'state': 'queued', 'updated_at': now_iso(), 'message': '已从TG菜单触发升级'})
                tg_notify(cfg, '🚀 已收到一键升级指令，开始异步升级。')
                log('TG菜单触发升级请求：/upgrade')
            elif txt == '/upgradestatus':
                st = load_json(UPGRADE_STATUS_PATH, {'state': 'idle', 'updated_at': now_iso(), 'message': '暂无升级任务'})
                tg_notify(cfg, f"📦 升级状态\n状态: {st.get('state')}\n时间: {st.get('updated_at')}\n信息: {st.get('message')}")
        except Exception:
            continue

    state['tg_update_offset'] = offset


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


def _ip_of(req: Request):
    xff = req.headers.get('x-forwarded-for', '')
    if xff:
        return xff.split(',')[0].strip()
    if req.client and req.client.host:
        return req.client.host
    return 'unknown'


def _hash_password(password: str):
    salt = secrets.token_hex(16)
    rounds = 200000
    digest = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), rounds).hex()
    return f'pbkdf2_sha256${rounds}${salt}${digest}'


def _verify_password(password: str, stored: str):
    try:
        algo, rounds_s, salt, digest = str(stored or '').split('$', 3)
        if algo != 'pbkdf2_sha256':
            return False
        rounds = int(rounds_s)
        cand = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), rounds).hex()
        return hmac.compare_digest(cand, digest)
    except Exception:
        return False


def _auth_enabled(cfg: dict):
    # 只要已配置密码，就强制要求登录访问
    return bool(str(cfg.get('web_password_hash') or '').strip())


def _guard_info(ip: str):
    now = int(time.time())
    g = AUTH_GUARD.get(ip) or {'fails': 0, 'window_start': now, 'lock_until': 0}
    if now - int(g.get('window_start') or 0) > AUTH_WINDOW_SECONDS:
        g['fails'] = 0
        g['window_start'] = now
    return g


def _is_locked(ip: str):
    now = int(time.time())
    g = _guard_info(ip)
    lock_until = int(g.get('lock_until') or 0)
    if lock_until > now:
        AUTH_GUARD[ip] = g
        return True, lock_until - now
    if lock_until and lock_until <= now:
        g['lock_until'] = 0
    AUTH_GUARD[ip] = g
    return False, 0


def _register_login_fail(ip: str):
    now = int(time.time())
    g = _guard_info(ip)
    g['fails'] = int(g.get('fails') or 0) + 1
    if g['fails'] >= AUTH_MAX_FAILS:
        g['lock_until'] = now + AUTH_LOCK_SECONDS
        g['fails'] = 0
        g['window_start'] = now
    AUTH_GUARD[ip] = g


def _register_login_success(ip: str):
    AUTH_GUARD.pop(ip, None)


def _new_auth_session(ip: str):
    sid = secrets.token_urlsafe(32)
    now = int(time.time())
    AUTH_SESSIONS[sid] = {'created_at': now, 'expire_at': now + AUTH_SESSION_TTL_SEC, 'ip': ip}
    return sid


def _valid_session(sid: str):
    if not sid:
        return False
    now = int(time.time())
    s = AUTH_SESSIONS.get(sid)
    if not s:
        return False
    if int(s.get('expire_at') or 0) <= now:
        AUTH_SESSIONS.pop(sid, None)
        return False
    return True


def _cleanup_auth_sessions():
    now = int(time.time())
    expired = [k for k, v in AUTH_SESSIONS.items() if int(v.get('expire_at') or 0) <= now]
    for k in expired:
        AUTH_SESSIONS.pop(k, None)


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


def qb_fetch_stats(client: dict, timeout_sec: int = 8):
    name = client.get('name') or client.get('qb_url') or 'unknown'
    try:
        sess, qb_url, err = qb_login(client)
    except Exception as e:
        return {'name': name, 'ok': False, 'error': str(e)}
    if err:
        return {'name': name, 'ok': False, 'error': err}
    try:
        resp = sess.get(f'{qb_url}/api/v2/sync/maindata', timeout=max(3, int(timeout_sec)))
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


def healthy_qb_clients(clients: list):
    ok_clients = []
    bad_names = []
    for c in clients:
        st = qb_fetch_stats(c)
        if st.get('ok'):
            ok_clients.append(c)
        else:
            bad_names.append(c.get('name') or c.get('qb_url') or 'unknown')
    return ok_clients, bad_names


def add_failed_push(state: dict, item: dict):
    arr = list(state.get('failed_pushes') or [])
    if not item.get('fail_ts'):
        item['fail_ts'] = int(time.time())
    arr.append(item)
    state['failed_pushes'] = arr[-200:]


def _failed_item_ts(item: dict):
    ts = item.get('fail_ts')
    if isinstance(ts, (int, float)) and ts > 0:
        return int(ts)
    t = str(item.get('time') or '').strip()
    if t:
        try:
            dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S CST').replace(tzinfo=LOCAL_TZ)
            return int(dt.timestamp())
        except Exception:
            return 0
    return 0


def retry_failed_pushes_once(cfg: dict, state: dict):
    failed = list(state.get('failed_pushes') or [])
    if not failed:
        return {'ok': True, 'retried': 0, 'success': 0, 'remain': 0, 'expired': 0}

    enabled = [c for c in (cfg.get('qb_clients') or []) if c.get('enabled', True)]
    healthy, _ = healthy_qb_clients(enabled)
    targets = healthy if healthy else enabled
    if not targets:
        return {'ok': False, 'error': '没有启用的 qB 客户端'}

    # 失败项最多保留30分钟；超时直接丢弃，避免旧种子集中补推。
    ttl_seconds = max(60, int(cfg.get('failed_push_ttl_seconds', 1800) or 1800))
    # 每轮重推限流，避免一次性爆量。
    batch = max(1, int(cfg.get('failed_retry_batch', 20) or 20))

    now_ts = int(time.time())
    valid = []
    expired = 0
    for it in failed:
        its = _failed_item_ts(it)
        if its > 0 and (now_ts - its) > ttl_seconds:
            expired += 1
            continue
        valid.append(it)

    to_retry = valid[:batch]
    pending = valid[batch:]

    success = 0
    remain_retry = []
    for it in to_retry:
        turl = it.get('torrent_url')
        upl = int(it.get('up_limit_bytes') or 0)
        ok_one = False
        for cli in targets:
            for _ in range(3):
                ok, _ = qb_add_torrent(cli, turl, upl)
                if ok:
                    ok_one = True
                    success += 1
                    break
                time.sleep(1)
            if ok_one:
                break
        if not ok_one:
            remain_retry.append(it)

    remain = remain_retry + pending
    state['failed_pushes'] = remain[-200:]
    return {
        'ok': True,
        'retried': len(to_retry),
        'success': success,
        'remain': len(remain),
        'expired': expired,
        'batch': batch,
    }


def summarize_error_cn(err: str):
    t = (err or '').lower()
    if '401' in t or 'unauthorized' in t or 'invalid token' in t:
        return '鉴权失败：U2 Token 无效或已过期'
    if 'timeout' in t or 'timed out' in t:
        return '请求超时：请检查网络或稍后重试'
    if 'name or service not known' in t or 'temporary failure in name resolution' in t:
        return '域名解析失败：请检查网络/DNS'
    if 'connection refused' in t:
        return '连接被拒绝：目标服务未启动或端口不可达'
    if 'u2_api_token 为空' in t:
        return '未配置 U2 Token'
    return '运行失败：请查看详细日志'



def qb_list_active_torrents(client: dict):
    sess, qb_url, err = qb_login(client)
    if err:
        return [], err
    try:
        r = sess.get(f'{qb_url}/api/v2/torrents/info', params={'filter':'active'}, timeout=20)
        r.raise_for_status()
        arr = r.json()
        if not isinstance(arr, list):
            return [], 'qB返回格式异常'
        return arr, None
    except Exception as e:
        return [], str(e)


def u2_tid_by_hash(cfg: dict, _hash: str):
    token = (cfg.get('u2_api_token') or '').strip()
    uid = int(cfg.get('u2_uid') or 0)
    if not token or not uid:
        return None, '缺少u2_api_token或u2_uid'
    try:
        r = requests.get('https://u2.kysdm.com/api/v1/history', params={'uid': uid, 'token': token, 'hash': _hash}, timeout=20)
        r.raise_for_status()
        d = r.json().get('data', {}).get('history', [])
        if d:
            return d[0].get('torrent_id'), None
        return None, '未查到对应种子ID'
    except Exception as e:
        return None, str(e)


def u2_promotion_snapshot(cfg: dict, scope: str, maximum: int = 200):
    token = (cfg.get('u2_api_token') or '').strip()
    uid = int(cfg.get('u2_uid') or 0)
    if not token or not uid:
        return None, '缺少u2_api_token或u2_uid'
    try:
        r = requests.get(
            'https://u2.kysdm.com/api/v1/promotion',
            params={'uid': uid, 'token': token, 'scope': scope, 'maximum': int(maximum)},
            timeout=20,
        )
        r.raise_for_status()
        arr = (r.json().get('data') or {}).get('promotion') or []
        m = {}
        for p in arr:
            if not isinstance(p, dict):
                continue
            tid = p.get('torrent_id')
            if not tid:
                continue
            ur = p.get('upload_ratio')
            dr = p.get('download_ratio')
            ratio = str(p.get('ratio') or '')
            if ratio and '/' in ratio:
                try:
                    left, right = [x.strip() for x in ratio.split('/', 1)]
                    ur = float(left)
                    dr = float(right)
                except Exception:
                    pass
            m[int(tid)] = {'ur': ur, 'dr': dr, 'ratio': ratio}
        return m, None
    except Exception as e:
        return None, str(e)


def _ratio_to_float_pair(ratio: str):
    try:
        if ratio and '/' in ratio:
            l, r = [x.strip() for x in str(ratio).split('/', 1)]
            return float(l), float(r)
    except Exception:
        pass
    return None, None


def _parse_uc_cost(text: str):
    if not text:
        return None

    # U2 test接口常见格式：{"status":"operational","price":"<span ... title=\"1,598.00\">..."}
    # 先匹配 钻/金/银/铜（1钻=100金=10000银）
    m_ggsc = re.search(
        r'ucoin-gem[^>]*>\s*([0-9]+)\s*<.*?ucoin-gold[^>]*>\s*([0-9]+)\s*<.*?ucoin-silver[^>]*>\s*([0-9]+)\s*<.*?ucoin-copper[^>]*>\s*([0-9]{1,2})\s*<',
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m_ggsc:
        try:
            gem = int(m_ggsc.group(1))
            gold = int(m_ggsc.group(2))
            silver = int(m_ggsc.group(3))
            copper = int(m_ggsc.group(4))
            total_silver = gem * 10000 + gold * 100 + silver + copper / 100.0
            return float(f'{total_silver:.2f}')
        except Exception:
            pass

    # 再匹配 金/银/铜
    m_gsc = re.search(
        r'ucoin-gold[^>]*>\s*([0-9]+)\s*<.*?ucoin-silver[^>]*>\s*([0-9]+)\s*<.*?ucoin-copper[^>]*>\s*([0-9]{1,2})\s*<',
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m_gsc:
        try:
            gold = int(m_gsc.group(1))
            silver = int(m_gsc.group(2))
            copper = int(m_gsc.group(3))
            total_silver = gold * 100 + silver + copper / 100.0
            return float(f'{total_silver:.2f}')
        except Exception:
            pass

    m_sc = re.search(r'ucoin-silver[^>]*>\s*([0-9]+)\s*<.*?ucoin-copper[^>]*>\s*([0-9]{1,2})\s*<', text, re.IGNORECASE | re.DOTALL)
    if m_sc:
        try:
            silver = int(m_sc.group(1))
            copper = int(m_sc.group(2))
            return float(f'{silver}.{copper:02d}')
        except Exception:
            pass

    m_title = re.search(r'title\s*=\s*[\"\']([0-9][0-9,，]*(?:\.[0-9]+)?)[\"\']', text, re.IGNORECASE)
    if m_title:
        try:
            return float(m_title.group(1).replace(',', '').replace('，', ''))
        except Exception:
            pass

    patterns = [
        r'([0-9][0-9,，]*(?:\.[0-9]+)?)\s*(?:UC|UCoin|论坛币|魔力|魔力值)',
        r'(?:花费|消耗|扣除)[^0-9]{0,20}([0-9][0-9,，]*(?:\.[0-9]+)?)',
        r'cost[^0-9]{0,10}([0-9][0-9,，]*(?:\.[0-9]+)?)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if not m:
            continue
        try:
            return float(m.group(1).replace(',', '').replace('，', ''))
        except Exception:
            continue
    return None


def _magic_day_key():
    return datetime.now(LOCAL_TZ).strftime('%Y-%m-%d')


def _uc_parts(uc: float):
    try:
        total_copper = int(round(float(uc) * 100))
    except Exception:
        total_copper = 0
    silver_total = total_copper // 100
    copper = total_copper % 100
    gold_total = silver_total // 100
    silver = silver_total % 100
    gem = gold_total // 100
    gold = gold_total % 100
    return {'gem': int(gem), 'gold': int(gold), 'silver': int(silver), 'copper': int(copper)}


def _uc_to_copper_value(uc: float):
    # 展示用：统一换算为“铜币”数值（保留2位）
    return float(f"{float(uc) * 100:.2f}")


def _format_uc_cn(uc: float):
    p = _uc_parts(uc)
    return f"{p['gem']}钻{p['gold']}金{p['silver']}银{p['copper']:02d}铜"


def _parse_cst_time(s: str):
    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S CST')
    except Exception:
        return None


def _fetch_ucoin_magic_entries(cfg: dict, max_pages: int = 3):
    cookie = (cfg.get('u2_cookie') or '').strip()
    uid = int(cfg.get('u2_uid') or 0)
    if not cookie or not uid:
        return [], '缺少u2_cookie或u2_uid'

    entries = []
    seen = set()
    ck = {'nexusphp_u2': cookie}

    for p in range(1, max_pages + 1):
        url = f'https://u2.dmhy.org/ucoin.php?id={uid}&log=1&page={p}'
        try:
            r = requests.get(url, cookies=ck, timeout=25)
            r.raise_for_status()
            html = r.text or ''
        except Exception as e:
            return entries, str(e)

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.IGNORECASE | re.DOTALL)
        found_in_page = 0
        for row in rows:
            m_tid = re.search(r'Magic\s*-\s*ID\s*(\d+),\s*Torrent\s*(\d+)', row, re.IGNORECASE)
            if not m_tid:
                continue
            m_time = re.search(r'<time[^>]*title="([0-9\-: ]+)"', row, re.IGNORECASE)
            if not m_time:
                continue
            t_forum = m_time.group(1).strip()
            dt = _parse_cst_time(t_forum + ' CST')
            if not dt:
                continue

            mge = re.search(r'ucoin-symbol\s+ucoin-gem">\s*([0-9]+)\s*<', row, re.IGNORECASE)
            mg = re.search(r'ucoin-symbol\s+ucoin-gold">\s*([0-9]+)\s*<', row, re.IGNORECASE)
            ms = re.search(r'ucoin-symbol\s+ucoin-silver">\s*([0-9]+)\s*<', row, re.IGNORECASE)
            mc = re.search(r'ucoin-symbol\s+ucoin-copper">\s*([0-9]+)\s*<', row, re.IGNORECASE)
            gem = int(mge.group(1)) if mge else 0
            gold = int(mg.group(1)) if mg else 0
            silver = int(ms.group(1)) if ms else 0
            copper = int(mc.group(1)) if mc else 0
            uc = float(f"{gem * 10000 + gold * 100 + silver + copper / 100.0:.2f}")
            pid = int(m_tid.group(1))
            tid = int(m_tid.group(2))

            key = (t_forum, pid, tid, gem, gold, silver, copper)
            if key in seen:
                continue
            seen.add(key)
            entries.append({
                'time': t_forum + ' CST',
                'dt': dt,
                'pid': pid,
                'tid': tid,
                'uc': uc,
                'ucParts': {'gem': gem, 'gold': gold, 'silver': silver, 'copper': copper},
            })
            found_in_page += 1

        if found_in_page == 0:
            break

    return entries, None


def verify_self_magic_effective(cfg: dict, tid: int, retries: int = 3, delay_sec: int = 2):
    token = (cfg.get('u2_api_token') or '').strip()
    uid = int(cfg.get('u2_uid') or 0)
    if not token or not uid:
        return False, '缺少u2_api_token或u2_uid，无法校验生效状态'

    last_seen = []
    for i in range(max(1, retries)):
        try:
            r = requests.get(
                'https://u2.kysdm.com/api/v1/promotion',
                params={'uid': uid, 'token': token, 'scope': 'all', 'maximum': 300},
                timeout=20,
            )
            r.raise_for_status()
            arr = (r.json().get('data') or {}).get('promotion') or []
            matched = []
            for p in arr:
                if int(p.get('torrent_id') or 0) != int(tid):
                    continue
                for_uid_raw = p.get('for_user_id')
                try:
                    for_uid = int(for_uid_raw) if for_uid_raw not in (None, '', 0, '0') else 0
                except Exception:
                    for_uid = 0
                ur, dr = _ratio_to_float_pair(p.get('ratio') or '')
                status = int(p.get('status') or 0)
                matched.append({'for_uid': for_uid, 'ratio': p.get('ratio'), 'status': status})
                # 兼容 U2 接口偶发 for_user_id 缺失/为0 的情况：
                # 只要同一 tid 已存在有效 2.33x/1x（或更高上传倍率）魔法，就视为已生效，避免重复施法。
                if ur is not None and dr is not None and ur >= 2.3 and abs(dr - 1.0) < 1e-6 and status == 1:
                    return True, 'ok'
            last_seen = matched
        except Exception as e:
            last_seen = [f'query_error:{e}']
        if i < retries - 1:
            time.sleep(max(1, int(delay_sec)))
    return False, f'未确认生效，tid={tid}，查询到={last_seen[:4]}'


def u2_send_self_magic(cfg: dict, tid: int):
    cookie = (cfg.get('u2_cookie') or '').strip()
    hours = int(cfg.get('auto_self_magic_hours') or 24)
    if not cookie:
        return False, '缺少u2_cookie', None

    # 提交前先查一次：若已存在有效 2.33x/1x 自放魔法，直接跳过，避免重复施法。
    already_ok, _ = verify_self_magic_effective(cfg, int(tid), retries=1, delay_sec=1)
    if already_ok:
        return True, 'already_effective', None

    data = {
        'action': 'magic', 'divergence': '', 'base_everyone': '', 'base_self': '', 'base_other': '',
        'torrent': tid, 'tsize': '', 'ttl': '', 'user_other': '', 'start': 0, 'promotion': 8,
        'comment': '', 'user': 'SELF', 'hours': hours, 'ur': 2.33, 'dr': 1,
    }
    ck = {'nexusphp_u2': cookie}
    try:
        # 先校验 cookie 是否有效（无效会返回 Access Point 登录页）
        chk = requests.get('https://u2.dmhy.org/', cookies=ck, timeout=20)
        txt_chk = chk.text or ''
        if ('Access Point :: U2' in txt_chk) or ('login.php' in txt_chk and 'logout.php' not in txt_chk):
            return False, 'u2_cookie 无效或已过期，请更新 cookie', None

        t = requests.post('https://u2.dmhy.org/promotion.php?test=1', data=data, cookies=ck, timeout=20)
        if t.status_code >= 300:
            return False, f'测试失败:{t.status_code}', None
        uc_cost = _parse_uc_cost(t.text or '')

        r = requests.post('https://u2.dmhy.org/promotion.php', data=data, cookies=ck, timeout=20)
        if r.status_code >= 300:
            return False, f'提交失败:{r.status_code}', uc_cost
        if uc_cost is None:
            uc_cost = _parse_uc_cost(r.text or '')
        ok, msg = verify_self_magic_effective(cfg, int(tid), retries=4, delay_sec=2)
        if ok:
            return True, 'ok', uc_cost
        return False, f'提交后校验未生效：{msg}', uc_cost
    except Exception as e:
        return False, str(e), None


def auto_self_magic_once(cfg: dict, state: dict, force: bool = False):
    if (not force) and (not cfg.get('auto_self_magic_enabled')):
        return {'ok': True, 'done': 0, 'msg': '未启用'}

    now = int(time.time())
    interval = max(10, int(cfg.get('auto_self_magic_interval') or 60))
    if (not force):
        last_ts = int(state.get('self_magic_last_ts') or 0)
        if now - last_ts < interval:
            return {'ok': True, 'done': 0, 'msg': f'未到检查间隔({interval}s)'}

    min_up = int(cfg.get('auto_self_magic_min_upload_kib') or 1024) * 1024
    min_size = int(cfg.get('auto_self_magic_min_size_gb') or 5) * 1024 * 1024 * 1024
    max_size_cfg = max(0, int(cfg.get('auto_self_magic_max_size_gb') or 0))
    max_size = max_size_cfg * 1024 * 1024 * 1024 if max_size_cfg > 0 else 0
    min_days = max(0, int(cfg.get('auto_self_magic_min_d') or 0))
    allow_downloading = bool(cfg.get('auto_self_magic_magic_downloading', True))

    candidates = []
    clients = [c for c in (cfg.get('qb_clients') or []) if c.get('enabled', True)]
    healthy, bad = healthy_qb_clients(clients)
    if bad:
        log(f'自放魔法：以下qB不可用，已跳过：{",".join(bad)}')
    for cli in healthy:
        arr, err = qb_list_active_torrents(cli)
        if err:
            log(f'自放魔法：读取qB活动种子失败：{err}')
            continue
        for t in arr:
            tr = str(t.get('tracker') or '')
            if ('daydream.dmhy.best' not in tr) and ('tracker.dmhy.org' not in tr):
                continue
            up = int(t.get('upspeed') or 0)
            size = int(t.get('size') or 0)
            prog = float(t.get('progress') or 0)
            added_on = int(t.get('added_on') or 0)
            if up < min_up:
                continue
            if size < min_size:
                continue
            if max_size > 0 and size > max_size:
                continue
            if (not allow_downloading) and prog < 1.0:
                continue
            if min_days > 0 and added_on > 0 and (now - added_on) < min_days * 86400:
                continue
            candidates.append({'hash': (t.get('hash') or '').lower(), 'up': up, 'size': size})

    if not candidates:
        log('自放魔法：没有符合条件的活跃做种')
        return {'ok': True, 'done': 0, 'msg': '无候选种子'}

    recent = dict(state.get('self_magic_recent') or {})
    recent_tid = dict(state.get('self_magic_recent_tid') or {})
    now = int(time.time())
    done = 0
    sent_tid_in_run = set()
    stat = {'nohash': 0, 'cooldown_hash': 0, 'tid_lookup_fail': 0, 'dup_tid': 0, 'cooldown_tid': 0, 'daily_limit_tid': 0, 'submit_fail': 0}
    day = _magic_day_key()
    uc_total = float(state.get('self_magic_uc_total') or 0.0)
    uc_by_day = dict(state.get('self_magic_uc_by_day') or {})
    fail_total = int(state.get('self_magic_fail_total') or 0)
    fail_by_day = dict(state.get('self_magic_fail_by_day') or {})
    daily_limit_total = int(state.get('self_magic_daily_limit_total') or 0)
    daily_limit_by_day = dict(state.get('self_magic_daily_limit_by_day') or {})
    # 硬限制：当天同 tid 最大施法次数=1（含“提交后校验未命中”这种防重冷却场景）
    daily_tid_sent = dict(state.get('self_magic_daily_tid_sent') or {})
    daily_for_today = set(str(x) for x in (daily_tid_sent.get(day) or []))
    for t in candidates[:20]:
        h = t['hash']
        if not h:
            stat['nohash'] += 1
            continue
        ts = int(recent.get(h) or 0)
        if now - ts < 20 * 3600:
            stat['cooldown_hash'] += 1
            continue
        tid, err = u2_tid_by_hash(cfg, h)
        if err or not tid:
            stat['tid_lookup_fail'] += 1
            log(f'自放魔法：hash={h[:8]} 查tid失败：{err}')
            continue
        tid = int(tid)
        if tid in sent_tid_in_run:
            stat['dup_tid'] += 1
            log(f'自放魔法：tid={tid} 本轮已处理，跳过重复hash={h[:8]}')
            continue
        ts_tid = int(recent_tid.get(str(tid)) or 0)
        if now - ts_tid < 20 * 3600:
            stat['cooldown_tid'] += 1
            log(f'自放魔法：tid={tid} 冷却中，跳过hash={h[:8]}')
            continue
        if str(tid) in daily_for_today:
            stat['cooldown_tid'] += 1
            stat['daily_limit_tid'] += 1
            log(f'自放魔法：tid={tid} 今日已施法，跳过hash={h[:8]}')
            continue
        ok, msg, uc_cost = u2_send_self_magic(cfg, tid)
        if ok:
            recent[h] = now
            recent_tid[str(tid)] = now
            sent_tid_in_run.add(tid)
            daily_for_today.add(str(tid))
            done += 1
            if uc_cost is not None:
                uc_total += float(uc_cost)
                uc_by_day[day] = float(uc_by_day.get(day) or 0.0) + float(uc_cost)
                log(f'自放魔法成功：tid={tid}，hash={h[:8]}，UC={float(uc_cost):.2f}（{_format_uc_cn(float(uc_cost))}）')
            else:
                log(f'自放魔法成功：tid={tid}，hash={h[:8]}')
        else:
            # 提交已成功但“生效校验”未及时看到时，给tid加冷却，避免短时间重复施法
            if str(msg).startswith('提交后校验未生效'):
                recent[h] = now
                recent_tid[str(tid)] = now
                sent_tid_in_run.add(tid)
                daily_for_today.add(str(tid))
                log(f'自放魔法：tid={tid} 提交成功但校验未命中，已进入冷却防重（{msg}）')
            else:
                stat['submit_fail'] += 1
                fail_total += 1
                fail_by_day[day] = int(fail_by_day.get(day) or 0) + 1

    # 仅保留近7天的日记录，避免状态无限增长
    daily_tid_sent[day] = sorted(daily_for_today, key=lambda x: int(x) if str(x).isdigit() else str(x))
    try:
        keep_days = set((datetime.now(LOCAL_TZ) - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7))
        daily_tid_sent = {k: v for k, v in daily_tid_sent.items() if k in keep_days}
    except Exception:
        pass

    state['self_magic_recent'] = recent
    state['self_magic_recent_tid'] = recent_tid
    state['self_magic_daily_tid_sent'] = daily_tid_sent
    state['self_magic_last_ts'] = now
    if stat.get('daily_limit_tid'):
        daily_limit_total += int(stat.get('daily_limit_tid') or 0)
        daily_limit_by_day[day] = int(daily_limit_by_day.get(day) or 0) + int(stat.get('daily_limit_tid') or 0)

    state['self_magic_uc_total'] = uc_total
    state['self_magic_uc_by_day'] = uc_by_day
    state['self_magic_fail_total'] = fail_total
    state['self_magic_fail_by_day'] = fail_by_day
    state['self_magic_daily_limit_total'] = daily_limit_total
    state['self_magic_daily_limit_by_day'] = daily_limit_by_day
    msg = f"处理{len(candidates)}，成功{done}，失败{stat['submit_fail']}，冷却(hash/tid)={stat['cooldown_hash']}/{stat['cooldown_tid']}，日限跳过={stat['daily_limit_tid']}，查tid失败={stat['tid_lookup_fail']}"
    return {'ok': True, 'done': done, 'msg': msg, 'stat': stat}


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
            state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})

            # 每小时同步一次 TG 菜单命令（/upgrade）
            now_ts = int(time.time())
            last_menu_sync = int(state.get('tg_menu_sync_ts') or 0)
            if now_ts - last_menu_sync >= 3600:
                ok_menu, _ = tg_set_upgrade_menu(cfg)
                if ok_menu:
                    state['tg_menu_sync_ts'] = now_ts

            # 轮询 TG 菜单指令
            tg_poll_upgrade_commands(cfg, state)
            save_json(STATE_PATH, state)

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
            state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})

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
            now_ts = int(time.time())
            v2_fail_count = int(state.get('v2_fail_count') or 0)
            v2_cooldown_until = int(state.get('v2_cooldown_until') or 0)
            use_v2 = now_ts >= v2_cooldown_until
            if not use_v2:
                left = v2_cooldown_until - now_ts
                log(f'v2处于冷却中，剩余{left}s，直接使用v1')

            if use_v2:
                try:
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
                    if v2_fail_count > 0 or v2_cooldown_until > 0:
                        log('v2恢复可用，清空失败计数/冷却')
                    state['v2_fail_count'] = 0
                    state['v2_cooldown_until'] = 0
                except Exception as e_v2:
                    v2_fail_count += 1
                    state['v2_fail_count'] = v2_fail_count
                    # 连续3次失败，进入1小时冷却
                    if v2_fail_count >= 3:
                        state['v2_cooldown_until'] = now_ts + 3600
                        log(f'v2连续失败{v2_fail_count}次，进入3600s冷却')
                    uid = int(cfg.get('u2_uid') or 0)
                    if uid <= 0:
                        raise
                    r1 = requests.get(
                        'https://u2.kysdm.com/api/v1/promotion',
                        params={
                            'uid': uid,
                            'token': token,
                            'scope': cfg.get('scope', 'public'),
                            'maximum': int(cfg.get('limit', 20)),
                        },
                        timeout=30,
                    )
                    r1.raise_for_status()
                    p1 = r1.json()
                    data = ((p1.get('data') or {}).get('promotion') or [])
                    log(f'v2 promotions失败，已回退v1：{e_v2}')
            else:
                uid = int(cfg.get('u2_uid') or 0)
                if uid <= 0:
                    raise RuntimeError('v2冷却中且缺少u2_uid，无法回退v1')
                r1 = requests.get(
                    'https://u2.kysdm.com/api/v1/promotion',
                    params={
                        'uid': uid,
                        'token': token,
                        'scope': cfg.get('scope', 'public'),
                        'maximum': int(cfg.get('limit', 20)),
                    },
                    timeout=30,
                )
                r1.raise_for_status()
                p1 = r1.json()
                data = ((p1.get('data') or {}).get('promotion') or [])

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
            promo_snapshot = None
            promo_snapshot_err = None
            uid_self = int(cfg.get('u2_uid') or 0)
            for p in new_items:
                pid = p.get('promotion_id') or p.get('id')
                tid = p.get('torrent_id')
                # 私人魔法过滤：只接受公共魔法或明确发给自己的私人魔法
                for_uid_raw = p.get('for_user_id')
                try:
                    for_uid = int(for_uid_raw) if for_uid_raw not in (None, '', 0, '0') else 0
                except Exception:
                    for_uid = 0
                if for_uid and uid_self and for_uid != uid_self:
                    log(f'跳过非本人私人魔法：ID={pid}，TID={tid}，for_user_id={for_uid}')
                    continue

                dr = p.get('download_ratio')
                ur = p.get('upload_ratio')
                ratio_txt = str(p.get('ratio') or '').strip()
                if ratio_txt and '/' in ratio_txt:
                    try:
                        left, right = [x.strip() for x in ratio_txt.split('/', 1)]
                        ur = float(left)
                        dr = float(right)
                    except Exception:
                        pass

                seeders_raw = p.get('seeders')
                try:
                    seeders = int(seeders_raw) if seeders_raw not in (None, '', '?') else 0
                except Exception:
                    seeders = 0

                is_free = str(dr) in ('0', '0.0') or dr in (0, 0.0)
                is_2x = False
                try:
                    is_2x = float(ur) >= 2.0
                except Exception:
                    is_2x = False

                if cfg.get('require_2x_free', True):
                    if not (is_free and is_2x):
                        ratio_disp = ratio_txt if ratio_txt else (str(ur) + ' / ' + str(dr))
                        log(f'跳过非2XFree推广：ID={pid}，比例={ratio_disp}')
                        continue
                elif (not cfg.get('download_non_free', False)) and (not is_free):
                    ratio_disp = ratio_txt if ratio_txt else (str(ur) + ' / ' + str(dr))
                    log(f'跳过非Free推广：ID={pid}，比例={ratio_disp}')
                    continue

                max_seeders = int(cfg.get('max_seeders', 5) or 0)
                if max_seeders > 0 and seeders > max_seeders:
                    log(f'跳过推广：ID={pid}，做种人数={seeders} 超过阈值={max_seeders}')
                    continue

                extra = f'，做种人数={seeders}' if seeders_raw not in (None, '', '?') else ''
                ratio_disp = ratio_txt if ratio_txt else (str(ur) + ' / ' + str(dr))
                log(f'检测到新的推广：ID={pid}，线程号={tid}，比例={ratio_disp}{extra}')
                if cfg.get('tg_notify_new', True):
                    ok_tg, msg_tg = tg_notify(cfg, f'🆕 U2新优惠\nID: {pid}\nTID: {tid}\nDR: {dr}')
                    if not ok_tg:
                        log(f'TG通知未发送：{msg_tg}')

                if not tid:
                    log(f'跳过推送到 qB：推广号={pid} 缺少线程号')
                    continue
                # 二次校验：推送前按实时promotion快照再确认是否free，避免列表时点差异导致非free误下
                if (not cfg.get('require_2x_free', True)) and (not cfg.get('download_non_free', False)):
                    if promo_snapshot is None and promo_snapshot_err is None:
                        promo_snapshot, promo_snapshot_err = u2_promotion_snapshot(
                            cfg,
                            str(cfg.get('scope', 'public')),
                            max(100, int(cfg.get('limit', 20)) * 4),
                        )
                        if promo_snapshot_err:
                            log(f'二次校验快照失败：{promo_snapshot_err}，本轮按一次判定继续')
                    if promo_snapshot:
                        v = promo_snapshot.get(int(tid))
                        dr2 = None if not v else v.get('dr')
                        is_free_2nd = str(dr2) in ('0', '0.0') or dr2 in (0, 0.0)
                        if not is_free_2nd:
                            ratio2 = v.get('ratio') if v else 'N/A'
                            log(f'二次校验非Free，已跳过：ID={pid}，TID={tid}，ratio={ratio2}')
                            continue
                if not passkey:
                    log('跳过推送到 qB：u2_passkey 未配置')
                    continue

                targets = pick_qb_clients(cfg, state)
                if not targets:
                    log('跳过推送到 qB：没有启用的客户端')
                    continue

                healthy, bad = healthy_qb_clients(targets)
                if bad:
                    log(f'以下 qB 探测不可用（推送阶段仍会尝试重连）：{",".join(bad)}')

                torrent_url = f'https://u2.dmhy.org/download.php?id={tid}&passkey={passkey}&https=1'
                # 连通性可能瞬时抖动：即使探测失败，也对目标客户端做实际推送重试，避免“探测失败即丢单”。
                push_targets = healthy if healthy else targets
                any_ok = False
                for cli in push_targets:
                    cname = cli.get('name') or cli.get('qb_url') or 'unknown'
                    ok, msg = False, '未知错误'
                    for attempt in range(1, 4):
                        ok, msg = qb_add_torrent(cli, torrent_url, up_limit_bytes)
                        if ok:
                            any_ok = True
                            if attempt > 1:
                                log(f'已重试成功：qB[{cname}]，第{attempt}次')
                            break
                        if attempt < 3:
                            log(f'qB[{cname}] 推送失败，第{attempt}次重试中：{msg}')
                            time.sleep(attempt)
                    if ok:
                        log(f'已推送至 qB[{cname}]：推广号={pid}，线程号={tid}，上传限制={up_limit_bytes}B/s')
                    else:
                        log(f'推送到 qB[{cname}] 失败：推广号={pid}，线程号={tid}，原因={msg}')

                # 所有目标都失败：放入失败队列，下一轮可重推，避免瞬时网络抖动导致丢单。
                if not any_ok:
                    if not healthy:
                        log('本次 qB 探测均失败，已做直连重试仍未成功，已加入失败重推队列')
                    add_failed_push(state, {
                        'promotion_id': pid,
                        'torrent_id': tid,
                        'torrent_url': torrent_url,
                        'up_limit_bytes': up_limit_bytes,
                        'qb_name': ','.join((c.get('name') or c.get('qb_url') or 'unknown') for c in push_targets),
                        'time': now_iso(),
                    })

            # 自放魔法（可选，串行防重入）
            if SELF_MAGIC_LOCK.acquire(blocking=False):
                try:
                    auto_self_magic_once(cfg, state)
                finally:
                    SELF_MAGIC_LOCK.release()
            else:
                log('自放魔法：已有任务执行中，跳过本轮')

            # 失败推送自动重试：每5分钟触发一次
            now_ts2 = int(time.time())
            last_retry_ts = int(state.get('failed_retry_last_ts') or 0)
            retry_interval = max(60, int(cfg.get('failed_retry_interval_seconds', 300) or 300))
            if now_ts2 - last_retry_ts >= retry_interval:
                rr = retry_failed_pushes_once(cfg, state)
                state['failed_retry_last_ts'] = now_ts2
                if rr.get('ok') and (int(rr.get('retried') or 0) > 0 or int(rr.get('expired') or 0) > 0):
                    log(
                        f"失败重推(自动{retry_interval//60}分钟)："
                        f"重试={rr.get('retried', 0)}，成功={rr.get('success', 0)}，"
                        f"过期丢弃={rr.get('expired', 0)}，剩余={rr.get('remain', 0)}"
                    )
                elif not rr.get('ok'):
                    log(f"失败重推(自动{retry_interval//60}分钟)跳过：{rr.get('error')}")

            state['last_seen'] = [str((p.get('promotion_id') or p.get('id'))) for p in data if (p.get('promotion_id') or p.get('id'))][:200]
            state['last_run'] = now_iso()
            state['last_error'] = None
            save_json(STATE_PATH, state)
            log(f'运行完成：已获取={len(data)}，新增={len(new_items)}')
        except Exception as e:
            state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
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
app.add_middleware(GZipMiddleware, minimum_size=1024)
if STATIC_DIR.exists():
    app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
runner = Runner()


@app.middleware('http')
async def auth_middleware(request: Request, call_next):
    path = request.url.path or '/'
    allow = (
        path.startswith('/static/')
        or path in ('/login', '/auth/login', '/api/version')
    )

    _cleanup_auth_sessions()
    cfg = load_config()
    if _auth_enabled(cfg) and not allow:
        sid = request.cookies.get(AUTH_COOKIE_NAME, '')
        if not _valid_session(sid):
            if path.startswith('/api/'):
                return JSONResponse({'ok': False, 'error': '未登录或登录已过期'}, status_code=401)
            return RedirectResponse(url='/login', status_code=302)

    resp = await call_next(request)
    return resp


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
    require_2x_free: bool = True
    auto_self_magic_enabled: bool = False
    auto_self_magic_min_upload_kib: int = 1024
    auto_self_magic_min_size_gb: int = 5
    auto_self_magic_max_size_gb: int = 0
    auto_self_magic_hours: int = 24
    auto_self_magic_interval: int = 60
    auto_self_magic_magic_downloading: bool = True
    auto_self_magic_min_d: int = 180
    u2_uid: int = 0
    u2_cookie: str = ''
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
        save_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
    if not UPGRADE_STATUS_PATH.exists():
        save_json(UPGRADE_STATUS_PATH, {'state': 'idle', 'updated_at': now_iso(), 'message': '等待升级请求'})
    runner.start()


@app.on_event('shutdown')
def shutdown_event():
    runner.stop()


def _human_speed(n: int):
    n = max(0, int(n or 0))
    units = ['B/s', 'KB/s', 'MB/s', 'GB/s', 'TB/s']
    v = float(n)
    i = 0
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    if i <= 1:
        return f"{v:.0f} {units[i]}"
    return f"{v:.2f} {units[i]}"


def _status_summary_html():
    cfg = load_config()
    st = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
    err = summarize_error_cn(st.get('last_error') or '') if st.get('last_error') else '无'
    run_state = '执行中' if runner._running else '空闲'
    return f"""
    <div class='grid grid-3'>
      <div class='card'><div class='k'>最后执行</div><div class='v'>{escape(str(st.get('last_run') or '-'))}</div></div>
      <div class='card'><div class='k'>运行状态</div><div class='v'>{run_state}</div></div>
      <div class='card'><div class='k'>最近报错</div><div class='v'>{escape(err if cfg.get('enabled') else '已暂停（未运行）')}</div></div>
    </div>
    """


def _qb_modules_html():
    cfg = load_config()
    clients = cfg.get('qb_clients') or []
    if not clients:
        return "<div class='card'>暂无 qB 配置</div>"
    stats = [None] * len(clients)
    with ThreadPoolExecutor(max_workers=min(8, len(clients))) as ex:
        fm = {ex.submit(qb_fetch_stats, c, 6): i for i, c in enumerate(clients)}
        for fut in as_completed(fm):
            i = fm[fut]
            try:
                stats[i] = fut.result()
            except Exception as e:
                name = clients[i].get('name') or clients[i].get('qb_url') or 'unknown'
                stats[i] = {'name': name, 'ok': False, 'error': str(e)}
    arr = []
    for it in stats:
        ok = bool(it.get('ok'))
        status = '在线' if ok else '离线'
        qb_url = str(it.get('qb_url') or '').strip()
        open_attr = f" onclick=\"window.open('{escape(qb_url)}','_blank','noopener')\" style='cursor:pointer'" if qb_url else ''
        arr.append(f"""
        <div class='card card-link'{open_attr}>
          <div class='row'><b>{escape(str(it.get('name') or 'unknown'))}</b><span class='{ 'ok' if ok else 'err' }'>{status}</span></div>
          <div class='k'>任务 {it.get('task_count','-')} · ↓ {_human_speed(it.get('dl_speed',0))} · ↑ {_human_speed(it.get('up_speed',0))}</div>
          <div class='k'>总下载 {_human_speed(it.get('dl_total',0)).replace('/s','')} · 总上传 {_human_speed(it.get('up_total',0)).replace('/s','')}</div>
        </div>
        """)
    return "<div class='grid grid-2'>" + ''.join(arr) + "</div>"


@app.get('/lite', response_class=HTMLResponse)
def lite(request: Request):
    return templates.TemplateResponse('lite.html', {'request': request, 'version': APP_VERSION})


@app.get('/partials/status', response_class=HTMLResponse)
def partial_status():
    return _status_summary_html()


@app.get('/partials/qb', response_class=HTMLResponse)
def partial_qb():
    return _qb_modules_html()


@app.get('/partials/logs', response_class=HTMLResponse)
def partial_logs(limit: int = Query(120, ge=20, le=300)):
    return '<pre class="log">' + escape('\n'.join(tail_lines(APP_LOG, limit))) + '</pre>'


@app.get('/', response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse('lite.html', {'request': request, 'version': APP_VERSION})


@app.get('/login', response_class=HTMLResponse)
def login_page(request: Request):
    cfg = load_config()
    if not _auth_enabled(cfg):
        return RedirectResponse(url='/', status_code=302)
    html = f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <title>登录 - Catch Magic</title>
  <style>
    body{{font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}
    .card{{width:min(420px,92vw);background:#111827;border:1px solid #334155;border-radius:12px;padding:18px}}
    input{{width:100%;padding:10px;border-radius:8px;border:1px solid #475569;background:#0b1220;color:#fff}}
    button{{margin-top:10px;width:100%;padding:10px;border:0;border-radius:8px;background:#2563eb;color:#fff;font-weight:600;cursor:pointer}}
    .tip{{font-size:12px;color:#94a3b8;margin-top:8px}}
  </style>
</head>
<body>
  <div class='card'>
    <h3 style='margin-top:0'>访问密码验证</h3>
    <form method='post' action='/auth/login'>
      <input type='password' name='password' placeholder='请输入访问密码' required />
      <button type='submit'>登录</button>
    </form>
    <div class='tip'>已启用访问保护。连续输错将临时锁定，防止暴力破解。</div>
  </div>
</body>
</html>
"""
    return HTMLResponse(content=html)


@app.post('/auth/login')
async def auth_login(request: Request):
    cfg = load_config()
    if not _auth_enabled(cfg):
        return {'ok': True, 'message': '未启用访问密码'}

    ip = _ip_of(request)
    locked, wait_sec = _is_locked(ip)
    if locked:
        return JSONResponse({'ok': False, 'error': f'尝试过多，请 {wait_sec}s 后再试'}, status_code=429)

    form = await request.form()
    password = str(form.get('password') or '').strip()
    ok = _verify_password(password, str(cfg.get('web_password_hash') or ''))
    if not ok:
        _register_login_fail(ip)
        time.sleep(AUTH_FAIL_DELAY_SEC)
        return JSONResponse({'ok': False, 'error': '密码错误'}, status_code=401)

    _register_login_success(ip)
    sid = _new_auth_session(ip)
    resp = RedirectResponse(url='/', status_code=302)
    resp.set_cookie(
        AUTH_COOKIE_NAME,
        sid,
        max_age=AUTH_SESSION_TTL_SEC,
        httponly=True,
        secure=False,
        samesite='lax',
        path='/',
    )
    return resp


@app.post('/auth/logout')
def auth_logout(request: Request):
    sid = request.cookies.get(AUTH_COOKIE_NAME, '')
    if sid:
        AUTH_SESSIONS.pop(sid, None)
    resp = JSONResponse({'ok': True})
    resp.delete_cookie(AUTH_COOKIE_NAME, path='/')
    return resp


@app.post('/api/security/password')
def set_web_password(payload: dict):
    raw = load_config()
    new_password = str(payload.get('password') or '').strip()

    if not new_password:
        return JSONResponse({'ok': False, 'error': '请输入新密码'}, status_code=400)
    if len(new_password) < 6:
        return JSONResponse({'ok': False, 'error': '密码至少6位'}, status_code=400)

    raw['web_password_hash'] = _hash_password(new_password)
    raw['web_auth_enabled'] = True
    save_json(CONFIG_PATH, raw)
    log('访问密码已设置/更新')
    return {'ok': True, 'enabled': True}


@app.post('/api/security/password/delete')
def delete_web_password(payload: dict = None):
    raw = load_config()
    current_password = str((payload or {}).get('current_password') or '').strip()
    stored = str(raw.get('web_password_hash') or '')

    if not stored:
        return JSONResponse({'ok': False, 'error': '当前未设置访问密码'}, status_code=400)
    if not current_password:
        return JSONResponse({'ok': False, 'error': '请输入当前密码'}, status_code=400)
    if not _verify_password(current_password, stored):
        return JSONResponse({'ok': False, 'error': '当前密码错误，无法删除'}, status_code=401)

    raw['web_password_hash'] = ''
    raw['web_auth_enabled'] = False
    save_json(CONFIG_PATH, raw)
    log('访问密码已删除，恢复免密访问（已校验当前密码）')
    return {'ok': True}


@app.get('/_legacy_hidden', response_class=HTMLResponse)
def legacy_index():
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
    .actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center} .title .actions{justify-content:flex-end} button{border:0;border-radius:10px;padding:10px 14px;color:white;background:var(--pri);cursor:pointer;font-weight:600}
    button:hover{background:var(--pri2)} button.ghost{background:transparent;border:1px solid var(--line);color:var(--text)} .actions .compact{padding:8px 10px;font-size:12px;line-height:1.1;flex:0 0 auto} button.danger{background:#b4232a}
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
    <div class='title'><h2>Catch Magic Web <span style='font-size:13px;color:var(--sub);font-weight:500'>v__APP_VERSION__</span></h2><div class='actions'><button class='ghost' type='button' onclick="openConfigPage('base')">基础配置</button><button class='ghost' type='button' onclick="openConfigPage('tg')">TG配置</button><button class='ghost compact' type='button' onclick="openConfigPage('magic');return false;">魔法配置</button><button class='ghost compact' type='button' onclick="openConfigPage('qb')">qB配置</button><button class='ghost compact' type='button' onclick="openConfigPage('security')">访问密码</button><button class='ghost compact' type='button' onclick="openConfigPage('theme')">主题</button><button class='ghost compact' type='button' onclick='runSelfMagicOnce()'>手动魔法</button><button class='ghost compact' type='button' onclick='retryFailedPushes()'>失败重推</button><div class='badge' id='runBadge'>状态读取中...</div></div></div>
    <div id='app' class='grid'>loading...</div>
  </div>
<script>
let qbClients=[];
let qbStatsTimer=null;
let editingQbIndex=null;
let logCursor=0;

async function j(u,o){const r=await fetch(u,o);return await r.json()}
function esc(t){return (t??'').toString().replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function fmtBytes(n){ n=Number(n||0); const u=['B','KB','MB','GB','TB']; let i=0; while(n>=1024&&i<u.length-1){n/=1024;i++;} return `${n.toFixed(i<=1?0:2)} ${u[i]}`; }
function fmtSpeed(n){ return `${fmtBytes(n)}/s`; }
function statusBadge(s, enabled){ if(!enabled) return `<span class='dot warn'></span>已暂停`; if(s.running) return `<span class='dot warn'></span>执行中`; if(s.last_error) return `<span class='dot err'></span>异常`; return `<span class='dot ok'></span>正常`; }
function getTheme(){ return localStorage.getItem('cm_theme')||'dark'; }
function applyTheme(){ const t=getTheme(); document.body.classList.toggle('theme-light', t==='light'); }
function toggleTheme(){ localStorage.setItem('cm_theme', getTheme()==='light'?'dark':'light'); applyTheme(); }




function openConfigPage(kind){
  const target = `/config/${kind}`;
  const win = window.open(target, '_blank', 'noopener,noreferrer');
  if(!win) alert('新窗口被浏览器拦截，请允许当前站点弹出窗口后重试');
}
function openQb(i){
  const q=qbClients[i]||{};
  if(q.qb_url) window.open(q.qb_url,'_blank');
}

function renderQbModules(items=[]){
  const el=document.getElementById('qbModules'); if(!el) return;
  if(!qbClients.length){ el.innerHTML = `<div class='tip'>暂无 qB 模块

</div>`; return; }
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
          <button onclick="openConfigPage('qb')">配置</button>
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
 const [c, s] = await Promise.all([j('/api/config'), j('/api/status')]);
 qbClients = JSON.parse(JSON.stringify(c.qb_clients || []));
 document.getElementById('runBadge').innerHTML=statusBadge(s, c.enabled);
 document.getElementById('app').innerHTML=`
 <div class='card'><div class='status'>
   <div><div class='k'>最后执行</div><div class='v'>${esc(s.last_run||'-')}</div></div>
   <div><div class='k'>运行状态</div><div class='v'>${s.running?'执行中':'空闲'}</div></div>
   <div><div class='k'>最近报错</div><div class='v'>${c.enabled ? esc(s.last_error_cn||'无') : '已暂停（未运行）'}</div></div>
 </div></div>
 <div class='card'>
   <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>
     <div class='k'>qB 模块看板（自动刷新）</div>
     <button type='button' onclick='addQb()'>+ 添加QB配置</button>
   </div>
   <div id='qbModules' class='modules'><div class='tip'>加载中...</div></div>
 </div>

 <div class='card'><div class='k' style='margin-bottom:8px'>最近日志（最多 200 行）</div><pre id='logs'>loading logs...</pre></div>
</div>`;
 await refreshQbStats();
 if(qbStatsTimer) clearInterval(qbStatsTimer);
 qbStatsTimer = setInterval(refreshQbStats, 8000);
 logCursor = 0;
 setTimeout(()=>refreshLogs(true), 0);
}
async function testTG(){ const r=await fetch('/api/tg/test',{method:'POST'}); const d=await r.json(); alert(d.ok?('测试通知已发送：'+(d.message||'')):('测试通知失败：'+(d.error||'unknown'))); }
async function runNow(){ await fetch('/api/run',{method:'POST'}); setTimeout(load, 900); }
async function retryFailedPushes(){ const r=await fetch('/api/retry_failed',{method:'POST'}); const d=await r.json(); if(!d.ok){ alert('重推失败：'+(d.error||'未知错误')); return;} alert(`重推完成：共${d.retried||0}条，成功${d.success||0}条，剩余${d.remain||0}条`); setTimeout(load,500); }
async function runSelfMagicOnce(){ const r=await fetch('/api/self_magic/once',{method:'POST'}); const d=await r.json(); if(!d.ok){ alert('手动自放失败：'+(d.error||'未知错误')); return;} alert(`手动自放完成：${d.msg||('成功'+(d.done||0)+'条')}`); setTimeout(load,500); }
async function refreshLogs(force=false){
  try{
    const node=document.getElementById('logs');
    const ctl=new AbortController();
    const tm=setTimeout(()=>ctl.abort(), 6000);
    const cursor = force ? 0 : logCursor;
    const d=await fetch(`/api/logs?cursor=${cursor}&limit=200`,{signal:ctl.signal}).then(r=>r.json());
    clearTimeout(tm);
    if(!node) return;
    if(force || d.reset){
      node.textContent=(d.text||'(暂无日志)');
    }else if(d.text){
      node.textContent=d.text + (node.textContent?('\n'+node.textContent):'');
    }
    logCursor = Number(d.cursor||logCursor||0);
    if(!node.textContent) node.textContent='(暂无日志)';
  }catch(e){
    const node=document.getElementById('logs');
    if(node) node.textContent='日志加载超时，请稍后点“刷新日志”重试';
  }
}
load();
</script>
</body></html>
""".replace('__APP_VERSION__', APP_VERSION)


CONFIG_PAGE_HTML = """
<!doctype html>
<html lang='zh-CN'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width,initial-scale=1' />
  <title>Catch Magic 配置</title>
  <style>
    :root{color-scheme:dark;background:#0b1220;color:#e5e7eb;--bg:#0b1220;--card:#111827;--line:#243041;--text:#e5e7eb;--sub:#94a3b8;--btn:#2563eb;--btn2:#1d4ed8;--ghost:#172033;--danger:#dc2626;--ok:#16a34a;--warn:#f59e0b}
    body.theme-light{color-scheme:light;--bg:#f6f8fb;--card:#ffffff;--line:#dbe3ef;--text:#0f172a;--sub:#475569;--btn:#2563eb;--btn2:#1d4ed8;--ghost:#eef3ff;--danger:#dc2626;--ok:#16a34a;--warn:#d97706}
    *{box-sizing:border-box} html,body{margin:0;background:var(--bg);color:var(--text);font:14px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,'PingFang SC','Noto Sans CJK SC','Microsoft YaHei',sans-serif}
    .wrap{max-width:980px;margin:0 auto;padding:18px}
    .head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;flex-wrap:wrap}
    .title h2{margin:0;font-size:22px}
    .sub{color:var(--sub);font-size:13px}
    .card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;box-shadow:0 10px 28px rgba(0,0,0,.18)}
    .form,.editor-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
    .full{grid-column:1 / -1}
    label{display:block;margin-bottom:6px;color:var(--sub);font-size:13px}
    input,select,button,textarea{width:100%;padding:11px 12px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--text);outline:none}
    textarea{min-height:140px;resize:vertical}
    button{background:var(--btn);border:none;color:#fff;font-weight:600;cursor:pointer}
    button:hover{background:var(--btn2)}
    button.ghost{background:var(--ghost);color:var(--text);border:1px solid var(--line)}
    button.danger{background:var(--danger)}
    .actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
    .actions button{width:auto;min-width:140px}
    .tip{color:var(--sub);font-size:13px;margin-top:12px}
    .switch{display:flex;align-items:center;gap:8px;padding-top:28px}
    .switch input{width:18px;height:18px;margin:0}
    .switch label{margin:0;color:var(--text)}
    .qb-item{border:1px solid var(--line);border-radius:12px;padding:14px;margin-bottom:12px;background:rgba(255,255,255,.02)}
    .qb-title{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:10px}
    .qb-title strong{font-size:15px}
    @media (max-width:720px){.form,.editor-grid{grid-template-columns:1fr}.actions button{width:100%}}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='head'>
      <div class='title'>
        <h2 id='pageTitle'>配置页</h2>
        <div class='sub' id='pageSub'>Catch Magic Web v__APP_VERSION__</div>
      </div>
      <div style='display:flex;gap:8px;flex-wrap:wrap'>
        <button class='ghost' type='button' onclick='window.close()'>关闭窗口</button>

      </div>
    </div>
    <div id='app' class='card'>加载中...</div>
  </div>
<script>
const PAGE_KIND='__PAGE_KIND__';
let qbClients=[];
function getTheme(){ return localStorage.getItem('cm_theme')||'dark'; }
function applyTheme(){ const t=getTheme(); document.body.classList.toggle('theme-light', t==='light'); }
function toggleTheme(){ localStorage.setItem('cm_theme', getTheme()==='light'?'dark':'light'); applyTheme(); }
async function j(u,o){ const r=await fetch(u,o); return await r.json(); }
function esc(t){return (t??'').toString().replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
function collectTG(){
  return {
    tg_enabled: document.getElementById('tg_enabled')?.checked||false,
    tg_bot_token: (document.getElementById('tg_bot_token')?.value||'').trim(),
    tg_chat_id: (document.getElementById('tg_chat_id')?.value||'').trim(),
    tg_notify_new: document.getElementById('tg_notify_new')?.checked!==false,
    tg_notify_error: document.getElementById('tg_notify_error')?.checked!==false,
  }
}
function renderBase(c){
  document.getElementById('pageTitle').textContent='基础配置';
  return `
  <div class='form'>
    <input style='display:none' type='text' name='fake_username' autocomplete='username'>
    <input style='display:none' type='password' name='fake_password' autocomplete='current-password'>
    <div class='full switch'><input id='enabled' type='checkbox' ${c.enabled?'checked':''}><label for='enabled'>启用定时任务</label></div>
    <div><label>执行间隔（秒）</label><input id='interval' type='number' min='10' value='${c.interval}'></div>
    <div><label>抓取条数（limit）</label><input id='limit' type='number' min='1' max='60' value='${c.limit}'></div>
    <div><label>最大做种人数</label><input id='max_seeders' type='number' min='0' value='${c.max_seeders??5}'></div>
    <div><label>qB 分发模式</label><select id='qb_mode'><option value='round_robin' ${c.qb_mode==='round_robin'?'selected':''}>轮询分发</option><option value='all' ${c.qb_mode==='all'?'selected':''}>全部推送</option></select></div>
    <div><label>单种上传限速(MB/s)</label><input id='qb_up_limit_mb' type='number' min='0' value='${c.qb_up_limit_mb??50}'></div>
    <div class='full'><label>U2 API Base</label><input id='u2_api_base' value='${esc(c.u2_api_base)}'></div>
    <div class='full'><label>U2 API Token</label><input id='u2_api_token' type='password' autocomplete='new-password' value='${esc(c.u2_api_token||'')}'></div>
    <div class='full'><label>U2 Passkey</label><input id='u2_passkey' type='password' autocomplete='new-password' value='${esc(c.u2_passkey||'')}'></div>
    <div><label>魔法范围（scope）</label><select id='scope'><option value='public' ${c.scope==='public'?'selected':''}>公共魔法（public）</option><option value='all' ${c.scope==='all'?'selected':''}>全部魔法（all）</option><option value='private' ${c.scope==='private'?'selected':''}>私人魔法（private）</option><option value='global' ${c.scope==='global'?'selected':''}>全局魔法（global）</option></select></div>
    <div class='switch'><input id='require_2x_free' type='checkbox' ${c.require_2x_free!==false?'checked':''}><label for='require_2x_free'>仅抓取 2XFree</label></div>
  </div>
  <div class='actions'><button onclick='saveBase()'>保存基础配置</button><button class='ghost' onclick='runNow()'>立即执行一次</button></div>
  <div class='tip'>该页面只处理基础配置，不再占用主看板页面。</div>`;
}
function renderTG(c){
  document.getElementById('pageTitle').textContent='TG 配置';
  return `
  <div class='editor-grid'>
    <div class='switch full'><input id='tg_enabled' type='checkbox' ${c.tg_enabled?'checked':''}><label for='tg_enabled'>启用TG通知</label></div>
    <div class='full'><label>Bot Token</label><input id='tg_bot_token' type='password' autocomplete='new-password' value='${esc(c.tg_bot_token||'')}' placeholder='123456:ABC...'></div>
    <div class='full'><label>Chat ID</label><input id='tg_chat_id' value='${esc(c.tg_chat_id||'')}' placeholder='例如 1036463619'></div>
    <div class='switch'><input id='tg_notify_new' type='checkbox' ${c.tg_notify_new!==false?'checked':''}><label for='tg_notify_new'>新推广通知</label></div>
    <div class='switch'><input id='tg_notify_error' type='checkbox' ${c.tg_notify_error!==false?'checked':''}><label for='tg_notify_error'>失败告警通知</label></div>
  </div>
  <div class='actions'><button onclick='saveTG()'>保存TG配置</button><button class='ghost' onclick='testTG()'>测试通知</button></div>`;
}
function renderMagic(c){
  document.getElementById('pageTitle').textContent='魔法配置';
  return `
  <div class='editor-grid'>
    <div class='switch full'><input id='auto_self_magic_enabled' type='checkbox' ${c.auto_self_magic_enabled?'checked':''}><label for='auto_self_magic_enabled'>启用自动给自己放2.33x魔法</label></div>
    <div><label>检查间隔(秒)</label><input id='auto_self_magic_interval' type='number' min='10' value='${c.auto_self_magic_interval??60}'></div>
    <div class='switch'><input id='auto_self_magic_magic_downloading' type='checkbox' ${c.auto_self_magic_magic_downloading!==false?'checked':''}><label for='auto_self_magic_magic_downloading'>包含下载中的种子</label></div>
    <div><label>最小上传速度(KiB/s)</label><input id='auto_self_magic_min_upload_kib' type='number' min='1' value='${c.auto_self_magic_min_upload_kib??1024}'></div>
    <div><label>最小体积(GB)</label><input id='auto_self_magic_min_size_gb' type='number' min='1' value='${c.auto_self_magic_min_size_gb??5}'></div>
    <div><label>最大体积(GB，0=不限制)</label><input id='auto_self_magic_max_size_gb' type='number' min='0' value='${c.auto_self_magic_max_size_gb??0}'></div>
    <div><label>种子最小生存天数</label><input id='auto_self_magic_min_d' type='number' min='0' value='${c.auto_self_magic_min_d??180}'></div>
    <div><label>魔法时长(小时)</label><input id='auto_self_magic_hours' type='number' min='1' max='360' value='${c.auto_self_magic_hours??24}'></div>
    <div><label>U2 UID</label><input id='u2_uid' type='number' min='0' value='${c.u2_uid??0}'></div>
    <div class='full'><label>U2 Cookie(nexusphp_u2)</label><input id='u2_cookie' type='password' autocomplete='new-password' value='${esc(c.u2_cookie||'')}'></div>
  </div>
  <div class='actions'><button onclick='saveMagic()'>保存自放魔法配置</button><button class='ghost' onclick='runSelfMagicOnce()'>立即手动执行一次</button></div>`;
}
function renderQB(c){
  document.getElementById('pageTitle').textContent='qB 配置';
  const items=(c.qb_clients||[]).map((q,i)=>`
    <div class='qb-item'>
      <div class='qb-title'><strong>${esc(q.name||('qb-'+(i+1)))}</strong><button class='danger' type='button' onclick='removeQb(${i})'>删除</button></div>
      <div class='editor-grid'>
        <div><label>名称</label><input data-key='name' data-idx='${i}' value='${esc(q.name||'')}'></div>
        <div class='switch'><input id='qb_enabled_${i}' data-key='enabled' data-idx='${i}' type='checkbox' ${q.enabled!==false?'checked':''}><label for='qb_enabled_${i}'>启用</label></div>
        <div><label>URL</label><input data-key='qb_url' data-idx='${i}' value='${esc(q.qb_url||'')}' placeholder='http://127.0.0.1:8080'></div>
        <div><label>用户名</label><input data-key='qb_username' data-idx='${i}' value='${esc(q.qb_username||'')}'></div>
        <div><label>密码</label><input data-key='qb_password' data-idx='${i}' type='password' value='${esc(q.qb_password||'')}'></div>
        <div><label>分类</label><input data-key='qb_category' data-idx='${i}' value='${esc(q.qb_category||'')}'></div>
        <div class='full'><label>保存路径</label><input data-key='qb_savepath' data-idx='${i}' value='${esc(q.qb_savepath||'')}' placeholder='/downloads/u2'></div>
        <div class='switch full'><input id='qb_paused_${i}' data-key='qb_paused' data-idx='${i}' type='checkbox' ${q.qb_paused?'checked':''}><label for='qb_paused_${i}'>推送后暂停</label></div>
      </div>
    </div>`).join('');
  return `${items || "<div class='tip'>暂无 qB 配置，先新增一个。</div>"}
  <div class='actions'><button onclick='addQb()'>+ 添加QB配置</button><button onclick='saveQB()'>保存qB配置</button></div>`;
}
function syncQBFromForm(){
  document.querySelectorAll('[data-idx]').forEach(el=>{
    const idx=Number(el.dataset.idx||0); const key=el.dataset.key;
    if(!qbClients[idx]) return;
    qbClients[idx][key]=el.type==='checkbox'?el.checked:el.value.trim();
  });
}
function addQb(){ qbClients.push({name:`qb-${qbClients.length+1}`,enabled:true,qb_url:'http://127.0.0.1:8080',qb_username:'',qb_password:'',qb_category:'',qb_savepath:'',qb_paused:false}); render(); }
function removeQb(i){ qbClients.splice(i,1); render(); }
async function saveBase(){ const c=await j('/api/config'); const body={...c,enabled:document.getElementById('enabled').checked,interval:parseInt(document.getElementById('interval').value||'120'),u2_api_base:document.getElementById('u2_api_base').value.trim(),u2_api_token:document.getElementById('u2_api_token').value.trim(),u2_passkey:document.getElementById('u2_passkey').value.trim(),scope:document.getElementById('scope').value,limit:parseInt(document.getElementById('limit').value||'20'),max_seeders:parseInt(document.getElementById('max_seeders').value||'5'),download_non_free:false,qb_mode:document.getElementById('qb_mode').value,qb_up_limit_mb:parseInt(document.getElementById('qb_up_limit_mb').value||'50'),require_2x_free:document.getElementById('require_2x_free').checked!==false,qb_clients:qbClients,...collectTG()}; await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); alert('基础配置已保存'); }
async function saveTG(){ const c=await j('/api/config'); const body={...c,...collectTG(),qb_clients:qbClients}; await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); alert('TG配置已保存'); }
async function saveMagic(){ const c=await j('/api/config'); const body={...c,auto_self_magic_enabled:document.getElementById('auto_self_magic_enabled').checked||false,auto_self_magic_min_upload_kib:parseInt(document.getElementById('auto_self_magic_min_upload_kib').value||'1024'),auto_self_magic_min_size_gb:parseInt(document.getElementById('auto_self_magic_min_size_gb').value||'5'),auto_self_magic_max_size_gb:parseInt(document.getElementById('auto_self_magic_max_size_gb').value||'0'),auto_self_magic_hours:parseInt(document.getElementById('auto_self_magic_hours').value||'24'),auto_self_magic_interval:parseInt(document.getElementById('auto_self_magic_interval').value||'60'),auto_self_magic_magic_downloading:document.getElementById('auto_self_magic_magic_downloading').checked!==false,auto_self_magic_min_d:parseInt(document.getElementById('auto_self_magic_min_d').value||'180'),u2_uid:parseInt(document.getElementById('u2_uid').value||'0'),u2_cookie:(document.getElementById('u2_cookie').value||'').trim(),qb_clients:qbClients,...collectTG()}; await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); alert('自放魔法配置已保存'); }
async function saveQB(){ const c=await j('/api/config'); syncQBFromForm(); const body={...c,qb_clients:qbClients,...collectTG()}; await fetch('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); alert('qB配置已保存'); }
async function runNow(){ await fetch('/api/run',{method:'POST'}); alert('已触发立即执行'); }
async function testTG(){ const r=await fetch('/api/tg/test',{method:'POST'}); const d=await r.json(); alert(d.ok?('测试通知已发送：'+(d.message||'')):('测试通知失败：'+(d.error||'unknown'))); }
async function runSelfMagicOnce(){ const r=await fetch('/api/self_magic/once',{method:'POST'}); const d=await r.json(); if(!d.ok){ alert('手动自放失败：'+(d.error||'未知错误')); return;} alert(`手动自放完成：${d.msg||('成功'+(d.done||0)+'条')}`); }
function renderSecurity(c){
  document.getElementById('pageTitle').textContent='访问密码';
  return `
  <div class='editor-grid'>
    <div class='full'><label>新密码</label><input id='security_password' type='password' autocomplete='new-password' placeholder='至少 6 位'></div>
    <div class='full'><label>删除密码时输入当前密码</label><input id='security_current_password' type='password' autocomplete='current-password' placeholder='仅删除时需要'></div>
    <div class='full tip'>当前状态：${c.web_auth_enabled ? '已启用访问密码' : '未启用访问密码'}</div>
  </div>
  <div class='actions'><button onclick='saveSecurityPassword()'>设置/更新访问密码</button><button class='ghost' onclick='deleteSecurityPassword()'>删除访问密码</button></div>`;
}
function renderTheme(){
  document.getElementById('pageTitle').textContent='主题';
  const current = getTheme()==='light' ? '浅色' : '深色';
  return `
  <div class='tip'>当前主题：${current}</div>
  <div class='actions'><button onclick="setThemeMode('dark')">切换深色主题</button><button class='ghost' onclick="setThemeMode('light')">切换浅色主题</button></div>`;
}
function setThemeMode(mode){ localStorage.setItem('cm_theme', mode); applyTheme(); alert('主题已切换'); }
async function saveSecurityPassword(){ const password=(document.getElementById('security_password')?.value||'').trim(); const r=await fetch('/api/security/password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password})}); const d=await r.json(); if(!d.ok){ alert('设置失败：'+(d.error||'未知错误')); return;} alert('访问密码已设置'); }
async function deleteSecurityPassword(){ const current_password=(document.getElementById('security_current_password')?.value||'').trim(); const r=await fetch('/api/security/password/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password})}); const d=await r.json(); if(!d.ok){ alert('删除失败：'+(d.error||'未知错误')); return;} alert('访问密码已删除'); }
async function render(){ applyTheme(); const c=await j('/api/config'); qbClients=JSON.parse(JSON.stringify(c.qb_clients||[])); const app=document.getElementById('app'); if(PAGE_KIND==='base') app.innerHTML=renderBase(c); else if(PAGE_KIND==='tg') app.innerHTML=renderTG(c); else if(PAGE_KIND==='magic') app.innerHTML=renderMagic(c); else if(PAGE_KIND==='qb') app.innerHTML=renderQB(c); else if(PAGE_KIND==='security') app.innerHTML=renderSecurity(c); else if(PAGE_KIND==='theme') app.innerHTML=renderTheme(); else app.innerHTML="<div class='tip'>未知配置页面</div>"; }
render();
</script>
</body>
</html>
"""


@app.get('/api/version')
def get_version():
    return {'version': APP_VERSION}


@app.get('/config/{kind}', response_class=HTMLResponse)
def config_page(kind: str):
    mapping = {
        'base': '基础配置',
        'tg': 'TG配置',
        'magic': '魔法配置',
        'qb': 'qB配置',
        'security': '访问密码',
        'theme': '主题',
    }
    if kind not in mapping:
        return HTMLResponse('Not Found', status_code=404)
    return HTMLResponse(CONFIG_PAGE_HTML.replace('__APP_VERSION__', APP_VERSION).replace('__PAGE_KIND__', kind))


@app.get('/api/config')
def get_config():
    c = load_config()
    raw_hash = str(c.get('web_password_hash') or '')
    enabled = bool(raw_hash.strip())
    c['web_password_hash'] = ''
    c['web_auth_ready'] = enabled
    c['web_auth_enabled'] = enabled
    return c


@app.put('/api/config')
def put_config(cfg: ConfigIn):
    old = load_config()
    data = cfg.dict()
    data['interval'] = max(10, int(data['interval']))
    data['qb_up_limit_mb'] = max(0, int(data.get('qb_up_limit_mb', 50)))
    data['auto_self_magic_max_size_gb'] = max(0, int(data.get('auto_self_magic_max_size_gb', 0)))
    if not data.get('qb_clients'):
        data['qb_clients'] = []

    # 保留安全配置，避免被普通配置覆盖丢失
    data['web_auth_enabled'] = bool(old.get('web_auth_enabled', False))
    data['web_password_hash'] = str(old.get('web_password_hash') or '')

    save_json(CONFIG_PATH, data)
    # TG开启后立即尝试同步菜单命令
    if data.get('tg_enabled'):
        tg_set_upgrade_menu(data)
    log('配置已更新')
    return {'ok': True}


@app.get('/api/status')
def status():
    st = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
    st['running'] = runner._running
    st['last_error_cn'] = summarize_error_cn(st.get('last_error') or '') if st.get('last_error') else ''
    st['failed_push_count'] = len(st.get('failed_pushes') or [])
    return st


@app.post('/api/run')
def run_now():
    threading.Thread(target=runner.run_once, daemon=True).start()
    return {'ok': True}


@app.get('/api/qb/stats')
def qb_stats():
    cfg = load_config()
    clients = cfg.get('qb_clients') or []
    if not clients:
        return {'items': [], 'mode': cfg.get('qb_mode', 'round_robin')}

    stats = [None] * len(clients)
    max_workers = min(8, len(clients))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(qb_fetch_stats, c, 8): i for i, c in enumerate(clients)}
        for fut in as_completed(future_map):
            i = future_map[fut]
            try:
                stats[i] = fut.result()
            except Exception as e:
                name = clients[i].get('name') or clients[i].get('qb_url') or 'unknown'
                stats[i] = {'name': name, 'ok': False, 'error': str(e)}

    return {'items': stats, 'mode': cfg.get('qb_mode', 'round_robin')}


@app.post('/api/qb/test')
def qb_test(payload: dict):
    try:
        client = payload.get('client') or {}
    except Exception:
        client = {}
    st = qb_fetch_stats(client, 6)
    if st.get('ok'):
        return {'ok': True, 'message': '连接成功', 'stats': st}
    return {'ok': False, 'error': st.get('error') or '连接失败'}


@app.post('/api/qb/jump')
def qb_jump(payload: dict):
    client = (payload or {}).get('client') or {}
    qb_url = str(client.get('qb_url') or '').strip().rstrip('/')
    if not qb_url:
        return {'ok': False, 'error': '缺少 qb_url'}
    p = urlparse(qb_url)
    if p.scheme not in ('http', 'https'):
        return {'ok': False, 'error': 'qb_url 必须是 http/https'}
    try:
        _, _, err = qb_login(client)
    except Exception as e:
        return {'ok': False, 'error': str(e)}
    if err:
        return {'ok': False, 'error': err}
    return {'ok': True, 'url': qb_url + '/'}


@app.post('/api/upgrade/request')
def request_upgrade(payload: dict = None):
    p = payload or {}
    req = {
        'requested_at': now_iso(),
        'source': str(p.get('source') or 'web'),
        'note': str(p.get('note') or ''),
    }
    save_json(UPGRADE_REQUEST_PATH, req)
    log(f"收到升级请求：source={req['source']}")
    return {'ok': True, 'message': '升级请求已提交，宿主机将异步执行'}


@app.get('/api/upgrade/status')
def upgrade_status():
    st = load_json(UPGRADE_STATUS_PATH, {'state': 'idle', 'updated_at': None, 'message': '暂无升级任务'})
    return st


@app.post('/api/tg/menu_sync')
def tg_menu_sync():
    cfg = load_config()
    ok, msg = tg_set_upgrade_menu(cfg)
    if ok:
        return {'ok': True, 'message': 'TG菜单已同步（含 /upgrade）'}
    return {'ok': False, 'error': msg}


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


@app.post('/api/retry_failed')
def retry_failed():
    cfg = load_config()
    state = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
    res = retry_failed_pushes_once(cfg, state)
    save_json(STATE_PATH, state)
    return res

@app.post('/api/self_magic/once')
def self_magic_once():
    if not SELF_MAGIC_LOCK.acquire(blocking=False):
        return {'ok': False, 'error': '自放魔法任务正在执行中，请稍后再试'}
    try:
        cfg = load_config()
        st = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
        res = auto_self_magic_once(cfg, st, force=True)
        save_json(STATE_PATH, st)
        return res
    finally:
        SELF_MAGIC_LOCK.release()


def tail_lines(path: Path, max_lines: int = 200, block_size: int = 8192):
    if not path.exists():
        return []
    with path.open('rb') as f:
        f.seek(0, 2)
        file_size = f.tell()
        data = b''
        pos = file_size
        while pos > 0 and data.count(b'\n') <= max_lines:
            read_size = min(block_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size) + data
    text = data.decode('utf-8', errors='ignore')
    lines = text.splitlines()[-max_lines:]
    lines.reverse()
    return lines


@app.get('/api/logs')
def logs(cursor: int = Query(0, ge=0), limit: int = Query(200, ge=50, le=500)):
    if not APP_LOG.exists():
        return {'cursor': 0, 'text': '', 'reset': False}

    size = APP_LOG.stat().st_size
    # 首次加载：直接返回尾部，避免读全量日志
    if cursor == 0:
        return {'cursor': size, 'text': '\n'.join(tail_lines(APP_LOG, limit)), 'reset': True}

    # 日志被截断或轮转
    if cursor > size:
        return {'cursor': size, 'text': '\n'.join(tail_lines(APP_LOG, limit)), 'reset': True}

    with APP_LOG.open('rb') as f:
        f.seek(cursor)
        chunk = f.read(max(0, size - cursor))

    text = chunk.decode('utf-8', errors='ignore').strip()
    if text:
        lines = text.splitlines()[-limit:]
        text = '\n'.join(reversed(lines))
    return {'cursor': size, 'text': text, 'reset': False}


@app.get('/api/self_magic/history')
def self_magic_history(limit: int = Query(0, ge=0, le=5000)):
    # 读取完整日志做全量统计（日志量通常可控；若后续变大可改为按日落盘）
    if APP_LOG.exists():
        text = APP_LOG.read_text(encoding='utf-8', errors='ignore')
        lines = text.splitlines()
    else:
        lines = []

    succ = []

    re_succ = re.compile(r'^\[(.*?)\]\s+自放魔法成功：tid=(\d+)，hash=([0-9a-fA-F]+)(?:，UC=([0-9]+(?:\.[0-9]+)?))?(?:（(?:([0-9]+)钻)?([0-9]+)金([0-9]+)银([0-9]{1,2})铜）)?')

    log_uc_total = 0.0
    log_uc_today = 0.0
    day = _magic_day_key()
    daily_spent = {}
    daily_count = {}

    for ln in lines:
        m = re_succ.search(ln)
        if not m:
            continue
        t = m.group(1)
        tid = int(m.group(2))
        h = m.group(3)

        uc = 0.0
        if m.group(6) is not None:
            # 新版日志优先按钻金银铜还原，避免历史小数显示歧义
            gem = int(m.group(5) or 0)
            gold = int(m.group(6) or 0)
            silver = int(m.group(7) or 0)
            copper = int(m.group(8) or 0)
            uc = float(f'{gem * 10000 + gold * 100 + silver + copper / 100.0:.2f}')
        elif m.group(4):
            uc = float(m.group(4))

        succ.append({'time': t, 'tid': tid, 'hash': h, 'uc': uc})

    # 二阶段修正：用论坛 UCoin 变动日志回填历史成本（按 tid + 时间邻近匹配）
    forum_entries, forum_err = _fetch_ucoin_magic_entries(load_config(), max_pages=3)
    matched_forum = 0
    forum_only_added = 0
    if forum_entries:
        used = set()
        for it in succ:
            dt = _parse_cst_time(str(it.get('time') or ''))
            tid = int(it.get('tid') or 0)
            if not dt or not tid:
                continue
            best_idx = -1
            best_gap = 10**9
            for idx, fe in enumerate(forum_entries):
                if idx in used:
                    continue
                if int(fe.get('tid') or 0) != tid:
                    continue
                gap = abs(int((fe.get('dt') - dt).total_seconds()))
                if gap < best_gap:
                    best_gap = gap
                    best_idx = idx
            if best_idx >= 0 and best_gap <= 8 * 3600:
                fe = forum_entries[best_idx]
                used.add(best_idx)
                it['uc'] = float(fe.get('uc') or 0.0)
                it['ucParts'] = dict(fe.get('ucParts') or _uc_parts(it['uc']))
                it['ucFrom'] = 'forum'
                matched_forum += 1

        # 论坛有但本地日志没有的魔法消费，也纳入统计（避免“今日消耗为0”）
        for idx, fe in enumerate(forum_entries):
            if idx in used:
                continue
            succ.append({
                'time': fe.get('time'),
                'tid': int(fe.get('tid') or 0),
                'hash': '-',
                'uc': float(fe.get('uc') or 0.0),
                'ucParts': dict(fe.get('ucParts') or _uc_parts(float(fe.get('uc') or 0.0))),
                'ucFrom': 'forum_only',
            })
            forum_only_added += 1

    # 统一补齐展示字段并统计
    for it in succ:
        p = dict(it.get('ucParts') or _uc_parts(float(it.get('uc') or 0.0)))
        it['ucParts'] = p
        it['ucText'] = _format_uc_cn(float(it.get('uc') or 0.0))
        it['ucCopper'] = _uc_to_copper_value(float(it.get('uc') or 0.0))
        uc = float(it.get('uc') or 0.0)
        log_uc_total += uc

        dkey = str(it.get('time') or '').split(' ', 1)[0]
        daily_spent[dkey] = float(daily_spent.get(dkey) or 0.0) + uc
        daily_count[dkey] = int(daily_count.get(dkey) or 0) + 1
        if str(it.get('time') or '').startswith(day):
            log_uc_today += uc

    st = load_json(STATE_PATH, {'last_seen': [], 'last_run': None, 'last_error': None, 'qb_rr_index': 0, 'failed_pushes': []})
    uc_total_state = float(st.get('self_magic_uc_total') or 0.0)
    uc_today_state = float((st.get('self_magic_uc_by_day') or {}).get(day) or 0.0)
    # 兼容历史版本未累计UC：使用 state 与日志统计的较大值，避免显示为0
    uc_total = max(uc_total_state, log_uc_total)
    uc_today = max(uc_today_state, log_uc_today)

    fail_total = int(st.get('self_magic_fail_total') or 0)
    fail_today = int((st.get('self_magic_fail_by_day') or {}).get(day) or 0)
    daily_limit_total = int(st.get('self_magic_daily_limit_total') or 0)
    daily_limit_today = int((st.get('self_magic_daily_limit_by_day') or {}).get(day) or 0)

    succ.sort(key=lambda x: str(x.get('time') or ''), reverse=True)
    items = succ if limit == 0 else succ[:limit]
    daily_items = []
    for d in sorted(daily_spent.keys(), reverse=True):
        u = float(daily_spent.get(d) or 0.0)
        daily_items.append({'day': d, 'count': int(daily_count.get(d) or 0), 'uc': u, 'ucText': _format_uc_cn(u), 'ucParts': _uc_parts(u), 'ucCopper': _uc_to_copper_value(u)})

    note = '历史记录按日志全量重算。'
    if forum_entries:
        note += f' 已用论坛UCoin日志修正 {matched_forum} 条，补入仅论坛记录 {forum_only_added} 条。'
    elif forum_err:
        note += f' 论坛UCoin日志获取失败：{forum_err}'
    else:
        note += ' 未获取到论坛UCoin日志。'

    return {
        'ok': True,
        'count': len(succ),
        'items': items,
        'dailyItems': daily_items,
        'ucSpentTotal': uc_total,
        'ucSpentToday': uc_today,
        'ucSpentTotalText': _format_uc_cn(uc_total),
        'ucSpentTodayText': _format_uc_cn(uc_today),
        'ucSpentTotalParts': _uc_parts(uc_total),
        'ucSpentTodayParts': _uc_parts(uc_today),
        'ucSpentTotalCopper': _uc_to_copper_value(uc_total),
        'ucSpentTodayCopper': _uc_to_copper_value(uc_today),
        'failTotal': fail_total,
        'failToday': fail_today,
        'dailyLimitTotal': daily_limit_total,
        'dailyLimitToday': daily_limit_today,
        'forumMatched': matched_forum,
        'forumOnlyAdded': forum_only_added,
        'note': note,
    }
