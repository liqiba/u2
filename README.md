# U2 魔法助手（catch-magic-web）

一个给 U2 用户准备的**自动化下载工具**：
它会按规则定时抓取 U2 种子，并自动分发到一个或多个 qBittorrent 客户端，带 Web 配置页面和运行日志，适合长期挂着跑。

---

## 这个项目能做什么？

- **定时抓取 U2 魔法种子**（可配置间隔）
- **按规则过滤**（免费/非免费、做种人数、抓取数量等）
- **支持多 qB 客户端**
  - 轮询分发（round_robin）
  - 全部推送（all）
- **Web 管理界面**
  - 在线修改配置
  - 查看任务状态与日志
- **Docker 一键运行**，部署简单，便于迁移

---

## 适合谁？

- 想自动化下载 U2 种子，不想手动盯站点的人
- 有 1 台或多台 qBittorrent 需要统一分发的人
- 希望用 Docker 稳定运行、重启自动恢复的人

---

## 一键安装（小白可用）

> 适用系统：Ubuntu / Debian / CentOS（已安装 Docker + Docker Compose 插件）

直接复制这一条命令执行：

```bash
bash -c "set -e; cd /opt; [ -d u2 ] || git clone https://github.com/liqiba/u2.git; cd u2; mkdir -p data logs; [ -f data/config.json ] || cp data/config.example.json data/config.json; docker compose up -d --build"
```

安装完成后访问：

- `http://你的服务器IP:18088`

---

## 首次使用（3 步）

### 1) 打开管理页面
访问 `http://你的服务器IP:18088`

### 2) 填写关键配置
在页面或 `data/config.json` 中填写：

- `u2_api_token`：你的 U2 API Token
- `u2_passkey`：你的 U2 passkey
- `qb_clients[*].qb_url`：qB 地址（例如 `http://127.0.0.1:8080`）
- `qb_clients[*].qb_username` / `qb_password`

### 3) 启用任务
将配置中的 `enabled` 改为 `true`，保存后即可开始按周期抓取。

---

## 常用命令

在项目目录（如 `/opt/u2`）执行：

```bash
# 启动/更新

docker compose up -d --build

# 查看运行状态

docker compose ps

# 查看日志

docker compose logs -f

# 停止

docker compose down
```

---

## 配置说明（核心字段）

配置文件：`data/config.json`

- `enabled`：是否启用自动抓取（`true/false`）
- `interval`：抓取间隔（秒）
- `limit`：每次最多抓取条数
- `max_seeders`：做种人数上限过滤
- `download_non_free`：是否下载非免费种子
- `qb_mode`：分发模式（`round_robin` / `all`）
- `qb_clients`：qB 客户端列表（可配置多个）

可参考示例：`data/config.example.json`

---

## 目录结构

```text
u2/
├─ main.py
├─ docker-compose.yml
├─ Dockerfile
├─ data/
│  ├─ config.example.json
│  └─ config.json          # 运行配置（本地）
└─ logs/                   # 运行日志（本地）
```

---

## 安全提醒（务必看）

- **不要**把真实 `u2_api_token / u2_passkey / qb_password` 提交到 GitHub
- `data/config.json` 只保留在本机
- 建议将 Web 管理端口 `18088` 仅开放给可信 IP

---

## 项目地址

- GitHub: https://github.com/liqiba/u2

如果这个项目对你有帮助，欢迎 Star ⭐
