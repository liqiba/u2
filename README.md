# catch-magic-web

一个基于 FastAPI + Docker 的 U2 魔法监控与 qBittorrent 分发工具。

## 功能
- 定时拉取 U2 魔法
- 多 qB 模块管理（轮询分发 / 全部推送）
- 看板显示任务数、实时流量、总流量
- Web 页面配置与日志查看

## 启动
```bash
docker-compose up -d --build
```

访问：`http://服务器IP:18088`

## 配置
- 实际配置文件：`data/config.json`（不要提交到 GitHub）
- 示例配置文件：`data/config.example.json`

## 安全
请勿把真实 `token/passkey/qb_password` 提交到公开仓库。
