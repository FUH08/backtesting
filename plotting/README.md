# plotting — 查询与前端

从 DolphinDB 拉取 OHLCV，组装 JSON，由内置 HTTP 服务提供 `tv_compare.html`（Lightweight Charts）。

| 文件 | 说明 |
|------|------|
| `app.py` | 编排：解析参数 → 查库 → `run_web` |
| `cli.py` | 命令行 → `PlotConfig` |
| `data.py` | `fetch_stock_data`、`build_payload` |
| `models.py` | `PlotConfig` |
| `table_kind.py` | 表名判断分钟/日线（默认 bars、前端 intraday） |
| `web.py` | `/`、`/api/data`、`/static/*` |
| `templates/` | HTML 模板 |
| `static/` | LWC 独立脚本等 |

DolphinDB 连接配置见 [config/](../config/) 包。

仓库总览见上级目录 [README.md](../README.md)。
