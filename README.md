# backtesting_system

DolphinDB 上的行情 **入库（Massive tgz）**、**查询** 与 **浏览器多标的对比图**（TradingView Lightweight Charts）。

## 目录说明

| 路径 | 作用 |
|------|------|
| `config/` | DolphinDB 连接：`defaults.py` 占位默认；本机复制 `local.example.py` → `local.py`（已 gitignore），或用环境变量 `DDB_*` |
| `ingestion/` | 数据入库：`python -m ingestion`，将 tgz 内 CSV 并行写入 DFS |
| `massive2dolphindb.py` | 兼容旧入口，等价于 `python -m ingestion` |
| `plotting/` | 读库 + 本地 HTTP + `templates/`、`static/` 前端 |
| `plot_single_stock.py` | 绘图 CLI 快捷入口 |
| `requirements.txt` | Python 依赖 |

## 环境

```bash
cp config/local.example.py config/local.py   # 编辑本机 DolphinDB 地址与密码
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**注意：** 仓库根目录不要放 `config.py` 文件，否则会遮蔽 `config/` 包导致 `from config import load_ddb_config` 失败。

## 常用命令

```bash
# 浏览器对比图（默认日线表，分钟表加 --table bars_1m）
python plot_single_stock.py --tickers QQQ,SPY --days 60 --table bars_1m

# Massive/Trade CSV → DolphinDB（GUI；无显示则交互命令行）
python -m ingestion
```

`ingestion` 支持 `daily`、`minute`、`trade` 三种 `Granularity`。其中 `trade` 会写入 `Trade Table`（可用 `DDB_TABLE_TRADE` 或 GUI/CLI 参数覆盖）。

## 数据与缓存

- 导入进度：`ingestion/massive_import_checkpoint.json`（可在 GUI/CLI 中改路径）
- Ticker 列表缓存：`.ddb_ticker_cache/`（勿提交大文件，已在 `.gitignore`）
