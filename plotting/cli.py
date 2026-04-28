"""命令行解析：DolphinDB 连接与查询窗口、Web 服务参数 → ``PlotConfig``。"""

import argparse
import os

from config import load_ddb_config
from plotting.models import PlotConfig
from plotting.table_kind import is_minute_table


def _default_bars_for_table(table: str, days: int) -> int:
    """日线默认 800；分钟表若未指定 --bars，按天数估算（避免只显示最近 ~800 分钟）。"""
    if is_minute_table(table):
        return min(150_000, max(3_000, int(days * 500)))
    return 800


def parse_args() -> PlotConfig:
    """解析 ``sys.argv``，合并环境变量与 ``config.load_ddb_config`` 默认值。"""
    ddb_cfg = load_ddb_config()
    parser = argparse.ArgumentParser(description="使用 lightweight-charts 展示多代码对比")
    parser.add_argument("--ticker", help="单个股票代码，例如 AAPL")
    parser.add_argument(
        "--tickers",
        help="多个股票代码，逗号分隔，例如 AAPL,MSFT,NVDA（优先于 --ticker）",
    )
    parser.add_argument("--days", type=int, default=365, help="查询最近天数，默认 365")
    parser.add_argument(
        "--bars",
        type=int,
        default=None,
        help="每个 ticker 最多保留的 K 线根数（SQL 仍按 days 取数，再按此截断最近 N 根）。"
        "默认：日线 800；表名含 1m 的分钟表按 days 自动估算（约 500×天数，上限 15 万）",
    )
    parser.add_argument(
        "--table", default=os.getenv("DDB_TABLE", ddb_cfg.table_daily), help="表名"
    )
    parser.add_argument("--db-path", default=ddb_cfg.db_path, help="DolphinDB 数据库路径")
    parser.add_argument("--host", default=ddb_cfg.host, help="DolphinDB Host")
    parser.add_argument("--port", type=int, default=ddb_cfg.port, help="DolphinDB Port")
    parser.add_argument("--user", default=ddb_cfg.user, help="DolphinDB 用户")
    parser.add_argument("--password", default=ddb_cfg.password, help="DolphinDB 密码")
    parser.add_argument(
        "--web-host", default=os.getenv("WEB_HOST", "0.0.0.0"), help="Web 监听地址"
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=int(os.getenv("WEB_PORT", "8050")),
        help="Web 监听端口",
    )
    args = parser.parse_args()
    raw_tickers = args.tickers if args.tickers else args.ticker
    if not raw_tickers:
        raise SystemExit("请至少提供 --ticker 或 --tickers")
    tickers = sorted(
        {str(x).strip().upper() for x in str(raw_tickers).split(",") if str(x).strip()}
    )
    if not tickers:
        raise SystemExit("ticker 参数为空")
    days = max(1, int(args.days))
    bars = (
        max(50, int(args.bars))
        if args.bars is not None
        else _default_bars_for_table(args.table, days)
    )
    return PlotConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        db_path=args.db_path,
        table=args.table,
        tickers=tickers,
        days=days,
        bars=bars,
        web_host=args.web_host,
        web_port=int(args.web_port),
    )

