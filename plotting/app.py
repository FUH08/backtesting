"""应用编排：解析参数 → 查库 → 起本地 HTTP 展示对比图。"""

from plotting.cli import parse_args
from plotting.data import build_payload, fetch_stock_data
from plotting.web import run_web


def main() -> None:
    """CLI 入口：无数据时打印提示并退出；否则阻塞在 ``run_web``。"""
    cfg = parse_args()
    df = fetch_stock_data(cfg)
    if len(df) == 0:
        print(
            f"未查询到数据: tickers={cfg.tickers}, table={cfg.table}, days={cfg.days}"
        )
        return
    range_start = str(df["date"].min())[:10]
    range_end = str(df["date"].max())[:10]
    print(f"rows={len(df)}, tickers={sorted(df['ticker'].unique().tolist())}")
    print(f"range={range_start} ~ {range_end}")
    payload = build_payload(df, cfg)
    run_web(payload, cfg)

