import dolphindb as ddb
import pandas as pd

from plotting.models import PlotConfig


def _table_is_minute(table: str) -> bool:
    t = (table or "").lower()
    return "_1m" in t or t.endswith("1m")


def ddb_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


def fetch_stock_data(cfg: PlotConfig) -> pd.DataFrame:
    s = ddb.session()
    s.connect(cfg.host, cfg.port, cfg.user, cfg.password)
    db = ddb_escape(cfg.db_path)
    tb = ddb_escape(cfg.table)
    tk = ",".join(f"`{ddb_escape(x)}" for x in cfg.tickers)
    script = f"""
t = loadTable("{db}", "{tb}");
select date, window_start, ticker, open, high, low, close, volume, transactions
from t
where date >= today() - {cfg.days} and ticker in ({tk})
order by window_start desc
"""
    out = s.run(script)
    if len(out) == 0:
        return out
    out = out.iloc[::-1].reset_index(drop=True)
    out["window_start"] = pd.to_datetime(out["window_start"], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out = (
        out.sort_values("window_start")
        .groupby("ticker", group_keys=False)
        .tail(cfg.bars)
        .reset_index(drop=True)
    )
    return out


def to_unix_seconds(ts) -> int | None:
    if pd.isna(ts):
        return None
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return int(t.timestamp())


def _compare_points_for_ticker(g: pd.DataFrame) -> tuple[list[dict], list[dict], list[dict]]:
    g = g.sort_values("window_start").reset_index(drop=True)
    fc = float(g["close"].iloc[0])
    first_close = fc if fc != 0 else 1.0
    close_pts, high_pts, low_pts = [], [], []
    for _, r in g.iterrows():
        ts_raw = r["window_start"] if pd.notna(r["window_start"]) else r["date"]
        ts = to_unix_seconds(ts_raw)
        if ts is None:
            continue
        close_pts.append({"time": ts, "value": float(r["close"]) / first_close * 100.0})
        high_pts.append({"time": ts, "value": float(r["high"]) / first_close * 100.0})
        low_pts.append({"time": ts, "value": float(r["low"]) / first_close * 100.0})
    return close_pts, high_pts, low_pts


def _compare_candles_normalized_for_ticker(g: pd.DataFrame) -> list[dict]:
    """上图多品种 K 线对比：OHLC 按首根收盘价归一到 100。"""
    g = g.sort_values("window_start").reset_index(drop=True)
    fc = float(g["close"].iloc[0])
    first_close = fc if fc != 0 else 1.0
    out: list[dict] = []
    for _, r in g.iterrows():
        ts_raw = r["window_start"] if pd.notna(r["window_start"]) else r["date"]
        ts = to_unix_seconds(ts_raw)
        if ts is None:
            continue
        o = float(r["open"])
        h = float(r["high"])
        l = float(r["low"])
        c = float(r["close"])
        out.append(
            {
                "time": ts,
                "open": o / first_close * 100.0,
                "high": h / first_close * 100.0,
                "low": l / first_close * 100.0,
                "close": c / first_close * 100.0,
            }
        )
    return out


def _candles_volumes_for_ticker(g: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    g = g.sort_values("window_start").reset_index(drop=True)
    candles, volumes = [], []
    for _, r in g.iterrows():
        ts_raw = r["window_start"] if pd.notna(r["window_start"]) else r["date"]
        ts = to_unix_seconds(ts_raw)
        if ts is None:
            continue
        o = float(r["open"])
        c = float(r["close"])
        candles.append(
            {
                "time": ts,
                "open": o,
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": c,
            }
        )
        volumes.append(
            {
                "time": ts,
                "value": int(r["volume"]),
                "color": "rgba(38,166,154,0.55)" if c >= o else "rgba(239,83,80,0.55)",
            }
        )
    return candles, volumes


def build_payload(df: pd.DataFrame, cfg: PlotConfig) -> dict:
    tickers = sorted(df["ticker"].unique().tolist())
    compare_close: dict[str, list[dict]] = {}
    compare_high: dict[str, list[dict]] = {}
    compare_low: dict[str, list[dict]] = {}
    compare_candles: dict[str, list[dict]] = {}
    candles_by_ticker: dict[str, dict] = {}

    for tk in tickers:
        g = df[df["ticker"] == tk].copy()
        if len(g) == 0:
            continue
        c_pts, h_pts, l_pts = _compare_points_for_ticker(g)
        compare_close[tk] = c_pts
        compare_high[tk] = h_pts
        compare_low[tk] = l_pts
        compare_candles[tk] = _compare_candles_normalized_for_ticker(g)
        candles, volumes = _candles_volumes_for_ticker(g)
        candles_by_ticker[tk] = {"candles": candles, "volumes": volumes}

    base_ticker = tickers[0] if tickers else cfg.tickers[0].upper()
    base_pack = candles_by_ticker.get(base_ticker, {"candles": [], "volumes": []})

    return {
        "meta": {
            "db_path": cfg.db_path,
            "table": cfg.table,
            "tickers": tickers,
            "base_ticker": base_ticker,
            "days": cfg.days,
            "bars": cfg.bars,
            "intraday": _table_is_minute(cfg.table),
        },
        "compare": {
            "high": compare_high,
            "low": compare_low,
            "close": compare_close,
        },
        "compare_series": compare_close,
        "compare_candles": compare_candles,
        "candles": base_pack["candles"],
        "volumes": base_pack["volumes"],
        "candles_by_ticker": candles_by_ticker,
    }

