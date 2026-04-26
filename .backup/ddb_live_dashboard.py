import hashlib
import json
import os
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote

import dolphindb as ddb
from config import load_ddb_config


@dataclass
class DashboardConfig:
    host: str = os.getenv("DDB_DASHBOARD_HOST", "0.0.0.0")
    port: int = int(os.getenv("DDB_DASHBOARD_PORT", "18080"))
    nas_ip: str = ""
    nas_port: int = 0
    nas_user: str = ""
    nas_password: str = ""
    db_path: str = ""
    default_table: str = ""


_DDB_CFG = load_ddb_config()
CFG = DashboardConfig(
    nas_ip=_DDB_CFG.host,
    nas_port=_DDB_CFG.port,
    nas_user=_DDB_CFG.user,
    nas_password=_DDB_CFG.password,
    db_path=_DDB_CFG.db_path,
    default_table=os.getenv("DDB_DEFAULT_TABLE", _DDB_CFG.table_daily),
)
SESSION_LOCK = threading.Lock()
SESSION = None
SUMMARY_CACHE: dict[str, tuple[float, dict]] = {}
SUMMARY_CACHE_LOCK = threading.Lock()
SUMMARY_CACHE_TTL_SECONDS = int(os.getenv("DDB_SUMMARY_CACHE_TTL", "120"))
SNAPSHOT_CACHE: dict[str, tuple[float, dict]] = {}
SNAPSHOT_CACHE_LOCK = threading.Lock()
SNAPSHOT_CACHE_TTL_SECONDS = int(os.getenv("DDB_SNAPSHOT_CACHE_TTL", "3"))
MINUTE_MAX_DAYS = int(os.getenv("DDB_MINUTE_MAX_DAYS", "30"))

TICKER_INDEX_DIR = Path(__file__).resolve().parent / ".ddb_ticker_cache"
TICKER_INDEX_MEM: dict[str, tuple[float, list[str]]] = {}
TICKER_INDEX_MEM_LOCK = threading.Lock()
TICKER_REBUILD_LOCK = threading.Lock()
TICKER_REBUILDING: set[str] = set()


@contextmanager
def suppress_stderr_fd():
    """Temporarily silence native stderr output (e.g. DolphinDB C++ warnings)."""
    saved_fd = os.dup(2)
    try:
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            os.dup2(devnull.fileno(), 2)
            yield
    finally:
        os.dup2(saved_fd, 2)
        os.close(saved_fd)


def create_connected_session() -> ddb.session:
    s = ddb.session()
    with suppress_stderr_fd():
        s.connect(CFG.nas_ip, CFG.nas_port, CFG.nas_user, CFG.nas_password)
    return s


def get_session() -> ddb.session:
    global SESSION
    with SESSION_LOCK:
        if SESSION is None:
            SESSION = create_connected_session()
        return SESSION


def sanitize_identifier(text: str, fallback: str) -> str:
    text = (text or "").strip()
    if not text:
        return fallback
    keep = []
    for ch in text:
        if ch.isalnum() or ch in ("_", "-"):
            keep.append(ch)
    out = "".join(keep)
    return out or fallback


def sanitize_ticker_symbol(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    out = "".join(c for c in s if c.isalnum() or c in "._-")[:32]
    return out.upper()


def safe_int(value: str, default: int, low: int, high: int) -> int:
    try:
        n = int(value)
    except Exception:
        return default
    return max(low, min(high, n))


_DFS_PATH_RE = re.compile(r"^dfs://[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$")


def sanitize_db_path(value: str, fallback: str) -> str:
    raw = (value or fallback or "").strip()
    if not _DFS_PATH_RE.match(raw):
        return fallback
    return raw


def ddb_str_literal(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def series_like_to_list(out) -> list[str]:
    if out is None:
        return []
    try:
        if hasattr(out, "empty") and out.empty:
            return []
    except Exception:
        pass
    if hasattr(out, "tolist"):
        return [str(x) for x in out.tolist() if str(x).strip()]
    if isinstance(out, (list, tuple)):
        return [str(x) for x in out if str(x).strip()]
    s = str(out).strip()
    return [s] if s else []


def list_dfs_table_names(db_path: str) -> list[str]:
    d = ddb_str_literal(db_path)
    try:
        out = run_query(f'getTables(database("{d}"))')
    except Exception:
        return []
    if isinstance(out, str):
        s = out.strip()
        return [s] if s else []
    if hasattr(out, "empty") and not out.empty and hasattr(out, "columns"):
        if len(out.columns):
            return [str(x) for x in out.iloc[:, 0].tolist() if str(x).strip()]
    if hasattr(out, "tolist"):
        try:
            raw = out.tolist()
            if raw and isinstance(raw[0], (list, tuple)):
                return [str(x[0]) for x in raw if x and str(x[0]).strip()]
            return [str(x) for x in raw if str(x).strip()]
        except Exception:
            pass
    return series_like_to_list(out)


def sanitize_ticker_prefix(value: str) -> str:
    s = (value or "").upper().strip()
    return "".join(c for c in s if c.isalnum() or c in "._-")[:32]


def _ticker_index_key(db_path: str, table: str) -> str:
    d = ddb_str_literal(db_path)
    t = ddb_str_literal(table)
    h = hashlib.md5(f"{d}\0{t}".encode("utf-8")).hexdigest()[:20]
    return f"{h}_{t}"


def ticker_index_path(db_path: str, table: str) -> Path:
    return TICKER_INDEX_DIR / f"{_ticker_index_key(db_path, table)}.json"


def _invalidate_ticker_index_memory(db_path: str, table: str) -> None:
    k = f"{db_path}::{table}"
    with TICKER_INDEX_MEM_LOCK:
        TICKER_INDEX_MEM.pop(k, None)


def _read_ticker_index_from_disk(
    path: Path,
) -> tuple[list[str], str] | tuple[None, str]:
    if not path.is_file():
        return None, ""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return None, f"read cache failed: {e}"
    if raw.get("error"):
        return None, str(raw.get("error", ""))[:2000] or "index build failed"
    tickers = raw.get("tickers")
    if not isinstance(tickers, list):
        return None, "invalid cache file (no tickers[])"
    out = [str(x).strip() for x in tickers if str(x).strip()]
    built_at = str(raw.get("built_at", ""))[:32]
    return out, built_at


def get_ticker_index_list(db_path: str, table: str) -> tuple[list[str] | None, str]:
    p = ticker_index_path(db_path, table)
    k = f"{db_path}::{table}"
    if not p.is_file():
        return None, ""
    mtime = p.stat().st_mtime
    with TICKER_INDEX_MEM_LOCK:
        hit = TICKER_INDEX_MEM.get(k)
        if hit and hit[0] == mtime:
            return hit[1], ""
    data, berr = _read_ticker_index_from_disk(p)
    if data is None:
        return None, berr
    with TICKER_INDEX_MEM_LOCK:
        TICKER_INDEX_MEM[k] = (mtime, data)
    return data, ""


def filter_ticker_list_by_prefix(
    tickers: list[str], prefix: str, limit: int
) -> list[str]:
    pfx = prefix.upper()
    if not pfx:
        return []
    out: list[str] = []
    for t in tickers:
        s = str(t)
        if s.upper().startswith(pfx):
            out.append(s)
            if len(out) >= limit:
                break
    return out


def _write_ticker_index_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(payload, ensure_ascii=False, indent=0).encode("utf-8")
    tmp.write_bytes(data)
    tmp.replace(path)


def fetch_distinct_tickers_ddb(db_path: str, table: str) -> list[str]:
    d = ddb_str_literal(db_path)
    tbn = ddb_str_literal(table)
    errors: list[str] = []
    for script in (
        f't = loadTable("{d}", "{tbn}");\nselect ticker from t group by ticker',
        f't = loadTable("{d}", "{tbn}");\nselect first(ticker) as ticker from t group by ticker',
        f'select distinct(ticker) as ticker from loadTable("{d}", "{tbn}")',
    ):
        try:
            out = run_query(script)
            if hasattr(out, "empty") and not out.empty and "ticker" in out.columns:
                s = [str(x).strip() for x in out["ticker"].tolist() if str(x).strip()]
                if s:
                    return s
        except Exception as e:
            errors.append(f"{e!r}")
            continue
    if errors:
        raise RuntimeError(" ; ".join(errors[:3])) from None
    raise RuntimeError("empty distinct ticker result")


def rebuild_ticker_index_sync(db_path: str, table: str) -> dict:
    tickers = fetch_distinct_tickers_ddb(db_path, table)
    tickers = sorted({str(x) for x in tickers if str(x).strip()}, key=str.upper)
    p = ticker_index_path(db_path, table)
    built = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    _write_ticker_index_atomic(
        p,
        {
            "db_path": db_path,
            "table": table,
            "built_at": built,
            "count": len(tickers),
            "tickers": tickers,
        },
    )
    _invalidate_ticker_index_memory(db_path, table)
    return {"ok": True, "count": len(tickers), "path": str(p), "built_at": built}


def _write_ticker_index_error(
    db_path: str, table: str, error: str
) -> None:
    p = ticker_index_path(db_path, table)
    built = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    _write_ticker_index_atomic(
        p,
        {
            "db_path": db_path,
            "table": table,
            "error": error[:2000],
            "built_at": built,
        },
    )
    _invalidate_ticker_index_memory(db_path, table)


def schedule_ticker_rebuild_async(db_path: str, table: str) -> str | None:
    """Return error message if already building; None if a background job was started."""
    key = f"{db_path}::{table}"
    with TICKER_REBUILD_LOCK:
        if key in TICKER_REBUILDING:
            return "代码索引正在后台构建，请稍候"
        TICKER_REBUILDING.add(key)

    def go() -> None:
        try:
            try:
                rebuild_ticker_index_sync(db_path, table)
            except Exception as e:
                _write_ticker_index_error(db_path, table, str(e))
        finally:
            with TICKER_REBUILD_LOCK:
                TICKER_REBUILDING.discard(key)

    threading.Thread(target=go, daemon=True).start()
    return None


def get_ticker_index_status(db_path: str, table: str) -> dict:
    p = ticker_index_path(db_path, table)
    key = f"{db_path}::{table}"
    building = key in TICKER_REBUILDING
    if not p.is_file():
        return {
            "ok": True,
            "exists": False,
            "building": building,
            "count": 0,
        }
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return {"ok": False, "error": str(e), "building": building, "path": str(p)}
    if raw.get("error"):
        return {
            "ok": True,
            "exists": True,
            "error": str(raw.get("error", ""))[:2000],
            "building": building,
            "built_at": raw.get("built_at"),
            "path": str(p),
        }
    tlist = raw.get("tickers")
    n = int(raw.get("count", 0) or 0)
    if isinstance(tlist, list) and n == 0:
        n = len(tlist)
    return {
        "ok": True,
        "exists": True,
        "count": n,
        "building": building,
        "built_at": raw.get("built_at"),
        "path": str(p),
    }


def run_query(script: str):
    s = get_session()
    try:
        return s.run(script)
    except Exception:
        # reconnect once
        with SESSION_LOCK:
            global SESSION
            SESSION = create_connected_session()
            return SESSION.run(script)


def table_exists(db_path: str, table: str) -> bool:
    d = ddb_str_literal(db_path)
    t = ddb_str_literal(table)
    script = f'existsTable("{d}", "{t}")'
    return bool(run_query(script))


def get_table_summary(db_path: str, table: str, days: int, prefer_recent: bool) -> dict:
    d = ddb_str_literal(db_path)
    tbn = ddb_str_literal(table)
    mode = "recent" if prefer_recent else "full"
    cache_key = f"{db_path}::{table}::{days}::{mode}"
    now = time.time()
    with SUMMARY_CACHE_LOCK:
        hit = SUMMARY_CACHE.get(cache_key)
        if hit and now - hit[0] <= SUMMARY_CACHE_TTL_SECONDS:
            return hit[1]

    if prefer_recent:
        q = f"""
t = loadTable("{d}", "{tbn}");
select count(*) as rows, min(date) as min_date, max(date) as max_date
from t
where date >= today() - {days}
"""
        row_scope = "recent"
    else:
        q = f"""
t = loadTable("{d}", "{tbn}");
select count(*) as rows, min(date) as min_date, max(date) as max_date from t
"""
        row_scope = "full"

    out = run_query(q)
    payload = {
        "rows": int(out["rows"].iloc[0]) if len(out) else 0,
        "min_date": str(out["min_date"].iloc[0])[:10] if len(out) else "",
        "max_date": str(out["max_date"].iloc[0])[:10] if len(out) else "",
        "row_scope": row_scope,
    }
    with SUMMARY_CACHE_LOCK:
        SUMMARY_CACHE[cache_key] = (now, payload)
    return payload


def get_cached_snapshot(cache_key: str) -> dict | None:
    now = time.time()
    with SNAPSHOT_CACHE_LOCK:
        hit = SNAPSHOT_CACHE.get(cache_key)
        if hit and now - hit[0] <= SNAPSHOT_CACHE_TTL_SECONDS:
            return hit[1]
    return None


def put_cached_snapshot(cache_key: str, payload: dict) -> None:
    with SNAPSHOT_CACHE_LOCK:
        SNAPSHOT_CACHE[cache_key] = (time.time(), payload)


def compute_sma(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    window_sum = 0.0
    for i, v in enumerate(values):
        window_sum += v
        if i >= period:
            window_sum -= values[i - period]
        if i + 1 < period:
            out.append(None)
        else:
            out.append(window_sum / period)
    return out


def build_snapshot(
    db_path: str, table: str, ticker: str, days: int, limit: int, bars: int
) -> dict:
    d = ddb_str_literal(db_path)
    tbn = ddb_str_literal(table)
    if not table_exists(db_path, table):
        return {"ok": False, "error": f"Table not found: {table}"}

    ticker = ticker.upper()
    # Guard expensive minute queries from freezing refresh cycles.
    if (table.endswith("_1m") or table.endswith("1m")) and days > MINUTE_MAX_DAYS:
        days = MINUTE_MAX_DAYS
    if not ticker:
        default_ticker = run_query(
            f"""
t = loadTable("{d}", "{tbn}");
select top 1 ticker from t where date >= today() - {days} order by window_start desc
"""
        )
        if len(default_ticker) == 0:
            return {"ok": False, "error": "No data in selected range"}
        ticker = str(default_ticker["ticker"].iloc[0])

    # Minute-level tables can be very large; avoid full-table aggregation every refresh.
    prefer_recent_summary = table.endswith("_1m") or table.endswith("1m")
    summary = get_table_summary(db_path, table, days, prefer_recent_summary)

    recent = run_query(
        f"""
t = loadTable("{d}", "{tbn}");
select top {limit} ticker, date, window_start, open, high, low, close, volume, transactions
from t
where date >= today() - {days} and ticker=`{ticker}
order by window_start desc
"""
    )

    series = run_query(
        f"""
t = loadTable("{d}", "{tbn}");
select top {bars} date, window_start, open, high, low, close, volume
from t
where date >= today() - {days} and ticker=`{ticker}
order by window_start desc
"""
    )

    candles = []
    volumes = []
    close_values: list[float] = []
    if len(series) > 0:
        asc = series.iloc[::-1].reset_index(drop=True)
        for i in range(len(asc)):
            time_val = str(asc["date"].iloc[i])[:10]
            o = float(asc["open"].iloc[i])
            h = float(asc["high"].iloc[i])
            l = float(asc["low"].iloc[i])
            c = float(asc["close"].iloc[i])
            v = int(asc["volume"].iloc[i])
            candles.append(
                {"time": time_val, "open": o, "high": h, "low": l, "close": c}
            )
            volumes.append(
                {
                    "time": time_val,
                    "value": v,
                    "color": "rgba(38,166,154,0.55)" if c >= o else "rgba(239,83,80,0.55)",
                }
            )
            close_values.append(c)
    sma9 = compute_sma(close_values, 9)
    sma21 = compute_sma(close_values, 21)
    ma9 = []
    ma21 = []
    for i, candle in enumerate(candles):
        if sma9[i] is not None:
            ma9.append({"time": candle["time"], "value": round(float(sma9[i]), 6)})
        if sma21[i] is not None:
            ma21.append({"time": candle["time"], "value": round(float(sma21[i]), 6)})

    rows = []
    if len(recent) > 0:
        for i in range(len(recent)):
            rows.append(
                {
                    "ticker": str(recent["ticker"].iloc[i]),
                    "date": str(recent["date"].iloc[i])[:10],
                    "window_start": str(recent["window_start"].iloc[i]),
                    "open": float(recent["open"].iloc[i]),
                    "high": float(recent["high"].iloc[i]),
                    "low": float(recent["low"].iloc[i]),
                    "close": float(recent["close"].iloc[i]),
                    "volume": int(recent["volume"].iloc[i]),
                    "transactions": int(recent["transactions"].iloc[i]),
                }
            )

    return {
        "ok": True,
        "meta": {
            "db_path": db_path,
            "table": table,
            "ticker": ticker,
            "query_days": days,
            "rows": int(summary["rows"]),
            "min_date": summary["min_date"],
            "max_date": summary["max_date"],
            "rows_scope": summary["row_scope"],
            "bars": len(candles),
            "recent_rows": len(rows),
        },
        "candles": candles,
        "volumes": volumes,
        "ma9": ma9,
        "ma21": ma21,
        "recent": rows,
    }




# 页面模板见同目录 ddb_dashboard.html（可单独编辑，免改 Python）
_DASHBOARD_HTML_PATH = Path(__file__).resolve().parent / "ddb_dashboard.html"


def load_dashboard_html_template() -> str:
    override = os.getenv("DDB_DASHBOARD_HTML", "").strip()
    path = Path(override) if override else _DASHBOARD_HTML_PATH
    return path.resolve().read_text(encoding="utf-8")


def render_dashboard_html() -> str:
    return load_dashboard_html_template().replace("__DDB_PATH__", CFG.db_path)


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = render_dashboard_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/tables":
            qs = parse_qs(parsed.query)
            db_path = sanitize_db_path(
                unquote((qs.get("db") or [""])[0] or ""), CFG.db_path
            )
            try:
                tables = list_dfs_table_names(db_path)
            except Exception as e:
                data = _json_bytes({"ok": False, "error": str(e), "tables": []})
            else:
                data = _json_bytes(
                    {
                        "ok": True,
                        "db_path": db_path,
                        "tables": sorted({t for t in tables if t}),
                    }
                )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if parsed.path == "/api/ticker_index/status":
            qs = parse_qs(parsed.query)
            db_path = sanitize_db_path(
                unquote((qs.get("db") or [""])[0] or ""), CFG.db_path
            )
            table = sanitize_identifier(
                (qs.get("table") or [CFG.default_table])[0], CFG.default_table
            )
            st = get_ticker_index_status(db_path, table)
            data = _json_bytes(
                {**st, "db_path": db_path, "table": table}
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if parsed.path == "/api/ticker_index/rebuild":
            qs = parse_qs(parsed.query)
            db_path = sanitize_db_path(
                unquote((qs.get("db") or [""])[0] or ""), CFG.db_path
            )
            table = sanitize_identifier(
                (qs.get("table") or [CFG.default_table])[0], CFG.default_table
            )
            use_async = safe_int(
                (qs.get("async") or ["1"])[0], 1, 0, 1
            )
            if not table_exists(db_path, table):
                data = _json_bytes(
                    {"ok": False, "error": f"Table not found: {table}"}
                )
            elif use_async == 0:
                try:
                    r = rebuild_ticker_index_sync(db_path, table)
                    data = _json_bytes(
                        {**r, "ok": True, "db_path": db_path, "table": table}
                    )
                except Exception as e:
                    _write_ticker_index_error(db_path, table, str(e))
                    data = _json_bytes(
                        {
                            "ok": False,
                            "error": str(e),
                            "db_path": db_path,
                            "table": table,
                        }
                    )
            else:
                err = schedule_ticker_rebuild_async(db_path, table)
                if err:
                    data = _json_bytes(
                        {
                            "ok": True,
                            "started": False,
                            "message": err,
                            "db_path": db_path,
                            "table": table,
                        }
                    )
                else:
                    data = _json_bytes(
                        {
                            "ok": True,
                            "started": True,
                            "message": "后台已开始全量去重,生成完成后可秒搜代码",
                            "db_path": db_path,
                            "table": table,
                        }
                    )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if parsed.path == "/api/tickers":
            qs = parse_qs(parsed.query)
            db_path = sanitize_db_path(
                unquote((qs.get("db") or [""])[0] or ""), CFG.db_path
            )
            table = sanitize_identifier(
                (qs.get("table") or [CFG.default_table])[0], CFG.default_table
            )
            tq = sanitize_ticker_prefix((qs.get("q") or [""])[0])
            limit = safe_int((qs.get("limit") or ["200"])[0], 200, 1, 500)
            if not tq:
                data = _json_bytes(
                    {
                        "ok": True,
                        "db_path": db_path,
                        "table": table,
                        "tickers": [],
                        "index_ready": False,
                    }
                )
            elif not table_exists(db_path, table):
                data = _json_bytes(
                    {
                        "ok": False,
                        "error": f"Table not found: {table}",
                        "tickers": [],
                    }
                )
            else:
                all_t, berr = get_ticker_index_list(db_path, table)
                if berr:
                    data = _json_bytes(
                        {
                            "ok": True,
                            "db_path": db_path,
                            "table": table,
                            "index_ready": False,
                            "index_error": berr,
                            "tickers": [],
                        }
                    )
                elif all_t is None:
                    data = _json_bytes(
                        {
                            "ok": True,
                            "db_path": db_path,
                            "table": table,
                            "index_ready": False,
                            "message": "无本地代码索引,请点击「预生成/刷新代码索引」(后台一次去重,之后秒搜).",
                            "tickers": [],
                        }
                    )
                else:
                    data = _json_bytes(
                        {
                            "ok": True,
                            "db_path": db_path,
                            "table": table,
                            "index_ready": True,
                            "tickers": filter_ticker_list_by_prefix(
                                all_t, tq, limit
                            ),
                        }
                    )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if parsed.path == "/api/snapshot":
            qs = parse_qs(parsed.query)
            db_path = sanitize_db_path(
                unquote((qs.get("db") or [""])[0] or ""), CFG.db_path
            )
            table = sanitize_identifier(
                (qs.get("table") or [CFG.default_table])[0], CFG.default_table
            )
            ticker = sanitize_ticker_symbol((qs.get("ticker") or [""])[0])
            days = safe_int((qs.get("days") or ["365"])[0], 365, 1, 5000)
            limit = safe_int((qs.get("limit") or ["30"])[0], 30, 5, 200)
            bars = safe_int((qs.get("bars") or ["600"])[0], 600, 50, 4000)
            cache_key = (
                f"{db_path}::{table}::{ticker}::{days}::{limit}::{bars}"
            )
            try:
                payload = get_cached_snapshot(cache_key)
                if payload is None:
                    payload = build_snapshot(
                        db_path, table, ticker, days, limit, bars
                    )
                    if payload.get("ok"):
                        put_cached_snapshot(cache_key, payload)
            except Exception as e:
                payload = {"ok": False, "error": str(e)}
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return


def main():
    server = ThreadingHTTPServer((CFG.host, CFG.port), Handler)
    print(f"Dashboard running: http://127.0.0.1:{CFG.port}")
    print(f"Page template: {_DASHBOARD_HTML_PATH}")
    print(f"Ticker index cache: {TICKER_INDEX_DIR}")
    server.serve_forever()


if __name__ == "__main__":
    main()
