import json
import hashlib
import tarfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import tkinter as tk
from tkinter import filedialog, messagebox

import dolphindb as ddb  # pyright: ignore[reportMissingImports]
import pandas as pd
from config import load_ddb_config

# ========== 默认参数（可在窗口中修改） ==========
_DDB_CFG = load_ddb_config()
DEFAULT_NAS_IP = _DDB_CFG.host
DEFAULT_NAS_PORT = _DDB_CFG.port
DEFAULT_NAS_USER = _DDB_CFG.user
DEFAULT_NAS_PASSWORD = _DDB_CFG.password
DEFAULT_DB_PATH = _DDB_CFG.db_path
DEFAULT_TABLE_NAME_DAILY = _DDB_CFG.table_daily
DEFAULT_TABLE_NAME_MINUTE = _DDB_CFG.table_minute
DEFAULT_SOURCE_PATH = "/home/restart668/Data/Massive/minute_aggs"
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_DATA_GRANULARITY = "minute"
DEFAULT_PARALLEL_WORKERS = 4
DEFAULT_CHECKPOINT_FILE = "massive_import_checkpoint.json"


@dataclass
class ImportConfig:
    nas_ip: str
    nas_port: int
    nas_user: str
    nas_password: str
    db_path: str
    table_name_daily: str
    table_name_minute: str
    source_path: str
    chunk_size: int
    data_granularity: str
    parallel_workers: int
    checkpoint_file: str


def ticker_cache_path(db_path: str, table_name: str) -> Path:
    key = hashlib.md5(f"{db_path}\0{table_name}".encode("utf-8")).hexdigest()[:20]
    cache_dir = Path(__file__).resolve().parent / ".ddb_ticker_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{key}_{table_name}.json"


def load_ticker_cache(cache_file: Path) -> set[str]:
    if not cache_file.exists():
        return set()
    try:
        raw = json.loads(cache_file.read_text(encoding="utf-8"))
    except Exception:
        return set()
    items = raw.get("tickers", [])
    if not isinstance(items, list):
        return set()
    return {str(x).strip() for x in items if str(x).strip()}


def save_ticker_cache(
    cache_file: Path, tickers: set[str], db_path: str, table_name: str
) -> None:
    payload = {
        "db_path": db_path,
        "table": table_name,
        "count": len(tickers),
        "tickers": sorted(tickers, key=str.upper),
    }
    cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def create_session(cfg: ImportConfig) -> ddb.session:
    s = ddb.session()
    s.connect(cfg.nas_ip, cfg.nas_port, cfg.nas_user, cfg.nas_password)
    return s


def get_target_table(cfg: ImportConfig) -> str:
    if cfg.data_granularity == "daily":
        return cfg.table_name_daily
    if cfg.data_granularity == "minute":
        return cfg.table_name_minute
    raise ValueError("data_granularity 仅支持 'daily' 或 'minute'")


def ensure_schema(s: ddb.session, db_path: str, table_name: str) -> None:
    script = f"""
dbPath="{db_path}";
tbName="{table_name}";
if(!existsDatabase(dbPath)){{
    db=database(dbPath, VALUE, 2000.01.01..2035.12.31);
    schema=table(1:0, `ticker`date`window_start`open`high`low`close`volume`transactions,
                 [SYMBOL, DATE, NANOTIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG]);
    createPartitionedTable(db, schema, tbName, `date);
}} else if(!existsTable(dbPath, tbName)){{
    db=database(dbPath);
    schema=table(1:0, `ticker`date`window_start`open`high`low`close`volume`transactions,
                 [SYMBOL, DATE, NANOTIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG]);
    createPartitionedTable(db, schema, tbName, `date);
}}
"""
    s.run(script)


def iter_tgz_files(source_path: str) -> Iterable[Path]:
    p = Path(source_path)
    if p.is_file() and p.suffix.lower() == ".tgz":
        yield p
        return
    if p.is_dir():
        yield from sorted(p.rglob("*.tgz"))
        return
    raise FileNotFoundError(f"SOURCE_PATH 不存在或不是 tgz/目录: {source_path}")


def is_tabular_member(member_name: str) -> bool:
    """True for plain CSV or gzip-compressed CSV inside tgz; skip macOS ._* sidecars."""
    p = Path(member_name)
    if p.name.startswith("._"):
        return False
    lower = p.name.lower()
    return lower.endswith(".csv") or lower.endswith(".csv.gz")


def csv_compression_for_member(member_name: str) -> str | None:
    n = member_name.lower()
    if n.endswith(".csv.gz"):
        return "gzip"
    if n.endswith(".csv"):
        return None
    return None


def read_csv_chunks_from_tar(
    member_name: str, f, *, chunk_size: int
) -> Iterable[pd.DataFrame]:
    comp = csv_compression_for_member(member_name)
    return pd.read_csv(f, compression=comp, chunksize=chunk_size)


def normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["window_start"], unit="ns", utc=True)
    df["date"] = ts.dt.date
    df["window_start"] = ts.dt.tz_localize(None)
    if "transactions" not in df.columns:
        df["transactions"] = 0

    out = df[
        [
            "ticker",
            "date",
            "window_start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "transactions",
        ]
    ].copy()
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")
    out["transactions"] = pd.to_numeric(out["transactions"], errors="coerce").fillna(0).astype(
        "int64"
    )
    out = out.drop_duplicates(subset=["ticker", "window_start"], keep="last")
    return out


def load_checkpoint(checkpoint_file: str) -> set[str]:
    p = Path(checkpoint_file)
    if not p.exists():
        return set()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return set(str(x) for x in data)
    except Exception:
        pass
    return set()


def save_checkpoint(checkpoint_file: str, processed: set[str]) -> None:
    p = Path(checkpoint_file)
    p.write_text(
        json.dumps(sorted(processed), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def process_single_tgz(job: tuple[str, ImportConfig, str]) -> dict:
    tgz_path_str, cfg, target_table = job
    tgz_path = Path(tgz_path_str)
    s = create_session(cfg)
    # loadTable().append() only accepts DolphinDB Table objects, not pandas.
    # TableAppender (AutoFitTableAppender) writes DataFrames to DFS tables.
    writer = ddb.TableAppender(db_path=cfg.db_path, table_name=target_table, conn=s)

    rows = 0
    csv_files = 0
    warnings = []
    ticker_set: set[str] = set()
    ok = True

    try:
        # Some files may have .tgz suffix but are not gzip streams.
        # Use auto-detection to support tar, tar.gz, tar.bz2, tar.xz, etc.
        with tarfile.open(tgz_path, "r:*") as tar:
            for member in tar.getmembers():
                if not (member.isfile() and is_tabular_member(member.name)):
                    continue
                f = tar.extractfile(member)
                if not f:
                    continue
                for chunk in read_csv_chunks_from_tar(
                    member.name, f, chunk_size=cfg.chunk_size
                ):
                    try:
                        normalized = normalize_chunk(chunk)
                        if len(normalized) == 0:
                            continue
                        ticker_set.update(
                            {
                                str(x).strip().upper()
                                for x in normalized["ticker"].dropna().unique().tolist()
                                if str(x).strip()
                            }
                        )
                        rows += int(writer.append(normalized))
                    except Exception as e:
                        ok = False
                        warnings.append(f"{member.name}: {e}")
                csv_files += 1
    except Exception as e:
        ok = False
        warnings.append(f"读取压缩包失败: {e}")

    return {
        "tgz": str(tgz_path.resolve()),
        "ok": ok,
        "rows": rows,
        "csv_files": csv_files,
        "tickers": sorted(ticker_set),
        "warnings": warnings,
    }


def process_and_upload(cfg: ImportConfig):
    s = create_session(cfg)
    target_table = get_target_table(cfg)
    ensure_schema(s, cfg.db_path, target_table)

    processed_tgz = load_checkpoint(cfg.checkpoint_file)
    all_tgz = [str(p.resolve()) for p in iter_tgz_files(cfg.source_path)]
    pending = [p for p in all_tgz if p not in processed_tgz]

    if not pending:
        print("没有待导入的 tgz（全部已在 checkpoint 中）")
        return

    print(f"目标表: {target_table}")
    print(f"总 tgz 数: {len(all_tgz)}, 待导入: {len(pending)}, 并行进程: {cfg.parallel_workers}")

    total_rows = 0
    total_csv_files = 0
    failed_tgz = []
    cache_file = ticker_cache_path(cfg.db_path, target_table)
    cached_tickers = load_ticker_cache(cache_file)
    jobs = [(p, cfg, target_table) for p in pending]

    with ProcessPoolExecutor(max_workers=cfg.parallel_workers) as executor:
        futures = [executor.submit(process_single_tgz, j) for j in jobs]
        for fut in as_completed(futures):
            result = fut.result()
            tgz = result["tgz"]
            total_rows += result["rows"]
            total_csv_files += result["csv_files"]
            current_tickers = {str(x).strip().upper() for x in result.get("tickers", []) if str(x).strip()}
            if current_tickers:
                old_n = len(cached_tickers)
                cached_tickers.update(current_tickers)
                if len(cached_tickers) != old_n:
                    save_ticker_cache(cache_file, cached_tickers, cfg.db_path, target_table)
            if result["ok"]:
                processed_tgz.add(tgz)
                save_checkpoint(cfg.checkpoint_file, processed_tgz)
                print(f"[OK] {tgz} | rows={result['rows']} csv={result['csv_files']}")
            else:
                failed_tgz.append(tgz)
                print(f"[FAIL] {tgz}")
                for msg in result["warnings"][:5]:
                    print(f"  [WARN] {msg}")

    print("\n==== 导入完成 ====")
    print(f"目标表: {target_table}")
    print(f"成功 tgz: {len(pending) - len(failed_tgz)}")
    print(f"失败 tgz: {len(failed_tgz)}")
    print(f"CSV 文件数: {total_csv_files}")
    print(f"写入行数: {total_rows}")
    print(f"checkpoint: {cfg.checkpoint_file}")
    print(f"ticker cache: {cache_file} (count={len(cached_tickers)})")


def launch_gui() -> ImportConfig | None:
    root = tk.Tk()
    root.title("Massive -> DolphinDB 并行导入")
    root.resizable(False, False)

    fields = {}

    def add_row(label: str, key: str, default: str):
        row = tk.Frame(root)
        row.pack(fill="x", padx=8, pady=3)
        tk.Label(row, text=label, width=18, anchor="w").pack(side="left")
        entry = tk.Entry(row, width=60)
        entry.insert(0, default)
        entry.pack(side="left", fill="x", expand=True)
        fields[key] = entry
        return row

    add_row("NAS IP", "nas_ip", DEFAULT_NAS_IP)
    add_row("NAS Port", "nas_port", str(DEFAULT_NAS_PORT))
    add_row("NAS User", "nas_user", DEFAULT_NAS_USER)
    add_row("NAS Password", "nas_password", DEFAULT_NAS_PASSWORD)
    add_row("DB Path", "db_path", DEFAULT_DB_PATH)
    add_row("Daily Table", "table_name_daily", DEFAULT_TABLE_NAME_DAILY)
    add_row("Minute Table", "table_name_minute", DEFAULT_TABLE_NAME_MINUTE)

    folder_row = add_row("Source Folder", "source_path", DEFAULT_SOURCE_PATH)

    def choose_folder():
        selected = filedialog.askdirectory(title="选择 Massive 数据目录")
        if selected:
            fields["source_path"].delete(0, tk.END)
            fields["source_path"].insert(0, selected)

    tk.Button(folder_row, text="选择文件夹", command=choose_folder).pack(side="left", padx=6)

    add_row("Chunk Size", "chunk_size", str(DEFAULT_CHUNK_SIZE))
    add_row("Granularity", "data_granularity", DEFAULT_DATA_GRANULARITY)
    add_row("Parallel Workers", "parallel_workers", str(DEFAULT_PARALLEL_WORKERS))
    add_row("Checkpoint File", "checkpoint_file", DEFAULT_CHECKPOINT_FILE)

    result = {"cfg": None}

    def on_start():
        try:
            cfg = ImportConfig(
                nas_ip=fields["nas_ip"].get().strip(),
                nas_port=int(fields["nas_port"].get().strip()),
                nas_user=fields["nas_user"].get().strip(),
                nas_password=fields["nas_password"].get(),
                db_path=fields["db_path"].get().strip(),
                table_name_daily=fields["table_name_daily"].get().strip(),
                table_name_minute=fields["table_name_minute"].get().strip(),
                source_path=fields["source_path"].get().strip(),
                chunk_size=int(fields["chunk_size"].get().strip()),
                data_granularity=fields["data_granularity"].get().strip().lower(),
                parallel_workers=max(1, int(fields["parallel_workers"].get().strip())),
                checkpoint_file=fields["checkpoint_file"].get().strip(),
            )
            if cfg.data_granularity not in {"daily", "minute"}:
                raise ValueError("Granularity 必须是 daily 或 minute")
            result["cfg"] = cfg
            root.destroy()
        except Exception as e:
            messagebox.showerror("参数错误", str(e))

    btn_row = tk.Frame(root)
    btn_row.pack(fill="x", padx=8, pady=8)
    tk.Button(btn_row, text="开始导入", command=on_start).pack(side="left")
    tk.Button(btn_row, text="取消", command=root.destroy).pack(side="left", padx=8)

    root.mainloop()
    return result["cfg"]


def _prompt_with_default(label: str, default: str, secret: bool = False) -> str:
    hint = "******" if secret and default else default
    value = input(f"{label} [{hint}]: ").strip()
    if not value:
        return default
    return value


def launch_cli() -> ImportConfig | None:
    print("检测到无图形界面，切换到命令行配置模式。")
    print("直接回车可使用方括号中的默认值。\n")
    try:
        cfg = ImportConfig(
            nas_ip=_prompt_with_default("NAS IP", DEFAULT_NAS_IP),
            nas_port=int(_prompt_with_default("NAS Port", str(DEFAULT_NAS_PORT))),
            nas_user=_prompt_with_default("NAS User", DEFAULT_NAS_USER),
            nas_password=_prompt_with_default(
                "NAS Password", DEFAULT_NAS_PASSWORD, secret=True
            ),
            db_path=_prompt_with_default("DB Path", DEFAULT_DB_PATH),
            table_name_daily=_prompt_with_default("Daily Table", DEFAULT_TABLE_NAME_DAILY),
            table_name_minute=_prompt_with_default(
                "Minute Table", DEFAULT_TABLE_NAME_MINUTE
            ),
            source_path=_prompt_with_default("Source Path", DEFAULT_SOURCE_PATH),
            chunk_size=int(_prompt_with_default("Chunk Size", str(DEFAULT_CHUNK_SIZE))),
            data_granularity=_prompt_with_default(
                "Granularity (daily/minute)", DEFAULT_DATA_GRANULARITY
            ).lower(),
            parallel_workers=max(
                1, int(_prompt_with_default("Parallel Workers", str(DEFAULT_PARALLEL_WORKERS)))
            ),
            checkpoint_file=_prompt_with_default(
                "Checkpoint File", DEFAULT_CHECKPOINT_FILE
            ),
        )
    except KeyboardInterrupt:
        print("\n已取消。")
        return None
    except Exception as e:
        print(f"参数错误: {e}")
        return None

    if cfg.data_granularity not in {"daily", "minute"}:
        print("参数错误: Granularity 必须是 daily 或 minute")
        return None
    return cfg


if __name__ == "__main__":
    try:
        config = launch_gui()
    except tk.TclError:
        config = launch_cli()
    if config:
        process_and_upload(config)