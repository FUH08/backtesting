"""
按 GICS 对 DolphinDB 中的股票做分类并写回分类表。

用法示例：
python classify_gics.py \
  --mapping-csv /path/to/gics_mapping.csv \
  --source-table bars_1d \
  --target-table stock_gics

CSV 至少需要包含：
- ticker
- gics_sector

可选列：
- gics_sector_code
- gics_industry_group
- gics_industry_group_code
- gics_industry
- gics_industry_code
- gics_sub_industry
- gics_sub_industry_code
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import dolphindb as ddb  # pyright: ignore[reportMissingImports]
import pandas as pd

from config import load_ddb_config


EXPECTED_COLUMNS = [
    "ticker",
    "gics_sector",
    "gics_sector_code",
    "gics_industry_group",
    "gics_industry_group_code",
    "gics_industry",
    "gics_industry_code",
    "gics_sub_industry",
    "gics_sub_industry_code",
]


@dataclass(frozen=True)
class JobConfig:
    mapping_csv: str
    source_db_path: str
    class_db_path: str
    source_table: str
    target_table: str
    host: str
    port: int
    user: str
    password: str


def ddb_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


def create_session(cfg: JobConfig) -> ddb.session:
    s = ddb.session()
    s.connect(cfg.host, cfg.port, cfg.user, cfg.password)
    return s


def ensure_target_table(s: ddb.session, db_path: str, table_name: str) -> None:
    script = f"""
dbPath="{ddb_escape(db_path)}";
tbName="{ddb_escape(table_name)}";
if(!existsDatabase(dbPath)){{
    // 分类小库不存在时自动创建；按 ticker 分区即可。
    database(dbPath, HASH, [SYMBOL, 50]);
}}
if(!existsTable(dbPath, tbName)){{
    db=database(dbPath);
    schema=table(1:0,
        `ticker`gics_sector`gics_sector_code`gics_industry_group`gics_industry_group_code`gics_industry`gics_industry_code`gics_sub_industry`gics_sub_industry_code`updated_at,
        [SYMBOL, SYMBOL, INT, SYMBOL, INT, SYMBOL, INT, SYMBOL, INT, NANOTIMESTAMP]
    );
    createPartitionedTable(db, schema, tbName, `ticker);
}}
"""
    s.run(script)


def fetch_existing_tickers(s: ddb.session, db_path: str, source_table: str) -> set[str]:
    script = f"""
t=loadTable("{ddb_escape(db_path)}", "{ddb_escape(source_table)}");
select distinct ticker from t where not isNull(ticker)
"""
    out = s.run(script)
    if len(out) == 0:
        return set()
    return {str(x).strip().upper() for x in out["ticker"].tolist() if str(x).strip()}


def _normalize_int_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = 0
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")


def _normalize_str_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = ""
    df[col] = df[col].fillna("").astype(str).str.strip()


def load_mapping_csv(mapping_csv: str) -> pd.DataFrame:
    p = Path(mapping_csv)
    if not p.exists():
        raise FileNotFoundError(f"mapping CSV 不存在: {mapping_csv}")

    df = pd.read_csv(p)
    df.columns = [str(c).strip() for c in df.columns]
    missing = {"ticker", "gics_sector"} - set(df.columns)
    if missing:
        raise ValueError(f"mapping CSV 缺少必填列: {sorted(missing)}")

    df["ticker"] = df["ticker"].fillna("").astype(str).str.strip().str.upper()
    df = df[df["ticker"] != ""].copy()

    for col in EXPECTED_COLUMNS:
        if col.endswith("_code"):
            _normalize_int_col(df, col)
        else:
            _normalize_str_col(df, col)

    df = df[EXPECTED_COLUMNS].drop_duplicates(subset=["ticker"], keep="last").copy()
    df["updated_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return df


def write_gics_table(s: ddb.session, cfg: JobConfig, gics_df: pd.DataFrame) -> int:
    if gics_df.empty:
        return 0
    tickers = sorted({str(x).strip().upper() for x in gics_df["ticker"].tolist() if str(x).strip()})
    quoted = ",".join(f"`{ddb_escape(tk)}" for tk in tickers)
    delete_script = f"""
t=loadTable("{ddb_escape(cfg.class_db_path)}", "{ddb_escape(cfg.target_table)}");
if(size([{quoted}]) > 0){{
    delete from t where ticker in [{quoted}];
}}
"""
    s.run(delete_script)

    writer = ddb.TableAppender(db_path=cfg.class_db_path, table_name=cfg.target_table, conn=s)
    return int(writer.append(gics_df))


def print_join_examples(cfg: JobConfig) -> None:
    print("\n可在 DolphinDB 中用以下方式按 ticker 关联：")
    print("1) 日线 + GICS")
    print(
        f"""select d.date, d.window_start, d.ticker, d.close, g.gics_sector, g.gics_industry, g.gics_sub_industry
from loadTable("{cfg.source_db_path}", "{cfg.source_table}") d
left join loadTable("{cfg.class_db_path}", "{cfg.target_table}") g
on d.ticker = g.ticker
limit 20"""
    )
    print("\n2) 分钟线 + GICS（把 bars_1m 替换成你的分钟表名）")
    print(
        f"""select m.date, m.window_start, m.ticker, m.close, g.gics_sector, g.gics_industry, g.gics_sub_industry
from loadTable("{cfg.source_db_path}", "bars_1m") m
left join loadTable("{cfg.class_db_path}", "{cfg.target_table}") g
on m.ticker = g.ticker
limit 20"""
    )


def run(cfg: JobConfig) -> None:
    s = create_session(cfg)
    ensure_target_table(s, cfg.class_db_path, cfg.target_table)

    raw_map = load_mapping_csv(cfg.mapping_csv)
    db_tickers = fetch_existing_tickers(s, cfg.source_db_path, cfg.source_table)
    if not db_tickers:
        print(f"源表 `{cfg.source_table}` 无 ticker，未写入分类。")
        return

    out = raw_map[raw_map["ticker"].isin(db_tickers)].copy()
    rows = write_gics_table(s, cfg, out)
    print(f"源表 ticker 数: {len(db_tickers)}")
    print(f"映射表 ticker 数: {len(raw_map)}")
    print(f"匹配 ticker 数: {len(out)}")
    print(f"写入行数: {rows}")
    print(f"分类库: {cfg.class_db_path}")
    print(f"目标表: {cfg.target_table}")
    print_join_examples(cfg)


def parse_args() -> JobConfig:
    ddb_cfg = load_ddb_config()
    parser = argparse.ArgumentParser(description="按 GICS 分类并写入 DolphinDB")
    parser.add_argument("--mapping-csv", required=True, help="ticker 到 GICS 的 CSV 映射文件")
    parser.add_argument(
        "--source-db-path",
        default=ddb_cfg.db_path,
        help=f"行情库 DFS 路径（读 ticker），默认: {ddb_cfg.db_path}",
    )
    parser.add_argument(
        "--class-db-path",
        default="dfs://stock_classification",
        help="分类小库 DFS 路径（写入 GICS），默认: dfs://stock_classification",
    )
    parser.add_argument(
        "--source-table",
        default=ddb_cfg.table_daily,
        help=f"用于提取 ticker 的行情表，默认: {ddb_cfg.table_daily}",
    )
    parser.add_argument(
        "--target-table",
        default="stock_gics",
        help="GICS 分类写入表名，默认: stock_gics",
    )
    parser.add_argument("--host", default=ddb_cfg.host, help=f"DDB host，默认: {ddb_cfg.host}")
    parser.add_argument("--port", type=int, default=ddb_cfg.port, help=f"DDB port，默认: {ddb_cfg.port}")
    parser.add_argument("--user", default=ddb_cfg.user, help=f"DDB user，默认: {ddb_cfg.user}")
    parser.add_argument("--password", default=ddb_cfg.password, help="DDB password")
    args = parser.parse_args()
    return JobConfig(
        mapping_csv=args.mapping_csv,
        source_db_path=args.source_db_path,
        class_db_path=args.class_db_path,
        source_table=args.source_table,
        target_table=args.target_table,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )


if __name__ == "__main__":
    run(parse_args())
