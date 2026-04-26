import tarfile
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox

import dolphindb as ddb
import pandas as pd
from config import load_ddb_config

_DDB_CFG = load_ddb_config()
DEFAULT_NAS_IP = _DDB_CFG.host
DEFAULT_NAS_PORT = _DDB_CFG.port
DEFAULT_NAS_USER = _DDB_CFG.user
DEFAULT_NAS_PASSWORD = _DDB_CFG.password
DEFAULT_DB_PATH = _DDB_CFG.db_path
DEFAULT_TABLE_NAME_DAILY = _DDB_CFG.table_daily
DEFAULT_TABLE_NAME_MINUTE = _DDB_CFG.table_minute
DEFAULT_SOURCE_PATH = r"Z:\Data\Massive\day_aggs"
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_DATA_GRANULARITY = "daily"


@dataclass
class VerifyConfig:
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


def get_target_table(cfg: VerifyConfig) -> str:
    if cfg.data_granularity == "daily":
        return cfg.table_name_daily
    if cfg.data_granularity == "minute":
        return cfg.table_name_minute
    raise ValueError("data_granularity 仅支持 'daily' 或 'minute'")


def iter_tgz_files(source_path: str):
    p = Path(source_path)
    if p.is_file() and p.suffix.lower() == ".tgz":
        yield p
        return
    if p.is_dir():
        yield from sorted(p.rglob("*.tgz"))
        return
    raise FileNotFoundError(f"SOURCE_PATH 不存在或不是 tgz/目录: {source_path}")


def count_source_rows(cfg: VerifyConfig) -> int:
    rows = 0
    for tgz_path in iter_tgz_files(cfg.source_path):
        with tarfile.open(tgz_path, "r:gz") as tar:
            for member in tar.getmembers():
                if not (member.isfile() and member.name.endswith(".csv")):
                    continue
                f = tar.extractfile(member)
                if not f:
                    continue
                for chunk in pd.read_csv(f, chunksize=cfg.chunk_size, usecols=["window_start"]):
                    rows += len(chunk)
    return rows


def count_db_rows(cfg: VerifyConfig, table_name: str) -> int:
    s = ddb.session()
    s.connect(cfg.nas_ip, cfg.nas_port, cfg.nas_user, cfg.nas_password)
    script = f"""
select count(*) as cnt from loadTable("{cfg.db_path}", "{table_name}")
"""
    out = s.run(script)
    return int(out["cnt"][0])


def run_verify(cfg: VerifyConfig):
    table_name = get_target_table(cfg)
    print(f"开始校验, 表: {table_name}")
    source_rows = count_source_rows(cfg)
    db_rows = count_db_rows(cfg, table_name)
    diff = db_rows - source_rows

    print("\n==== 校验结果 ====")
    print(f"源数据行数: {source_rows}")
    print(f"DolphinDB 行数: {db_rows}")
    print(f"差值(DB-Source): {diff}")
    if diff == 0:
        print("校验通过: 行数一致")
    else:
        print("校验不一致: 需检查去重策略、断点重跑或写入失败日志")


def launch_gui() -> VerifyConfig | None:
    root = tk.Tk()
    root.title("Massive 导入后校验")
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

    result = {"cfg": None}

    def on_start():
        try:
            cfg = VerifyConfig(
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
            )
            if cfg.data_granularity not in {"daily", "minute"}:
                raise ValueError("Granularity 必须是 daily 或 minute")
            result["cfg"] = cfg
            root.destroy()
        except Exception as e:
            messagebox.showerror("参数错误", str(e))

    btn_row = tk.Frame(root)
    btn_row.pack(fill="x", padx=8, pady=8)
    tk.Button(btn_row, text="开始校验", command=on_start).pack(side="left")
    tk.Button(btn_row, text="取消", command=root.destroy).pack(side="left", padx=8)

    root.mainloop()
    return result["cfg"]


if __name__ == "__main__":
    config = launch_gui()
    if config:
        run_verify(config)
