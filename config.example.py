import os
from dataclasses import dataclass

# 复制本文件为 config.py 后填写真实连接信息；config.py 已被 .gitignore 忽略。

DEFAULT_NAS_IP = "127.0.0.1"
DEFAULT_NAS_PORT = 8848
DEFAULT_NAS_USER = "admin"
DEFAULT_NAS_PASSWORD = ""
DEFAULT_DB_PATH = "dfs://massive_data"
DEFAULT_TABLE_NAME_DAILY = "bars_1d"
DEFAULT_TABLE_NAME_MINUTE = "bars_1m"


@dataclass(frozen=True)
class DdbConnectionConfig:
    host: str
    port: int
    user: str
    password: str
    db_path: str
    table_daily: str
    table_minute: str


def load_ddb_config() -> DdbConnectionConfig:
    return DdbConnectionConfig(
        host=os.getenv("DDB_NAS_IP", DEFAULT_NAS_IP),
        port=int(os.getenv("DDB_NAS_PORT", str(DEFAULT_NAS_PORT))),
        user=os.getenv("DDB_NAS_USER", DEFAULT_NAS_USER),
        password=os.getenv("DDB_NAS_PASSWORD", DEFAULT_NAS_PASSWORD),
        db_path=os.getenv("DDB_DB_PATH", DEFAULT_DB_PATH),
        table_daily=os.getenv("DDB_TABLE_DAILY", DEFAULT_TABLE_NAME_DAILY),
        table_minute=os.getenv("DDB_TABLE_MINUTE", DEFAULT_TABLE_NAME_MINUTE),
    )
