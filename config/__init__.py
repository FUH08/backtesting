"""
DolphinDB 连接配置。

- ``defaults.py``：仓库内占位默认值
- ``local.py``：本机覆盖（需自行创建，见 ``local.example.py``）
- 环境变量 ``DDB_NAS_IP``、``DDB_NAS_PORT`` 等优先于上述常量

注意：仓库根目录勿再放名为 ``config.py`` 的文件，否则会遮蔽本包导致导入失败。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from . import defaults

try:
    from . import local as _local  # type: ignore[import-untyped]
except ImportError:
    _local = None


def _pick_str(env_key: str, local_name: str, fallback: str) -> str:
    v = os.getenv(env_key)
    if v is not None and v != "":
        return v
    if _local is not None and hasattr(_local, local_name):
        lv = getattr(_local, local_name)
        if lv is not None and str(lv) != "":
            return str(lv)
    return fallback


def _pick_int(env_key: str, local_name: str, fallback: int) -> int:
    v = os.getenv(env_key)
    if v is not None and v != "":
        return int(v)
    if _local is not None and hasattr(_local, local_name):
        lv = getattr(_local, local_name)
        if lv is not None:
            return int(lv)
    return fallback


@dataclass(frozen=True)
class DdbConnectionConfig:
    """入库与绘图共用的只读连接配置快照。"""

    host: str
    port: int
    user: str
    password: str
    db_path: str
    table_daily: str
    table_minute: str
    table_trade: str


def load_ddb_config() -> DdbConnectionConfig:
    """环境变量 > ``local.py`` > ``defaults.py``。"""
    return DdbConnectionConfig(
        host=_pick_str("DDB_NAS_IP", "DEFAULT_NAS_IP", defaults.DEFAULT_NAS_IP),
        port=_pick_int("DDB_NAS_PORT", "DEFAULT_NAS_PORT", defaults.DEFAULT_NAS_PORT),
        user=_pick_str("DDB_NAS_USER", "DEFAULT_NAS_USER", defaults.DEFAULT_NAS_USER),
        password=_pick_str(
            "DDB_NAS_PASSWORD", "DEFAULT_NAS_PASSWORD", defaults.DEFAULT_NAS_PASSWORD
        ),
        db_path=_pick_str("DDB_DB_PATH", "DEFAULT_DB_PATH", defaults.DEFAULT_DB_PATH),
        table_daily=_pick_str(
            "DDB_TABLE_DAILY", "DEFAULT_TABLE_NAME_DAILY", defaults.DEFAULT_TABLE_NAME_DAILY
        ),
        table_minute=_pick_str(
            "DDB_TABLE_MINUTE", "DEFAULT_TABLE_NAME_MINUTE", defaults.DEFAULT_TABLE_NAME_MINUTE
        ),
        table_trade=_pick_str(
            "DDB_TABLE_TRADE", "DEFAULT_TABLE_NAME_TRADE", defaults.DEFAULT_TABLE_NAME_TRADE
        ),
    )


__all__ = ["DdbConnectionConfig", "load_ddb_config"]
