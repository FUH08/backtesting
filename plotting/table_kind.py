"""表名约定：根据 DolphinDB 表名判断是否分钟线（用于默认 bars / 前端 intraday 行为）。"""


def is_minute_table(table: str) -> bool:
    """表名含 ``_1m`` 或以 ``1m`` 结尾时视为分钟表（如 ``bars_1m``）。"""
    t = (table or "").lower()
    return "_1m" in t or t.endswith("1m")
