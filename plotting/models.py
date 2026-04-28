"""绘图与查询共用的小型配置模型。"""

from dataclasses import dataclass


@dataclass
class PlotConfig:
    """单次启动内 DolphinDB 连接、目标表、代码列表、回溯窗口与 Web 监听参数。"""
    host: str
    port: int
    user: str
    password: str
    db_path: str
    table: str
    tickers: list[str]
    days: int
    bars: int
    web_host: str
    web_port: int

