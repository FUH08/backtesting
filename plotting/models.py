from dataclasses import dataclass


@dataclass
class PlotConfig:
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

