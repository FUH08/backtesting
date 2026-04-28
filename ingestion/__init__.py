"""
数据入库（ingestion）：将 Massive 等来源的 OHLCV / trade 批量写入 DolphinDB。

运行：在仓库根目录执行 ``python -m ingestion``（会加载 GUI/CLI 与 DolphinDB 依赖）。

连接信息见仓库 ``config/`` 包（本机 ``config/local.py`` 或环境变量）。

若需在代码中调用 API，请 ``from ingestion.massive_import import process_and_upload, ImportConfig``，
避免 ``import ingestion`` 本身拉取全部子模块。
"""

__all__: list[str] = []
