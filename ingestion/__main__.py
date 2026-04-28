"""包入口：``python -m ingestion``。"""

import tkinter as tk

# 延迟在子模块中加载 dolphindb / pandas，避免仅 ``import ingestion`` 时拉起重依赖
from ingestion.massive_import import launch_cli, launch_gui, process_and_upload

if __name__ == "__main__":
    try:
        config = launch_gui()
    except tk.TclError:
        config = launch_cli()
    if config:
        process_and_upload(config)
