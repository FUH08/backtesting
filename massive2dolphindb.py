"""
兼容旧脚本名：与 ``python -m ingestion`` 等价。

推荐新用法：在仓库根目录执行 ``python -m ingestion``。
"""

import runpy

if __name__ == "__main__":
    runpy.run_module("ingestion", run_name="__main__")
