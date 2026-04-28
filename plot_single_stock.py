"""仓库根目录便捷入口：``python plot_single_stock.py --tickers AAPL,MSFT ...``。"""
import sys
from plotting.app import main

if __name__ == "__main__":
    # 这里写你想固定的参数
    sys.argv = [
        "plot_single_stock.py",
        "--tickers", "AAPL,MSFT,NVDA",
        "--days", "60",
        "--table", "bars_1m",
        "--web-port", "8050",
    ]
    main()
