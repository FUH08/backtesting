# Plotting 模块

该目录集中放置画图相关逻辑，避免单文件过大，便于维护。

## 文件说明

- `models.py`：配置数据结构（`PlotConfig`）
- `cli.py`：命令行参数解析
- `data.py`：DolphinDB 数据查询与图表数据 payload 组装
- `web.py`：HTTP 服务与模板加载
- `templates/tv_compare.html`：TradingView lightweight-charts 页面模板
- `app.py`：主流程编排（parse -> fetch -> run）

根目录 `plot_single_stock.py` 仅保留入口转发：

```python
from plotting.app import main
```

