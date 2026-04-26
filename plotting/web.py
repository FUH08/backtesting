import json
import mimetypes
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from plotting.models import PlotConfig


_TEMPLATE_PATH = Path(__file__).resolve().parent / "templates" / "tv_compare.html"
_STATIC_ROOT = Path(__file__).resolve().parent / "static"

# 与 plotting/static 内文件一致：https://github.com/tradingview/lightweight-charts
_LIGHTWEIGHT_CHARTS_VERSION = "5.2.0"


def _safe_static_file(relative: str) -> Path | None:
    relative = relative.strip("/\\")
    if not relative or ".." in Path(relative).parts:
        return None
    candidate = (_STATIC_ROOT / relative).resolve()
    try:
        candidate.relative_to(_STATIC_ROOT.resolve())
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def build_html(cfg: PlotConfig) -> str:
    raw = _TEMPLATE_PATH.read_text(encoding="utf-8")
    title = f"TV Compare - {','.join(cfg.tickers)}"
    return raw.replace("__PAGE_TITLE__", title)


def run_web(payload: dict, cfg: PlotConfig) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            p = urlparse(self.path).path
            if p.startswith("/static/"):
                rel = unquote(p[len("/static/") :])
                static_path = _safe_static_file(rel)
                if static_path is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                body = static_path.read_bytes()
                ctype, _ = mimetypes.guess_type(static_path.name)
                self.send_response(200)
                self.send_header("Content-Type", ctype or "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                self.wfile.write(body)
                return
            if p == "/":
                # 每次请求重新读模板，避免改 tv_compare.html 后必须重启进程
                body = build_html(cfg).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(body)
                return
            if p == "/api/data":
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer((cfg.web_host, cfg.web_port), Handler)
    print(
        f"TV chart running: http://127.0.0.1:{cfg.web_port} "
        f"(lightweight-charts {_LIGHTWEIGHT_CHARTS_VERSION} from /static/)"
    )
    server.serve_forever()

