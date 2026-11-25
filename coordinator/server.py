"""仅依赖标准库的 HTTP 入口，供值班系统与仿真参与方调用。

接口：

- ``POST /event``　提交一个契约事件 ``{type, data}``，语义与 JSONL 行一致；
- ``GET  /state?site=&run=``　查看一轮交接的可查询结果；
- ``GET  /handovers``　列出全部交接身份。
"""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .runner import TraceRunner
from .service import Coordinator


def serve_http(
    coordinator: Coordinator, runner: TraceRunner, host: str, port: int
) -> None:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: dict) -> None:
            raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802 - 标准库命名
            parsed = urlparse(self.path)
            if parsed.path == "/handovers":
                self._send(200, {"handovers": coordinator.list_handovers()})
                return
            if parsed.path == "/state":
                query = parse_qs(parsed.query)
                site = query.get("site", [None])[0]
                run = query.get("run", [None])[0]
                if not site or not run:
                    self._send(400, {"status": "error", "error": "需要 site 与 run 参数"})
                    return
                try:
                    self._send(200, coordinator.inspect(site, run))
                except Exception as error:  # 查询类错误统一转 404
                    self._send(404, {"status": "error", "error": str(error)})
                return
            self._send(404, {"status": "error", "error": "未知路径"})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/event":
                self._send(404, {"status": "error", "error": "未知路径"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                event_type = payload["type"]
                data = payload["data"]
                if not isinstance(data, dict):
                    raise ValueError("data 必须是对象")
            except (ValueError, KeyError, json.JSONDecodeError) as error:
                self._send(400, {"status": "error", "error": f"请求体无效: {error}"})
                return
            result = runner.dispatch(event_type, data)
            status = 409 if result.get("status") == "error" else 200
            self._send(status, result)

        def log_message(self, fmt: str, *args) -> None:  # 安静日志
            return

    class _Server(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    httpd = _Server((host, port), Handler)
    print(f"协调服务监听 http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
