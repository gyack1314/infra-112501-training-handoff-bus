"""命令行入口：回放演练、查看交接状态、启动 HTTP 服务。"""

import argparse
import json
from pathlib import Path

from .errors import CoordinationError
from .runner import TraceRunner
from .service import Coordinator
from .store import Store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="训练采集交接的事务消息协调服务")
    parser.add_argument("--db", default="coordinator.db", help="SQLite 状态库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    replay = sub.add_parser("replay", help="回放一个 JSONL 演练文件")
    replay.add_argument("trace", type=Path)

    sub.add_parser("handovers", help="列出所有交接身份")

    inspect_cmd = sub.add_parser("inspect", help="查看一轮交接的完整结果")
    inspect_cmd.add_argument("site")
    inspect_cmd.add_argument("run")

    serve = sub.add_parser("serve", help="启动 HTTP 服务")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with Store(args.db) as store:
        coordinator = Coordinator(store)
        runner = TraceRunner(coordinator)

        if args.command == "replay":
            outputs = runner.run_file(args.trace)
            print(json.dumps(outputs, ensure_ascii=False, indent=2))
        elif args.command == "handovers":
            print(json.dumps(coordinator.list_handovers(), ensure_ascii=False, indent=2))
        elif args.command == "inspect":
            try:
                print(json.dumps(coordinator.inspect(args.site, args.run), ensure_ascii=False, indent=2))
            except CoordinationError as error:
                print(json.dumps({"status": "error", "error": str(error)}, ensure_ascii=False))
                return 1
        elif args.command == "serve":
            # 延迟导入，避免无 HTTP 需求时加载 server 模块
            from .server import serve_http

            serve_http(coordinator, runner, args.host, args.port)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
