"""把 JSONL 演练事件（资料契约）逐条送入协调器。

每个输入事件映射到协调器的一个命令；协调器的领域错误被捕获并以
``error`` 结果记录，单条坏输入不会中断整段演练。
"""

import json
from pathlib import Path
from typing import Any, Callable

from .errors import CoordinationError
from .service import Coordinator


class TraceRunner:
    def __init__(self, coordinator: Coordinator):
        self.coordinator = coordinator
        self.handlers: dict[str, Callable[[dict], Any]] = {
            "manifest": self._manifest,
            "signoff": self._signoff,
            "prepare_ack": self._prepare_ack,
            "receipt": self._receipt,
            "withdraw_ack": self._withdraw_ack,
            "void": self._void,
            "receive": self._receive,
            "ack": self._ack,
            "start_next": self._start_next,
            "inspect": self._inspect,
        }

    def dispatch(self, event_type: str, data: dict) -> dict[str, Any]:
        handler = self.handlers.get(event_type)
        if handler is None:
            return {"status": "error", "error": f"未知事件类型 {event_type}"}
        try:
            return handler(data)
        except CoordinationError as error:
            return {"status": "error", "error": str(error)}

    def run_file(self, path: str | Path) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        with Path(path).open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                row = json.loads(line)
                result = self.dispatch(row["type"], row["data"])
                outputs.append(
                    {
                        "timestamp": row["timestamp"],
                        "line": line_number,
                        "type": row["type"],
                        "input": row["data"],
                        "result": result,
                    }
                )
        return outputs

    # ---- 事件映射 ---------------------------------------------------------

    def _manifest(self, data: dict) -> dict:
        return self.coordinator.submit_manifest(
            data["site"], data["run"], data["revision"], data["collection"], data["use"]
        )

    def _signoff(self, data: dict) -> dict:
        return self.coordinator.record_signoff(
            data["site"], data["run"], data["revision"], data["use"], data["decision"]
        )

    def _prepare_ack(self, data: dict) -> dict:
        return self.coordinator.record_prepare_ack(
            data["site"], data["run"], data["revision"], data["reservation"], data["outcome"]
        )

    def _receipt(self, data: dict) -> dict:
        return self.coordinator.record_receipt(
            data["site"], data["run"], data["reservation"], data["outcome"]
        )

    def _withdraw_ack(self, data: dict) -> dict:
        return self.coordinator.confirm_withdraw(
            data["site"], data["run"], data["reservation"]
        )

    def _void(self, data: dict) -> dict:
        return self.coordinator.request_void(data["site"], data["run"])

    def _receive(self, data: dict) -> dict:
        result = self.coordinator.receive(
            data["client"], data.get("site"), data.get("run")
        )
        return {"status": "ok", "delivery": result}

    def _ack(self, data: dict) -> dict:
        return self.coordinator.ack(data["client"], data["delivery"])

    def _start_next(self, data: dict) -> dict:
        return self.coordinator.start_next(data["site"], data["run"], data["next_run"])

    def _inspect(self, data: dict) -> dict:
        return {"status": "ok", "inspection": self.coordinator.inspect(data["site"], data["run"])}
