"""读取本地合成事件的时间与消息封装。"""

import argparse
import json
from pathlib import Path


def read_trace(path, types):
    previous = -1
    with Path(path).open(encoding="utf-8") as source:
        for row_number, line in enumerate(source, 1):
            try:
                event = json.loads(line)
                if not isinstance(event, dict) or set(event) != {"timestamp", "type", "data"}:
                    raise ValueError("事件应包含 timestamp、type、data")
                time = event["timestamp"]
                if type(time) is not int or time < 0 or time < previous:
                    raise ValueError("逻辑秒点应为非递减的非负整数")
                if event["type"] not in types or not isinstance(event["data"], dict):
                    raise ValueError("事件种类未知或 data 不是对象")
            except (ValueError, TypeError) as error:
                raise ValueError(f"{path}:{row_number} {error}") from error
            previous = time
            yield event


def inspect(directory):
    allowed = set(json.loads((directory / "event_types.json").read_text(encoding="utf-8")))
    for path in sorted(directory.glob("*.jsonl")):
        events = list(read_trace(path, allowed))
        if not events:
            raise ValueError(f"{path} 没有事件")
        print(f"{path.name}　{len(events)} 条事件　{events[0]['timestamp']} 至 {events[-1]['timestamp']} 秒")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="读取合成演练的消息封装")
    parser.add_argument("directory", type=Path)
    inspect(parser.parse_args().directory)
