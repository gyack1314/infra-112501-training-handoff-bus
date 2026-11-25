"""检查资料入口及消息封装。"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from trace_io import read_trace


class TraceTests(unittest.TestCase):
    def test_all_scenarios_can_be_read(self):
        directory = Path(__file__).with_name("data")
        allowed = set(json.loads((directory / "event_types.json").read_text(encoding="utf-8")))
        scenarios = sorted(directory.glob("*.jsonl"))
        self.assertTrue(scenarios)
        for path in scenarios:
            with self.subTest(path=path.name):
                rows = list(read_trace(path, allowed))
                self.assertTrue(rows)
                self.assertEqual(sorted(row["timestamp"] for row in rows), [row["timestamp"] for row in rows])

    def test_bad_time_reports_location(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.jsonl"
            path.write_text(
                '{"timestamp":3,"type":"hello","data":{}}\n'
                '{"timestamp":2,"type":"hello","data":{}}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, r"invalid\.jsonl:2"):
                list(read_trace(path, {"hello"}))


if __name__ == "__main__":
    unittest.main()
