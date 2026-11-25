"""SQLite 持久层：事件日志与每消费方事务发件箱。

库文件即全部真相。领域事件与外发通知在同一个数据库事务内提交，
因此“决定已作出但通知丢失”或“通知已发但决定未落库”都不会发生。
"""

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from .events import EVENT_TYPES, Event
from .messages import CLIENTS, Delivery, ROUTING

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    site     TEXT NOT NULL,
    run      TEXT NOT NULL,
    type     TEXT NOT NULL,
    data     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_handover ON events(site, run, seq);

CREATE TABLE IF NOT EXISTS outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    client        TEXT NOT NULL,
    delivery_tag  TEXT NOT NULL,
    site          TEXT NOT NULL,
    run           TEXT NOT NULL,
    message_type  TEXT NOT NULL,
    payload       TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'pending',
    UNIQUE(client, delivery_tag)
);
CREATE INDEX IF NOT EXISTS outbox_client ON outbox(client, id);
"""


class Store:
    """线程内使用的持久化门面；每个方法自己管理短事务。"""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # HTTP 模式下请求由工作线程处理；写操作另有线程锁与 BEGIN IMMEDIATE
        # 共同保证串行化，关闭同线程检查是安全的
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- 事务与事件 -------------------------------------------------------

    @contextmanager
    def transaction(self):
        """串行写事务上下文。

        正常退出提交、异常退出回滚，无论走哪条分支（含幂等早返回）
        都会释放锁，杜绝“先拿锁后忘记释放”的死锁。
        """
        self._lock.acquire()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
        finally:
            self._lock.release()

    def append_event(
        self,
        conn: sqlite3.Connection,
        site: str,
        run: str,
        event_type: str,
        data: dict,
    ) -> Event:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"未知领域事件: {event_type}")
        cur = conn.execute(
            "INSERT INTO events(site, run, type, data) VALUES (?, ?, ?, ?)",
            (site, run, event_type, json.dumps(data, ensure_ascii=False, sort_keys=True)),
        )
        return Event(seq=cur.lastrowid, site=site, run=run, type=event_type, data=dict(data))

    def load_events(self, site: str | None = None, run: str | None = None) -> list[Event]:
        with self._lock:
            if site is not None and run is not None:
                rows = self._conn.execute(
                    "SELECT * FROM events WHERE site = ? AND run = ? ORDER BY seq",
                    (site, run),
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        return [
            Event(
                seq=row["seq"],
                site=row["site"],
                run=row["run"],
                type=row["type"],
                data=json.loads(row["data"]),
            )
            for row in rows
        ]

    def list_handovers(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT site, run FROM events ORDER BY site, run"
            ).fetchall()
        return [(row["site"], row["run"]) for row in rows]

    # ---- 发件箱 -----------------------------------------------------------

    def enqueue(
        self,
        conn: sqlite3.Connection,
        site: str,
        run: str,
        message_type: str,
        payload: dict,
        clients: tuple[str, ...] | None = None,
    ) -> list[Delivery]:
        """在当前事务内为每个目标消费方写入一条通知。

        投递标识 delivery:N 是该消费方发件箱内的单调序号，天然保证
        同一消费方按入箱顺序领取，received 不会排在 withdrawn 之后。
        """
        targets = clients if clients is not None else ROUTING[message_type]
        created: list[Delivery] = []
        for client in targets:
            if client not in CLIENTS:
                raise ValueError(f"未知消费方: {client}")
            cur = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM outbox WHERE client = ?",
                (client,),
            )
            next_id = cur.fetchone()["next_id"]
            tag = f"delivery:{next_id}"
            conn.execute(
                "INSERT INTO outbox(client, delivery_tag, site, run, message_type, payload)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    client,
                    tag,
                    site,
                    run,
                    message_type,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            created.append(
                Delivery(
                    id=next_id,
                    client=client,
                    delivery_tag=tag,
                    site=site,
                    run=run,
                    message_type=message_type,
                    payload=dict(payload),
                    state="pending",
                )
            )
        return created

    def fetch_for_receive(
        self, client: str, site: str | None = None, run: str | None = None
    ) -> Delivery | None:
        """模拟队列领取：先返回尚未 ack 的在途通知，否则取下一条待发通知。

        重发请求（receive）按身份恢复已知进度：只要上一条还没确认，
        总是返回同一个投递标识，不会跳过未处理决定。
        """
        row = None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbox WHERE client = ? AND state = 'delivered' ORDER BY id LIMIT 1",
                (client,),
            ).fetchone()
            if row is None:
                query = (
                    "SELECT * FROM outbox WHERE client = ? AND state = 'pending'"
                    + (" AND site = ?" if site is not None else "")
                    + (" AND run = ?" if run is not None else "")
                    + " ORDER BY id LIMIT 1"
                )
                params: list = [client]
                if site is not None:
                    params.append(site)
                if run is not None:
                    params.append(run)
                row = self._conn.execute(query, params).fetchone()
                if row is None:
                    return None
                self._conn.execute(
                    "UPDATE outbox SET state = 'delivered' WHERE id = ? AND client = ?",
                    (row["id"], client),
                )
                self._conn.commit()
        return self._row_to_delivery(row)

    def fetch_delivered(self, client: str) -> Delivery | None:
        """返回消费方当前在途（已领取未确认）的通知，不改变状态。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbox WHERE client = ? AND state = 'delivered' ORDER BY id LIMIT 1",
                (client,),
            ).fetchone()
        return self._row_to_delivery(row) if row is not None else None

    def ack(self, client: str, delivery_tag: str) -> tuple[Delivery, bool] | None:
        """确认一条通知，返回 (通知, 是否首次确认)。

        重复确认保持幂等：第二次起 first_ack=False，调用方不得再触发副作用，
        因此样本不会被算两份、后继资格也不会开两次。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbox WHERE client = ? AND delivery_tag = ?",
                (client, delivery_tag),
            ).fetchone()
            if row is None:
                return None
            first_ack = row["state"] != "acked"
            if first_ack:
                self._conn.execute(
                    "UPDATE outbox SET state = 'acked' WHERE id = ? AND client = ?",
                    (row["id"], client),
                )
                self._conn.commit()
                row = self._conn.execute(
                    "SELECT * FROM outbox WHERE id = ?", (row["id"],)
                ).fetchone()
        return self._row_to_delivery(row), first_ack

    def list_deliveries(
        self, client: str | None = None, site: str | None = None, run: str | None = None
    ) -> list[Delivery]:
        query = "SELECT * FROM outbox WHERE 1 = 1"
        params: list = []
        if client is not None:
            query += " AND client = ?"
            params.append(client)
        if site is not None:
            query += " AND site = ?"
            params.append(site)
        if run is not None:
            query += " AND run = ?"
            params.append(run)
        query += " ORDER BY client, id"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_delivery(row) for row in rows]

    @staticmethod
    def _row_to_delivery(row: sqlite3.Row) -> Delivery:
        return Delivery(
            id=row["id"],
            client=row["client"],
            delivery_tag=row["delivery_tag"],
            site=row["site"],
            run=row["run"],
            message_type=row["message_type"],
            payload=json.loads(row["payload"]),
            state=row["state"],
        )
