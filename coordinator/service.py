"""事务消息协调器：接收各方输入，产出可查询结果与可执行决定。

所有命令遵循同一套纪律：

1. 开启串行事务，重放事件得到当前状态；
2. 校验业务规则，把状态变化写成新事件，并在同一事务内写入发件箱；
3. 提交。校验失败则回滚，外部状态不明时只登记“待确认”，绝不猜测推进。
"""

from typing import Any

from .errors import CoordinationError, UnknownHandoverError
from .events import (
    Event,
    MANIFEST_CONFLICT,
    MANIFEST_SUBMITTED,
    PREPARE_RECORDED,
    RECEIPT_RECORDED,
    RESERVATION_RELEASED,
    RESERVATION_REQUESTED,
    SIGNOFF_RECORDED,
    SUCCESSOR_CANCELLED,
    SUCCESSOR_MANUAL,
    SUCCESSOR_OPENED,
    SUCCESSOR_STARTED,
    VOID_COMPLETED,
    VOID_REQUESTED,
    WITHDRAW_CONFIRMED,
    WITHDRAW_REQUESTED,
)
from .messages import (
    CLIENTS,
    RECEIVED,
    REFUSED,
    RESERVED,
    SAMPLE_INDEX,
    SCHEDULE,
    VOID_COMPLETED as MSG_VOID_COMPLETED,
    VOID_PENDING,
    WITHDRAWN,
    Delivery,
)
from .state import (
    ELIG_OPEN,
    ELIG_STARTED,
    Handover,
    SLOT_RELEASED,
    SLOT_REQUESTED,
    rebuild,
)
from .store import Store


class Coordinator:
    def __init__(self, store: Store):
        self.store = store

    # ======================================================================
    # 采集端：清单提交
    # ======================================================================

    def submit_manifest(
        self, site: str, run: str, revision: int, collection: str, use: str
    ) -> dict[str, Any]:
        self._require_text(site=site, run=run, collection=collection, use=use)
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise CoordinationError("revision 必须是不小于 1 的整数")

        with self.store.transaction() as conn:
            handover = self._load(conn, site, run, create=True)
            existing = handover.manifests.get(revision)
            if existing is not None:
                if existing.collection == collection and existing.use == use:
                    # 重放/重送同一份清单：幂等，不产生新事件
                    return self._result("idempotent", handover)
                # 相同修订号换了内容：留冲突，绝不覆盖正在处理的清单
                event = self.store.append_event(
                    conn, site, run, MANIFEST_CONFLICT,
                    {"revision": revision, "collection": collection,
                     "expected": existing.collection},
                )
            else:
                event = self.store.append_event(
                    conn, site, run, MANIFEST_SUBMITTED,
                    {"revision": revision, "collection": collection, "use": use},
                )
            deliveries = self._maybe_request_reservation(conn, site, run)

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result(
            "conflict" if existing is not None else "ok", refreshed, [event], deliveries
        )

    # ======================================================================
    # 质量复核：签认
    # ======================================================================

    def record_signoff(
        self, site: str, run: str, revision: int, use: str, decision: str
    ) -> dict[str, Any]:
        if decision not in ("accepted", "rejected"):
            raise CoordinationError("decision 只能是 accepted 或 rejected")
        self._require_text(use=use)
        with self.store.transaction() as conn:
            handover = self._load(conn, site, run)
            manifest = handover.manifests.get(revision)
            if manifest is None:
                raise CoordinationError(
                    f"签认引用的清单 {site}/{run}/revision={revision} 不存在"
                )
            if manifest.signoffs.get(use) == decision:
                return self._result("idempotent", handover)
            # 签认只对它引用的修订与用途有效；晚到的旧签认挂在旧修订上，
            # 重放后不会让当前新清单通过
            event = self.store.append_event(
                conn, site, run, SIGNOFF_RECORDED,
                {"revision": revision, "use": use, "decision": decision},
            )
            deliveries = self._maybe_request_reservation(conn, site, run)

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result("ok", refreshed, [event], deliveries)

    # ======================================================================
    # 样本接收方：预留回应
    # ======================================================================

    def record_prepare_ack(
        self, site: str, run: str, revision: int, reservation: str, outcome: str
    ) -> dict[str, Any]:
        if outcome not in ("reserved", "refused"):
            raise CoordinationError("prepare_ack 的 outcome 只能是 reserved 或 refused")
        with self.store.transaction() as conn:
            handover = self._load(conn, site, run)
            target = self._find_reservation(handover, reservation)
            if target is None or target.revision != revision:
                raise CoordinationError(f"预留 {reservation} 与修订 {revision} 不匹配")
            if target.slot != SLOT_REQUESTED:
                if (outcome == "reserved" and target.slot == "reserved") or (
                    outcome == "refused"
                    and target.slot == SLOT_RELEASED
                    and target.receipt is None
                ):
                    return self._result("idempotent", handover)
                raise CoordinationError(
                    f"预留 {reservation} 已处于 {target.slot}，不能再登记 {outcome}"
                )

            events: list[Event] = []
            deliveries: list[Delivery] = []
            events.append(
                self.store.append_event(
                    conn, site, run, PREPARE_RECORDED,
                    {"reservation": reservation, "revision": revision, "outcome": outcome},
                )
            )
            if outcome == "reserved":
                # 位置留好，但还不是已接收样本，不可对外当作接收事实
                deliveries += self.store.enqueue(
                    conn, site, run, RESERVED,
                    {"reservation": reservation, "revision": revision},
                )
            else:
                # 拒收即释放位置，采集方才可以处理新修订
                events.append(
                    self.store.append_event(
                        conn, site, run, RESERVATION_RELEASED,
                        {"reservation": reservation, "revision": revision,
                         "reason": "refused"},
                    )
                )
                deliveries += self.store.enqueue(
                    conn, site, run, REFUSED,
                    {"reservation": reservation, "revision": revision},
                )
                # 拒收回执追上作废意图：位置已可放弃，作废闭环
                if handover.void_registered and not handover.void_terminal:
                    events += self._complete_void(conn, site, run, reservation)
                    deliveries += self._enqueue_void_completed(conn, site, run, reservation)

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result("ok", refreshed, events, deliveries)

    # ======================================================================
    # 样本接收方：实际接收回执
    # ======================================================================

    def record_receipt(
        self, site: str, run: str, reservation: str, outcome: str
    ) -> dict[str, Any]:
        if outcome not in ("committed", "not_committed", "unknown"):
            raise CoordinationError(
                "receipt 的 outcome 只能是 committed/not_committed/unknown"
            )
        with self.store.transaction() as conn:
            handover = self._load(conn, site, run)
            target = self._find_reservation(handover, reservation)
            if target is None:
                raise CoordinationError(f"预留 {reservation} 不存在")

            # 已接收是必须保留的事实：后续 unknown/not_committed 不能抹掉它
            if target.receipt == outcome:
                return self._result("idempotent", handover)
            if target.receipt == "committed":
                if outcome == "unknown":
                    return self._result("idempotent", handover)
                raise CoordinationError("该预留已确认接收，不能再改写为未接收")
            if target.receipt == "not_committed":
                raise CoordinationError("该预留已确认今后不能提交，迟到的接收回执与事实冲突")

            events: list[Event] = []
            deliveries: list[Delivery] = []
            events.append(
                self.store.append_event(
                    conn, site, run, RECEIPT_RECORDED,
                    {"reservation": reservation, "outcome": outcome},
                )
            )

            if outcome == "committed":
                deliveries += self.store.enqueue(
                    conn, site, run, RECEIVED,
                    {"reservation": reservation, "revision": target.revision},
                )
            elif outcome == "not_committed":
                # 外部确认永远不会提交：释放预留
                events.append(
                    self.store.append_event(
                        conn, site, run, RESERVATION_RELEASED,
                        {"reservation": reservation, "revision": target.revision,
                         "reason": "not_committed"},
                    )
                )
                if handover.void_registered and not handover.void_terminal:
                    events += self._complete_void(conn, site, run, reservation)
                    deliveries += self._enqueue_void_completed(conn, site, run, reservation)
            # unknown：外部状态不明，保留待确认，不发任何可执行决定

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result("ok", refreshed, events, deliveries)

    # ======================================================================
    # 运维：作废意图
    # ======================================================================

    def request_void(self, site: str, run: str) -> dict[str, Any]:
        with self.store.transaction() as conn:
            handover = self._load(conn, site, run)
            if handover.void_registered:
                return self._result("idempotent", handover)

            events: list[Event] = []
            deliveries: list[Delivery] = []
            active = handover.active_reservation
            can_abandon_now = (
                active is None
                or active.slot == SLOT_RELEASED
                or active.receipt == "not_committed"
            )
            if can_abandon_now:
                # 能确定指定预留可放弃：直接给出最终结果
                reservation_id = active.reservation_id if active else None
                events += self._complete_void(conn, site, run, reservation_id)
                deliveries += self._enqueue_void_completed(conn, site, run, reservation_id)
            else:
                # 接收回执未确认（reserved/requested/unknown）：只登记意图。
                # 排班端收到 void_pending 后暂停发放新的开始许可。
                events.append(
                    self.store.append_event(conn, site, run, VOID_REQUESTED, {})
                )
                deliveries += self.store.enqueue(
                    conn, site, run, VOID_PENDING, {"reason": "void_registered"}
                )
                if active is not None and active.receipt == "committed":
                    # 已经接收：保留接收事实，改走撤回流程，等待接收方确认撤回
                    events.append(
                        self.store.append_event(
                            conn, site, run, WITHDRAW_REQUESTED,
                            {"reservation": active.reservation_id},
                        )
                    )

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result("ok", refreshed, events, deliveries)

    # ======================================================================
    # 样本接收方：撤回确认
    # ======================================================================

    def confirm_withdraw(self, site: str, run: str, reservation: str) -> dict[str, Any]:
        with self.store.transaction() as conn:
            handover = self._load(conn, site, run)
            target = self._find_reservation(handover, reservation)
            if target is None:
                raise CoordinationError(f"预留 {reservation} 不存在")
            if handover.withdrawn:
                return self._result("idempotent", handover)
            if not (handover.void_registered and target.receipt == "committed"):
                raise CoordinationError(
                    "只有已接收样本且已登记作废的交接才能确认撤回"
                )
            event = self.store.append_event(
                conn, site, run, WITHDRAW_CONFIRMED,
                {"reservation": reservation, "revision": target.revision},
            )
            # 撤回消息引用原交接；发件箱按序号投递，天然保证 received 先于 withdrawn
            deliveries = self.store.enqueue(
                conn, site, run, WITHDRAWN,
                {"reservation": reservation, "revision": target.revision,
                 "original_handover": {"site": site, "run": run}},
            )

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result("ok", refreshed, [event], deliveries)

    # ======================================================================
    # 消费方：领取、确认与后继作业
    # ======================================================================

    def receive(
        self, client: str, site: str | None = None, run: str | None = None
    ) -> dict[str, Any] | None:
        if client not in CLIENTS:
            raise CoordinationError(f"未知消费方: {client}")
        delivery = self.store.fetch_for_receive(client, site, run)
        return delivery.to_dict() if delivery is not None else None

    def ack(self, client: str, delivery_tag: str) -> dict[str, Any]:
        if client not in CLIENTS:
            raise CoordinationError(f"未知消费方: {client}")
        result = self.store.ack(client, delivery_tag)
        if result is None:
            # 资料契约里 delivery:N 表示“该消费方自开头收到的第 N 次通知”，
            # 若外部按这个序号引用而标识不直接匹配，则回退到该消费方当前
            # 在途（已领取未确认）的同序号通知，恢复已知进度而不是报错丢失
            in_flight = self.store.fetch_delivered(client)
            if in_flight is None:
                raise CoordinationError(
                    f"投递标识 {delivery_tag} 对 {client} 不存在且无在途通知"
                )
            result = self.store.ack(client, in_flight.delivery_tag)
        delivery, first_ack = result

        # 仅首次确认触发领域副作用；重复确认保持幂等，
        # 不会让样本变成两份或开启两次后继资格
        side_events: list[Event] = (
            self._apply_ack_side_effect(client, delivery) if first_ack else []
        )
        handover = rebuild(
            self.store.load_events(delivery.site, delivery.run),
            delivery.site, delivery.run,
        )
        return {
            "status": "ok",
            "first_ack": first_ack,
            "delivery": delivery.to_dict(),
            "side_effect_events": [event.to_dict() for event in side_events],
            "handover": self.inspect(delivery.site, delivery.run, handover=handover),
        }

    def _apply_ack_side_effect(self, client: str, delivery: Delivery) -> list[Event]:
        """消费方首认通知时触发的领域副作用（双方各自独立触发）。"""
        if client != SCHEDULE:
            return []
        events: list[Event] = []
        with self.store.transaction() as conn:
            handover = self._load(conn, delivery.site, delivery.run)
            if delivery.message_type == RECEIVED:
                # 排班端处理完接收消息后才看作本轮完成，开启唯一一次后继资格
                if handover.successor is None and not handover.void_registered:
                    events.append(
                        self.store.append_event(
                            conn, delivery.site, delivery.run, SUCCESSOR_OPENED, {}
                        )
                    )
            elif delivery.message_type == WITHDRAWN:
                # 处理撤回：未开始的资格取消；已开始的转人工，不改写历史
                if handover.successor is not None:
                    if handover.successor.state == ELIG_STARTED:
                        events.append(
                            self.store.append_event(
                                conn, delivery.site, delivery.run, SUCCESSOR_MANUAL,
                                {"next_run": handover.successor.next_run},
                            )
                        )
                    elif handover.successor.state == ELIG_OPEN:
                        events.append(
                            self.store.append_event(
                                conn, delivery.site, delivery.run,
                                SUCCESSOR_CANCELLED, {},
                            )
                        )
        return events

    def start_next(self, site: str, run: str, next_run: str) -> dict[str, Any]:
        self._require_text(next_run=next_run)
        with self.store.transaction() as conn:
            handover = self._load(conn, site, run)
            successor = handover.successor
            if (
                successor is not None
                and successor.next_run == next_run
                and successor.state == ELIG_STARTED
            ):
                return self._result("idempotent", handover)
            if handover.void_registered and not handover.withdrawn:
                raise CoordinationError("作废意图待确认，排班端暂停发放新的开始许可")
            if successor is None:
                raise CoordinationError("接收消息尚未经排班端处理，本轮未取得后继资格")
            if successor.state != ELIG_OPEN:
                raise CoordinationError(
                    f"后继资格状态为 {successor.state}，不能再开始后继作业"
                )
            event = self.store.append_event(
                conn, site, run, SUCCESSOR_STARTED, {"next_run": next_run}
            )

        refreshed = rebuild(self.store.load_events(site, run), site, run)
        return self._result("ok", refreshed, [event])

    # ======================================================================
    # 查询
    # ======================================================================

    def list_handovers(self) -> list[dict[str, str]]:
        return [
            {"site": site, "run": run}
            for site, run in self.store.list_handovers()
        ]

    def inspect(
        self, site: str, run: str, handover: Handover | None = None
    ) -> dict[str, Any]:
        if handover is None:
            events = self.store.load_events(site, run)
            if not events:
                raise UnknownHandoverError(f"尚无 {site}/{run} 的任何记录")
            handover = rebuild(events, site, run)

        deliveries = self.store.list_deliveries(site=site, run=run)
        missing_acks: dict[str, list[dict[str, str]]] = {
            client: [
                {"delivery": d.delivery_tag, "message": d.message_type, "state": d.state}
                for d in deliveries
                if d.client == client and d.state != "acked"
            ]
            for client in CLIENTS
        }

        withdrawn_delivery = {
            client: next(
                (
                    {"delivery": d.delivery_tag, "state": d.state}
                    for d in deliveries
                    if d.client == client and d.message_type == WITHDRAWN
                ),
                None,
            )
            for client in CLIENTS
        }
        withdrawal_propagation = (
            {
                "withdrawn": handover.withdrawn,
                "deliveries": withdrawn_delivery,
                "propagated_to_all": all(
                    item is not None and item["state"] == "acked"
                    for item in withdrawn_delivery.values()
                ),
            }
            if handover.void_registered
            else None
        )

        can_start, reason = self._can_start_reason(handover)
        return {
            "site": site,
            "run": run,
            "phase": handover.phase,
            "current_revision": handover.current_revision,
            "manifests": [
                {
                    "revision": manifest.revision,
                    "collection": manifest.collection,
                    "use": manifest.use,
                    "signoffs": dict(manifest.signoffs),
                    "accepted_for_use": manifest.signoffs.get(manifest.use) == "accepted",
                    "conflicts": list(manifest.conflicts),
                    "superseded": manifest.superseded,
                }
                for manifest in sorted(
                    handover.manifests.values(), key=lambda m: m.revision
                )
            ],
            "reservations": [
                {
                    "reservation": r.reservation_id,
                    "revision": r.revision,
                    "slot": r.slot,
                    "receipt": r.receipt,
                }
                for r in handover.reservations
            ],
            "slot_busy": handover.slot_busy,
            "void_registered": handover.void_registered,
            "void_terminal": handover.void_terminal,
            "received": handover.received,
            "withdrawn": handover.withdrawn,
            "successor": (
                {
                    "state": handover.successor.state,
                    "next_run": handover.successor.next_run,
                }
                if handover.successor is not None
                else None
            ),
            "can_start_next": {"allowed": can_start, "reason": reason},
            "missing_acks": missing_acks,
            "withdrawal_propagation": withdrawal_propagation,
            "deliveries": [d.to_dict() for d in deliveries],
        }

    # ======================================================================
    # 内部辅助
    # ======================================================================

    def _maybe_request_reservation(
        self, conn, site: str, run: str
    ) -> list[Delivery]:
        """清单满足要求后请求预留；位置未释放或已为当前修订请求过则不占用。"""
        handover = rebuild(self.store.load_events(site, run), site, run)
        if handover.void_registered or not handover.current_accepted or handover.slot_busy:
            return []
        active = handover.active_reservation
        current_rev = handover.current_revision
        if active is not None and active.revision == current_rev:
            # 同一修订的预留已释放（如拒收）：没有新修订就不自动再次占用
            return []
        reservation_id = f"reservation:{len(handover.reservations) + 1}"
        self.store.append_event(
            conn, site, run, RESERVATION_REQUESTED,
            {"reservation": reservation_id, "revision": current_rev},
        )
        # 预留请求是给样本接收方的指令，本身不进消费方发件箱；
        # 可执行决定（reserved/refused）在回执到达后投递
        return []

    def _complete_void(
        self, conn, site: str, run: str, reservation_id: str | None
    ) -> list[Event]:
        return [
            self.store.append_event(
                conn, site, run, VOID_COMPLETED,
                {"reservation": reservation_id},
            )
        ]

    def _enqueue_void_completed(
        self, conn, site: str, run: str, reservation_id: str | None
    ) -> list[Delivery]:
        return self.store.enqueue(
            conn, site, run, MSG_VOID_COMPLETED,
            {"reservation": reservation_id, "outcome": "not_received"},
        )

    def _can_start_reason(self, handover: Handover) -> tuple[bool, str]:
        if handover.withdrawn:
            return False, "交接已撤回"
        if handover.void_registered and not handover.void_terminal:
            return False, "作废意图待确认，暂停发放开始许可"
        if handover.void_terminal:
            return False, "交接已作废"
        if handover.successor is None:
            return False, "排班端尚未处理接收消息，后继资格未开启"
        if handover.successor.state == ELIG_STARTED:
            return False, "后继作业已经开始"
        if handover.successor.state != ELIG_OPEN:
            return False, f"后继资格状态为 {handover.successor.state}"
        return True, "可以开始后继作业"

    def _load(self, conn, site: str, run: str, create: bool = False) -> Handover:
        events = self.store.load_events(site, run)
        if not events and not create:
            raise UnknownHandoverError(f"尚无 {site}/{run} 的任何记录")
        return rebuild(events, site, run)

    @staticmethod
    def _find_reservation(handover: Handover, reservation_id: str):
        for reservation in reversed(handover.reservations):
            if reservation.reservation_id == reservation_id:
                return reservation
        return None

    @staticmethod
    def _require_text(**values: str) -> None:
        for name, value in values.items():
            if not isinstance(value, str) or not value.strip():
                raise CoordinationError(f"{name} 必须是非空字符串")

    def _result(
        self,
        status: str,
        handover: Handover,
        events: list[Event] | None = None,
        deliveries: list[Delivery] | None = None,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "events": [event.to_dict() for event in (events or [])],
            "deliveries": [delivery.to_dict() for delivery in (deliveries or [])],
            "phase": handover.phase,
        }
