"""事件重放：把不可变事件折叠成一轮交接的当前状态。"""

from dataclasses import dataclass, field

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

# 交接阶段（派生值，便于查询）
PHASE_AWAITING = "awaiting_manifest"       # 尚无清单
PHASE_PENDING_REVIEW = "pending_review"    # 有清单但签认未齐
PHASE_AWAIT_SLOT = "awaiting_slot"         # 签认齐，等待/处理预留
PHASE_RESERVED = "reserved"                # 位置留好，等待实际接收结果
PHASE_RECEIVED = "received"                # 样本确实接收
PHASE_REFUSED = "refused"                  # 预留被拒收，等待新修订
PHASE_VOID_PENDING = "void_pending"        # 作废意图待外部确认
PHASE_VOIDED = "voided"                    # 未接收即作废闭环
PHASE_WITHDRAW_PENDING = "withdraw_pending"  # 已接收，等待接收方撤回
PHASE_WITHDRAWN = "withdrawn"              # 撤回已确认

# 预留状态
SLOT_FREE = "free"
SLOT_REQUESTED = "requested"
SLOT_RESERVED = "reserved"
SLOT_REFUSED = "refused"
SLOT_RELEASED = "released"

# 后继资格状态
ELIG_OPEN = "open"        # 可开始但尚未开始
ELIG_STARTED = "started"  # 已开始
ELIG_CANCELLED = "cancelled"
ELIG_MANUAL = "manual"


@dataclass
class Manifest:
    revision: int
    collection: str
    use: str
    signoffs: dict[str, str] = field(default_factory=dict)  # use -> decision
    conflicts: list[str] = field(default_factory=list)      # 冲突指纹
    superseded: bool = False


@dataclass
class Reservation:
    reservation_id: str
    revision: int
    slot: str = SLOT_REQUESTED
    receipt: str | None = None  # committed | not_committed | unknown


@dataclass
class Successor:
    state: str = ELIG_OPEN
    next_run: str | None = None


@dataclass
class Handover:
    site: str
    run: str
    manifests: dict[int, Manifest] = field(default_factory=dict)
    reservations: list[Reservation] = field(default_factory=list)
    void_registered: bool = False
    void_terminal: bool = False
    withdrawn: bool = False
    successor: Successor | None = None
    last_event_seq: int = 0

    # ---- 派生视图 ---------------------------------------------------------

    @property
    def current_revision(self) -> int | None:
        return max(self.manifests) if self.manifests else None

    @property
    def current_manifest(self) -> Manifest | None:
        rev = self.current_revision
        return self.manifests.get(rev) if rev is not None else None

    @property
    def active_reservation(self) -> Reservation | None:
        """最近一项预留；其 slot 表示位置当前是否还被占用。"""
        return self.reservations[-1] if self.reservations else None

    @property
    def slot_busy(self) -> bool:
        active = self.active_reservation
        return active is not None and active.slot in (SLOT_REQUESTED, SLOT_RESERVED)

    @property
    def current_accepted(self) -> bool:
        """当前清单是否持有与自身用途匹配的 accepted 签认。

        签认只对它引用的修订与用途有效；旧修订的签认不会让新清单通过。
        """
        manifest = self.current_manifest
        if manifest is None:
            return False
        return manifest.signoffs.get(manifest.use) == "accepted"

    @property
    def received(self) -> bool:
        active = self.active_reservation
        return active is not None and active.receipt == "committed"

    @property
    def phase(self) -> str:
        if self.withdrawn:
            return PHASE_WITHDRAWN
        if self.void_terminal:
            return PHASE_VOIDED
        if self.received:
            if self.void_registered:
                return PHASE_WITHDRAW_PENDING
            return PHASE_RECEIVED
        if self.void_registered:
            return PHASE_VOID_PENDING
        if not self.manifests:
            return PHASE_AWAITING
        active = self.active_reservation
        if active is not None and active.slot == SLOT_RESERVED:
            return PHASE_RESERVED
        if active is not None and active.slot == SLOT_REQUESTED:
            return PHASE_AWAIT_SLOT
        if active is not None and active.slot == SLOT_REFUSED:
            return PHASE_REFUSED
        return PHASE_PENDING_REVIEW


def apply_event(handover: Handover, event: Event) -> None:
    """把单条事件折叠进交接状态（纯函数式更新，无副作用）。"""
    data = event.data
    handover.last_event_seq = event.seq
    etype = event.type

    if etype == MANIFEST_SUBMITTED:
        revision = data["revision"]
        older = [m for rev, m in handover.manifests.items() if rev < revision]
        for manifest in older:
            manifest.superseded = True
        handover.manifests[revision] = Manifest(
            revision=revision, collection=data["collection"], use=data["use"]
        )

    elif etype == MANIFEST_CONFLICT:
        manifest = handover.manifests.get(data["revision"])
        if manifest is not None:
            manifest.conflicts.append(data["collection"])

    elif etype == SIGNOFF_RECORDED:
        manifest = handover.manifests.get(data["revision"])
        if manifest is not None:
            # 晚到的旧签认只挂在它引用的修订上，不影响当前修订
            manifest.signoffs[data["use"]] = data["decision"]

    elif etype == RESERVATION_REQUESTED:
        handover.reservations.append(
            Reservation(reservation_id=data["reservation"], revision=data["revision"])
        )

    elif etype == PREPARE_RECORDED:
        reservation = _find_reservation(handover, data["reservation"])
        if reservation is not None:
            reservation.slot = SLOT_RESERVED if data["outcome"] == "reserved" else SLOT_REFUSED
            if reservation.slot == SLOT_REFUSED:
                # 拒收即释放该位置，采集方可以处理新修订
                reservation.slot = SLOT_RELEASED

    elif etype == RESERVATION_RELEASED:
        reservation = _find_reservation(handover, data["reservation"])
        if reservation is not None:
            reservation.slot = SLOT_RELEASED

    elif etype == RECEIPT_RECORDED:
        reservation = _find_reservation(handover, data["reservation"])
        if reservation is not None:
            reservation.receipt = data["outcome"]
            if data["outcome"] == "not_committed":
                reservation.slot = SLOT_RELEASED

    elif etype == VOID_REQUESTED:
        handover.void_registered = True

    elif etype == VOID_COMPLETED:
        handover.void_registered = True
        handover.void_terminal = True

    elif etype == WITHDRAW_REQUESTED:
        handover.void_registered = True

    elif etype == WITHDRAW_CONFIRMED:
        handover.void_registered = True
        handover.void_terminal = True
        handover.withdrawn = True
        reservation = _find_reservation(handover, data["reservation"])
        if reservation is not None:
            reservation.slot = SLOT_RELEASED
        if handover.successor is not None:
            if handover.successor.state == ELIG_STARTED:
                handover.successor.state = ELIG_MANUAL
            elif handover.successor.state == ELIG_OPEN:
                handover.successor.state = ELIG_CANCELLED

    elif etype == SUCCESSOR_OPENED:
        handover.successor = Successor(state=ELIG_OPEN)

    elif etype == SUCCESSOR_STARTED:
        if handover.successor is None:
            handover.successor = Successor(state=ELIG_STARTED, next_run=data["next_run"])
        else:
            handover.successor.state = ELIG_STARTED
            handover.successor.next_run = data["next_run"]

    elif etype == SUCCESSOR_CANCELLED:
        if handover.successor is not None:
            handover.successor.state = ELIG_CANCELLED

    elif etype == SUCCESSOR_MANUAL:
        if handover.successor is not None:
            handover.successor.state = ELIG_MANUAL


def _find_reservation(handover: Handover, reservation_id: str) -> Reservation | None:
    for reservation in reversed(handover.reservations):
        if reservation.reservation_id == reservation_id:
            return reservation
    return None


def rebuild(events: list[Event], site: str, run: str) -> Handover:
    handover = Handover(site=site, run=run)
    for event in events:
        apply_event(handover, event)
    return handover
