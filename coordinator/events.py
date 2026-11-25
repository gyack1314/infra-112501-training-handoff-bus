"""领域事件定义。

协调器不直接修改可变状态，而是把每一次状态变化写成不可变事件，
重放事件即可重建任意一轮交接的完整进度。
"""

from dataclasses import dataclass
from typing import Any

# 清单登记：同一 site/run/revision 的内容（collection）必须稳定
MANIFEST_SUBMITTED = "manifest_submitted"
# 修订号相同但 collection 变化：保留冲突，不覆盖正在处理的清单
MANIFEST_CONFLICT = "manifest_conflict"
# 复核签认到达（accepted/rejected），按修订与用途绑定
SIGNOFF_RECORDED = "signoff_recorded"
# 清单满足要求，向样本接收方请求预留位置
RESERVATION_REQUESTED = "reservation_requested"
# 接收方对预留的回应：reserved（留好位置，尚未接收）或 refused
PREPARE_RECORDED = "prepare_recorded"
# 预留位置释放（拒收后、确认未接收后），释放后同位置才可再次占用
RESERVATION_RELEASED = "reservation_released"
# 接收方报告实际提交
RECEIPT_RECORDED = "receipt_recorded"
# 运维登记作废意图（外部回执未决时只登记、不终结）
VOID_REQUESTED = "void_requested"
# 确认未接收，作废闭环
VOID_COMPLETED = "void_completed"
# 已接收样本的作废：请求接收方撤回
WITHDRAW_REQUESTED = "withdraw_requested"
# 接收方确认撤回特定预留
WITHDRAW_CONFIRMED = "withdraw_confirmed"
# 后继作业资格开启（仅在排班端处理完 received 通知后）
SUCCESSOR_OPENED = "successor_opened"
# 后继作业已经开始（历史事实，不可改写）
SUCCESSOR_STARTED = "successor_started"
# 撤回传播后取消尚未开始的后继资格
SUCCESSOR_CANCELLED = "successor_cancelled"
# 撤回传播时后继已开始：转人工处理
SUCCESSOR_MANUAL = "successor_manual"

EVENT_TYPES = frozenset(
    {
        MANIFEST_SUBMITTED,
        MANIFEST_CONFLICT,
        SIGNOFF_RECORDED,
        RESERVATION_REQUESTED,
        PREPARE_RECORDED,
        RESERVATION_RELEASED,
        RECEIPT_RECORDED,
        VOID_REQUESTED,
        VOID_COMPLETED,
        WITHDRAW_REQUESTED,
        WITHDRAW_CONFIRMED,
        SUCCESSOR_OPENED,
        SUCCESSOR_STARTED,
        SUCCESSOR_CANCELLED,
        SUCCESSOR_MANUAL,
    }
)


@dataclass(frozen=True)
class Event:
    """一条不可变领域事件。"""

    seq: int
    site: str
    run: str
    type: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "site": self.site,
            "run": self.run,
            "type": self.type,
            "data": self.data,
        }
