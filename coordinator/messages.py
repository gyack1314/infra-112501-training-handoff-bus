"""投递到下游消费方的通知消息。

协调器只产生两类消费方的可执行决定：

- ``sample-index``：样本接收方/接收索引，关注样本接收、撤回与作废闭环；
- ``schedule``：后续排班消费方，关注能否放行下一轮、撤回传播与暂停。

每条通知在每个消费方的发件箱里有独立投递标识与确认状态，双方互不代确认。
"""

from dataclasses import dataclass
from typing import Any

SAMPLE_INDEX = "sample-index"
SCHEDULE = "schedule"
CLIENTS = (SAMPLE_INDEX, SCHEDULE)

# 预留位置已确认留好（仍不是“已接收样本”，不可当作接收事实对外）
RESERVED = "reserved"
# 预留被拒收，采集方可提交新修订
REFUSED = "refused"
# 样本实际接收：排班端处理此消息后本轮才算完成、后继资格才开启
RECEIVED = "received"
# 确认未接收并完成作废
VOID_COMPLETED = "void_completed"
# 已接收样本撤回，引用原交接与预留；与 received 在同一消费方严格保序
WITHDRAWN = "withdrawn"
# 作废意图待外部确认：排班端暂停发放新的开始许可
VOID_PENDING = "void_pending"

MESSAGE_TYPES = frozenset(
    {RESERVED, REFUSED, RECEIVED, VOID_COMPLETED, WITHDRAWN, VOID_PENDING}
)

# 各消息类型应投递给哪些消费方
ROUTING: dict[str, tuple[str, ...]] = {
    RESERVED: (SAMPLE_INDEX, SCHEDULE),
    REFUSED: (SAMPLE_INDEX, SCHEDULE),
    RECEIVED: (SAMPLE_INDEX, SCHEDULE),
    VOID_COMPLETED: (SAMPLE_INDEX, SCHEDULE),
    WITHDRAWN: (SAMPLE_INDEX, SCHEDULE),
    VOID_PENDING: (SCHEDULE,),
}


@dataclass(frozen=True)
class Delivery:
    """一条发件箱通知及其投递/确认状态。"""

    id: int
    client: str
    delivery_tag: str
    site: str
    run: str
    message_type: str
    payload: dict[str, Any]
    state: str  # pending | delivered | acked

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "client": self.client,
            "delivery": self.delivery_tag,
            "site": self.site,
            "run": self.run,
            "message": self.message_type,
            "state": self.state,
            "payload": self.payload,
        }
