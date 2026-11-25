"""训练采集交接的事务消息协调服务。

协调器位于采集/复核（作业来源）、样本接收方与排班消费方之间，以
``(site, run)`` 作为一轮交接的业务身份。内部采用事件溯源保存全部状态变化，
并为每个下游消费方维护独立的事务发件箱（outbox）：

- 决定先落库再投递，服务重启后继续履行已作出的决定；
- 两个消费方各自领取、各自确认，一方已处理不代表另一方可以跳过；
- 外部状态不明（如接收回执 unknown）时保留待确认，不用猜测推进状态。
"""

from .service import Coordinator
from .store import Store
from .errors import CoordinationError, ConflictError, UnknownHandoverError

__all__ = [
    "Coordinator",
    "Store",
    "CoordinationError",
    "ConflictError",
    "UnknownHandoverError",
]
