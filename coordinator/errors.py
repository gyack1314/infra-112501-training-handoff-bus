"""协调服务的领域错误。"""


class CoordinationError(Exception):
    """命令违反交接状态机或消息契约。"""


class ConflictError(CoordinationError):
    """同一修订号携带了与在处理清单不同的内容指纹。"""


class UnknownHandoverError(CoordinationError):
    """引用的站点/轮次尚未登记任何清单。"""
