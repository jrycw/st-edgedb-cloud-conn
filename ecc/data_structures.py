from enum import Enum, auto
from typing import Any, NamedTuple


class RespJson(Enum):
    NO = auto()
    YES = auto()


class RespConstraint(Enum):
    FREE = auto()
    NO_MORE_THAN_ONE = auto()
    EXACTLY_ONE = auto()


class QueryRecord(NamedTuple):
    qry: str
    extra_args: tuple[Any, ...]
    jsonify: RespJson
    required_single: RespConstraint
    extra_kwargs: dict[str, Any]
    task_name: str
