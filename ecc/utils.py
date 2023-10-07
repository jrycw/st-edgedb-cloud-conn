import logging
from typing import Any, assert_never

import tomllib

from .data_structures import RespConstraint, RespJson


def get_logger(logger_name: str = 'edgedb-cloud') -> logging.Logger:
    return logging.getLogger(logger_name)


def load_toml(toml_name: str = 'edgedbcloud.toml',
              table_name: str = 'edgedb-cloud') -> dict[str, Any]:
    with open(toml_name, 'rb') as f:
        data: dict[str, dict[str, Any]] = tomllib.load(f)
    return data[table_name]


def match_func_name(jsonify: RespJson, required_single: RespConstraint) -> str:
    match (jsonify, required_single):
        case (RespJson.NO, RespConstraint.FREE):
            func_name = 'query'
        case (RespJson.NO, RespConstraint.NO_MORE_THAN_ONE):
            func_name = 'query_single'
        case (RespJson.NO, RespConstraint.EXACTLY_ONE):
            func_name = 'query_required_single'
        case (RespJson.YES, RespConstraint.FREE):
            func_name = 'query_json'
        case (RespJson.YES, RespConstraint.NO_MORE_THAN_ONE):
            func_name = 'query_single_json'
        case (RespJson.YES, RespConstraint.EXACTLY_ONE):
            func_name = 'query_required_single_json'
        case _ as unreachable:
            assert_never(unreachable)
    return func_name
