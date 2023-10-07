from typing import NamedTuple


class FormContent(NamedTuple):
    submitted: bool
    qry: str
    qry_args_str: str
    jsonify: bool
    required_single: str
    qry_kwargs_str: str
