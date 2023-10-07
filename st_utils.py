import asyncio
import base64
import datetime
import uuid
from pathlib import Path
from typing import Any

import httpx
import streamlit as st

from ecc.connection import EdgeDBCloudConn
from ecc.data_structures import QueryRecord, RespConstraint, RespJson
from ecc.utils import load_toml, match_func_name
from st_data_structures import FormContent


def get_cur_ts() -> int:
    return int(datetime.datetime.now().timestamp())


def generate_token() -> str:
    return f'{uuid.uuid4().hex[:24]}|{get_cur_ts()}'


def load_st_toml() -> dict[str, Any]:
    try:
        return load_toml()
    except FileNotFoundError:
        return st.secrets['edgedb-cloud']


def load_db_info() -> dict[str, Any]:
    dbinfo = dict(**load_st_toml())
    dbinfo.pop('secret_key')
    return dbinfo


@st.cache_resource
def get_loop_dict() -> dict[str, Any]:
    return {}


@st.cache_resource
def get_conn_dict() -> dict[str, Any]:
    return {}


def _routine_clean(excluded_token: list[str],
                   threshold: float = 300) -> None:
    cur_ts = get_cur_ts()

    ld = get_loop_dict()
    to_del_loop_tokens = {t
                          for t, (_, loop_ts) in ld.items()
                          if cur_ts - loop_ts > threshold}
    for _token in excluded_token:
        to_del_loop_tokens.discard(_token)
    for k in to_del_loop_tokens:
        try:
            del ld[k]
        except Exception as ex:
            st.toast(f'{ex=} happened in del loops', icon="🚨")

    cd = get_conn_dict()
    to_del_conn_tokens = {t
                          for t, (_, conn_ts) in cd.items()
                          if cur_ts - conn_ts > threshold}
    for _token in excluded_token:
        to_del_conn_tokens.discard(_token)
    for k in to_del_conn_tokens:
        try:
            del cd[k]
        except Exception as ex:
            st.toast(f'{ex=} happened in del conns', icon="🚨")


def count_loops() -> int:
    return len(get_loop_dict())


def count_conns() -> int:
    return len(get_conn_dict())


def render_png(png: str) -> str:
    """https://stackoverflow.com/questions/70932538/how-to-center-the-title-and-an-image-in-streamlit"""
    img_bytes = Path(png).read_bytes()
    b64 = base64.b64encode(img_bytes).decode()
    return f'''<div style="text-align:center;">
               <img src="data:image/png;base64,{b64}"/></div>'''


def get_func_table() -> list[tuple[str, str, str]]:
    return [(str(j), str(c), match_func_name(j, c))
            for j in RespJson
            for c in RespConstraint]


def required_single_format_func(option: str) -> str:
    keys = (str(member) for member in RespConstraint)
    values = ('FREE: The query will just return whatever it got.',
              'NO_MORE_THAN_ONE: The query must return no more than one element.',
              'EXACTLY_ONE: The query must return exactly one element.')
    d = dict(zip(keys, values))
    if feedback := d.get(option):
        return feedback
    raise TypeError(f'{option} may not be the RespConstraint Enum member')


def convert_bool_to_jsonify(jsonify: bool) -> RespJson:
    return RespJson.YES if jsonify else RespJson.NO


def convert_str_to_required_single(required_single: str) -> RespConstraint:
    d = {str(member): member
         for member in RespConstraint}
    return d[required_single]


def _populate_qry_args(qry_args_str: str) -> tuple[Any, ...]:
    qry_args: list[Any] = []
    for arg_str in qry_args_str.split(';'):
        if arg_str.strip() and \
                isinstance(arg_str, (str, datetime.date, datetime.datetime)):
            try:
                eval_arg = eval(arg_str)
            except SyntaxError as e:
                st.warning(
                    'Can not parse the positional query arguments!')
                raise e
            else:
                qry_args.append(eval_arg)
    return tuple(qry_args)


def _populate_qry_kwargs(qry_kwargs_str: str) -> dict[str, Any]:
    qry_kwargs: dict[str, Any] = {}
    for kwarg_str in qry_kwargs_str.split(';'):
        try:
            exec(kwarg_str.strip(), globals(), qry_kwargs)
        except SyntaxError as e:
            st.warning(
                'Can not parse the named query arguments!')
            raise e
    return qry_kwargs


def _convert_form_to_record(form: FormContent) -> QueryRecord:
    qry = form.qry
    extra_args = _populate_qry_args(form.qry_args_str)
    jsonify = convert_bool_to_jsonify(form.jsonify)
    required_single = convert_str_to_required_single(form.required_single)
    extra_kwargs = _populate_qry_kwargs(form.qry_kwargs_str)
    task_name = uuid.uuid4().hex[:6]

    return QueryRecord(qry,
                       extra_args,
                       jsonify,
                       required_single,
                       extra_kwargs,
                       task_name)


async def _create_task_from_form(tg: asyncio.TaskGroup,
                                 conn: EdgeDBCloudConn,
                                 form: FormContent,
                                 tasks: set[asyncio.Task[Any]]) -> None:
    record = _convert_form_to_record(form)
    async with conn:
        task = tg.create_task(conn.query(record.qry,
                                         *record.extra_args,
                                         jsonify=record.jsonify,
                                         required_single=record.required_single,
                                         **record.extra_kwargs),
                              name=record.task_name)
        tasks.add(task)


def is_db_healthy() -> bool:
    """https://www.edgedb.com/docs/guides/deployment/health_checks#health-checks"""
    st_info = load_st_toml()
    host, port = st_info['host'], st_info['port']
    _healthy_check_url = f'https://{host}:{port}/server/status/alive'
    try:
        return httpx.get(_healthy_check_url,
                         follow_redirects=True,
                         verify=False,
                         timeout=30).status_code == 200
    except httpx.HTTPError:
        return False
