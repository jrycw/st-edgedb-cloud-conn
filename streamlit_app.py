import asyncio
import logging
from typing import Any

import nest_asyncio
import streamlit as st

from ecc.connection import EdgeDBCloudConn
from st_comps import (
    _display_big_red_btn_and_db_calls,
    _display_res,
    _display_sidebar,
    _get_query_form,
    _render_exception,
    _render_result,
)
from st_utils import (
    _create_task_from_form,
    _routine_clean,
    generate_token,
    get_conn_dict,
    get_cur_ts,
    get_loop_dict,
    load_st_toml,
)

nest_asyncio.apply()

st.set_page_config(
    page_title='Streamlit EdgeDB Cloud Connection',
    layout='centered')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if 'token' not in st.session_state:
    token = generate_token()
    logging.info(f'Generating token: {token}')
    st.session_state['token'] = token


async def main(tg: asyncio.TaskGroup,
               conn: EdgeDBCloudConn,
               token: str,
               tasks: set[asyncio.Task[Any]]) -> None:
    _display_sidebar()
    form = _get_query_form()
    if form.submitted:
        await _create_task_from_form(tg, conn, form, tasks)
    _display_big_red_btn_and_db_calls(conn, token)


async def run(algo, conn: EdgeDBCloudConn, token: str) -> None:
    # https://youtu.be/-CzqsgaXUM8?list=PLhNSoGM2ik6SIkVGXWBwerucXjgP1rHmB&t=2375
    top_name = 'top'
    tasks: set[asyncio.Task[Any]] = set()
    try:
        async with asyncio.TaskGroup() as tg:
            task = tg.create_task(algo(tg, conn, token, tasks), name=top_name)
            tasks.add(task)
    except* Exception as ex:
        for exc in ex.exceptions:
            st.warning(f'Exception: {type(exc).__name__}')
            _render_exception(exc)
    else:
        for task in tasks:
            if (task_name := task.get_name()) != top_name:
                st.write(f'task_name: {task_name}')
                _render_result(task.result())


def _prepare_loop(cur_ts: int, token: str) -> asyncio.AbstractEventLoop:
    loop_dict = get_loop_dict()
    if token not in loop_dict:
        loop = asyncio.new_event_loop()
    else:
        loop, _ = loop_dict[token]
    loop_dict[token] = (loop, cur_ts)
    return loop


def _prepare_conn(cur_ts: int, token: str) -> EdgeDBCloudConn:
    conn_dict = get_conn_dict()
    if token not in conn_dict:
        conn = EdgeDBCloudConn(**load_st_toml())
    else:
        conn, _ = conn_dict[token]
    conn_dict[token] = (conn, cur_ts)
    return conn


if __name__ == '__main__':
    # TODO
    # How to gracefully close the db connection if the Streamlit server is shutdown?
    cur_ts = get_cur_ts()
    token = st.session_state.token
    excluded_token = [token]

    loop = _prepare_loop(cur_ts, token)
    conn = _prepare_conn(cur_ts, token)

    _display_res(token, loop, conn, excluded_token)
    _routine_clean(excluded_token)

    asyncio.set_event_loop(loop)
    loop.run_until_complete(run(main, conn, token))
