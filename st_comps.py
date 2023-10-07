import asyncio
import json
import re
from typing import TypeAlias

import pandas as pd
import streamlit as st

from ecc.connection import EdgeDBCloudConn
from ecc.data_structures import RespConstraint
from st_data_structures import FormContent
from st_utils import (
    _routine_clean,
    count_conns,
    count_loops,
    get_conn_dict,
    get_func_table,
    is_db_healthy,
    load_db_info,
    render_png,
    required_single_format_func,
)

ExtractedTaskResult: TypeAlias = str | None


def _display_refs() -> None:
    st.markdown(
        '[EdgeDB Cheat Sheet](https://www.edgedb.com/docs/guides/cheatsheet/index)')
    st.markdown(
        '[EdgeDB-Python API](https://www.edgedb.com/docs/clients/python)')
    st.markdown(
        '[Easy EdgeDB](https://www.edgedb.com/easy-edgedb)')


def _display_sidebar() -> None:
    with st.sidebar:
        st.write(render_png('images/edb_logo_green.png'),
                 unsafe_allow_html=True)
        st.write('')
        with st.expander('Database Info', expanded=True):
            st.write(load_db_info())
            _, last_col = st.columns([1, 1])
            with last_col:
                if st.button('Healthy Check', type='secondary'):
                    if is_db_healthy():
                        st.toast('Connected successfully', icon="✅")
                    else:
                        st.toast('Connected unsuccessfully', icon="🚨")
        _display_refs()


def _display_func_table() -> None:
    df = pd.DataFrame(get_func_table(), columns=['jsonify',
                                                 'required_single',
                                                 'EdgeDB function call'])
    st.dataframe(df, hide_index=True)


def _display_tabs() -> None:
    create_snip_tab, read_snip_tab, update_snip_tab, delete_snip_tab, func_ref_tab = st.tabs(
        ['Create', 'Read', 'Update', 'Delete', 'Enum-Func match table'])

    with create_snip_tab:
        st.code(
            '''WITH m:= (INSERT Movie {title :=<str>$title }) SELECT m {title};''')

    with read_snip_tab:
        st.code(('''SELECT Movie {title} FILTER .title = <str>$title;'''))
        st.code(
            '''SELECT Movie {title} FILTER .title = <str>$0;''')
        st.code(
            '''SELECT assert_single((SELECT Movie {title} FILTER .title = <str>$title));''')

    with update_snip_tab:
        st.code('''WITH movie := (SELECT assert_single(
                    (UPDATE Movie
                    FILTER .title = <str>$old_title
                    SET {title := <str>$new_title})))\nSELECT movie {title};''')
        st.code(
            '''SELECT (UPDATE Movie FILTER .title = <str>$old_title SET {title := <str>$new_title}}) {title};''')

    with delete_snip_tab:
        st.code('''WITH movie := (SELECT assert_single(
                    (DELETE Movie
                    FILTER .title = <str>$title)))\nSELECT movie {title};''')
        st.code(
            '''SELECT (DELETE Movie FILTER .title = <str>$title) {title};''')

    with func_ref_tab:
        _display_func_table()


def _receive_qry() -> str:
    return st.text_area(
        'EdgeDB Query (`str`, `datetime.date` and `datetime.datetime` object are supported)',
        'SELECT Movie {title} FILTER .title = <str>$title;',
        placeholder='Example: \nSELECT Movie {title};')


def _receive_qry_args_str() -> str:
    return st.text_area(
        'Positional query arguments (separated by semicolon)',
        placeholder="Example: \n1; 2.5; 'Continental Hotel'; " +
        "datetime.date(1964, 9, 2)")


def _receive_qry_kwargs_str() -> str:
    return st.text_area(
        'Named query arguments (separated by semicolon)',
        "title='Thor';",
        placeholder="Example: \ntitle='John Wick 5';" +
        " name='Keanu Charles Reeves';" +
        " birthday=datetime.date(1964, 9, 2)")


def _receive_required_single() -> str:
    return st.radio('required_single?',
                    [str(m) for m in RespConstraint],
                    index=0,
                    format_func=required_single_format_func,
                    help='Refer to the table above to ' +
                    'identify the corresponding EdgeDB ' +
                    'function being called.')


def _receive_jsonify() -> bool:
    return st.checkbox('Jsonify',
                       value=True,
                       help='jsonify provides better visibility')


def _get_query_form() -> FormContent:
    with st.form('query-form'):
        _display_tabs()
        qry = _receive_qry()

        with st.container():
            args_col, kwargs_col = st.columns(2)
            with args_col:
                qry_args_str = _receive_qry_args_str()

            with kwargs_col:
                qry_kwargs_str = _receive_qry_kwargs_str()

        required_single = _receive_required_single()

        json_first_col, _, qry_last_col = st.columns([2, 5, 1])
        with json_first_col:
            jsonify = _receive_jsonify()
        with qry_last_col:
            submitted = st.form_submit_button('Query')

        return FormContent(submitted,
                           qry,
                           qry_args_str,
                           jsonify,
                           required_single,
                           qry_kwargs_str)


def _display_big_red_btn_and_db_calls(conn: EdgeDBCloudConn, token: str) -> None:
    new_conn_btn_col, db_calls_col = st.columns([1, 3])
    with new_conn_btn_col:
        if st.button('Clear Conn', type='primary', on_click=conn._reset_total_db_calls):
            try:
                del get_conn_dict()[token]
            except Exception as ex:
                st.toast(f'{ex=} happened in clear conn', icon="🚨")

    with db_calls_col:
        with st.empty():
            try:
                calls = conn._total_dbcalls
            except Exception as ex:
                st.toast(f'{ex=} happened in db calls', icon="🚨")
                calls = 0
            st.write(
                f'**Total DB calls for this section: :blue[{calls:02}]** :sunglasses:')


def _display_res(token: str,
                 loop: asyncio.AbstractEventLoop,
                 conn: EdgeDBCloudConn,
                 excluded_token: list[str]) -> None:
    with st.expander('Resource Info', expanded=True):
        col1, col2 = st.columns([65, 35])
    with col1:
        st.code(f'token: {token}')
        st.code(f'loop_id:{id(loop)}/conn_id:{id(conn)}')

    with col2:
        st.info(
            f'n_loops/n_conns: {count_loops()}/{count_conns()}')
        col3, col4 = st.columns([2, 3])
        with col3:
            if st.button('Refresh', type='secondary'):
                st.rerun()
        with col4:
            if st.button('Try Free Res', type='primary'):
                _routine_clean(excluded_token, threshold=3)


def _render_exception(exc: Exception) -> None:
    # https://discuss.streamlit.io/t/cant-print-pyinstruments-output-in-streamlit/8388/2
    ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    st.code(ANSI_ESCAPE.sub('', str(exc)))


def _render_result(result: ExtractedTaskResult) -> None:
    if result is None or (isinstance(result, str) and str(result) == 'null'):
        st.write('No data')
    elif isinstance(result, list) and not result:
        st.write('[]')
    else:
        try:
            st.json(json.loads(result))
        except Exception:
            if 'Object' in str(result):
                st.write(str(result))
            else:
                st.warning(str(result))
    st.write('')
