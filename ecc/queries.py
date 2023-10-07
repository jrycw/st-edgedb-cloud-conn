from typing import Any

from .data_structures import QueryRecord, RespConstraint, RespJson


def pack_imqry_records() -> list[QueryRecord]:
    qries = ['SELECT Movie {title};',
             *['''SELECT assert_single(
                         (SELECT Movie {title, release_year} 
                          FILTER .title=<str>$title and 
                                 .release_year=<int64>$release_year));''']*3,
             'SELECT Account {username};',
             *['''SELECT assert_single(
                         (SELECT Account {username} 
                          FILTER .username=<str>$username))''']*3]
    args_collector = [()]*8
    jsons = [*[RespJson.NO]*4, *[RespJson.YES]*4]
    required_singles = [RespConstraint.FREE,
                        *[RespConstraint.NO_MORE_THAN_ONE]*2,
                        RespConstraint.EXACTLY_ONE]*2
    kwargs_collector = [{},
                        {'title': 'Ant-Man', 'release_year': 2015},
                        {'title': 'Ant-Man100', 'release_year': 2015},
                        {'title': 'Ant-Man', 'release_year': 2015},
                        {},
                        {'username': 'Alice'},
                        {'username': 'AliceCCC'},
                        {'username': 'Alice'}]
    task_names = [*[f'QueryMovie{n}' for n in range(4)],
                  *[f'QueryAccount{n}' for n in range(4)]]

    return [QueryRecord(*qr)
            for qr in zip(qries,
                          args_collector,
                          jsons,
                          required_singles,
                          kwargs_collector,
                          task_names)]


def pack_mqry_records() -> list[QueryRecord]:
    qries = ['''WITH p := (INSERT Person {name:=<str>$name}) 
           SELECT p {name};''',
             '''WITH p:= (DELETE Person FILTER .name=<str>$name) 
           SELECT p {name};''']
    args_collector = [()]*2
    jsons = [RespJson.NO]*2
    required_singles = [RespConstraint.FREE]*2
    kwargs_collector = [{'name': 'Adam Gramham'}]*2
    task_names = ['insert', 'delete']

    return [QueryRecord(*qr)
            for qr in zip(qries,
                          args_collector,
                          jsons, required_singles,
                          kwargs_collector,
                          task_names)]


def pack_imqry_records_by_args() -> list[QueryRecord]:
    qries = ['''SELECT Movie {title, release_year} 
                FILTER .title=<str>$0 and .release_year=<int64>$1;''']
    args_collector = [('Ant-Man', 2015)]
    jsons = [RespJson.NO]
    required_singles = [RespConstraint.FREE]
    kwargs_collector: list[dict[str, Any]] = [{}]
    task_names = ['QueryMovieTitleByArgs']

    return [QueryRecord(*qr)
            for qr in zip(qries,
                          args_collector,
                          jsons,
                          required_singles,
                          kwargs_collector,
                          task_names)]
