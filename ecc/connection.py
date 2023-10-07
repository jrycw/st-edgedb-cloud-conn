import asyncio
import logging
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from types import TracebackType
from typing import Any, Callable, Self, TypeAlias

import edgedb
import httpx
from async_lru import alru_cache
from edgedb import AsyncIOClient as EdgeDBAsyncClient
from edgedb import Object as EdgeDBObject

from .data_structures import RespConstraint, RespJson
from .utils import get_logger, match_func_name

QueryResult: TypeAlias = None | str | EdgeDBObject | list[EdgeDBObject | str]


class EdgeDBCloudConn(AbstractAsyncContextManager):
    _mutated_kws = ('insert', 'update', 'delete')

    def __init__(self,
                 *,
                 host: str,
                 port: int,
                 database: str,
                 secret_key: str,
                 ttl: float = 0,
                 logger: logging.Logger | None = None,
                 log_level: int | None = None) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._secret_key = secret_key
        self._logger = logger or get_logger()
        self._log_level = log_level or logging.INFO
        self._logger.setLevel(self._log_level)

        self._client: EdgeDBAsyncClient | None = None
        self._start = 0.0
        self._dbcalls = 0
        self._total_dbcalls = 0

        if ttl > 0:
            self._imquery = alru_cache(ttl=ttl)(self._imquery)

    @property
    def client(self) -> EdgeDBAsyncClient:
        if self._client is None:
            self._client = edgedb.create_async_client(host=self._host,
                                                      port=self._port,
                                                      database=self._database,
                                                      secret_key=self._secret_key)
            del self._secret_key
        return self._client

    @staticmethod
    def get_cur_timestamp() -> float:
        return datetime.now().timestamp()

    def _is_qry_immutable(self, qry: str) -> bool:
        return all(mutated_kw not in qry.casefold()
                   for mutated_kw in self._mutated_kws)

    def _fmt_query_log_msg(self,
                           qry: str,
                           args: Any,
                           jsonify: RespJson,
                           required_single: RespConstraint,
                           kwargs: dict[str, Any]) -> str:
        return f'query called, {qry=}, {args=}, {jsonify=}, ' + \
               f'{required_single=}, {kwargs=}'

    def _fmt_enter_aenter_log_msg(self) -> str:
        return '__enter__ called'

    def _fmt_enter_aexit_log_msg(self) -> str:
        return '__exit__ called'

    def _fmt_db_calls_log_msg(self) -> str:
        return f'DB calls = {self._dbcalls}'

    def _fmt_exit_aexit_log_msg(self, elapsed: float) -> str:
        cls_name = type(self).__name__
        return f'Time in {cls_name} = {elapsed:.4f} secs'

    def _fmt_aexit_exception_log_msg(self, exc_value: BaseException | None) -> str:
        return f'found {exc_value=}'

    def _get_client_qry_func(self,
                             jsonify: RespJson,
                             required_single: RespConstraint) -> Callable[..., Any]:
        return getattr(self.client, match_func_name(jsonify, required_single))

    async def query(self,
                    qry: str,
                    *args: Any,
                    jsonify: RespJson = RespJson.NO,
                    required_single: RespConstraint = RespConstraint.FREE,
                    **kwargs: Any) -> QueryResult:
        if self._is_qry_immutable(qry):
            return await self._imquery(qry,
                                       *args,
                                       jsonify=jsonify,
                                       required_single=required_single,
                                       **kwargs)
        return await self._mquery(qry,
                                  *args,
                                  jsonify=jsonify,
                                  required_single=required_single,
                                  **kwargs)

    async def _query(self,
                     qry: str,
                     *args: Any,
                     jsonify: RespJson = RespJson.NO,
                     required_single: RespConstraint = RespConstraint.FREE,
                     **kwargs: Any) -> QueryResult:
        self._logger.info(self._fmt_query_log_msg(
            qry, args, jsonify, required_single, kwargs))
        self._dbcalls += 1
        qry_func = self._get_client_qry_func(jsonify, required_single)
        return await qry_func(qry, *args, **kwargs)

    async def _imquery(self,
                       qry: str,
                       *args: Any,
                       jsonify: RespJson = RespJson.NO,
                       required_single: RespConstraint = RespConstraint.FREE,
                       **kwargs: Any) -> QueryResult:
        return await self._query(qry,
                                 *args,
                                 jsonify=jsonify,
                                 required_single=required_single,
                                 **kwargs)

    async def _mquery(self,
                      qry: str,
                      *args: Any,
                      jsonify: RespJson = RespJson.NO,
                      required_single: RespConstraint = RespConstraint.FREE,
                      **kwargs: Any) -> QueryResult:
        return await self._query(qry,
                                 *args,
                                 jsonify=jsonify,
                                 required_single=required_single,
                                 **kwargs)

    def _reset_db_calls(self) -> None:
        self._dbcalls = 0

    def _reset_total_db_calls(self) -> None:
        self._total_dbcalls = 0

    def _reset_start(self) -> None:
        self._start = 0.0

    async def __aenter__(self) -> Self:
        self._logger.info(self._fmt_enter_aenter_log_msg())
        self._start = self.get_cur_timestamp()
        return self

    async def __aexit__(self,
                        exc_type: type[BaseException] | None,
                        exc_value: BaseException | None,
                        exc_tb: TracebackType | None) -> None:
        # TODO
        # 1. Should we await self.aclose()?
        # 2. If not await, db_calls might become weird.
        # 3. asyncTearDown should be on/off?
        await asyncio.sleep(1e-5)
        self._logger.info(self._fmt_enter_aexit_log_msg())
        self._logger.info(self._fmt_db_calls_log_msg())
        if exc_type:
            self._logger.error(self._fmt_aexit_exception_log_msg(exc_value))

        # await self.aclose()

        self._total_dbcalls += self._dbcalls
        self._reset_db_calls()
        elapsed = self.get_cur_timestamp() - self._start
        self._logger.info(self._fmt_exit_aexit_log_msg(elapsed))
        self._reset_start()

    async def aclose(self, timeout: float = 5) -> None:
        print('aclose called')
        await asyncio.wait_for(self.client.aclose(), timeout)

    @property
    def _healthy_check_url(self) -> str:
        return f'https://{self._host}:{self._port}/server/status/alive'

    @property
    def is_healthy(self) -> bool:
        """https://www.edgedb.com/docs/guides/deployment/health_checks#health-checks"""
        try:
            return httpx.get(self._healthy_check_url,
                             follow_redirects=True,
                             verify=False,
                             timeout=30).status_code == 200
        except httpx.HTTPError:
            return False
