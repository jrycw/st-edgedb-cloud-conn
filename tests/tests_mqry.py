import asyncio
import logging
import unittest

from edgedb import create_async_client

from ecc.connection import EdgeDBCloudConn
from ecc.queries import pack_mqry_records

from .utils import load_test_toml


class TestMqryConn(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.disable(level=logging.CRITICAL)
        self.records = pack_mqry_records()
        self.conn = EdgeDBCloudConn(
            **load_test_toml(), ttl=999)  # intentionally
        self.conn.client

    async def asyncSetUp(self):
        delete_record = self.records[1]
        async with create_async_client(**load_test_toml()) as client:
            await client.query(delete_record.qry,
                               *delete_record.extra_args,
                               **delete_record.extra_kwargs)

    async def asyncTearDown(self):
        await self.conn.aclose()

    def tearDown(self):
        del self.conn

    async def test_insert_then_delete(self):
        insert_record, delete_record = self.records

        async with self.conn:
            async with asyncio.TaskGroup() as tg:
                insert_task = tg.create_task(self.conn.query(insert_record.qry,
                                                             *insert_record.extra_args,
                                                             jsonify=insert_record.jsonify,
                                                             required_single=insert_record.required_single,
                                                             **insert_record.extra_kwargs),
                                             name=insert_record.task_name)
            self.assertTrue(insert_task.done())
            self.assertEqual(len(insert_task.result()), 1)

            async with asyncio.TaskGroup() as tg:
                insert_task = tg.create_task(self.conn.query(insert_record.qry,
                                                             *insert_record.extra_args,
                                                             jsonify=insert_record.jsonify,
                                                             required_single=insert_record.required_single,
                                                             **insert_record.extra_kwargs),
                                             name=insert_record.task_name)
            self.assertTrue(insert_task.done())
            self.assertEqual(len(insert_task.result()), 1)

            async with asyncio.TaskGroup() as tg:
                delete_task = tg.create_task(self.conn.query(delete_record.qry,
                                                             *delete_record.extra_args,
                                                             jsonify=delete_record.jsonify,
                                                             required_single=delete_record.required_single,
                                                             **delete_record.extra_kwargs),
                                             name=delete_record.task_name)
            self.assertTrue(delete_task.done())
            self.assertEqual(len(delete_task.result()), 2)

        self.assertEqual(self.conn._total_dbcalls, 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
