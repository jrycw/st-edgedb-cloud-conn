import asyncio
import json
import logging
import unittest

from edgedb import Object as EdgeDBObject

from ecc.connection import EdgeDBCloudConn
from ecc.queries import pack_imqry_records

from .utils import load_test_toml


class TestBaseConn:
    def setUp(self):
        logging.disable(level=logging.CRITICAL)
        self.records = pack_imqry_records()

    async def asyncSetUp(self):
        self.tasks = []
        async with self.conn:
            async with asyncio.TaskGroup() as tg:
                for record in self.records:
                    task = tg.create_task(self.conn.query(record.qry,
                                                          *record.extra_args,
                                                          jsonify=record.jsonify,
                                                          required_single=record.required_single,
                                                          **record.extra_kwargs),
                                          name=record.task_name)
                    self.tasks.append(task)

    async def asyncTearDown(self):
        await self.conn.aclose()

    def tearDown(self):
        del self.conn

    async def test_qries(self):
        tasks = self.tasks
        self.assertEqual(self.conn._total_dbcalls, len(self.records))
        self.assertTrue(all(task.done() for task in tasks))
        t1, t2, t3, t4, t5, t6, t7, t8 = [task.result() for task in tasks]

        self.assertEqual(len(t1), 28)
        self.assertIsInstance(t2, EdgeDBObject)
        self.assertEqual(t2.title, 'Ant-Man')
        self.assertEqual(t2.release_year, 2015)
        self.assertIsNone(t3)
        self.assertIsInstance(t4, EdgeDBObject)
        self.assertEqual(t4.title, 'Ant-Man')
        self.assertEqual(t4.release_year, 2015)

        self.assertEqual(len(json.loads(t5)), 4)
        self.assertEqual(json.loads(t6), {'username': 'Alice'})
        self.assertIsNone(json.loads(t7))
        self.assertEqual(json.loads(t8), {'username': 'Alice'})


class TestImqryCachedConn(TestBaseConn, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.conn = EdgeDBCloudConn(**load_test_toml(), ttl=999)
        self.conn.client

    async def test_small_qries_cached(self):
        n = 5
        tasks = []
        async with self.conn:
            async with asyncio.TaskGroup() as tg:
                for record in [*self.records]*n:
                    task = tg.create_task(self.conn.query(record.qry,
                                                          *record.extra_args,
                                                          jsonify=record.jsonify,
                                                          required_single=record.required_single,
                                                          **record.extra_kwargs),
                                          name=record.task_name)
                    tasks.append(task)
        self.assertEqual(self.conn._total_dbcalls,  len(self.records))

    async def test_large_qries_cached(self):
        n = 50
        tasks = []
        async with self.conn:
            async with asyncio.TaskGroup() as tg:
                for record in [*self.records]*n:
                    task = tg.create_task(self.conn.query(record.qry,
                                                          *record.extra_args,
                                                          jsonify=record.jsonify,
                                                          required_single=record.required_single,
                                                          **record.extra_kwargs),
                                          name=record.task_name)
                    tasks.append(task)
        self.assertEqual(self.conn._total_dbcalls,  len(self.records))


class TestImqryNonCachedConn(TestBaseConn, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.conn = EdgeDBCloudConn(**load_test_toml())
        self.conn.client

    async def test_small_qries_noncached(self):
        n = 2
        tasks = []
        async with self.conn:
            async with asyncio.TaskGroup() as tg:
                for record in [*self.records]*n:
                    task = tg.create_task(self.conn.query(record.qry,
                                                          *record.extra_args,
                                                          jsonify=record.jsonify,
                                                          required_single=record.required_single,
                                                          **record.extra_kwargs),
                                          name=record.task_name)
                    tasks.append(task)
        self.assertEqual(self.conn._total_dbcalls, len(self.records)*(n+1))


if __name__ == '__main__':
    unittest.main(verbosity=2)
