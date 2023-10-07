import asyncio
import logging
import unittest

from edgedb import Object as EdgeDBObject

from ecc.connection import EdgeDBCloudConn
from ecc.queries import pack_imqry_records_by_args

from .utils import load_test_toml


class TestImqryCachedByArgsConn(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.disable(level=logging.CRITICAL)
        self.records = pack_imqry_records_by_args()
        self.conn = EdgeDBCloudConn(**load_test_toml(), ttl=999)
        self.conn.client

    async def asyncTearDown(self):
        await self.conn.aclose()

    def tearDown(self):
        del self.conn

    async def test_small_qries_cached(self):
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

        self.assertEqual(self.conn._total_dbcalls, 1)
        self.assertTrue(all(task.done() for task in tasks))

        [[t1], [t2]] = [task.result() for task in tasks]
        self.assertIsInstance(t1, EdgeDBObject)
        self.assertEqual(t1.title, 'Ant-Man')
        self.assertEqual(t1.release_year, 2015)

        self.assertIsInstance(t2, EdgeDBObject)
        self.assertEqual(t2.title, 'Ant-Man')
        self.assertEqual(t2.release_year, 2015)


if __name__ == '__main__':
    unittest.main(verbosity=2)
