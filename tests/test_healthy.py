import logging
import unittest

from ecc.connection import EdgeDBCloudConn

from .utils import load_test_toml


class TestHealthy(unittest.TestCase):
    def setUp(self):
        logging.disable(level=logging.CRITICAL)
        self.conn = EdgeDBCloudConn(**load_test_toml())

    def test_healthy_check_url(self):
        host, port = self.conn._host, self.conn._port
        self.assertEqual(f'https://{host}:{port}/server/status/alive',
                         self.conn._healthy_check_url)

    def test_alive(self):
        self.assertTrue(self.conn.is_healthy)


if __name__ == '__main__':
    unittest.main(verbosity=2)
