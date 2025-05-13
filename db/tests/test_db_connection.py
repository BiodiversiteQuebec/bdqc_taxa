import sys
import os
# Add the parent directory to sys.path so we can import config
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config import connect, DB_CONFIG
import unittest
import psycopg2

class TestDatabaseConnection(unittest.TestCase):
    def setUp(self):
        self.conn = None
        
    def tearDown(self):
        if self.conn is not None:
            self.conn.close()

    def test_connection(self):
        """Test that we can connect to the database using the configuration"""
        self.conn = connect()
        self.assertIsNotNone(self.conn)
        self.assertIsInstance(self.conn, psycopg2.extensions.connection)
        self.assertTrue(self.conn.status == psycopg2.extensions.STATUS_READY)
        
    def test_db_config(self):
        """Test that all required configuration variables are set"""
        required_config = ['host', 'port', 'user', 'password', 'dbname']
        for config_key in required_config:
            self.assertIn(config_key, DB_CONFIG)
            self.assertIsNotNone(DB_CONFIG[config_key])

if __name__ == '__main__':
    unittest.main()