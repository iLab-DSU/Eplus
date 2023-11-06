# test_connector.py

import unittest
from unittest.mock import patch, MagicMock
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class TestSnowflakeHook(unittest.TestCase):
    @patch('airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.get_conn')
    def test_snowflake_hook_connection(self, mock_get_conn):
        # Mock the get_conn method
        mock_get_conn.return_value = MagicMock()

        # Instantiate the SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='MY_SNOWFLAKE_CONN_ID')

        # Call the get_conn method
        conn = hook.get_conn()

        # Assert the connection was called and is a MagicMock instance
        mock_get_conn.assert_called_once()
        self.assertIsInstance(conn, MagicMock)

if __name__ == '__main__':
    unittest.main()
