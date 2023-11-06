# import pyodbc
# import pymssql
# import snowflake.connector

# connector.py with Airflow hooks
import os
import pandas as pd
import snowflake.connector
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from snowflake.connector.pandas_tools import write_pandas

# Environment variables for sensitive information
SQL_SERVER_CONN_ID = os.getenv('SQL_SERVER_CONN_ID')
SNOWFLAKE_CONN_ID = os.getenv('SNOWFLAKE_CONN_ID')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE')

def get_data_from_sql_server(query):
    """
    This function to connect to SQL Server and return a dataframe
    """
    mssql_hook = MsSqlHook(mssql_conn_id=SQL_SERVER_CONN_ID)
    df = mssql_hook.get_pandas_df(sql=query)
    return df


def load_data_to_snowflake(df):
    """
    # takes df as arg, Function to connect to Snowflake
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()

    # Load DataFrame to Snowflake with idempotency checks
    # Assuming the table has an ID column that uniquely identifies records
    for index, row in df.iterrows():
        merge_sql = f"""
        MERGE INTO {SNOWFLAKE_TABLE} T
        USING (SELECT {row['ID']} AS ID, ...) S
        ON T.ID = S.ID
        WHEN MATCHED THEN UPDATE SET ...
        WHEN NOT MATCHED THEN INSERT (ID, ...) VALUES (S.ID, ...);
        """
        cursor.execute(merge_sql)

    cursor.close()
    connection.close()

# Function to run the ETL process
def run_etl(query):
    df = get_data_from_sql_server(query)
    if not df.empty:
        load_data_to_snowflake(df)

# Entry point for the script
if __name__ == "__main__":
    # Define your SQL Server query
    sql_query = "SELECT * FROM my_table1 WHERE condition='new_data'"

    # Run ETL process
    run_etl(sql_query)
