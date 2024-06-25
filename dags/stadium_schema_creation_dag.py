import airflow
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Constants
SNOWFLAKE_WAREHOUSE = 'STADIUM_WAREHOUSE'
SNOWFLAKE_DB = 'STADIUM_DB'
SNOWFLAKE_SCHEMA = 'STADIUM_SCHEMA'
SNOWFLAKE_STAGE = 'SNOW_S3_STAGE'
SNOWFLAKE_TABLE_STADIUM = 'stadium_data'


# Define default arguments
default_args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Initialize the DAG
dag = DAG(
    dag_id='stadium_schema_creation_dag',
    default_args=default_args,
    description='Create Snowflake schema, tables, storage integration, stage, and file formats',
    schedule_interval='@once',
    catchup=False,
)

with dag:
    snowflake_create_db_schema = SQLExecuteQueryOperator(
        task_id='snowflake_create_db_schema',
        sql=f"""
        CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} WITH WAREHOUSE_SIZE='x-small';
        CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DB};
        CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA};
        """,
        conn_id='snowflake_conn_stadium',
    )

    snowflake_create_storage_integration = SQLExecuteQueryOperator(
        task_id='snowflake_create_storage_integration',
        sql=f"""
        USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
        USE {SNOWFLAKE_DB};
        CREATE OR REPLACE STORAGE INTEGRATION SNOW_S3
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = S3
        ENABLED = TRUE
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397772312:role/stadiums_de_role'
        STORAGE_ALLOWED_LOCATIONS = ('s3://stadiums-bucket/');
        """,
        conn_id='snowflake_conn_stadium'
    )

    snowflake_create_stage = SQLExecuteQueryOperator(
        task_id='snowflake_create_stage',
        sql=f"""
        USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
        USE {SNOWFLAKE_DB};
        CREATE OR REPLACE STAGE {SNOWFLAKE_STAGE}
        STORAGE_INTEGRATION = SNOW_S3
        URL = 's3://stadiums-bucket/';
        """,
        conn_id='snowflake_conn_stadium'
    )

    snowflake_create_file_formats = SQLExecuteQueryOperator(
        task_id='snowflake_create_file_formats',
        sql=f"""
        USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
        USE {SNOWFLAKE_DB};
        CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT 
        TYPE = 'PARQUET';
        """,
        conn_id='snowflake_conn_stadium'
    )

    snowflake_create_table_stadium = SQLExecuteQueryOperator(
        task_id='snowflake_create_table_stadium',
        sql=f"""
        USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
        USE {SNOWFLAKE_DB};
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE_STADIUM} (
            Stadium STRING,
            Capacity INT,
            City_State STRING,
            Country STRING,
            Region STRING,
            Tenants STRING,
            Sports STRING,
            Image STRING,
            Location STRING
        );
        """,
        conn_id='snowflake_conn_stadium'
    )

snowflake_create_db_schema >> snowflake_create_storage_integration >> snowflake_create_stage >> snowflake_create_file_formats >> snowflake_create_table_stadium
