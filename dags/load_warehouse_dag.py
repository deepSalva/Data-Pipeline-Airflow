from datetime import datetime, timedelta
import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from sql_files import create_tables_sql, stage_redshift_sql

DEFAULT_ARGS = {
    'owner': 'salvig',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

CREATE_TABLE_QUERIES = [
    create_tables_sql.ARTISTS,
    create_tables_sql.SONGPLAYS,
    create_tables_sql.SONGS,
    create_tables_sql.STAGING_EVENTS,
    create_tables_sql.STAGING_SONGS,
    create_tables_sql.TIME,
    create_tables_sql.USERS
]

LOAD_REDSHIFT_QUERIES = stage_redshift_sql.SqlQueries

REDSHIFT_CONN_ID = 'redshift'

AWS_CREDENTIALS = 'aws_credentials'


@dag(
    default_args=DEFAULT_ARGS,
    description='Load and transform data in Redshift with Airflow',
    start_date=pendulum.now(),
    schedule_interval='0 * * * *'
)
def load_warehouse():
    @task
    def create_tables():
        redshift_hook = PostgresHook(postgres_conn_id='redshift')
        for table_query in CREATE_TABLE_QUERIES:
            redshift_hook.run(table_query)

    @task
    def stage_redshift(**kwargs):
        table = kwargs['params']['table']
        bucket = kwargs['params']['bucket']
        json_format = kwargs['params']['json_format']
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(AWS_CREDENTIALS)
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        sql_stmt = LOAD_REDSHIFT_QUERIES.COPY_SQL.format(
            table,
            bucket,
            aws_connection.login,
            aws_connection.password,
            json_format
        )
        redshift_hook.run(sql_stmt)
        print('Stage To Redshift Task Implementation Success')

    @task
    def load_fact_table(**kwargs):
        table = kwargs['params']['table']
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        redshift_hook.run(LOAD_REDSHIFT_QUERIES.songplay_table_insert)
        print('Fact Table {} load succesfully'.format(table))

    @task
    def load_dimension_tables(**kwargs):
        table = kwargs['params']['table']
        truncate_switch = kwargs['params']['truncate']
        insert_query = kwargs['params']['insert_query']
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        if truncate_switch:
            redshift_hook.run(insert_query[1])
        else:
            redshift_hook.run(insert_query[0])
        print('Dimension Table {} load succesfully'.format(table))

    @task
    def data_quality_check(**kwargs):
        table = kwargs["params"]["table"]
        column = kwargs["params"]["column"]
        redshift_hook = PostgresHook(REDSHIFT_CONN_ID)
        records = redshift_hook.get_records(LOAD_REDSHIFT_QUERIES.quality_null.format(column, table))
        if records[0][0] > 0:
            raise ValueError(f"Data quality check failed. Table {table} with column {column} returned NULL values")
        print(f"Data quality on table {table} with column {column} check passed with no NULL values")

    # Tasks Instantiation:

    start_operator = EmptyOperator(task_id='Begin_execution')

    create_tables = create_tables()

    stage_events_to_redshift = stage_redshift(
        params={
            "table": "staging_events",
            "bucket": "s3://<your-bucket-example>/log-data",
            "json_format": "json 's3://<your-bucket-example>/log_json_path.json'"
        }
    )

    stage_songs_to_redshift = stage_redshift(
        params={
            "table": "staging_songs",
            "bucket": "s3://<your-bucket-example>/song-data",
            "json_format": "format as json 'auto'",
            "aws_credentials": "aws_credentials",
            "redshift_conn": "redshift"
        }
    )

    load_fact_table = load_fact_table(
        params={
            'table': 'Songplay'
        }
    )

    load_user_table = load_dimension_tables(
        params={
            'table': 'Users',
            'insert_query': LOAD_REDSHIFT_QUERIES.user_table_insert,
            'truncate': False
        }
    )

    load_artists_table = load_dimension_tables(
        params={
            'table': 'Artists',
            'insert_query': LOAD_REDSHIFT_QUERIES.artist_table_insert,
            'truncate': False
        }
    )

    load_time_table = load_dimension_tables(
        params={
            'table': 'Time',
            'insert_query': LOAD_REDSHIFT_QUERIES.time_table_insert,
            'truncate': False
        }
    )

    load_songs_table = load_dimension_tables(
        params={
            'table': 'Songs',
            'insert_query': LOAD_REDSHIFT_QUERIES.song_table_insert,
            'truncate': False
        }
    )

    data_quality_check = data_quality_check(
        params={
            "table": "users",
            "column": "first_name"
        }
    )

    end_operator = EmptyOperator(task_id='end_execution')

    start_operator >> create_tables

    create_tables >> stage_events_to_redshift
    create_tables >> stage_songs_to_redshift

    load_fact_table << stage_events_to_redshift
    load_fact_table << stage_songs_to_redshift

    load_fact_table >> load_user_table
    load_fact_table >> load_artists_table
    load_fact_table >> load_songs_table
    load_fact_table >> load_time_table

    load_user_table >> data_quality_check
    load_artists_table >> data_quality_check
    load_songs_table >> data_quality_check
    load_time_table >> data_quality_check

    data_quality_check >> end_operator


run_project = load_warehouse()
