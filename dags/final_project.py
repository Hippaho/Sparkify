# Import datetime, airflow, operators, plugins, and logging modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator

from plugins.helpers import final_project_sql_statements as sql

import logging

# Configure basic logging settings
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

# Get a logger instance for this DAG
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,  # Do not depend on the success of previous DAG runs
    'start_date': datetime(2025, 1, 18),  # Start date of the DAG
    'email_on_failure': True,  # Send email on task failure
    'retries': 3,  # Retry the task 3 times on failure
    'email_on_retry': False,  # Do not send email on task retry
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'catchup': False  # Do not backfill missed DAG runs
}

# Define the DAG
with DAG('udac_example_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='0 * * * *'  # Run the DAG hourly at the 0th minute
         ) as dag:

    # Start dummy operator
    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    # Stage events data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        dag=dag,
        redshift_conn_id="redshift",  # Redshift connection ID
        aws_credentials_id="aws_credentials",  # AWS credentials connection ID
        table="staging_events",  # Redshift table name
        s3_bucket='tpride',  # S3 bucket name
        s3_key='log_json_path.json',  # S3 key for the JSON log data
        s3_json='s3://udacity-dend/log_json_path.json', #Path to the json path file.
        region='us-east-1'  # AWS region
    )

    # Stage songs data from S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        dag=dag,
        redshift_conn_id="redshift",  # Redshift connection ID
        aws_credentials_id="aws_credentials",  # AWS credentials connection ID
        table="staging_songs",  # Redshift table name
        s3_bucket="tpride",  # S3 bucket name
        s3_key="song_data",  # S3 key for the song data
        s3_json='auto',  # JSON format is auto-detected
        region='us-east-1'  # AWS region
    )

    # Load songplays fact table from staging tables
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        dag=dag,
        table="songplays",  # Redshift table name
        sql=sql.SqlQueries.songplay_table_insert,  # SQL query for inserting data
        redshift_conn_id="redshift"  # Redshift connection ID
    )

    # Task group for loading dimension tables
    with TaskGroup(group_id='load_dimension_tables') as load_dimension_tables:
        # Load users dimension table
        load_user_dimension_table = LoadDimensionOperator(
            task_id='load_user_dim_table',
            table="users",  # Redshift table name
            sql=sql.SqlQueries.user_table_insert,  # SQL query for inserting data
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Load songs dimension table
        load_song_dimension_table = LoadDimensionOperator(
            task_id='load_song_dim_table',
            table="songs",  # Redshift table name
            sql=sql.SqlQueries.song_table_insert,  # SQL query for inserting data
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Load artists dimension table
        load_artist_dimension_table = LoadDimensionOperator(
            task_id='load_artist_dim_table',
            table="artists",  # Redshift table name
            sql=sql.SqlQueries.artist_table_insert,  # SQL query for inserting data
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Load time dimension table
        load_time_dimension_table = LoadDimensionOperator(
            task_id='load_time_dim_table',
            table="time",  # Redshift table name
            sql=sql.SqlQueries.time_table_insert,  # SQL query for inserting data
            redshift_conn_id="redshift"  # Redshift connection ID
        )

    # Task group for data quality checks
    with TaskGroup(group_id='data_quality_check') as data_quality_check:
        # Data quality checks for artists table
        artists_data_quality_checks = DataQualityOperator(
            task_id='artists_data_quality_checks',
            table="artists",  # Redshift table name
            col_name="artist_id",  # Column to check for nulls
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Data quality checks for songplays table
        songplays_data_quality_checks = DataQualityOperator(
            task_id='songplays_data_quality_checks',
            table="songplays",  # Redshift table name
            col_name="playid",  # Column to check for nulls
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Data quality checks for songs table
        songs_data_quality_checks = DataQualityOperator(
            task_id='songs_data_quality_checks',
            table="songs",  # Redshift table name
            col_name="songid",  # Column to check for nulls
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Data quality checks for time table
        time_data_quality_checks = DataQualityOperator(
            task_id='time_data_quality_checks',
            table="time",  # Redshift table name
            col_name="start_time",  # Column to check for nulls
            redshift_conn_id="redshift"  # Redshift connection ID
        )

        # Data quality checks for users table
        users_data_quality_checks = DataQualityOperator(
            task_id='users_data_quality_checks',
            table="users",  # Redshift table name
            col_name="userid",  # Column to check for nulls
            redshift_conn_id="redshift"  # Redshift connection ID
        )

    # End dummy operator
    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    # Define task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> load_dimension_tables >> data_quality_check >> end_operator