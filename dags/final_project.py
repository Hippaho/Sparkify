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

# Set up logging so we can keep an eye on things
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Default settings for the DAG - like who owns it, when it runs, etc.
default_args = {
    'owner': 'udacity-student',  # Let's give it a more personal touch
    'depends_on_past': False,  # No need to wait for past runs
    'start_date': datetime(2025, 1, 18),  # Pick a date
    'email_on_failure': True,  # Email me if something goes wrong
    'retries': 3,  # Give it a few tries before exiting job
    'email_on_retry': False,  # Don't spam me on retries, just failures
    'retry_delay': timedelta(minutes=5),  # Wait a bit before retrying
    'catchup': False,  # No backfilling
}

# The main event: our DAG!
with DAG('sparkify_data_warehouse_pipeline',  # A more descriptive name
         default_args=default_args,
         description='Loads and transforms data in Redshift for Sparkify',
         schedule_interval='0 * * * *'  # Runs hourly, on the hour.
         ) as dag:

    # Start it off with a simple start marker
    start_operator = DummyOperator(task_id='Begin_pipeline', dag=dag)

    # Get the raw data from S3 into Redshift staging tables
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_user_activity_logs',  # More descriptive task name
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket='tpride',
        s3_key='log_json_path.json',  # Path to the JSON log data
        s3_json='s3://udacity-dend/log_json_path.json', #Path to the json path file.
        region='us-east-1'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_song_metadata',  # Another descriptive task name
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="tpride",
        s3_key="song_data",  # Path to the song metadata
        s3_json='auto',  # Let Redshift figure out the JSON format
        region='us-east-1'
    )

    # Load the fact table - the important section of the data warehouse
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact',  # Clearer task name
        dag=dag,
        table="songplays",
        sql=sql.SqlQueries.songplay_table_insert,
        redshift_conn_id="redshift"
    )

    # Now, let's tackle the dimension tables in a neat task group
    with TaskGroup(group_id='load_dimensions') as load_dimension_tables:
        load_user_dimension_table = LoadDimensionOperator(
            task_id='load_users_dim',
            table="users",
            sql=sql.SqlQueries.user_table_insert,
            redshift_conn_id="redshift"
        )

        load_song_dimension_table = LoadDimensionOperator(
            task_id='load_songs_dim',
            table="songs",
            sql=sql.SqlQueries.song_table_insert,
            redshift_conn_id="redshift"
        )

        load_artist_dimension_table = LoadDimensionOperator(
            task_id='load_artists_dim',
            table="artists",
            sql=sql.SqlQueries.artist_table_insert,
            redshift_conn_id="redshift"
        )

        load_time_dimension_table = LoadDimensionOperator(
            task_id='load_time_dim',
            table="time",
            sql=sql.SqlQueries.time_table_insert,
            redshift_conn_id="redshift"
        )

    # Daily quality checks
    with TaskGroup(group_id='check_data_quality') as data_quality_check:
        artists_data_quality_checks = DataQualityOperator(
            task_id='check_artists_quality',
            table="artists",
            col_name="artist_id",
            redshift_conn_id="redshift"
        )

        songplays_data_quality_checks = DataQualityOperator(
            task_id='check_songplays_quality',
            table="songplays",
            col_name="playid",
            redshift_conn_id="redshift"
        )

        songs_data_quality_checks = DataQualityOperator(
            task_id='check_songs_quality',
            table="songs",
            col_name="songid",
            redshift_conn_id="redshift"
        )

        time_data_quality_checks = DataQualityOperator(
            task_id='check_time_quality',
            table="time",
            col_name="start_time",
            redshift_conn_id="redshift"
        )

        users_data_quality_checks = DataQualityOperator(
            task_id='check_users_quality',
            table="users",
            col_name="userid",
            redshift_conn_id="redshift"
        )

    # A dummy operator to mark the end of the pipeline
    end_operator = DummyOperator(task_id='End_pipeline', dag=dag)

    # Define the order of operations
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> load_dimension_tables >> data_quality_check >> end_operator