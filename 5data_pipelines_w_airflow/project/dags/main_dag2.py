from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                RunSQLStatements)

from helpers import SqlQueries


default_args = {
    'owner': 'chriseal',
    'start_date': datetime(2019, 12, 1, 0),
    'end_date': datetime(2019, 12, 1, 1),
    'retries': 0,
    'max_active_runs': 1,
    'depends_on_past': False,
    'catchup': False,
    'retry_delay': timedelta(seconds=5)
}
DAG_NAME = 'sparkify_etl2'
UDACITY_S3_BUCKET = 'udacity-dend'
REGION = "us-west-2"
AWS_CREDENTIALS_ID = "aws_credentials"
REDSHIFT_CONN_ID = "redshift"
DEBUG = True

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *' #@daily if debug else @hourly
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

initialize_tables = RunSQLStatements(
    dag=dag,
    task_id='Initialize_tables',
    queries=SqlQueries.create_table_statements,
    redshift_conn_id=REDSHIFT_CONN_ID
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table="staging_events",
    source_file_format='JSON',
    # log_fpath='s3://udacity-dend/log_json_path.json',
    s3_bucket=UDACITY_S3_BUCKET,
    s3_key="log_data/2018/11" if DEBUG else "log_data",
    aws_credentials_id=AWS_CREDENTIALS_ID,
    redshift_conn_id=REDSHIFT_CONN_ID,
    region=REGION
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    target_table="staging_songs",
    source_file_format='JSON',
    s3_bucket=UDACITY_S3_BUCKET,
    s3_key="song_data/A/A" if DEBUG else "song_data",
    aws_credentials_id=AWS_CREDENTIALS_ID,
    redshift_conn_id=REDSHIFT_CONN_ID,
    region=REGION
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    target_table='songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)',
    query=SqlQueries.songplay_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    truncate_target_table=True,
    target_table='user',
    query=SqlQueries.user_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    truncate_target_table=True,
    target_table='song',
    query=SqlQueries.song_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    truncate_target_table=True,
    target_table='artist',
    query=SqlQueries.artist_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    truncate_target_table=True,
    target_table='time',
    query=SqlQueries.time_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> initialize_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
        load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> end_operator