from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.udacity_plugin.stage_redshift import StageToRedshiftOperator
# from airflow.operators.udacity_plugin.load_fact import LoadFactOperator
# from airflow.operators.udacity_plugin.load_dimension import LoadDimensionOperator
# from airflow.operators.udacity_plugin.data_quality import DataQualityOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'chriseal',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 3, 12),
    'retries': 0,
    'max_active_runs': 1
}
DAG_NAME = 'sparkify_etl'
UDACITY_S3_BUCKET = 'udacity-dend'
REGION = "us-west-2"
AWS_CREDENTIALS_ID = "aws_credentials"
REDSHIFT_CONN_ID = "redshift"

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table="staging_events",
    source_file_format='JSON',
    log_fpath='s3://udacity-dend/log_json_path.json',
    s3_bucket=UDACITY_S3_BUCKET,
    s3_key="log_data",
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
    s3_key="song_data",
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


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
        load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> end_operator