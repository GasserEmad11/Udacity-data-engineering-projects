from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
from operators.stage_redshift import StageToRedshiftOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# data test 
data_tests=[{'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0},
            { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
            {'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0}]

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry':False,
    'email_on_failure':False,
    'catchup':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          
          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials='aws_credentials',
    s3_bucket="udacity-dend",
    s3_key="log_data",
    format_instructions='false'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials='aws_credentials',
    region_name='us-west-2',
    s3_bucket="udacity-dend",
    s3_key="song_data",
    format_instructions='false'
)

load_songplays_table = LoadFactOperator(
      task_id='Load_songplays_fact_table',
    dag=dag,
    database='songplays',
    redshift_conn_id="redshift",
    load_sql_statment=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
     task_id='Load_user_dim_table',
    dag=dag,
    database='users',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_statment=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    database='songs',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_statment=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
      task_id='Load_artist_dim_table',
    dag=dag,
    database='artists',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_statment=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
     task_id='Load_time_dim_table',
    dag=dag,
    database='time',
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_statment=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    data_tests=data_tests,
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>stage_events_to_redshift
start_operator>>stage_songs_to_redshift
stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
