from datetime import datetime, timedelta
import os
from airflow.configuration import conf
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'bhoang',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'bhoang_dag',
    default_args = default_args,
    start_date = datetime.datetime.now(),
    schedule_interval = '@hourly'
)

f= open(os.path.join(conf.get('core','dags_folder'),'create_tables.sql'))
create_tables_sql = f.read()

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_events",
    s3_bucket = "s3://bhoang/log_data"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,  
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_songs",
    s3_bucket = "s3://bhoang/song_data"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,    
    redshift_conn_id="redshift",
    table="songplays",
    load_sql=SqlQueries.songplay_table_insert,
)

load_songs_table = LoadDimensionOperator(
    task_id='Load_songs_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="songs",
    load_sql=SqlQueries.song_table_insert,
)


load_users_table = LoadDimensionOperator(
    task_id='Load_users_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="users",
    load_sql=SqlQueries.user_table_insert,
)

load_artists_table = LoadDimensionOperator(
    task_id='Load_artists_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="artists",
    load_sql=SqlQueries.artist_table_insert,
)

load_time_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="time",
    load_sql=SqlQueries.time_table_insert,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(1) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator  \
    >> create_trips_table \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [ load_songs_table, load_artists_table, load_time_table, load_users_table] \
    >> run_quality_checks \
    >> end_operator