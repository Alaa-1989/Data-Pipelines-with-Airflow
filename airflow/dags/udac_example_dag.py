from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'alaa',
    'email': ['alaa.a.alaboud@gmail.com'],
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 3),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *', #@hourly
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='[public].staging_events',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_bucket='alaa-dend-bucket',
    s3_key='log_data',
    file_type="JSON 's3://alaa-dend-bucket/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='[public].Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_bucket='alaa-dend-bucket',
    s3_key='song_data',
    file_type="JSON 'auto'"
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name='songplays',
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name='user',
    redshift_conn_id='redshift',
    append_data=False,
    sql_statement=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name='songs',
    redshift_conn_id='redshift',
    append_data=False,
    sql_statement=SqlQueries.song_table_insert,
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name='artist',
    redshift_conn_id='redshift',
    append_data=False,
    sql_statement=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name='time',
    redshift_conn_id='redshift',
    append_data=False,
    sql_statement=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_statement_check_sql="SELECT COUNT(*) FROM  songplays",
    expected_value="320",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# The DAG dependencies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
