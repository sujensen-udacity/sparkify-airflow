from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project(**kwargs):

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_key='log-data/{{ execution_date.year }}/{{ \'%02d\' % execution_date.month }}/{{ ds }}-events.json',
        s3_bucket='susan-airflow-bucket-3',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        json_format='s3://susan-airflow-bucket-3/log_json_path.json',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_key='song-data/A',
        s3_bucket='susan-airflow-bucket-3',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        sql_statement=SqlQueries.songplay_table_insert,
        target_db='dev',
        target_table='public.songplays',
        redshift_conn_id='redshift',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_statement=SqlQueries.user_table_insert,
        target_db='dev',
        target_table='public.users',
        redshift_conn_id='redshift',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_statement=SqlQueries.song_table_insert,
        target_db='dev',
        target_table='public.songs',
        redshift_conn_id='redshift',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_statement=SqlQueries.artist_table_insert,
        target_db='dev',
        target_table='public.artists',
        redshift_conn_id='redshift',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_statement=SqlQueries.time_table_insert,
        target_db='dev',
        target_table='public.time',
        redshift_conn_id='redshift',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        sql_statements=[
            'select count(*) from songplays where userid is null',
            'select count(*) from songs where title is null',
        ],
        expected_results=[
            0,  # we expect every song play (event) to have a userid
            0,  # we expect every song to have a title
        ],
        redshift_conn_id='redshift',
    )

    end_operator = DummyOperator(task_id='End_execution')

    # Add dependencies to the graph
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
