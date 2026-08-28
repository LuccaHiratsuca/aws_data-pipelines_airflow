"""Sparkify ETL: S3 -> Redshift staging -> star schema -> quality gate.

Runs hourly. Every task is one of the four custom operators in
``plugins/operators``; all of the SQL lives in ``plugins/helpers``.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from helpers import SqlQueries
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

default_args = {
    'owner': 'sparkify',
    # Each run rebuilds the warehouse from the staging tables, so a failed run
    # must not block the next one.
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

# Bucket and prefix come from Airflow Variables so the DAG is portable across
# accounts. Resolved through Jinja at run time rather than Variable.get() at
# import time, which would hit the metadata DB on every scheduler parse.
S3_BUCKET = "{{ var.value.get('s3_bucket', 'udacity-dend') }}"
S3_PREFIX = "{{ var.value.get('s3_prefix', '') }}"


@dag(
    dag_id='final_project',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    start_date=datetime(2018, 11, 1),
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['sparkify', 'redshift', 'etl'],
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    # `s3_key` is a template_field. The Udacity log files are laid out as
    # log-data/<year>/<month>/<yyyy-mm-dd>-events.json, so swapping the key
    # below for
    #     f"{S3_PREFIX}log-data/{{{{ execution_date.strftime('%Y/%m') }}}}"
    # makes each run load only its own interval and makes backfills meaningful.
    # The whole prefix is used here so the DAG works against the full dataset.
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket=S3_BUCKET,
        s3_key=f'{S3_PREFIX}log-data',
        # Event logs need an explicit JSONPaths file: the JSON keys do not
        # match the staging_events column names one-for-one.
        json_path=f's3://{S3_BUCKET}/{S3_PREFIX}log_json_path.json',
        extra_copy_options="TIMEFORMAT AS 'epochmillisecs'",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket=S3_BUCKET,
        s3_key=f'{S3_PREFIX}song-data',
        # Song files map field-for-field, so Redshift can infer the mapping.
        json_path='auto',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        select_sql=SqlQueries.songplay_table_insert,
        # Reserved words are quoted to match the DDL in create_tables.sql.
        target_columns=['playid', 'start_time', 'userid', '"level"', 'songid',
                        'artistid', 'sessionid', 'location', 'user_agent'],
        # Facts accumulate; they are never rebuilt on a routine run.
        truncate_before_load=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        select_sql=SqlQueries.user_table_insert,
        target_columns=['userid', 'first_name', 'last_name', 'gender', '"level"'],
        truncate_before_load=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        select_sql=SqlQueries.song_table_insert,
        target_columns=['songid', 'title', 'artistid', '"year"', 'duration'],
        truncate_before_load=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        select_sql=SqlQueries.artist_table_insert,
        target_columns=['artistid', 'name', 'location', 'lattitude', 'longitude'],
        truncate_before_load=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='public."time"',
        select_sql=SqlQueries.time_table_insert,
        target_columns=['start_time', '"hour"', '"day"', 'week', '"month"',
                        '"year"', 'weekday'],
        truncate_before_load=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dq_checks=[
            {
                'description': 'songplays fact table is not empty',
                'check_sql': 'SELECT COUNT(*) FROM songplays',
                'expected_result': 0,
                'comparison': '>',
            },
            {
                'description': 'every songplay has a user',
                'check_sql': 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL',
                'expected_result': 0,
            },
            {
                'description': 'users dimension has no null keys',
                'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
                'expected_result': 0,
            },
            {
                'description': 'songs dimension has no null keys',
                'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL',
                'expected_result': 0,
            },
            {
                'description': 'artists dimension is populated',
                'check_sql': 'SELECT COUNT(*) FROM artists',
                'expected_result': 0,
                'comparison': '>',
            },
            {
                'description': 'time dimension keys are unique',
                'check_sql': (
                    'SELECT COUNT(*) - COUNT(DISTINCT start_time) FROM public."time"'
                ),
                'expected_result': 0,
            },
        ],
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    # Staging runs in parallel, then the fact table, then the four dimensions
    # in parallel. The time dimension reads FROM songplays, so no dimension can
    # start before the fact load has committed.
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_song_dimension_table,
        load_user_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
