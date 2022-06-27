"""
Import NYC Taxi data from S3 into CrateDB

Prerequisite: In the CrateDB schema "nyc_taxi", create "load_files_processed",
"load_trips_staging" and "trips" tables. The CREATE TABLE statements
are present in the file sql/taxi-schema.sql in this repository.
"""
import logging
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from flyte_provider.operators.flyte import FlyteOperator


def get_processed_files():
    pg_hook = PostgresHook(postgres_conn_id="cratedb_demo_connection")
    records = pg_hook.get_records(
        sql="SELECT file_name FROM nyc_taxi.load_files_processed"
    )

    # flatten nested list as there is only one column
    return list(map(lambda record: record[0], records))


def data_urls(ti):
    data_urls_csv = []
    for month in range(1, 2):
        data_urls_csv.append(
            "https://s3.us-east-2.amazonaws.com/nyctaxi-airflow/2021/"
            + "yellow_tripdata_2021-{0:0=2d}.csv".format(month)
        )

    return data_urls_csv


def identify_missing_urls(ti):
    data_urls_processed = ti.xcom_pull(task_ids="get_processed_files")
    data_urls_available = ti.xcom_pull(task_ids="data_urls")

    return list(set(data_urls_available) - set(data_urls_processed))


def process_new_files(ti):
    missing_urls = ti.xcom_pull(task_ids="identify_missing_urls")

    for missing_url in missing_urls:
        logging.info(missing_url)

        file_name = missing_url.split("/").pop().replace("parquet", "csv")

        PostgresOperator(
            task_id=f"copy_{file_name}",
            postgres_conn_id="cratedb_demo_connection",
            sql=f"""
                    COPY nyc_taxi.load_trips_staging
                    FROM '{missing_url}'
                    WITH (format = 'csv', empty_string_as_null = true)
                    RETURN SUMMARY;
                """,
        ).execute({})

        PostgresOperator(
            task_id=f"log_{file_name}",
            postgres_conn_id="cratedb_demo_connection",
            sql=Path("/usr/local/airflow/dags/sql/taxi-insert.sql").read_text(
                encoding="utf-8"
            ),
        ).execute({})

        PostgresOperator(
            task_id=f"mark_processed_{file_name}",
            postgres_conn_id="cratedb_demo_connection",
            sql=f"INSERT INTO nyc_taxi.load_files_processed VALUES ('{missing_url}');",
        ).execute({})

        PostgresOperator(
            task_id=f"purge_staging_{file_name}",
            postgres_conn_id="cratedb_demo_connection",
            sql="DELETE FROM nyc_taxi.load_trips_staging;",
        ).execute({})


with DAG(
    dag_id="nyc-taxi",
    start_date=pendulum.datetime(2021, 11, 11, tz="UTC"),
    schedule_interval="@monthly",
    catchup=False,
) as dag:
    data_urls = PythonOperator(
        task_id="data_urls",
        python_callable=data_urls,
        op_kwargs={},
    )

    get_processed_files = PythonOperator(
        task_id="get_processed_files",
        python_callable=get_processed_files,
        op_kwargs={},
    )

    identify_missing_urls = PythonOperator(
        task_id="identify_missing_urls",
        python_callable=identify_missing_urls,
        op_kwargs={},
    )

    identify_missing_urls << [data_urls, get_processed_files]

    # The staging table should be empty already. Purging it again in case of
    # an abort or other error case.
    purge_staging_init = PostgresOperator(
        task_id="purge_staging_init",
        postgres_conn_id="cratedb_demo_connection",
        sql="DELETE FROM nyc_taxi.load_trips_staging;",
    )

    process_new_files = PythonOperator(
        task_id="process_new_files",
        python_callable=process_new_files,
        op_kwargs={},
    )

    process_new_files << [identify_missing_urls, purge_staging_init]

    predictions = FlyteOperator(
        task_id="train_model",
        flyte_conn_id="flyte_conn",
        project="flytesnacks",
        domain="development",
        launchplan_name="core.test.nyc_data_airflow.wf",
        inputs={"limit": 100},
    )

    predictions << process_new_files
