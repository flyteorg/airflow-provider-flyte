from datetime import datetime, timedelta

from airflow import DAG

from flyte_provider.operators.flyte import FlyteOperator
from flyte_provider.sensors.flyte import FlyteSensor

with DAG(
    dag_id="example_flyte",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
) as dag:
    task = FlyteOperator(
        task_id="diabetes_predictions",
        flyte_conn_id="flyte_conn",
        project="flytesnacks",
        domain="development",
        launchplan_name="ml_training.pima_diabetes.diabetes.diabetes_xgboost_model",
        inputs={"test_split_ratio": 0.66, "seed": 5},
    )

    sensor = FlyteSensor(
        task_id="sensor",
        execution_name=task.output,
        project="flytesnacks",
        domain="development",
        flyte_conn_id="flyte_conn",
    )

    task >> sensor
