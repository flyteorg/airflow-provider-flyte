from datetime import datetime, timedelta

from airflow import DAG
from flyte_providers.flyte.operators.flyte import FlyteOperator
from flyte_providers.flyte.sensors.flyte import FlyteSensor

with DAG(
    dag_id="example_flyte_operator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    catchup=False,
) as dag:
    # wait for the execution to complete
    flyte_execution_start = FlyteOperator(
        task_id="flyte_task",
        flyte_conn_id="flyte_conn_example",
        project="flytesnacks",
        domain="development",
        launchplan_name="core.basic.lp.my_wf",
        max_parallelism=2,
        raw_data_prefix="s3://flyte-demo/raw_data",
        kubernetes_service_account="demo",
        version="v1",
        inputs={"val": 19},
    )

    flyte_execution_wait = FlyteSensor(
        task_id="flyte_sensor_one",
        execution_name=flyte_execution_start.output,
        project="flytesnacks",
        domain="development",
        flyte_conn_id="flyte_conn_example",
    )  # poke every 60 seconds (default)

    flyte_execution_start >> flyte_execution_wait

    # wait for a long-running execution to complete
    flyte_execution_wait_long = FlyteSensor(
        task_id="flyte_sensor_two",
        execution_name=flyte_execution_start.output,
        project="flytesnacks",
        domain="development",
        flyte_conn_id="flyte_conn_example",
        mode="reschedule",
        poke_interval=5 * 60,  # check every 5 minutes
        timeout="86400",  # wait for a day
        soft_fail=True,  # task is skipped if the condition is not met by timeout
    )

    flyte_execution_start >> flyte_execution_wait_long
