from datetime import datetime, timedelta

from airflow import DAG
from flyte_providers.flyte.operators.flyte import FlyteOperator

with DAG(
    dag_id="example_flyte_operator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    catchup=False,
) as dag:
    # do not wait for the execution to complete
    flyte_execution = FlyteOperator(
        task_id="flyte_task",
        flyte_conn_id="flyte_conn_example",
        project="flytesnacks",
        domain="development",
        launchplan_name="core.basic.lp.my_wf",
        assumable_iam_role="default",
        kubernetes_service_account="demo",
        version="v1",
        inputs={"val": 19},
    )
