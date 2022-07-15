import unittest
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.models import Connection, TaskInstance
from airflow.models.dagrun import DagRun

from flyte_provider.operators.flyte import FlyteOperator


class TestFlyteOperator(unittest.TestCase):

    task_id = "test_flyte_operator"
    flyte_conn_id = "flyte_default"
    run_id = "manual__2022-03-30T13:55:08.715694+00:00"
    conn_type = "flyte"
    host = "localhost"
    port = "30081"
    project = "flytesnacks"
    domain = "development"
    launchplan_name = "core.basic.hello_world.my_wf"
    raw_output_data_config = "s3://flyte-demo/raw_data"
    kubernetes_service_account = "default"
    labels = {"key1": "value1"}
    version = "v1"
    inputs = {"name": "hello world"}
    execution_name = "test1202203301355087"
    oauth2_client = {"client_id": "123", "client_secret": "456"}
    secrets = [{"group": "secrets", "key": "123"}]
    notifications = [{"phases": [1], "email": {"recipients_email": ["abc@flyte.org"]}}]
    wrong_notifications = [
        {"phases": [1], "email": {"recipient_email": ["abc@flyte.org"]}}
    ]

    @classmethod
    def get_connection(cls):
        return Connection(
            conn_id=cls.flyte_conn_id,
            conn_type=cls.conn_type,
            host=cls.host,
            port=cls.port,
            extra={"project": cls.project, "domain": cls.domain},
        )

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.trigger_execution")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    def test_execute(self, mock_get_connection, mock_trigger_execution):
        mock_get_connection.return_value = self.get_connection()

        operator = FlyteOperator(
            task_id=self.task_id,
            flyte_conn_id=self.flyte_conn_id,
            project=self.project,
            domain=self.domain,
            launchplan_name=self.launchplan_name,
            raw_output_data_config=self.raw_output_data_config,
            kubernetes_service_account=self.kubernetes_service_account,
            labels=self.labels,
            version=self.version,
            inputs=self.inputs,
            oauth2_client=self.oauth2_client,
            secrets=self.secrets,
            notifications=self.notifications,
        )
        result = operator.execute(
            {
                "dag_run": DagRun(run_id=self.run_id),
                "task": operator,
                "task_instance": TaskInstance(task=operator),
            }
        )

        assert result == self.execution_name
        mock_get_connection.assert_called_once_with(self.flyte_conn_id)
        mock_trigger_execution.assert_called_once_with(
            launchplan_name=self.launchplan_name,
            task_name=None,
            max_parallelism=None,
            raw_output_data_config=self.raw_output_data_config,
            kubernetes_service_account=self.kubernetes_service_account,
            oauth2_client=self.oauth2_client,
            labels=self.labels,
            annotations={},
            secrets=self.secrets,
            notifications=self.notifications,
            version=self.version,
            inputs=self.inputs,
            execution_name=self.execution_name,
            disable_notifications=None,
        )

    @mock.patch(
        "flyte_provider.hooks.flyte.FlyteHook.trigger_execution", return_value=None
    )
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.terminate")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    def test_on_kill_success(
        self, mock_get_connection, mock_terminate, mock_trigger_execution
    ):
        mock_get_connection.return_value = self.get_connection()

        operator = FlyteOperator(
            task_id=self.task_id,
            flyte_conn_id=self.flyte_conn_id,
            project=self.project,
            domain=self.domain,
            launchplan_name=self.launchplan_name,
            inputs=self.inputs,
            oauth2_client=self.oauth2_client,
            secrets=self.secrets,
            notifications=self.notifications,
        )
        operator.execute(
            {
                "dag_run": DagRun(run_id=self.run_id),
                "task": operator,
                "task_instance": TaskInstance(task=operator),
            }
        )
        operator.on_kill()

        mock_get_connection.has_calls([mock.call(self.flyte_conn_id)] * 2)
        mock_trigger_execution.assert_called()
        mock_terminate.assert_called_once_with(
            execution_name=self.execution_name, cause="Killed by Airflow"
        )

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.terminate")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    def test_on_kill_noop(self, mock_get_connection, mock_terminate):
        mock_get_connection.return_value = self.get_connection()

        operator = FlyteOperator(
            task_id=self.task_id,
            flyte_conn_id=self.flyte_conn_id,
            project=self.project,
            domain=self.domain,
            launchplan_name=self.launchplan_name,
            inputs=self.inputs,
            oauth2_client=self.oauth2_client,
            secrets=self.secrets,
            notifications=self.notifications,
        )
        operator.on_kill()

        mock_get_connection.assert_not_called()
        mock_terminate.assert_not_called()

    def test_execute_failure(self):
        with pytest.raises(AirflowException):
            FlyteOperator(
                task_id=self.task_id,
                flyte_conn_id=self.flyte_conn_id,
                project=self.project,
                domain=self.domain,
                launchplan_name=self.launchplan_name,
                inputs=self.inputs,
                oauth2_client=self.oauth2_client,
                secrets=self.secrets,
                notifications=self.wrong_notifications,
            )
