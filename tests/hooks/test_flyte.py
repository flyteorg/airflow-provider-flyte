import unittest
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection
from flytekit.configuration import Config, PlatformConfig
from flytekit.exceptions.user import FlyteEntityNotExistException, FlyteValueException
from flytekit.remote import FlyteRemote

from flyte_provider.hooks.flyte import FlyteHook


class TestFlyteHook(unittest.TestCase):

    flyte_conn_id = "flyte_default"
    execution_name = "a038d0f1339c4e7700c0"
    conn_type = "flyte"
    host = "localhost"
    port = "30081"
    extra = {"project": "flytesnacks", "domain": "development"}
    launchplan_name = "core.basic.hello_world.my_wf"
    task_name = "core.basic.hello_world.say_hello"
    raw_output_data_config = "s3://flyte-demo/raw_data"
    kubernetes_service_account = "default"
    version = "v1"
    inputs = {"name": "hello world"}
    oauth2_client = {"client_id": "123", "client_secret": "456"}
    secrets = [{"group": "secrets", "key": "123"}]
    notifications = [{"phases": [1], "email": {"recipients_email": ["abc@flyte.org"]}}]

    @classmethod
    def get_mock_connection(cls):
        return Connection(
            conn_id=cls.flyte_conn_id,
            conn_type=cls.conn_type,
            host=cls.host,
            port=cls.port,
            extra=cls.extra,
        )

    @classmethod
    def create_remote(cls):
        return FlyteRemote(
            config=Config(
                platform=PlatformConfig(
                    endpoint=":".join([cls.host, cls.port]), insecure=True
                ),
            )
        )

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.create_flyte_remote")
    def test_trigger_execution_success(
        self, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = FlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_launch_plan = mock.MagicMock()

        mock_remote.execute = mock.MagicMock()

        test_hook.trigger_execution(
            launchplan_name=self.launchplan_name,
            raw_output_data_config=self.raw_output_data_config,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
            execution_name=self.execution_name,
            oauth2_client=self.oauth2_client,
            secrets=self.secrets,
            notifications=self.notifications,
        )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.create_flyte_remote")
    def test_trigger_task_execution_success(
        self, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = FlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_task = mock.MagicMock()

        mock_remote.execute = mock.MagicMock()

        test_hook.trigger_execution(
            task_name=self.task_name,
            raw_output_data_config=self.raw_output_data_config,
            kubernetes_service_account=self.kubernetes_service_account,
            version=self.version,
            inputs=self.inputs,
            execution_name=self.execution_name,
            oauth2_client=self.oauth2_client,
            secrets=self.secrets,
            notifications=self.notifications,
        )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.create_flyte_remote")
    def test_trigger_execution_failed_to_fetch(
        self, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = FlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_launch_plan = mock.MagicMock(
            side_effect=FlyteEntityNotExistException
        )

        with pytest.raises(AirflowException):
            test_hook.trigger_execution(
                launchplan_name=self.launchplan_name,
                raw_output_data_config=self.raw_output_data_config,
                kubernetes_service_account=self.kubernetes_service_account,
                version=self.version,
                inputs=self.inputs,
                execution_name=self.execution_name,
                oauth2_client=self.oauth2_client,
                secrets=self.secrets,
                notifications=self.notifications,
            )
        mock_create_flyte_remote.assert_called_once()

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.create_flyte_remote")
    def test_trigger_execution_failed_to_trigger(
        self, mock_create_flyte_remote, mock_get_connection
    ):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        test_hook = FlyteHook(self.flyte_conn_id)

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote
        mock_remote.fetch_launch_plan = mock.MagicMock()
        mock_remote.execute = mock.MagicMock(side_effect=FlyteValueException)

        with pytest.raises(AirflowException):
            test_hook.trigger_execution(
                launchplan_name=self.launchplan_name,
                raw_output_data_config=self.raw_output_data_config,
                kubernetes_service_account=self.kubernetes_service_account,
                version=self.version,
                inputs=self.inputs,
                execution_name=self.execution_name,
                oauth2_client=self.oauth2_client,
                secrets=self.secrets,
                notifications=self.notifications,
            )
        mock_create_flyte_remote.assert_called_once()
