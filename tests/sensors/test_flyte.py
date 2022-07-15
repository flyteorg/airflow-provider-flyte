import unittest
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.models import Connection
from flytekit.configuration import Config, PlatformConfig
from flytekit.models.core import execution as core_execution_models
from flytekit.remote import FlyteRemote

from flyte_provider.hooks.flyte import FlyteHook
from flyte_provider.sensors.flyte import FlyteSensor


class TestFlyteSensor(unittest.TestCase):

    task_id = "test_flyte_sensor"
    flyte_conn_id = "flyte_default"
    conn_type = "flyte"
    host = "localhost"
    port = "30081"
    project = "flytesnacks"
    domain = "development"
    execution_name = "test1202203301355081"

    @classmethod
    def get_connection(cls):
        return Connection(
            conn_id=cls.flyte_conn_id,
            conn_type=cls.conn_type,
            host=cls.host,
            port=cls.port,
            extra={"project": cls.project, "domain": cls.domain},
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
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.execution_id")
    def test_poke_done(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_get_connection.return_value = self.get_connection()

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(return_value=FlyteHook.SUCCEEDED)
        type(mock_get_execution().closure).phase = mock_phase

        sensor = FlyteSensor(
            task_id=self.task_id,
            execution_name=self.execution_name,
            project=self.project,
            domain=self.domain,
            flyte_conn_id=self.flyte_conn_id,
        )

        return_value = sensor.poke({})

        assert return_value
        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_called_with(self.execution_name)

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.create_flyte_remote")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.execution_id")
    def test_poke_failed(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_get_connection.return_value = self.get_connection()

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        sensor = FlyteSensor(
            task_id=self.task_id,
            execution_name=self.execution_name,
            project=self.project,
            domain=self.domain,
            flyte_conn_id=self.flyte_conn_id,
        )

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        for phase in [FlyteHook.ABORTED, FlyteHook.FAILED, FlyteHook.TIMED_OUT]:
            mock_get_execution = mock.MagicMock()
            mock_remote.client.get_execution = mock_get_execution
            mock_phase = mock.PropertyMock(return_value=phase)
            type(mock_get_execution().closure).phase = mock_phase

            with pytest.raises(AirflowException):
                sensor.poke({})

        mock_create_flyte_remote.has_calls([mock.call()] * 3)
        mock_execution_id.has_calls([mock.call(self.execution_name)] * 3)

    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.get_connection")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.create_flyte_remote")
    @mock.patch("flyte_provider.hooks.flyte.FlyteHook.execution_id")
    def test_poke_running(
        self, mock_execution_id, mock_create_flyte_remote, mock_get_connection
    ):
        mock_get_connection.return_value = self.get_connection()

        mock_remote = self.create_remote()
        mock_create_flyte_remote.return_value = mock_remote

        execution_id = mock.MagicMock()
        mock_execution_id.return_value = execution_id

        mock_get_execution = mock.MagicMock()
        mock_remote.client.get_execution = mock_get_execution
        mock_phase = mock.PropertyMock(
            return_value=core_execution_models.WorkflowExecutionPhase.RUNNING
        )
        type(mock_get_execution().closure).phase = mock_phase

        sensor = FlyteSensor(
            task_id=self.task_id,
            execution_name=self.execution_name,
            project=self.project,
            domain=self.domain,
            flyte_conn_id=self.flyte_conn_id,
        )

        return_value = sensor.poke({})
        assert not return_value

        mock_create_flyte_remote.assert_called_once()
        mock_execution_id.assert_called_with(self.execution_name)
