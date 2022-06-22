from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from flytekit.configuration import Config, PlatformConfig
from flytekit.exceptions.user import FlyteEntityNotExistException
from flytekit.models.common import (
    Annotations,
    EmailNotification,
    Labels,
    Notification,
    PagerDutyNotification,
    SlackNotification,
)
from flytekit.models.core import execution as core_execution_models
from flytekit.models.core.identifier import WorkflowExecutionIdentifier
from flytekit.models.security import Identity, OAuth2Client, Secret, SecurityContext
from flytekit.remote.remote import FlyteRemote, Options


class FlyteHook(BaseHook):
    """
    Interact with the FlyteRemote API.

    :param flyte_conn_id: Required. The name of the Flyte connection to get
                          the connection information for Flyte.
    :param project: Optional. The project under consideration.
    :param domain: Optional. The domain under consideration.
    """

    SUCCEEDED = core_execution_models.WorkflowExecutionPhase.SUCCEEDED
    FAILED = core_execution_models.WorkflowExecutionPhase.FAILED
    TIMED_OUT = core_execution_models.WorkflowExecutionPhase.TIMED_OUT
    ABORTED = core_execution_models.WorkflowExecutionPhase.ABORTED

    flyte_conn_id = "flyte_default"
    conn_type = "flyte"

    def __init__(
        self,
        flyte_conn_id: str = flyte_conn_id,
        project: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.flyte_conn_id = flyte_conn_id
        self.flyte_conn = self.get_connection(self.flyte_conn_id)
        self.project = project or self.flyte_conn.extra_dejson.get("project")
        self.domain = domain or self.flyte_conn.extra_dejson.get("domain")

        if not (self.project and self.domain):
            raise AirflowException("Please provide a project and domain.")

    def execution_id(self, execution_name: str) -> WorkflowExecutionIdentifier:
        """Get the execution id."""
        return WorkflowExecutionIdentifier(self.project, self.domain, execution_name)

    def create_flyte_remote(self) -> FlyteRemote:
        """Create a FlyteRemote object."""
        remote = FlyteRemote(
            config=Config(
                platform=PlatformConfig(
                    endpoint=":".join([self.flyte_conn.host, self.flyte_conn.port])
                    if (self.flyte_conn.host and self.flyte_conn.port)
                    else (self.flyte_conn.host or "localhost:30081"),
                    insecure=self.flyte_conn.extra_dejson.get("insecure", False),
                    insecure_skip_verify=self.flyte_conn.extra_dejson.get(
                        "insecure_skip_verify", False
                    ),
                    client_id=self.flyte_conn.login or None,
                    client_credentials_secret=self.flyte_conn.password or None,
                    command=self.flyte_conn.extra_dejson.get("command", None),
                    scopes=self.flyte_conn.extra_dejson.get("scopes", None),
                    auth_mode=self.flyte_conn.extra_dejson.get("auth_mode", "standard"),
                )
            ),
        )
        return remote

    def trigger_execution(
        self,
        execution_name: str,
        launchplan_name: Optional[str] = None,
        task_name: Optional[str] = None,
        max_parallelism: Optional[int] = None,
        raw_output_data_config: Optional[str] = None,
        assumable_iam_role: Optional[str] = None,
        kubernetes_service_account: Optional[str] = None,
        oauth2_client: Optional[Dict[str, str]] = None,
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
        secrets: Optional[List[Dict[str, str]]] = None,
        notifications: Optional[List[Dict[str, Any]]] = None,
        disable_notifications: Optional[bool] = None,
        version: Optional[str] = None,
        inputs: Dict[str, Any] = {},
    ) -> None:
        """
        Trigger an execution.

        :param execution_name: Required. The name of the execution to trigger.
        :param launchplan_name: Optional. The name of the launchplan to trigger.
        :param task_name: Optional. The name of the task to trigger.
        :param max_parallelism: Optional. The maximum number of parallel executions to allow.
        :param raw_output_data_config: Optional. Location of offloaded data for things like S3, etc.
        :param assumable_iam_role: Optional. The assumable IAM role to use.
        :param kubernetes_service_account: Optional. The kubernetes service account to use.
        :param oauth2_client: Optional. The OAuth2 client to use.
        :param labels: Optional. The labels to use.
        :param annotations: Optional. The annotations to use.
        :param secrets: Optional. Custom secrets to be applied to the execution resource.
        :param notifications: Optional. List of notifications to be applied to the execution resource.
        :param disable_notifications: Optional. Whether to disable notifications.
        :param version: Optional. The version of the launchplan to trigger.
        :param inputs: Optional. The inputs to the launchplan.
        """
        remote = self.create_flyte_remote()
        try:
            if launchplan_name:
                flyte_entity = remote.fetch_launch_plan(
                    name=launchplan_name,
                    project=self.project,
                    domain=self.domain,
                    version=version,
                )
            elif task_name:
                flyte_entity = remote.fetch_task(
                    name=task_name,
                    project=self.project,
                    domain=self.domain,
                    version=version,
                )
        except FlyteEntityNotExistException as e:
            raise AirflowException(f"Failed to fetch entity: {e}")

        try:
            remote.execute(
                flyte_entity,
                inputs=inputs,
                project=self.project,
                domain=self.domain,
                execution_name=execution_name,
                options=Options(
                    raw_output_data_config=raw_output_data_config,
                    max_parallelism=max_parallelism,
                    security_context=SecurityContext(
                        run_as=Identity(
                            k8s_service_account=kubernetes_service_account,
                            iam_role=assumable_iam_role,
                            oauth2_client=OAuth2Client(
                                client_id=oauth2_client.get("client_id"),
                                client_secret=oauth2_client.get("client_secret"),
                            )
                            if oauth2_client
                            else None,
                        ),
                        secrets=[
                            Secret(
                                group=secret.get("group"),
                                key=secret.get("key"),
                                group_version=secret.get("group_version"),
                            )
                            for secret in secrets
                        ]
                        if secrets
                        else None,
                    ),
                    labels=Labels(labels),
                    annotations=Annotations(annotations),
                    notifications=[
                        Notification(
                            phases=notification.get("phases"),
                            email=EmailNotification(
                                recipients_email=notification.get("email", {}).get(
                                    "recipients_email"
                                )
                            ),
                            slack=SlackNotification(
                                recipients_email=notification.get("slack", {}).get(
                                    "recipients_email"
                                )
                            ),
                            pager_duty=PagerDutyNotification(
                                recipients_email=notification.get("pager_duty", {}).get(
                                    "recipients_email"
                                )
                            ),
                        )
                        for notification in notifications
                    ]
                    if notifications
                    else None,
                    disable_notifications=disable_notifications,
                ),
            )
        except Exception as e:
            raise AirflowException(f"Failed to trigger execution: {e}")

    def execution_status(self, execution_name: str, remote: FlyteRemote):
        phase = remote.client.get_execution(
            self.execution_id(execution_name)
        ).closure.phase

        if phase == self.SUCCEEDED:
            return True
        elif phase == self.FAILED:
            raise AirflowException(f"Execution {execution_name} failed")
        elif phase == self.TIMED_OUT:
            raise AirflowException(f"Execution {execution_name} timedout")
        elif phase == self.ABORTED:
            raise AirflowException(f"Execution {execution_name} aborted")
        else:
            return False

    def terminate(
        self,
        execution_name: str,
        cause: str,
    ) -> None:
        """
        Terminate an execution.

        :param execution: Required. The execution to terminate.
        :param cause: Required. The cause of the termination.
        """
        remote = self.create_flyte_remote()
        execution_id = self.execution_id(execution_name)
        remote.client.terminate_execution(id=execution_id, cause=cause)
