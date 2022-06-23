import inspect
import re
from dataclasses import fields
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.abstractoperator import AbstractOperator
from airflow.models.taskinstance import TaskInstanceKey
from flytekit.models.common import (
    EmailNotification,
    Notification,
    PagerDutyNotification,
    SlackNotification,
)
from flytekit.models.security import OAuth2Client, Secret

from flyte_provider.hooks.flyte import FlyteHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RegistryLink(BaseOperatorLink):
    """Link to Registry"""

    name = "Astronomer Registry"

    def get_link(self, operator: AbstractOperator, ti_key: TaskInstanceKey) -> str:
        """Get link to registry page."""

        registry_link = (
            "https://registry.astronomer.io/providers/{provider}/modules/{operator}"
        )
        return registry_link.format(provider="flyte", operator="flyteoperator")


class FlyteOperator(BaseOperator):
    """
    Launch Flyte executions from within Airflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirflowFlyteOperator`

    :param flyte_conn_id: Required. The connection to Flyte setup, containing metadata.
    :param project: Optional. The project to connect to.
    :param domain: Optional. The domain to connect to.
    :param launchplan_name: Optional. The name of the launchplan to trigger.
    :param task_name: Optional. The name of the task to trigger.
    :param max_parallelism: Optional. The maximum number of parallel executions to allow.
    :param raw_output_data_config: Optional. Location of offloaded data for things like S3, etc.
    :param kubernetes_service_account: Optional. The Kubernetes service account to use.
    :param oauth2_client: Optional. The OAuth2 client to use.
    :param labels: Optional. Custom labels to be applied to the execution resource.
    :param annotations: Optional. Custom annotations to be applied to the execution resource.
    :param secrets: Optional. Custom secrets to be applied to the execution resource.
    :param notifications: Optional. List of notifications to be applied to the execution resource.
    :param disable_notifications: Optional. Whether to disable notifications.
    :param version: Optional. The version of the launchplan/task to trigger.
    :param inputs: Optional. The inputs to the launchplan/task.
    """

    template_fields: Sequence[str] = ("flyte_conn_id",)  # mypy fix

    def __init__(
        self,
        flyte_conn_id: str,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        launchplan_name: Optional[str] = None,
        task_name: Optional[str] = None,
        max_parallelism: Optional[int] = None,
        raw_output_data_config: Optional[str] = None,
        kubernetes_service_account: Optional[str] = None,
        oauth2_client: Optional[Dict[str, str]] = None,
        labels: Dict[str, str] = {},
        annotations: Dict[str, str] = {},
        secrets: Optional[List[Dict[str, str]]] = None,
        notifications: Optional[
            List[Dict[str, Union[List[str], Dict[str, List[str]]]]]
        ] = None,
        disable_notifications: Optional[bool] = None,
        version: Optional[str] = None,
        inputs: Dict[str, Any] = {},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.flyte_conn_id = flyte_conn_id
        self.project = project
        self.domain = domain
        self.launchplan_name = launchplan_name
        self.task_name = task_name
        self.max_parallelism = max_parallelism
        self.raw_output_data_config = raw_output_data_config
        self.kubernetes_service_account = kubernetes_service_account
        self.oauth2_client = oauth2_client
        self.labels = labels
        self.annotations = annotations
        self.secrets = secrets
        self.notifications = notifications
        self.disable_notifications = disable_notifications
        self.version = version
        self.inputs = inputs
        self.execution_name: str = ""

        if (not (self.task_name or self.launchplan_name)) or (
            self.task_name and self.launchplan_name
        ):
            raise AirflowException("Either task_name or launchplan_name is required.")

        if oauth2_client:
            if not isinstance(oauth2_client, dict):
                raise AirflowException(
                    f"oauth2_client isn't a dict, instead it is of type {type(oauth2_client)}"
                )
            if not (
                set(
                    field.name
                    for field in fields(OAuth2Client)
                    if not hasattr(OAuth2Client, field.name)
                )
                <= set(oauth2_client.keys())
                <= set(map(lambda x: x.name, fields(OAuth2Client)))
            ):
                raise AirflowException(
                    "oauth2_client doesn't have all the required keys or the key names do not match."
                )

        if secrets:
            if not isinstance(secrets, list):
                raise AirflowException(
                    f"secrets isn't a list, instead it is of type {type(oauth2_client)}"
                )
            for secret in secrets:
                if not isinstance(secret, dict):
                    raise AirflowException(
                        f"secret isn't a dict, instead it is of type {type(oauth2_client)}"
                    )
                if secret and not (
                    set(
                        field.name
                        for field in fields(Secret)
                        if not hasattr(Secret, field.name)
                    )
                    <= set(secret.keys())
                    <= set(map(lambda x: x.name, fields(Secret)))
                ):
                    raise AirflowException(
                        "secret doesn't have all the required keys or the key names do not match."
                    )

        if notifications:
            map_key_class = {
                "email": EmailNotification,
                "slack": SlackNotification,
                "pagerduty": PagerDutyNotification,
            }

            if not isinstance(notifications, list):
                raise AirflowException(
                    f"notifications isn't a dict, instead it is of type {type(oauth2_client)}"
                )
            for notification in notifications:
                if not isinstance(notification, dict):
                    raise AirflowException(
                        f"notification isn't a dict, instead it is of type {type(oauth2_client)}"
                    )
                if notification and not set(
                    arg_name
                    for arg_name, v in inspect.signature(
                        Notification.__init__
                    ).parameters.items()
                    if v.default is inspect._empty and arg_name != "self"
                ) <= set(notification.keys()) <= set(
                    map(
                        lambda x: x,
                        inspect.signature(Notification.__init__).parameters.keys(),
                    )
                ):
                    raise AirflowException(
                        "notification doesn't have all the required keys or the key names do not match."
                    )

                for key in notification.keys():
                    if key in {"email", "slack", "pager_duty"}:
                        if (not isinstance(notification[key], dict)) or not (
                            set(
                                arg_name
                                for arg_name, v in inspect.signature(
                                    map_key_class[key].__init__
                                ).parameters.items()
                                if v.default is inspect._empty and arg_name != "self"
                            )
                            <= set(notification[key].keys())
                            <= set(
                                map(
                                    lambda x: x,
                                    inspect.signature(
                                        map_key_class[key].__init__
                                    ).parameters.keys(),
                                )
                            )
                        ):
                            raise AirflowException(
                                f"notification[{key}] isn't a dict/doesn't have all the required keys/the key names do not match."
                            )

    def execute(self, context: "Context") -> str:
        """Trigger an execution."""

        # create a deterministic execution name
        task_id = re.sub(r"[\W_]+", "", context["task"].task_id)[:5]
        self.execution_name = (
            task_id
            + re.sub(
                r"[\W_]+",
                "",
                context["dag_run"].run_id.split("__")[-1].lower(),
            )[: (20 - len(task_id))]
        )

        hook = FlyteHook(
            flyte_conn_id=self.flyte_conn_id, project=self.project, domain=self.domain
        )
        hook.trigger_execution(
            launchplan_name=self.launchplan_name,
            task_name=self.task_name,
            max_parallelism=self.max_parallelism,
            raw_output_data_config=self.raw_output_data_config,
            kubernetes_service_account=self.kubernetes_service_account,
            oauth2_client=self.oauth2_client,
            labels=self.labels,
            annotations=self.annotations,
            secrets=self.secrets,
            notifications=self.notifications,
            disable_notifications=self.disable_notifications,
            version=self.version,
            inputs=self.inputs,
            execution_name=self.execution_name,
        )
        self.log.info("Execution %s submitted", self.execution_name)

        return self.execution_name

    def on_kill(self) -> None:
        """Kill the execution."""
        if self.execution_name:
            print(f"Killing execution {self.execution_name}")
            hook = FlyteHook(
                flyte_conn_id=self.flyte_conn_id,
                project=self.project,
                domain=self.domain,
            )
            hook.terminate(
                execution_name=self.execution_name,
                cause="Killed by Airflow",
            )
