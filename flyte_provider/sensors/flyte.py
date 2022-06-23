from typing import TYPE_CHECKING, Optional, Sequence

from airflow.sensors.base import BaseSensorOperator

from flyte_provider.hooks.flyte import FlyteHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FlyteSensor(BaseSensorOperator):
    """
    Check for the status of a Flyte execution.

    :param execution_name: Required. The name of the execution to check.
    :param project: Optional. The project to connect to.
    :param domain: Optional. The domain to connect to.
    :param flyte_conn_id: Required. The name of the Flyte connection to
                          get the connection information for Flyte.
    """

    template_fields: Sequence[str] = ("execution_name",)  # mypy fix

    def __init__(
        self,
        execution_name: str,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        flyte_conn_id: str = "flyte_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.execution_name = execution_name
        self.project = project
        self.domain = domain
        self.flyte_conn_id = flyte_conn_id

    def poke(self, context: "Context") -> bool:
        """Check for the status of a Flyte execution."""
        hook = FlyteHook(
            flyte_conn_id=self.flyte_conn_id, project=self.project, domain=self.domain
        )
        remote = hook.create_flyte_remote()

        if hook.execution_status(self.execution_name, remote):
            return True

        self.log.info("Waiting for execution %s to complete", self.execution_name)
        return False
